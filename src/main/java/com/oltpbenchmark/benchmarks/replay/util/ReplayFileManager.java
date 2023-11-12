package com.oltpbenchmark.benchmarks.replay.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oltpbenchmark.DBWorkload;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.util.ConsoleUtil;
import com.oltpbenchmark.util.Pair;

// ReplayFileManager provides a queue-like interface over a replay file.
// This currently only works for Postgres' log file format.

/**
 * @author phw2
 */
public class ReplayFileManager {
    private static final Logger LOG = LoggerFactory.getLogger(DBWorkload.class);
    private static final int PGLOG_LOG_TIME_INDEX = 0;
    private static final int PGLOG_VXID_INDEX = 9;
    private static final int PGLOG_MESSAGE_INDEX = 13;
    private static final int PGLOG_DETAIL_INDEX = 14;
    private static final String BEGIN_STRING = "BEGIN";
    private static final String COMMIT_STRING = "COMMIT";
    private static final String ROLLBACK_STRING = "ROLLBACK";
    private static final String REPLAY_FILE_SECTION_DELIM = "###";
    private static final FastLogDateTimeParser fastDTParser = new FastLogDateTimeParser();

    private String logFilePath;
    private String replayFilePath;
    private Queue<ReplayTransaction> replayTransactionQueue;
    private boolean hasSuccessfullyLoaded;
    
    private Map<Integer, SQLStmt> sqlStmtCache;

    /**
     * The constructor reads the entire file into memory
     */
    public ReplayFileManager(String logFilePath) {
        this.logFilePath = logFilePath;
        this.replayFilePath = getReplayFilePath(logFilePath);
        this.hasSuccessfullyLoaded = false;
    }

    private static String getReplayFilePath(String logFilePath) {
        if (logFilePath.endsWith(".csv")) {
            return logFilePath.replace(".csv", ".rply");
        } else {
            throw new RuntimeException("logFilePath does not end with .csv");
        }
    }

    public void load() {
        synchronized (this) {
            File logFile = new File(logFilePath);
            File replayFile = new File(replayFilePath);
            boolean doConvert;

            if (logFile.exists() && replayFile.exists()) {
                long lastLogModified = logFile.lastModified();
                long lastReplayModified = replayFile.lastModified();

                if (lastLogModified >= lastReplayModified) {
                    LOG.info("Log file last modified at " + Long.toString(lastLogModified) + ", which is later than when replay file was last modified (" + Long.toString(lastReplayModified) + "). Will convert again.");
                    doConvert = true;
                } else {
                    LOG.info("Log file last modified at " + Long.toString(lastLogModified) + ", which is earlier than when replay file was last modified (" + Long.toString(lastReplayModified) + "). Will not convert.");
                    doConvert = false;
                }
            } else if (!logFile.exists() && replayFile.exists()) {
                LOG.info("Log file doesn't exist but replay file does. Will assume that user already converted to this replay file and then deleted the original log file, so will just directly use the replay file.");
                doConvert = false;
            } else if (logFile.exists() && !replayFile.exists()) {
                LOG.info("Replay file doesn't exist but log file does. Will convert log file to replay file.");
                doConvert = true;
            } else {
                throw new RuntimeException("Both the log file (" + logFilePath + ") and replay file (" + replayFilePath + ") do not exist.");
            }

            if (doConvert) {
                convertLogFileToReplayFile();
            }
            loadReplayFile();
            throw new RuntimeException("early exit");
        }
    }

    private void convertLogFileToReplayFile() {
        LOG.info("Converting the log file " + this.logFilePath + " into the replay file " + this.replayFilePath + "...");
        File logFile = new File(this.logFilePath);
        File replayFile = new File(this.replayFilePath);
        long totalBytes = logFile.length();
        FileInputStream logInputStream;
        CSVReader logCSVReader;
        try {
            // FileInputStream is used to track # of bytes read for the progress bar
            logInputStream = new FileInputStream(this.logFilePath);
            // BufferedReader is used for performance
            // CSVReader handles CSV values which have newlines embedded in them
            logCSVReader = new CSVReader(new BufferedReader(new InputStreamReader(logInputStream)));
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Log file " + this.logFilePath + " does not exist");
        }
        long fnStartTime = System.nanoTime();
        long totalInLoopComputeTime = 0;
        try (FileWriter replayFileWriter = new FileWriter(this.replayFilePath)) {
            try {
                Queue<LogTransaction> logTransactionQueue = new LinkedList<>();
                int nextSQLStmtID = 0;
                Map<String, Integer> sqlStringIDs = new HashMap<>();
                // Virtual Transaction IDs (VXIDs) vs Transaction IDs (XIDs)
                // Source: PostgreSQL 10 High Performance -> Database Activity and Statistics -> Locks -> Virtual Transactions
                //  - XIDs don't work because (1) XIDs aren't even assigned to read-only transactions and (2) the XID wraparound issue means different transactions active at the same time may have the same XID
                //  - VXIDs do work as they are (1) always assigned and (2) guaranteed to be unique amongst active transactions as they are used for locking
                Map<String, LogTransaction> activeTransactions = new HashMap<String, LogTransaction>();

                String[] fields;
                int lastProgressPercent = -1;
                long loopOuterStartTime = System.nanoTime();
                while ((fields = logCSVReader.readNext()) != null) {
                    long loopInnerStartTime = System.nanoTime();
                    // we parse the line in ReplayFileManager instead of sending it to the constructor of ReplayTransaction
                    // because sometimes transactions are built from multiple lines
                    String logTimeString = fields[PGLOG_LOG_TIME_INDEX];
                    long logTime = fastDTParser.dtStringToNanoTime(logTimeString);
                    String messageString = fields[PGLOG_MESSAGE_INDEX];
                    String sqlString = ReplayFileManager.parseSQLFromMessage(messageString);
                    if (sqlString == null) {
                        // ignore any lines which are not SQL statements
                        continue;
                    }
                    String detailString = fields[PGLOG_DETAIL_INDEX];
                    String vxid = fields[PGLOG_VXID_INDEX];

                    // manage activeTransactions and logTransactionQueue based on what the line is
                    if (sqlString.equals(BEGIN_STRING)) {
                        assert(!activeTransactions.containsKey(vxid));
                        LogTransaction newExplicitLogTransaction = new LogTransaction(true);
                        newExplicitLogTransaction.addSQLStmtLine(sqlString, detailString, logTime);
                        activeTransactions.put(vxid, newExplicitLogTransaction);
                        // in the output replay file, replay transactions are ordered by the time they first appear in the log file
                        logTransactionQueue.add(newExplicitLogTransaction);
                    } else if (sqlString.equals(COMMIT_STRING) || sqlString.equals(ROLLBACK_STRING)) {
                        assert(!activeTransactions.containsKey(vxid));
                        LogTransaction explicitLogTransaction = activeTransactions.get(vxid);
                        explicitLogTransaction.addSQLStmtLine(sqlString, detailString, logTime);
                        explicitLogTransaction.markComplete();
                        activeTransactions.remove(vxid);
                    } else {
                        Integer sqlStmtID = sqlStringIDs.get(sqlString);
                        if (sqlStmtID == null) {
                            sqlStmtID = nextSQLStmtID++;
                            sqlStringIDs.put(sqlString, sqlStmtID);
                        }

                        if (activeTransactions.containsKey(vxid)) {
                            // if it's in active transactions, it should be added to the corresponding explicit transaction
                            LogTransaction explicitLogTransaction = activeTransactions.get(vxid);
                            explicitLogTransaction.addSQLStmtLine(sqlStmtID, detailString, logTime);
                        } else {
                            // if it's not in active transactions, it must be an implicit transaction
                            LogTransaction implicitLogTransaction = new LogTransaction(false);
                            implicitLogTransaction.addSQLStmtLine(sqlStmtID, detailString, logTime);
                            logTransactionQueue.add(implicitLogTransaction);
                        }
                    }

                    long bytesRead = logInputStream.getChannel().position();
                    lastProgressPercent = ConsoleUtil.printProgressBar(bytesRead, totalBytes, lastProgressPercent);
                    totalInLoopComputeTime += System.nanoTime() - loopInnerStartTime;
                }
                System.out.println(); // the progress bar doesn't have a newline at the end of it. this adds one
                System.out.printf("The whole function took %.4fms\n", (double)(System.nanoTime() - fnStartTime) / 1000000);
                System.out.printf("The whole loop took %.4fms\n", (double)(System.nanoTime() - loopOuterStartTime) / 1000000);
                System.out.printf("We spent %.4fms doing compute inside the loop\n", (double)totalInLoopComputeTime / 1000000);

                // write all transactions at the end instead of inside the loop so that both the log file
                // read and the replay file write can be sequential 
                int numUncompletedTransactions = 0;
                while (!logTransactionQueue.isEmpty()) {
                    LogTransaction logTransaction = logTransactionQueue.peek();
                    if (logTransaction.getIsComplete()) {
                        replayFileWriter.write(logTransaction.getFormattedString());
                    } else {
                        numUncompletedTransactions++;
                    }
                    logTransactionQueue.remove();
                }

                if (numUncompletedTransactions > 0) {
                    LOG.warn("There are %d unfinished transactions in the log file", numUncompletedTransactions);
                }

                // write sqlStringIDs to the file as well
                // in the current implementation, it's possible to write sqlStringIDs before writing replay transactions
                // however, I chose to write sqlStringIDs after to allow for implementations where we write replay transactions
                // to the replay file during the loop where we're reading from the log file
                replayFileWriter.write(REPLAY_FILE_SECTION_DELIM + "\n");
                for (Map.Entry<String, Integer> entry : sqlStringIDs.entrySet()) {
                    int sqlStmtID = entry.getValue();
                    String sqlString = entry.getKey();
                    // a weird edge case with CSVReader is that it replaces two consecutive " characters inside a quoted string with a single " character
                    // I couldn't figure out how to configure CSVReader to not have this behavior
                    // this line doesn't solve all instances of this edge case but it does solve the one which shows up in the TPCC log file
                    // solving this edge case in general is difficult because we need to detect whether a lone " is supposed to be a terminal " or not (I
                    // have no idea how CSVReader can detect this but my guess is they say any " followed by a , is a terminal ")
                    sqlString = sqlString.replaceAll("'\"'", "'\"\"'");
                    replayFileWriter.write(Integer.toString(sqlStmtID));
                    replayFileWriter.write(",");
                    replayFileWriter.write("\"" + sqlString + "\"");
                    replayFileWriter.write("\n");
                }
            } catch (CsvValidationException e) {
                throw new RuntimeException("Log file not in a valid CSV format");
            } catch (IOException e) {
                throw new RuntimeException("I/O exception " + e + " when reading log file");
            }
        // delete the replay file in all error cases since the file won't be finished
        } catch (FileNotFoundException e) {
            replayFile.delete();
            throw new RuntimeException("Replay file " + this.replayFilePath + " does not exist");
        } catch (IOException e) {
            replayFile.delete();
            throw new RuntimeException("Encountered IOException " + e + " when opening replay file");
        } catch (RuntimeException e) {
            replayFile.delete();
            throw e;
        }
    }

    private void loadReplayFile() {
        LOG.info("Loading the replay file " + replayFilePath + "...");
        CSVReader replayCSVReader;
        try {
            // CSVReader handles CSV values which have newlines embedded in them
            replayCSVReader = new CSVReader(new BufferedReader(new FileReader(replayFilePath)));
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Replay file " + replayFilePath + " does not exist");
        }

        try {
            this.sqlStmtCache = new HashMap<>();
            this.replayTransactionQueue = new LinkedList<>();
            ReplayTransaction currentActiveTransaction = null;
            String[] fields;

            // read replay transaction section of file
            long loopOuterStartTime = System.nanoTime();
            while ((fields = replayCSVReader.readNext()) != null) {
                if (fields.length == 1 && fields[0].equals(REPLAY_FILE_SECTION_DELIM)) {
                    break;
                }

                // we parse the line in ReplayFileManager instead of sending it to the constructor of ReplayTransaction
                // because sometimes transactions are built from multiple lines
                String logTimeString = fields[0];
                long logTime = Long.parseLong(logTimeString);
                String sqlStmtIDOrString = fields[1];
                String detailString = fields[2];
                List<Object> params = ReplayFileManager.parseParamsFromDetail(detailString);

                if (sqlStmtIDOrString.equals(BEGIN_STRING)) {
                    if (currentActiveTransaction != null) {
                        throw new RuntimeException("Found BEGIN when previous transaction hadn't ended yet");
                    }
                    currentActiveTransaction = new ReplayTransaction(logTime, true);
                } else if (sqlStmtIDOrString.equals(COMMIT_STRING) || sqlStmtIDOrString.equals(ROLLBACK_STRING)) {
                    if (currentActiveTransaction == null) {
                        throw new RuntimeException("Found COMMIT or ROLLBACK when there is no active transaction");
                    }
                    currentActiveTransaction.setShouldRollback(sqlStmtIDOrString.equals(ROLLBACK_STRING));
                    replayTransactionQueue.add(currentActiveTransaction);
                    currentActiveTransaction = null;
                } else {
                    int sqlStmtID = Integer.parseInt(sqlStmtIDOrString);
                    if (!sqlStmtCache.containsKey(sqlStmtID)) {
                        // put a placeholder SQLStmt for now. later, when we read the sqlStmtCache section
                        // of the replay file, we'll fill in the strings in the sqlStmtCache
                        sqlStmtCache.put(sqlStmtID, new SQLStmt(""));
                    }
                    SQLStmt sqlStmt = sqlStmtCache.get(sqlStmtID);

                    if (currentActiveTransaction != null) {
                        currentActiveTransaction.addSQLStmtCall(sqlStmt, params, logTime);
                    } else {
                        // if there's isn't a current active transaction, it must be an implicit transaction
                        ReplayTransaction implicitReplayTransaction = new ReplayTransaction(logTime, false);
                        implicitReplayTransaction.addSQLStmtCall(sqlStmt, params, logTime);
                        replayTransactionQueue.add(implicitReplayTransaction);
                    }
                }
            }
            System.out.printf("The whole loop took %.4fms\n", (double)(System.nanoTime() - loopOuterStartTime) / 1000000);

            // read SQL statement cache section of file
            while ((fields = replayCSVReader.readNext()) != null) {
                String sqlStmtIDString = fields[0];
                int sqlStmtID = Integer.parseInt(sqlStmtIDString);
                String sqlString = fields[1];

                if (!sqlStmtCache.containsKey(sqlStmtID)) {
                    LOG.warn("Read SQL statement of ID " + sqlStmtID + ", except this ID didn't show up in the replay transactions");
                } else {
                    SQLStmt sqlStmt = sqlStmtCache.get(sqlStmtID);
                    sqlStmt.setSQL(sqlString);
                }
            }
            
            // perform validations
            for (Map.Entry<Integer, SQLStmt> entry : sqlStmtCache.entrySet()) {
                int sqlStmtID = entry.getKey();
                SQLStmt sqlStmt = entry.getValue();

                if (sqlStmt.getSQL().equals("")) {
                    throw new RuntimeException("sqlStmtID " + sqlStmtID + " showed up in the replay transactions but not in the cache");
                }
            }

            hasSuccessfullyLoaded = true;
        } catch (CsvValidationException e) {
            throw new RuntimeException("Replay file not in a valid CSV format");
        } catch (IOException e) {
            throw new RuntimeException("I/O exception " + e + " when reading replay file");
        }
    }

    // TODO: make the interfaces of all the queue classes consistent in terms of what peek and remove do
    //       if the queue is empty

    /**
     * Return the "front" replay transaction
     * 
     * Will block until there are buffered transactions or the end of the file is reached.
     * 
     * @return A transaction (which is list of SQLStmts) or Optional.empty() if the "front" is the EOF
     */
    public Optional<ReplayTransaction> peek() {
        synchronized (this) {
            if (!hasSuccessfullyLoaded) {
                throw new RuntimeException("Cannot call ReplayFileManager.peek() before ReplayFileManager.load() succeeds");
            }

            ReplayTransaction replayTransaction = this.replayTransactionQueue.peek();
            if (replayTransaction == null) {
                return Optional.empty();
            }
            return Optional.of(replayTransaction);
        }
    }

    /**
     * Advance to the next replay transaction
     * 
     * Will block until there are buffered transactions or the end of the file is reached.
     * 
     * TODO: what does this do if we're at the end of the file?
     */
    public void remove() {
        synchronized (this) {
            if (!hasSuccessfullyLoaded) {
                throw new RuntimeException("Cannot call ReplayFileManager.peek() before ReplayFileManager.load() succeeds");
            }

            this.replayTransactionQueue.remove();
        }
    }

    /**
     * Parse a SQL string from a raw "message" string in a Postgres CSV log file
     * 
     * SQL messages start with "statement:" or "execute <[something]>:"
     * 
     * @param messageString The string in the "message" field of the log file
     * @return A SQL string or null if the message is not a SQL message
     */
    private static String parseSQLFromMessage(String messageString) {
        Pair<String, String> typeAndContent = ReplayFileManager.splitTypeAndContent(messageString);
        if (typeAndContent != null) {
            String messageType = typeAndContent.first;
            String messageContent = typeAndContent.second;
            boolean typeIsStatement = messageType.equals("statement");
            boolean typeIsExecute = messageType.startsWith("execute");

            if (typeIsStatement || typeIsExecute) {
                // replace all $[number] with '?' in an efficient way
                StringBuilder sqlWithQuestionMarks = new StringBuilder();
                boolean isInDollarNum = false;

                for (int i = 0; i < messageContent.length(); i++) {
                    char c = messageContent.charAt(i);

                    if (c == '$') {
                        sqlWithQuestionMarks.append('?');
                        isInDollarNum = true;
                    } else {
                        if (isInDollarNum) {
                            if (Character.isDigit(c)) {
                                // still in dollar num
                            } else {
                                isInDollarNum = false;
                                sqlWithQuestionMarks.append(c);
                            }
                        } else {
                            sqlWithQuestionMarks.append(c);
                        }
                    }
                }

                return sqlWithQuestionMarks.toString();
            } else {
                return null;
            }
        } else {
            // some messages don't follow the "[type]: [content]" format, so we ignore them completely
            return null;
        }
    }

    /**
     * Parse SQL parameters from a raw "detail" string in a Postgres CSV log file
     * 
     * SQL parameter details start with "parameters: "
     * This function assumes that all parameters in the detail string are listed in order ($1= comes before $2= as so on)
     * We return an empty list so that it can be passed into Procedure.getPreparedStatement() safely
     * 
     * @param detailString The string in the "detail" field of the log file
     * @return A SQL string or an empty list if the detail is not a SQL parameter detail
     */
    private static List<Object> parseParamsFromDetail(String detailString) {
        List<Object> valuesList = new ArrayList<>();
        Pair<String, String> typeAndContent = ReplayFileManager.splitTypeAndContent(detailString);
        
        if (typeAndContent != null) {
            String detailType = typeAndContent.first;
            String detailContent = typeAndContent.second;

            if (detailType.equals("parameters")) {
                StringBuilder currentParamStr = new StringBuilder();
                boolean isInQuotes = false;
                for (int i = 0; i < detailContent.length(); i++) {
                    char c = detailContent.charAt(i);
                    if (isInQuotes) {
                        if (c == '\'') {
                            valuesList.add(ReplayFileManager.parseSQLLogStringToObject(currentParamStr.toString()));
                            isInQuotes = false;
                        } else {
                            currentParamStr.append(c);
                        }
                    } else {
                        if (c == '\'') {
                            isInQuotes = true;
                        }
                    }
                }
            }
        }

        return valuesList;
    }

    private static Object parseSQLLogStringToObject(String sqlLogString) {
        // Check if the string is null or empty
        if (sqlLogString == null || sqlLogString.isEmpty()) {
            return null;
        }
    
        // Check if the string represents an integer
        try {
            return Integer.parseInt(sqlLogString);
        } catch (NumberFormatException e) {
            // Not an integer
        }
    
        // Check if the string represents a double
        try {
            return Double.parseDouble(sqlLogString);
        } catch (NumberFormatException e) {
            // Not a double
        }
    
        // Check if the string is enclosed in single quotes, indicating a string
        if (sqlLogString.startsWith("'") && sqlLogString.endsWith("'")) {
            return sqlLogString.substring(1, sqlLogString.length() - 1);
        }

        // Check if the string represents a timestamp
        try {
            DateTimeFormatterBuilder formatterBuilder = new DateTimeFormatterBuilder();
            formatterBuilder.appendPattern("yyyy-MM-dd HH:mm:ss");
            formatterBuilder.appendFraction(ChronoField.MILLI_OF_SECOND, 0, 9, true);
            DateTimeFormatter formatter = formatterBuilder.toFormatter();
            LocalDateTime localDateTime = LocalDateTime.parse(sqlLogString, formatter);
            return Timestamp.valueOf(localDateTime);
        } catch (DateTimeParseException e) {
            // Not a timestamp
        }
    
        // If all else fails, return the string as-is
        return sqlLogString;
    }

    /**
     * @brief Split a log string into "type" and "content"
     * 
     * The format is "[type]: [content]"
     * Both "message" and "detail" strings are in the same format
     * 
     * @param logString A message or detail string
     * @return The type and content if it follows the format, or a null if it doesn't
     */
    private static Pair<String, String> splitTypeAndContent(String logString) {
        // DOTALL allows . to match newlines, which may be present in the SQL string
        String[] components = logString.split(": ");
        if (components.length == 2) {
            return Pair.of(components[0], components[1]);
        } else {
            return null;
        }
    }
}

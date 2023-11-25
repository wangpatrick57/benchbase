package com.oltpbenchmark.benchmarks.replay.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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
import java.util.Map;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oltpbenchmark.util.ConsoleUtil;
import com.oltpbenchmark.util.Pair;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

/**
 * @author phw2
 */
public class PostgresLogFileParser implements LogFileParser {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresLogFileParser.class);
    private static final int PGLOG_LOG_TIME_INDEX = 0;
    private static final int PGLOG_VXID_INDEX = 9;
    private static final int PGLOG_MESSAGE_INDEX = 13;
    private static final int PGLOG_DETAIL_INDEX = 14;
    private static final String BEGIN_STRING = "BEGIN";
    private static final String COMMIT_STRING = "COMMIT";
    private static final String ROLLBACK_STRING = "ROLLBACK";
    private final FastLogDateTimeParser fastDTParser;

    public static long timeInParseParamsFromDetail = 0;

    /**
     * @brief Construct a PostgresLogFileParser instance
     * 
     * convertLogFileToReplayFile() is thread-safe when called on different PostgresLogFileParser instances
     */
    public PostgresLogFileParser() {
        this.fastDTParser = new FastLogDateTimeParser();
    }

    @Override
    public void convertLogFileToReplayFile(String logFilePath, String replayFilePath) {
        LOG.info("Converting the log file " + logFilePath + " into the replay file " + replayFilePath + "...");
        File logFile = new File(logFilePath);
        File replayFile = new File(replayFilePath);
        long totalBytes = logFile.length();
        FileInputStream logInputStream;
        CSVReader logCSVReader;
        try {
            // FileInputStream is used to track # of bytes read for the progress bar
            logInputStream = new FileInputStream(logFilePath);
            // BufferedReader is used for performance
            // CSVReader handles CSV values which have newlines embedded in them
            logCSVReader = new CSVReader(new BufferedReader(new InputStreamReader(logInputStream)));
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Log file " + logFilePath + " does not exist");
        }

        long totalInLoopComputeTime = 0;
        try (FileWriter replayFileWriter = new FileWriter(replayFilePath)) {
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
                    long logTime = this.fastDTParser.dtStringToNanoTime(logTimeString);
                    String messageString = fields[PGLOG_MESSAGE_INDEX];
                    String sqlString = PostgresLogFileParser.parseSQLFromMessage(messageString);
                    if (sqlString == null) {
                        // ignore any lines which are not SQL statements
                        continue;
                    }
                    String detailString = fields[PGLOG_DETAIL_INDEX];
                    Object[] params = PostgresLogFileParser.parseParamsFromDetail(detailString);
                    String vxid = fields[PGLOG_VXID_INDEX];

                    // manage activeTransactions and logTransactionQueue based on what the line is
                    if (sqlString.equals(BEGIN_STRING)) {
                        assert(!activeTransactions.containsKey(vxid));
                        LogTransaction newExplicitLogTransaction = new LogTransaction(true);
                        newExplicitLogTransaction.addSQLStmtLine(sqlString, params, logTime);
                        activeTransactions.put(vxid, newExplicitLogTransaction);
                        // in the output replay file, replay transactions are ordered by the time they first appear in the log file
                        logTransactionQueue.add(newExplicitLogTransaction);
                    } else if (sqlString.equals(COMMIT_STRING) || sqlString.equals(ROLLBACK_STRING)) {
                        LogTransaction explicitLogTransaction = activeTransactions.get(vxid);
                        // if the sqlString is ROLLBACK, it's actually possible for that to be the only statement for that vxid and thus for explicitLogTransaction to be null
                        // one way this can happen is if a txn deadlocks and thus gets aborted before it is able to execute any statements
                        assert sqlString.equals(ROLLBACK_STRING) || explicitLogTransaction != null : "It's only allowable for vxid to not be in activeTransactions if sqlString is ROLLBACK";
                        if (explicitLogTransaction != null) {
                            explicitLogTransaction.addSQLStmtLine(sqlString, params, logTime);
                            explicitLogTransaction.markComplete();
                            activeTransactions.remove(vxid);
                        }
                    } else {
                        Integer sqlStmtID = sqlStringIDs.get(sqlString);
                        if (sqlStmtID == null) {
                            sqlStmtID = nextSQLStmtID++;
                            sqlStringIDs.put(sqlString, sqlStmtID);
                        }

                        if (activeTransactions.containsKey(vxid)) {
                            // if it's in active transactions, it should be added to the corresponding explicit transaction
                            LogTransaction explicitLogTransaction = activeTransactions.get(vxid);
                            explicitLogTransaction.addSQLStmtLine(sqlStmtID, params, logTime);
                        } else {
                            // if it's not in active transactions, it must be an implicit transaction
                            LogTransaction implicitLogTransaction = new LogTransaction(false);
                            implicitLogTransaction.addSQLStmtLine(sqlStmtID, params, logTime);
                            logTransactionQueue.add(implicitLogTransaction);
                        }
                    }

                    long bytesRead = logInputStream.getChannel().position();
                    lastProgressPercent = ConsoleUtil.printProgressBar(bytesRead, totalBytes, lastProgressPercent);
                    totalInLoopComputeTime += System.nanoTime() - loopInnerStartTime;
                }
                System.out.println(); // the progress bar doesn't have a newline at the end of it. this adds one
                System.out.printf("convertLogFileToReplayFile: the whole loop took %.4fms\n", (double)(System.nanoTime() - loopOuterStartTime) / 1000000);
                System.out.printf("convertLogFileToReplayFile: we spent %.4fms doing compute inside the loop\n", (double)totalInLoopComputeTime / 1000000);

                // write all transactions at the end instead of inside the loop so that both the log file
                // read and the replay file write can be sequential 
                int numUncompletedTransactions = 0;
                while (!logTransactionQueue.isEmpty()) {
                    LogTransaction logTransaction = logTransactionQueue.peek();
                    if (logTransaction.getIsComplete()) {
                        replayFileWriter.write(logTransaction.toString());
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
                replayFileWriter.write(ReplayFileManager.REPLAY_FILE_SECTION_DELIM + "\n");
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
            throw new RuntimeException("Replay file " + replayFilePath + " does not exist");
        } catch (IOException e) {
            replayFile.delete();
            throw new RuntimeException("Encountered IOException " + e + " when opening replay file");
        } catch (RuntimeException e) {
            replayFile.delete();
            throw e;
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
        Pair<String, String> typeAndContent = PostgresLogFileParser.splitTypeAndContent(messageString);
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
     * @return An Object[] or null if the detail is not a SQL parameter detail
     */
    public static Object[] parseParamsFromDetail(String detailString) { // PAT DEBUG: make this private later
        long startTime = System.nanoTime();
        List<Object> valuesList = new ArrayList<>();
        Pair<String, String> typeAndContent = PostgresLogFileParser.splitTypeAndContent(detailString);
        
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
                            valuesList.add(PostgresLogFileParser.parseSQLLogStringToObject(currentParamStr.toString()));
                            currentParamStr = new StringBuilder(); // reset builder
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

        timeInParseParamsFromDetail += System.nanoTime() - startTime;
        return new Object[1];
    }

    private static Object parseSQLLogStringToObject(String sqlLogString) {
        // Check if the string is null or empty
        if (sqlLogString == null || sqlLogString.isEmpty()) {
            return null;
        }
    
        // Check if the string represents an integer
        try {
            int data = Integer.parseInt(sqlLogString);
            return data;
        } catch (NumberFormatException e) {
            // Not an integer
        }
    
        // Check if the string represents a double
        try {
            double data = Double.parseDouble(sqlLogString);
            return data;
        } catch (NumberFormatException e) {
            // Not a double
        }

        // Check if the string represents a timestamp
        try {
            DateTimeFormatterBuilder formatterBuilder = new DateTimeFormatterBuilder();
            formatterBuilder.appendPattern("yyyy-MM-dd HH:mm:ss");
            formatterBuilder.appendFraction(ChronoField.MILLI_OF_SECOND, 0, 9, true);
            DateTimeFormatter formatter = formatterBuilder.toFormatter();
            LocalDateTime localDateTime = LocalDateTime.parse(sqlLogString, formatter);
            Timestamp data = Timestamp.valueOf(localDateTime);
            return data;
        } catch (DateTimeParseException e) {
            // Not a timestamp
        }
    
        // If all else fails, assume it's a string type
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

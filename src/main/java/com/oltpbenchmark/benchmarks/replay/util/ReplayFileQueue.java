package com.oltpbenchmark.benchmarks.replay.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
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

import com.opencsv.CSVReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oltpbenchmark.DBWorkload;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.util.Pair;

/**
 * TODO: after finishing, decide whether this should be a subclass of queue
 */
// TODO: this is currently hardcoded for Postgres' log file format
public class ReplayFileQueue {
    private static final Logger LOG = LoggerFactory.getLogger(DBWorkload.class);
    private static final int LOG_TIME_INDEX = 0;
    private static final int VXID_INDEX = 9;
    private static final int MESSAGE_INDEX = 13;
    private static final int DETAIL_INDEX = 14;
    private static final String BEGIN_REGEX = "BEGIN;?";
    private static final String COMMIT_REGEX = "COMMIT;?";
    private static final String ABORT_REGEX = "(ABORT|ROLLBACK);?";

    private Queue<ReplayTransaction> queue;
    private CSVReader csvReader;

    /**
     * ReplayFileQueue provides a queue interface over a replay file.
     * 
     * One important note is that the peek() and pop() operations may block for disk reads.
     * Prefetching is used to avoid this as much as possible, but if the rate of peek() and pop()
     * calls is higher than the maximum disk throughput, blocking is inevitable.
     */
    public ReplayFileQueue(String replayFilePath) {
        try {
            // CSVReader handles CSV values which have newlines embedded in them
            this.csvReader = new CSVReader(new BufferedReader(new FileReader(replayFilePath)));
        } catch (FileNotFoundException e) {
            LOG.error("Replay file " + replayFilePath + " does not exist");
            System.exit(-1);
        }
        
        queue = new LinkedList<>();
        String[] fields;

        try {
            // Virtual Transaction IDs (VXIDs) vs Transaction IDs (XIDs)
            // Source: PostgreSQL 10 High Performance -> Database Activity and Statistics -> Locks -> Virtual Transactions
            //  - XIDs don't work because (1) XIDs aren't even assigned to read-only transactions and (2) the XID wraparound issue means different transactions active at the same time may have the same XID
            //  - VXIDs do work as they are (1) always assigned and (2) guaranteed to be unique amongst active transactions as they are used for locking
            Map<String, ReplayTransaction> activeTransactions = new HashMap<String, ReplayTransaction>();

            while ((fields = this.csvReader.readNext()) != null) {
                // we parse the line in ReplayFileQueue instead of sending it to the constructor of ReplayTransaction
                // because sometimes transactions are built from multiple lines
                String logTimeString = fields[LOG_TIME_INDEX];
                long logTime = ReplayFileQueue.dtStringToNanoTime(logTimeString);
                String messageString = fields[MESSAGE_INDEX];
                String sqlString = ReplayFileQueue.parseSQLFromMessage(messageString);
                if (sqlString == null) {
                    // ignore any lines which are not SQL statements
                    continue;
                }
                String detailString = fields[DETAIL_INDEX];
                List<Object> params = ReplayFileQueue.parseParamsFromDetail(detailString);
                String vxid = fields[VXID_INDEX];

                if (sqlString.matches(BEGIN_REGEX)) {
                    if (activeTransactions.containsKey(vxid)) {
                        throw new RuntimeException("Found BEGIN for an already active transaction");
                    }
                    ReplayTransaction emptyExplicitReplayTransaction = new ReplayTransaction(logTime, true);
                    activeTransactions.put(vxid, emptyExplicitReplayTransaction);
                    // in the queue, replay transactions are ordered by the time they first appear in the log file
                    queue.add(emptyExplicitReplayTransaction);
                } else if (sqlString.matches(COMMIT_REGEX) || sqlString.matches(ABORT_REGEX)) {
                    if (!activeTransactions.containsKey(vxid)) {
                        throw new RuntimeException("Found COMMIT or ABORT for a non-active transaction");
                    }
                    ReplayTransaction explicitReplayTransaction = activeTransactions.get(vxid);
                    explicitReplayTransaction.setShouldAbort(sqlString.matches(ABORT_REGEX));
                    activeTransactions.remove(vxid);
                } else {
                    // this is the only scope in which where we need sqlStmt, so we instantiate it here instead of before the if
                    SQLStmt sqlStmt = new SQLStmt(sqlString);
                    if (activeTransactions.containsKey(vxid)) {
                        // if it's in active transactions, it should be added to the corresponding explicit transaction
                        ReplayTransaction explicitReplayTransaction = activeTransactions.get(vxid);
                        explicitReplayTransaction.addSQLStmtCall(sqlStmt, params, logTime);
                    } else {
                        // if it's not in active transactions, it must be an implicit transaction
                        ReplayTransaction implicitReplayTransaction = new ReplayTransaction(logTime, false);
                        implicitReplayTransaction.addSQLStmtCall(sqlStmt, params, logTime);
                        queue.add(implicitReplayTransaction);
                    }
                }
            }
            int oldSize = queue.size();
            queue.removeIf(replayTransaction -> !replayTransaction.getIsShouldAbortSet());
            int newSize = queue.size();
            if (newSize < oldSize) {
                LOG.warn("removed %d unfinished transactions", oldSize - newSize);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private static long dtStringToNanoTime(String dtString) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS z", Locale.US);
        ZonedDateTime zdt = ZonedDateTime.parse(dtString, formatter);
        Instant instant = zdt.toInstant();
        long nanoseconds = instant.getEpochSecond() * 1_000_000_000L + instant.getNano();
        return nanoseconds;        
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
        Pair<String, String> typeAndContent = ReplayFileQueue.splitTypeAndContent(messageString);
        if (typeAndContent != null) {
            String messageType = typeAndContent.first;
            String messageContent = typeAndContent.second;
            boolean typeIsStatement = messageType.equals("statement");
            boolean typeIsExecute = messageType.matches("^execute .*$");

            if (typeIsStatement || typeIsExecute) {
                String questionMarks = messageContent.replaceAll("\\$\\d+", "?");
                return questionMarks;
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
        Pair<String, String> typeAndContent = ReplayFileQueue.splitTypeAndContent(detailString);
        
        if (typeAndContent != null) {
            String detailType = typeAndContent.first;
            String detailContent = typeAndContent.second;

            if (detailType.equals("parameters")) {
                // Regular expression to match single-quoted values.
                Pattern pattern = Pattern.compile("'(.*?)'");
                Matcher matcher = pattern.matcher(detailContent);

                while (matcher.find()) {
                    valuesList.add(ReplayFileQueue.parseSQLLogStringToObject(matcher.group(1)));
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
     * Split a log string into "type" and "content"
     * 
     * The format is "[type]: [content]"
     * Both "message" and "detail" strings are in the same format
     * 
     * @param logString A message or detail string
     * @return The type and content if it follows the format, or a null if it doesn't
     */
    private static Pair<String, String> splitTypeAndContent(String logString) {
        // DOTALL allows . to match newlines, which may be present in the SQL string
        Pattern pattern = Pattern.compile("^(.*?):\s+(.*?)$", Pattern.DOTALL);
        Matcher matcher = pattern.matcher(logString);
        if (matcher.find()) {
            String type = matcher.group(1);
            String content = matcher.group(2);
            return Pair.of(type, content);
        } else {
            return null;
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
            ReplayTransaction replayTransaction = this.queue.peek();
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
        if (DBWorkload.DEBUG) {
            System.out.println("Entering RFQ.remove");
        }
        synchronized (this) {
            this.queue.remove();
        }
    }
}

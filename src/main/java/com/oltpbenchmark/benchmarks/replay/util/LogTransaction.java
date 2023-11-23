package com.oltpbenchmark.benchmarks.replay.util;

import java.util.LinkedList;
import java.util.List;

// A LogTransaction represents a single transaction to write to the replay file
// These transactions are created by reading from the log file
public class LogTransaction {
    // A SQLStmtLine represents a single SQL statement line to write to the replay file
    private static class SQLStmtLine {
        private String sqlStmtOutputString;
        private String detailString;
        private long callTime;

        /**
         * @param sqlStmtID The ID of the SQL statement of this line
         * @param params    The string of parameters of this SQL line in the log file
         * @param callTime  The timestamp, in nanoseconds, of this statement's line in the log file
         */
        private SQLStmtLine(int sqlStmtID, String detailString, long callTime) {
            this.sqlStmtOutputString = Integer.toString(sqlStmtID);
            this.detailString = detailString;
            this.callTime = callTime;
        }

        /**
         * @param sqlStmtString The String of the SQL statement of this line
         * @param params        The string of parameters of this SQL line in the log file
         * @param callTime      The timestamp, in nanoseconds, of this statement's line in the log file
         */
        private SQLStmtLine(String sqlStmtString, String detailString, long callTime) {
            this.sqlStmtOutputString = sqlStmtString;
            this.detailString = detailString;
            this.callTime = callTime;
        }

        private String getFormattedString() {
            return String.format("%s,\"%s\",\"%s\"", Long.toHexString(this.callTime), this.sqlStmtOutputString, this.detailString);
        }
    }

    private List<SQLStmtLine> sqlStmtLines;
    private boolean isComplete;

    /**
     * @param isExplicitTransaction Whether this object represents an explicit or implicit transaction
     */
    public LogTransaction(boolean isExplicitTransaction) {
        this.sqlStmtLines = new LinkedList<>();
        this.isComplete = !isExplicitTransaction;
    }

    public void markComplete() {
        if (this.isComplete) {
            throw new RuntimeException("markComplete was called when isComplete is already true");
        }
        this.isComplete = true;
    }

    public boolean getIsComplete() {
        return isComplete;
    }

    public void addSQLStmtLine(int sqlStmtID, String detailString, long callTime) {
        this.sqlStmtLines.add(new SQLStmtLine(sqlStmtID, detailString, callTime));
    }

    public void addSQLStmtLine(String sqlStmtString, String detailString, long callTime) {
        this.sqlStmtLines.add(new SQLStmtLine(sqlStmtString, detailString, callTime));
    }

    public String getFormattedString() {
        if (!this.isComplete) {
            throw new RuntimeException("getFormattedString should only be called if the transaction is complete");
        }

        StringBuilder sb = new StringBuilder();
        for (SQLStmtLine sqlStmtLine : this.sqlStmtLines) {
            sb.append(sqlStmtLine.getFormattedString() + "\n");
        }

        return sb.toString();
    }
}

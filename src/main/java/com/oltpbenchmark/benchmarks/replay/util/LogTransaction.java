package com.oltpbenchmark.benchmarks.replay.util;

import java.util.LinkedList;
import java.util.List;

/**
 * @brief A LogTransaction represents a single transaction to write to the replay file
 * 
 * These transactions are created by reading from the log file
 * 
 * @author phw2
 */
public class LogTransaction {
    // A SQLStmtLine represents a single SQL statement line to write to the replay file
    private static class SQLStmtLine {
        private String sqlStmtOutputString;
        private Object[] params;
        private long callTime;

        /**
         * @param sqlStmtID The ID of the SQL statement of this line
         * @param params    The parameters of this SQL line in the log file
         * @param callTime  The timestamp, in nanoseconds, of this statement's line in the log file
         */
        private SQLStmtLine(int sqlStmtID, Object[] params, long callTime) {
            this.sqlStmtOutputString = Integer.toString(sqlStmtID);
            this.params = params;
            this.callTime = callTime;
        }

        /**
         * @param sqlStmtString The String of the SQL statement of this line
         * @param params        The parameters of this SQL line in the log file
         * @param callTime      The timestamp, in nanoseconds, of this statement's line in the log file
         */
        private SQLStmtLine(String sqlStmtString, Object[] params, long callTime) {
            this.sqlStmtOutputString = sqlStmtString;
            this.params = params;
            this.callTime = callTime;
        }

        public String toString() {
            return String.format("%d,\"%s\",\"%s\"", this.callTime, this.sqlStmtOutputString, ReplayFileReader.paramsToReplayString(this.params));
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

    public void addSQLStmtLine(int sqlStmtID, Object[] params, long callTime) {
        this.sqlStmtLines.add(new SQLStmtLine(sqlStmtID, params, callTime));
    }

    public void addSQLStmtLine(String sqlStmtString, Object[] params, long callTime) {
        this.sqlStmtLines.add(new SQLStmtLine(sqlStmtString, params, callTime));
    }

    public String toString() {
        if (!this.isComplete) {
            throw new RuntimeException("toString should only be called if the transaction is complete");
        }

        StringBuilder sb = new StringBuilder();
        for (SQLStmtLine sqlStmtLine : this.sqlStmtLines) {
            sb.append(sqlStmtLine.toString() + "\n");
        }

        return sb.toString();
    }
}

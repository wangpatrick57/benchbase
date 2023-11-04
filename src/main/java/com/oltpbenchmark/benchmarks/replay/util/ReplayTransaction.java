package com.oltpbenchmark.benchmarks.replay.util;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;

import com.oltpbenchmark.api.SQLStmt;

public class ReplayTransaction {
    private static class SQLStmtCall {
        private SQLStmt sqlStmt;
        private List<Object> params;
        private long callTime;

        /**
         * A SQLStmtCall represents a single call of a SQL statement within a ReplayTransaction
         * @param sqlStmt The statement that was called
         * @param params The parameters it was called with. Should be an empty List if the SQL statement
         *               is not parameterized
         * @param callTime The timestamp, in nanoseconds, of this statement's line in the log file
         */
        private SQLStmtCall(SQLStmt sqlStmt, List<Object> params, long callTime) {
            this.sqlStmt = sqlStmt;
            this.params = params;
            this.callTime = callTime;
        }
    }

    private Queue<SQLStmtCall> sqlStmtCalls;
    private long firstLogTime;
    private Optional<Boolean> shouldAbort;

    /**
     * A ReplayTransaction represents a single transaction to replay
     * @param firstLogTime The timestamp, in nanoseconds, the first line of this transaction was logged at
     * @param isExplicitTransaction Whether this object represents an explicit or implicit transaction
     */
    public ReplayTransaction(long firstLogTime, boolean isExplicitTransaction) {
        this.firstLogTime = firstLogTime;
        this.sqlStmtCalls = new LinkedList<>();
        
        if (isExplicitTransaction) {
            this.shouldAbort = Optional.empty(); // for explicit transactions, this needs to be set later on when COMMIT or ABORT is read
        } else {
            this.shouldAbort = Optional.of(false); // implicit transactions are never aborted
        }
    }

    /**
     * @pre shouldAbort has not already been set (either by the constructor or by this function)
     */
    public void setShouldAbort(boolean shouldAbort) {
        if (this.getIsShouldAbortSet()) {
            throw new RuntimeException("setShouldAbort should only be called if shouldAbort is not already set");
        }
        this.shouldAbort = Optional.of(shouldAbort);
    }

    public void addSQLStmtCall(SQLStmt sqlStmt, List<Object> params, long callTime) {
        this.sqlStmtCalls.add(new SQLStmtCall(sqlStmt, params, callTime));
    }

    public int getSQLStmtCallCount() {
        return this.sqlStmtCalls.size();
    }

    public boolean hasSQLStmtCall() {
        return !this.sqlStmtCalls.isEmpty();
    }

    /**
     * @pre The queue is not empty
     */
    public SQLStmt peekSQLStmt() {
        return this.sqlStmtCalls.peek().sqlStmt;
    }

    /**
     * @pre The queue is not empty
     */
    public List<Object> peekParams() {
        return this.sqlStmtCalls.peek().params;
    }

    /**
     * @pre The queue is not empty
     */
    public long peekCallTime() {
        return this.sqlStmtCalls.peek().callTime;
    }

    /**
     * @pre The queue is not empty
     */
    public void removeSQLStmtCall() {
        this.sqlStmtCalls.remove();
    }

    /**
     * @pre If the transaction is explicit, setShouldAbort() has been called
     */
    public boolean getShouldAbort() {
        if (!this.getIsShouldAbortSet()) {
            throw new RuntimeException("getShouldAbort was called when shouldAbort is unknown");
        }
        return this.shouldAbort.get();
    }

    public long getFirstLogTime() {
        return this.firstLogTime;
    }

    public boolean getIsShouldAbortSet() {
        return !this.shouldAbort.isEmpty();
    }
}

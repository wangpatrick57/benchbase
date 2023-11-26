package com.oltpbenchmark.benchmarks.replay.util;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;

import com.oltpbenchmark.api.SQLStmt;

/**
 * @brief A ReplayTransaction represents a single transaction to replay
 * 
 * These transactions are created by reading from the replay file
 * 
 * @author phw2
 */
public class ReplayTransaction {
    private static class SQLStmtCall {
        private SQLStmt sqlStmt;
        private Object[] params;
        private long callTime;

        /**
         * A SQLStmtCall represents a single call of a SQL statement within a ReplayTransaction
         * @param sqlStmt The statement that was called
         * @param params The parameters it was called with. Should be an empty List if the SQL statement
         *               is not parameterized
         * @param callTime The timestamp, in nanoseconds, of this statement's line in the log file
         */
        private SQLStmtCall(SQLStmt sqlStmt, Object[] params, long callTime) {
            this.sqlStmt = sqlStmt;
            this.params = params;
            this.callTime = callTime;
        }
    }

    private Queue<SQLStmtCall> sqlStmtCalls;
    private long firstLogTime;
    private Optional<Boolean> shouldRollback;

    /**
     * @param firstLogTime The timestamp, in nanoseconds, the first line of this transaction was logged at
     * @param isExplicitTransaction Whether this object represents an explicit or implicit transaction
     */
    public ReplayTransaction(long firstLogTime, boolean isExplicitTransaction) {
        this.firstLogTime = firstLogTime;
        this.sqlStmtCalls = new LinkedList<>();
        
        if (isExplicitTransaction) {
            this.shouldRollback = Optional.empty(); // for explicit transactions, this needs to be set later on when COMMIT or ABORT is read
        } else {
            this.shouldRollback = Optional.of(false); // implicit transactions are never aborted
        }
    }

    /**
     * @pre shouldRollback has not already been set (either by the constructor or by this function)
     */
    public void setShouldRollback(boolean shouldRollback) {
        if (this.getIsShouldRollbackSet()) {
            throw new RuntimeException("setShouldRollback should only be called if shouldRollback is not already set");
        }
        this.shouldRollback = Optional.of(shouldRollback);
    }

    public void addSQLStmtCall(SQLStmt sqlStmt, Object[] params, long callTime) {
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
    public Object[] peekParams() {
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
     * @pre If the transaction is explicit, setShouldRollback() has been called
     */
    public boolean getShouldRollback() {
        if (!this.getIsShouldRollbackSet()) {
            throw new RuntimeException("getShouldRollback was called when shouldRollback is unknown");
        }
        return this.shouldRollback.get();
    }

    public long getFirstLogTime() {
        return this.firstLogTime;
    }

    public boolean getIsShouldRollbackSet() {
        return !this.shouldRollback.isEmpty();
    }
}

package com.oltpbenchmark.benchmarks.replay.util;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;

import com.oltpbenchmark.api.SQLStmt;
import org.apache.commons.lang3.tuple.Pair;

public class ReplayTransaction {
    private Queue<Pair<SQLStmt, Long>> sqlStmtCalls;
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
        if (!this.shouldAbort.isEmpty()) {
            throw new RuntimeException("setShouldAbort should only be called if shouldAbort is not already set");
        }
        this.shouldAbort = Optional.of(shouldAbort);
    }

    public void addSQLStmtCall(SQLStmt sqlStmt, long callTime) {
        this.sqlStmtCalls.add(Pair.of(sqlStmt, callTime));
    }

    public boolean hasSQLStmtCall() {
        return !this.sqlStmtCalls.isEmpty();
    }

    /**
     * @pre The queue is not empty
     */
    public SQLStmt peekSQLStmt() {
        return this.sqlStmtCalls.peek().getLeft();
    }

    /**
     * @pre The queue is not empty
     */
    public long peekCallTime() {
        return this.sqlStmtCalls.peek().getRight();
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
        if (!this.shouldAbort.isEmpty()) {
            throw new RuntimeException("getShouldAbort was called when shouldAbort is unknown");
        }
        return this.shouldAbort.get();
    }

    public long getFirstLogTime() {
        return this.firstLogTime;
    }
}

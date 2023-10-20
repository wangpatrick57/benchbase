package com.oltpbenchmark.benchmarks.replay.util;

import java.util.List;

import com.oltpbenchmark.api.SQLStmt;

public class ReplayTransaction {
    private List<SQLStmt> sqlStmts;
    private long logTime;

    /**
     * A ReplayTransaction represents a single transaction to replay
     * @param[in] sqlStmts The SQLStmts in this transaction
     * @param[in] logTime The timestamp, in nanoseconds, this transaction was logged at
     */
    public ReplayTransaction(List<SQLStmt> sqlStmts, long logTime) {
        this.sqlStmts = sqlStmts;
        this.logTime = logTime;
    }

    public List<SQLStmt> getSQLStmts() {
        return this.sqlStmts;
    }

    public long getLogTime() {
        return this.logTime;
    }
}

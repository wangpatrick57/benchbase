package com.oltpbenchmark.benchmarks.replay.util;

import java.util.List;

import com.oltpbenchmark.api.SQLStmt;

public class ReplayTransaction {
    private List<SQLStmt> sqlStmts;
    private long replayTime;

    /**
     * @brief A ReplayTransaction represents a single transaction to replay
     * @param[in] sqlStmts The SQLStmts in this transaction
     * @param[in] replayTime The timestamp, in nanoseconds, this transaction should be replayed at. This is
     *                       the transaction's timestamp in the replay file shifted over.
     */
    public ReplayTransaction(List<SQLStmt> sqlStmts, long replayTime) {
        this.sqlStmts = sqlStmts;
        this.replayTime = replayTime;
    }

    public List<SQLStmt> getSQLStmts() {
        return this.sqlStmts;
    }

    public long getReplayTime() {
        return this.replayTime;
    }
}

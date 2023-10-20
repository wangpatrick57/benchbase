package com.oltpbenchmark.benchmarks.replay.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.oltpbenchmark.api.SQLStmt;

public class ReplayFileQueue {
    /**
     * ReplayFileQueue provides a queue interface over a replay file.
     * 
     * One important note is that the peek() and pop() operations may block for disk reads.
     * Prefetching is used to avoid this as much as possible, but if the rate of peek() and pop()
     * calls is higher than the maximum disk throughput, blocking is inevitable.
     */
    public ReplayFileQueue() {

    }

    /**
     * Return the "front" replay transaction
     * 
     * Will block until there are buffered transactions or the end of the file is reached.
     * 
     * @return A transaction (which is list of SQLStmts) or Optional.empty() if the "front" is the EOF
     */
    public Optional<ReplayTransaction> peek() {
        List<SQLStmt> sqlStmts = new ArrayList<>();
        sqlStmts.add(new SQLStmt("SELECT * FROM customer LIMIT 10;"));
        sqlStmts.add(new SQLStmt("SELECT * FROM delivery LIMIT 5;"));
        return Optional.of(new ReplayTransaction(sqlStmts, System.nanoTime()));
    }

    /**
     * Advance to the next replay transaction
     * 
     * Will block until there are buffered transactions or the end of the file is reached.
     */
    public void pop() {
    }
}

package com.oltpbenchmark.benchmarks.replay.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.oltpbenchmark.api.SQLStmt;

public class ReplayFileQueue {
    /**
     * @brief The file is viewed as a queue. Return the "front" replay transaction
     * 
     * Will block until there are buffered transactions or the end of the file is reached.
     * 
     * @return A transaction (which is list of SQLStmts) or Optional.empty() if the "front" is the EOF
     */
    public Optional<List<SQLStmt>> front() {
        Optional<List<SQLStmt>> transaction = Optional.of(new ArrayList<>());
        transaction.get().add(new SQLStmt("SELECT * FROM customer LIMIT 10;"));
        transaction.get().add(new SQLStmt("SELECT * FROM delivery LIMIT 5;"));
        return transaction;
    }

    /**
     * @brief The file is viewed as a queue. Advance to the next replay transaction
     * 
     * Will block until there are buffered transactions or the end of the file is reached.
     */
    public void pop() {
    }
}

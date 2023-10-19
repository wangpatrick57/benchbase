package com.oltpbenchmark.benchmarks.replay.util;

import java.util.ArrayList;
import java.util.List;

import com.oltpbenchmark.api.SQLStmt;

public class ReplayFileProcessor {
    private int timesHasNextTransactionCalled;

    public ReplayFileProcessor() {
        this.timesHasNextTransactionCalled = 0;
    }

    /**
     * @brief Gets "ready" transactions which haven't been returned yet.
     * 
     * "Ready" transactions are transactions whose relative timestamps have passed.
     * May block if there are no buffered transactions.
     * 
     * @return A list of transactions (which are lists of SQLStmts) in ascending timestamp order
     * 
     * @pre There is at least one next transaction.
     */
    public List<List<SQLStmt>> getNextReadyTransactions() {
        List<List<SQLStmt>> transactions = new ArrayList<>();
        List<SQLStmt> transaction = new ArrayList<>();
        transaction.add(new SQLStmt("SELECT * FROM customer LIMIT 10;"));
        transaction.add(new SQLStmt("SELECT * FROM delivery LIMIT 5;"));
        transactions.add(transaction);
        return transactions;
    }

    /**
     * @brief Gets the timestamp that the next transaction will be ready at.
     * 
     * May block if there are no buffered transactions.
     * 
     * @return The timestamp in nanoseconds
     */
    public long getNextReadyTimestamp() {
        return System.nanoTime() + 1000;
    }

    /**
     * @brief Return whether there is another transaction to process.
     * 
     * May block if there are no buffered transactions.
     * 
     * @return Whether there is another transaction
     */
    public boolean hasNextTransaction() {
        return !(this.timesHasNextTransactionCalled++ > 100);
    }
}

package com.oltpbenchmark.benchmarks.replay.util;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Random;

import com.oltpbenchmark.api.SQLStmt;

/**
 * TODO: maybe make this extend queue?
 */
public class ReplayFileQueue {
    private Queue<ReplayTransaction> queue;

    /**
     * ReplayFileQueue provides a queue interface over a replay file.
     * 
     * One important note is that the peek() and pop() operations may block for disk reads.
     * Prefetching is used to avoid this as much as possible, but if the rate of peek() and pop()
     * calls is higher than the maximum disk throughput, blocking is inevitable.
     */
    public ReplayFileQueue() {
        queue = new LinkedList<>();
        long start = System.nanoTime();

        for (long i = 0; i < 100; i++) {
            queue.add(new ReplayTransaction(generateRandomList(), start + 100000000 * i));
        }
    }

    // DEBUG
    private static List<SQLStmt> generateRandomList() {
        Random rand = new Random();
        int numberOfStmts = 2 + rand.nextInt(3); // Random number between 2 and 4 inclusive

        List<SQLStmt> sqlStmts = new ArrayList<>();
        for (int i = 0; i < numberOfStmts; i++) {
            sqlStmts.add(new SQLStmt("SELECT * FROM table LIMIT 10;"));
        }
        return sqlStmts;
    }

    /**
     * Return the "front" replay transaction
     * 
     * Will block until there are buffered transactions or the end of the file is reached.
     * 
     * @return A transaction (which is list of SQLStmts) or Optional.empty() if the "front" is the EOF
     */
    public Optional<ReplayTransaction> peek() {
        synchronized (this) {
            ReplayTransaction replayTransaction = this.queue.peek();
            if (replayTransaction == null) {
                return Optional.empty();
            }
            return Optional.of(replayTransaction);
        }
    }

    /**
     * Advance to the next replay transaction
     * 
     * Will block until there are buffered transactions or the end of the file is reached.
     */
    public void pop() {
        synchronized (this) {
            this.queue.remove();
        }
    }
}

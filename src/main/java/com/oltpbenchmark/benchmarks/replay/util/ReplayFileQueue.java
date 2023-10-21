package com.oltpbenchmark.benchmarks.replay.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Queue;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oltpbenchmark.DBWorkload;
import com.oltpbenchmark.api.SQLStmt;

/**
 * TODO: after finishing, decide whether this should be a subclass of queue
 */
// TODO: this is currently hardcoded for Postgres' log file format
public class ReplayFileQueue {
    private static final Logger LOG = LoggerFactory.getLogger(DBWorkload.class);
    private static final int LOG_TIME_INDEX = 0;
    private static final int MESSAGE_INDEX = 12;

    private Queue<ReplayTransaction> queue;
    private CSVReader csvReader;

    /**
     * ReplayFileQueue provides a queue interface over a replay file.
     * 
     * One important note is that the peek() and pop() operations may block for disk reads.
     * Prefetching is used to avoid this as much as possible, but if the rate of peek() and pop()
     * calls is higher than the maximum disk throughput, blocking is inevitable.
     */
    public ReplayFileQueue(String logFilePath) {
        try {
            // CSVReader handles CSV values which have newlines embedded in them
            this.csvReader = new CSVReader(new BufferedReader(new FileReader(logFilePath)));
        } catch (FileNotFoundException e) {
            LOG.error("Log file " + logFilePath + " does not exist");
            System.exit(-1);
        }
        
        queue = new LinkedList<>();
        String[] fields;

        try {
            while ((fields = this.csvReader.readNext()) != null) {
                // we parse the line in ReplayFileQueue instead of sending it to the constructor of ReplayTransaction
                // because sometimes transactions are built from multiple lines
                String logTimeString = fields[LOG_TIME_INDEX];
                long logTime = ReplayFileQueue.dtStringToNanoTime(logTimeString);
                String messageString = fields[MESSAGE_INDEX];
                String sqlString = messageString.replaceFirst("^[^:]*: ", "");
                SQLStmt sqlStmt = new SQLStmt(sqlString);
                List<SQLStmt> sqlStmts = new ArrayList<>();
                sqlStmts.add(sqlStmt);
                ReplayTransaction replayTransaction = new ReplayTransaction(sqlStmts, logTime);
                queue.add(replayTransaction);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private static long dtStringToNanoTime(String dtString) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS z", Locale.US);
        ZonedDateTime zdt = ZonedDateTime.parse(dtString, formatter);
        Instant instant = zdt.toInstant();
        long nanoseconds = instant.getEpochSecond() * 1_000_000_000L + instant.getNano();
        return nanoseconds;        
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

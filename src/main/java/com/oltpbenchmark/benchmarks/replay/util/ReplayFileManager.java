package com.oltpbenchmark.benchmarks.replay.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.replay.util.FastReplayFileReader;
import com.oltpbenchmark.benchmarks.replay.util.FastReplayFileReader.ReplayFileLine;
import com.oltpbenchmark.util.ConsoleUtil;

/**
 * @brief ReplayFileManager orchestrates log-to-replay conversion, reading the replay file, and building the in-memory txn queue
 * @author phw2
 */
public class ReplayFileManager {
    public static final char REPLAY_FILE_SECTION_DELIM = '#';

    private static final Logger LOG = LoggerFactory.getLogger(ReplayFileManager.class);
    private static final String BEGIN_STRING = "BEGIN";
    private static final String COMMIT_STRING = "COMMIT";
    private static final String ROLLBACK_STRING = "ROLLBACK";
    private static final int CBUF_MAX_SIZE = 65536;

    private String logFilePath;
    private String replayFilePath;
    private Queue<ReplayTransaction> replayTransactionQueue;
    private boolean hasSuccessfullyLoaded;
    private ReplayTransaction currentActiveTransaction;
    
    private Map<Integer, SQLStmt> sqlStmtCache;

    public ReplayFileManager(String logFilePath) {
        // check that REPLAY_FILE_SECTION_DELIM never appears in any positive or negative hexadecimal number
        // it checks for a few extra chars (namely all letters after 'f' or 'F'), but that's okay
        assert(REPLAY_FILE_SECTION_DELIM != '-' && !Character.isDigit(REPLAY_FILE_SECTION_DELIM) && !Character.isAlphabetic(REPLAY_FILE_SECTION_DELIM));
        this.logFilePath = logFilePath;
        this.replayFilePath = getReplayFilePath(logFilePath);
        this.hasSuccessfullyLoaded = false;
    }

    private static String getReplayFilePath(String logFilePath) {
        if (logFilePath.endsWith(".csv")) {
            return logFilePath.replace(".csv", ".rply");
        } else {
            throw new RuntimeException("logFilePath does not end with .csv");
        }
    }

    /**
     * @brief Will create/recreate the replay file if necessary and
     *        load the entire replay file into memory
     */
    public void load() {
        synchronized (this) {
            File logFile = new File(logFilePath);
            File replayFile = new File(replayFilePath);
            boolean doConvert;

            if (logFile.exists() && replayFile.exists()) {
                long lastLogModified = logFile.lastModified();
                long lastReplayModified = replayFile.lastModified();

                if (lastLogModified >= lastReplayModified) {
                    LOG.info("Log file last modified at " + Long.toString(lastLogModified) + ", which is later than when replay file was last modified (" + Long.toString(lastReplayModified) + "). Will convert again.");
                    doConvert = true;
                } else {
                    LOG.info("Log file last modified at " + Long.toString(lastLogModified) + ", which is earlier than when replay file was last modified (" + Long.toString(lastReplayModified) + "). Will not convert.");
                    doConvert = false;
                }
            } else if (!logFile.exists() && replayFile.exists()) {
                LOG.info("Log file doesn't exist but replay file does. Will assume that user already converted to this replay file and then deleted the original log file, so will just directly use the replay file.");
                doConvert = false;
            } else if (logFile.exists() && !replayFile.exists()) {
                LOG.info("Replay file doesn't exist but log file does. Will convert log file to replay file.");
                doConvert = true;
            } else {
                throw new RuntimeException("Both the log file (" + logFilePath + ") and replay file (" + replayFilePath + ") do not exist.");
            }

            // PrivateBench.run(this.logFilePath, this.replayFilePath);

            if (doConvert) {
                LogFileParser logFileParser = new PostgresLogFileParser();
                logFileParser.convertLogFileToReplayFile(this.logFilePath, this.replayFilePath);
            }
            loadReplayFile();
        }
    }

    /**
     * @brief Read the replay file and build the in-memory txn queue
     * 
     * "Loading" refers to building the in-memory txn queue. On the other hand, "reading" refers to
     * simply reading the replay file line-by-line. FastReplayFileReader takes care of reading while
     * this function takes care of loading.
     */
    private void loadReplayFile() {
        LOG.info("Loading the replay file " + this.replayFilePath + "...");
        File replayFile = new File(this.replayFilePath);
        long totalBytes = replayFile.length();
        
        FileInputStream replayInputStream;
        try {
            replayInputStream = new FileInputStream(this.replayFilePath);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Replay file " + this.replayFilePath + " does not exist");
        }
        InputStreamReader replayInputStreamReader = new InputStreamReader(replayInputStream);

        try (CSVReader csvReader = new CSVReader(replayInputStreamReader)) {
            this.sqlStmtCache = new HashMap<>();
            this.replayTransactionQueue = new LinkedList<>();
            this.currentActiveTransaction = null;
            ReplayFileLine replayFileLine;

            // Read replay transaction section of file
            // the outer loop fills cbuf. cbuf may contain the start of a line which was not complete in the last cbuf
            long totalInLoopComputeTime = 0;
            long loopOuterStartTime = System.nanoTime();
            int lastProgressPercent = -1;
            String[] fields;
            while ((fields = csvReader.readNext()) != null) {
                long loopInnerStartTime = System.nanoTime();
                
                if (fields[0].charAt(0) == ReplayFileManager.REPLAY_FILE_SECTION_DELIM) {
                    break;
                }
                
                replayFileLine = FastCSVReader.fieldsToReplayFileLine(fields);
                processReplayFileLine(replayFileLine);
                
                long bytesRead = replayInputStream.getChannel().position();
                lastProgressPercent = ConsoleUtil.printProgressBar(bytesRead, totalBytes, lastProgressPercent);
                totalInLoopComputeTime += System.nanoTime() - loopInnerStartTime;
            }
            System.out.println(); // the progress bar doesn't have a newline at the end of it. this adds one
            System.out.printf("loadReplayFile: the whole loop took %.4fms\n", (double)(System.nanoTime() - loopOuterStartTime) / 1000000);
            System.out.printf("loadReplayFile: we spent %.4fms doing compute inside the loop\n", (double)totalInLoopComputeTime / 1000000);
            System.out.printf("loadReplayFile: we loaded %d transactions\n", this.replayTransactionQueue.size());

            // read SQL statement cache section of file
            while ((fields = csvReader.readNext()) != null) {
                String sqlStmtIDString = fields[0];
                int sqlStmtID = Integer.parseInt(sqlStmtIDString);
                String sqlString = fields[1];

                if (!sqlStmtCache.containsKey(sqlStmtID)) {
                    LOG.warn("Read SQL statement of ID " + sqlStmtID + ", except this ID didn't show up in the replay transactions");
                } else {
                    SQLStmt sqlStmt = sqlStmtCache.get(sqlStmtID);
                    sqlStmt.setSQL(sqlString);
                }
            }
            
            // perform validations
            for (Map.Entry<Integer, SQLStmt> entry : sqlStmtCache.entrySet()) {
                int sqlStmtID = entry.getKey();
                SQLStmt sqlStmt = entry.getValue();

                if (sqlStmt.getSQL().equals("")) {
                    throw new RuntimeException("sqlStmtID " + sqlStmtID + " showed up in the replay transactions but not in the cache");
                }
            }

            hasSuccessfullyLoaded = true;
        } catch (CsvValidationException e) {
            throw new RuntimeException("Replay file not in a valid CSV format");
        } catch (IOException e) {
            throw new RuntimeException("I/O exception " + e + " when reading replay file");
        }
    }

    private void processReplayFileLine(ReplayFileLine replayFileLine) {
        if (replayFileLine.sqlStmtIDOrString.equals(BEGIN_STRING)) {
            if (this.currentActiveTransaction != null) {
                throw new RuntimeException("Found BEGIN when previous transaction hadn't ended yet");
            }
            this.currentActiveTransaction = new ReplayTransaction(replayFileLine.logTime, true);
        } else if (replayFileLine.sqlStmtIDOrString.equals(COMMIT_STRING) || replayFileLine.sqlStmtIDOrString.equals(ROLLBACK_STRING)) {
            if (this.currentActiveTransaction == null) {
                throw new RuntimeException("Found COMMIT or ROLLBACK when there is no active transaction");
            }
            this.currentActiveTransaction.setShouldRollback(replayFileLine.sqlStmtIDOrString.equals(ROLLBACK_STRING));
            replayTransactionQueue.add(this.currentActiveTransaction);
            this.currentActiveTransaction = null;
        } else {
            int sqlStmtID = Integer.parseInt(replayFileLine.sqlStmtIDOrString);
            if (!sqlStmtCache.containsKey(sqlStmtID)) {
                // put a placeholder SQLStmt for now. later, when we read the sqlStmtCache section
                // of the replay file, we'll fill in the strings in the sqlStmtCache
                sqlStmtCache.put(sqlStmtID, new SQLStmt(""));
            }
            SQLStmt sqlStmt = sqlStmtCache.get(sqlStmtID);

            if (this.currentActiveTransaction != null) {
                this.currentActiveTransaction.addSQLStmtCall(sqlStmt, replayFileLine.params, replayFileLine.logTime);
            } else {
                // if there's isn't a current active transaction, it must be an implicit transaction
                ReplayTransaction implicitReplayTransaction = new ReplayTransaction(replayFileLine.logTime, false);
                implicitReplayTransaction.addSQLStmtCall(sqlStmt, replayFileLine.params, replayFileLine.logTime);
                replayTransactionQueue.add(implicitReplayTransaction);
            }
        }
    }

    public static String charBufToString(char[] cbuf, int start, int end) {
        StringBuilder sb = new StringBuilder();

        for (int i = start; i < end; i++) {
            sb.append(cbuf[i]);
        }

        return sb.toString();
    }

    // TODO: make the interfaces of all the queue classes consistent in terms of what peek and remove do
    //       if the queue is empty

    /**
     * Return the "front" replay transaction
     * 
     * Will block until there are buffered transactions or the end of the file is reached.
     * 
     * @return A transaction (which is list of SQLStmts) or Optional.empty() if the "front" is the EOF
     */
    public Optional<ReplayTransaction> peek() {
        synchronized (this) {
            if (!hasSuccessfullyLoaded) {
                throw new RuntimeException("Cannot call ReplayFileManager.peek() before ReplayFileManager.load() succeeds");
            }

            ReplayTransaction replayTransaction = this.replayTransactionQueue.peek();
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
     * 
     * TODO: what does this do if we're at the end of the file?
     */
    public void remove() {
        synchronized (this) {
            if (!hasSuccessfullyLoaded) {
                throw new RuntimeException("Cannot call ReplayFileManager.peek() before ReplayFileManager.load() succeeds");
            }

            this.replayTransactionQueue.remove();
        }
    }
}

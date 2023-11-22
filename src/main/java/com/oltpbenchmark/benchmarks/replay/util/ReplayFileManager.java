package com.oltpbenchmark.benchmarks.replay.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
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

// ReplayFileManager provides a queue-like interface over a replay file.
// This currently only works for Postgres' log file format.

/**
 * @author phw2
 */
public class ReplayFileManager {
    public static final String REPLAY_FILE_SECTION_DELIM = "###";

    private static final Logger LOG = LoggerFactory.getLogger(ReplayFileManager.class);
    private static final String BEGIN_STRING = "BEGIN";
    private static final String COMMIT_STRING = "COMMIT";
    private static final String ROLLBACK_STRING = "ROLLBACK";

    private String logFilePath;
    private String replayFilePath;
    private Queue<ReplayTransaction> replayTransactionQueue;
    private boolean hasSuccessfullyLoaded;
    
    private Map<Integer, SQLStmt> sqlStmtCache;

    /**
     * The constructor reads the entire file into memory
     */
    public ReplayFileManager(String logFilePath) {
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

            bufferedReaderScan(this.logFilePath);
            bufferedReaderScan(this.replayFilePath);
            csvReaderScan(this.logFilePath);
            csvReaderScan(this.replayFilePath);
            if (doConvert) {
                PostgresLogFileParser.convertLogFileToReplayFile(this.logFilePath, this.replayFilePath);
            }
            loadReplayFile();
            // throw new RuntimeException("early exit");
        }
    }

    private void bufferedReaderScan(String filePath) {
        FileInputStream inputStream;
        try {
            inputStream = new FileInputStream(filePath);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("File " + filePath + " does not exist");
        }

        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            long loopOuterStartTime = System.nanoTime();
            while ((line = bufferedReader.readLine()) != null) {}
            System.out.printf("bufferedReaderScan(filePath=%s): the whole loop took %.4fms\n", filePath, (double)(System.nanoTime() - loopOuterStartTime) / 1000000);
        } catch (IOException e) {
            throw new RuntimeException("I/O exception " + e + " when reading log file");
        }
    }

    private void csvReaderScan(String filePath) {
        FileInputStream inputStream;
        try {
            inputStream = new FileInputStream(filePath);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("File " + filePath + " does not exist");
        }

        try (CSVReader csvReader = new CSVReader(new InputStreamReader(inputStream))) {
            String[] fields;
            long loopOuterStartTime = System.nanoTime();
            while ((fields = csvReader.readNext()) != null) {}
            System.out.printf("csvReaderScan(filePath=%s): the whole loop took %.4fms\n", filePath, (double)(System.nanoTime() - loopOuterStartTime) / 1000000);
        } catch (CsvValidationException e) {
            throw new RuntimeException("Log file not in a valid CSV format");
        } catch (IOException e) {
            throw new RuntimeException("I/O exception " + e + " when reading log file");
        }
    }

    private void loadReplayFile() {
        LOG.info("Loading the replay file " + replayFilePath + "...");
        CSVReader replayCSVReader;
        try {
            // CSVReader handles CSV values which have newlines embedded in them
            replayCSVReader = new CSVReader(new BufferedReader(new FileReader(replayFilePath)));
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Replay file " + replayFilePath + " does not exist");
        }

        try {
            this.sqlStmtCache = new HashMap<>();
            this.replayTransactionQueue = new LinkedList<>();
            ReplayTransaction currentActiveTransaction = null;
            String[] fields;

            // read replay transaction section of file
            long loopOuterStartTime = System.nanoTime();
            while ((fields = replayCSVReader.readNext()) != null) {
                if (fields.length == 1 && fields[0].equals(REPLAY_FILE_SECTION_DELIM)) {
                    break;
                }

                // we parse the line in ReplayFileManager instead of sending it to the constructor of ReplayTransaction
                // because sometimes transactions are built from multiple lines
                String logTimeString = fields[0];
                long logTime = Long.parseLong(logTimeString);
                String sqlStmtIDOrString = fields[1];
                String detailString = fields[2];
                List<Object> params = PostgresLogFileParser.parseParamsFromDetail(detailString);

                if (sqlStmtIDOrString.equals(BEGIN_STRING)) {
                    if (currentActiveTransaction != null) {
                        throw new RuntimeException("Found BEGIN when previous transaction hadn't ended yet");
                    }
                    currentActiveTransaction = new ReplayTransaction(logTime, true);
                } else if (sqlStmtIDOrString.equals(COMMIT_STRING) || sqlStmtIDOrString.equals(ROLLBACK_STRING)) {
                    if (currentActiveTransaction == null) {
                        throw new RuntimeException("Found COMMIT or ROLLBACK when there is no active transaction");
                    }
                    currentActiveTransaction.setShouldRollback(sqlStmtIDOrString.equals(ROLLBACK_STRING));
                    replayTransactionQueue.add(currentActiveTransaction);
                    currentActiveTransaction = null;
                } else {
                    int sqlStmtID = Integer.parseInt(sqlStmtIDOrString);
                    if (!sqlStmtCache.containsKey(sqlStmtID)) {
                        // put a placeholder SQLStmt for now. later, when we read the sqlStmtCache section
                        // of the replay file, we'll fill in the strings in the sqlStmtCache
                        sqlStmtCache.put(sqlStmtID, new SQLStmt(""));
                    }
                    SQLStmt sqlStmt = sqlStmtCache.get(sqlStmtID);

                    if (currentActiveTransaction != null) {
                        currentActiveTransaction.addSQLStmtCall(sqlStmt, params, logTime);
                    } else {
                        // if there's isn't a current active transaction, it must be an implicit transaction
                        ReplayTransaction implicitReplayTransaction = new ReplayTransaction(logTime, false);
                        implicitReplayTransaction.addSQLStmtCall(sqlStmt, params, logTime);
                        replayTransactionQueue.add(implicitReplayTransaction);
                    }
                }
            }
            System.out.printf("loadReplayFile: the whole loop took %.4fms\n", (double)(System.nanoTime() - loopOuterStartTime) / 1000000);

            // read SQL statement cache section of file
            while ((fields = replayCSVReader.readNext()) != null) {
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

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

/**
 * @brief ReplayFileManager orchestrates log-to-replay conversion and replay file parsing
 * @author phw2
 */
public class ReplayFileManager {
    // An object representation of a line in the replay file which is fully parsed but not yet processed
    public static class ReplayFileLine {
        public long logTime;
        public String sqlStmtIDOrString;
        public List<Object> params;
        public int endParseOffset;

        public ReplayFileLine(long logTime, String sqlStmtIDOrString, List<Object> params, int endParseOffset) {
            this.logTime = logTime;
            this.sqlStmtIDOrString = sqlStmtIDOrString;
            this.params = params;
            this.endParseOffset = endParseOffset;
        }
    }

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

            // inputStreamReaderScan(this.logFilePath);
            // inputStreamReaderScan(this.replayFilePath);
            // bufferedReaderScan(this.logFilePath);
            // bufferedReaderScan(this.replayFilePath);
            // csvReaderScan(this.logFilePath);
            // csvReaderScan(this.replayFilePath);
            if (doConvert) {
                LogFileParser logFileParser = new PostgresLogFileParser();
                logFileParser.convertLogFileToReplayFile(this.logFilePath, this.replayFilePath);
            }
            loadReplayFile();
            throw new RuntimeException("early exit");
        }
    }

    private void inputStreamReaderScan(String filePath) {
        FileInputStream inputStream;
        try {
            inputStream = new FileInputStream(filePath);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("File " + filePath + " does not exist");
        }

        try (InputStreamReader inputStreamReader = new InputStreamReader(inputStream)) {
            char[] cbuf = new char[4096];
            int n;
            long loopOuterStartTime = System.nanoTime();
            while ((n = inputStreamReader.read(cbuf, 0, 4096)) == 4096) {}
            System.out.printf("readerScan(filePath=%s): the whole loop took %.4fms\n", filePath, (double)(System.nanoTime() - loopOuterStartTime) / 1000000);
        } catch (IOException e) {
            throw new RuntimeException("I/O exception " + e + " when reading log file");
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
            System.out.printf("readerScan(filePath=%s): the whole loop took %.4fms\n", filePath, (double)(System.nanoTime() - loopOuterStartTime) / 1000000);
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

        try (CSVReader csvReader = new CSVReader(new BufferedReader(new InputStreamReader(inputStream)))) {
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
        LOG.info("Loading the replay file " + this.replayFilePath + "...");
        
        FileInputStream replayInputStream;
        CSVReader replayCSVReader; // PAT DEBUG: here temporarily for SQL statement parsing
        try {
            replayInputStream = new FileInputStream(this.replayFilePath);
            replayCSVReader = new CSVReader(new InputStreamReader(replayInputStream));
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Replay file " + replayFilePath + " does not exist");
        }

        try (InputStreamReader replayInputStreamReader = new InputStreamReader(replayInputStream)) {
            this.sqlStmtCache = new HashMap<>();
            this.replayTransactionQueue = new LinkedList<>();
            ReplayTransaction currentActiveTransaction = null;
            char[] cbuf = new char[ReplayFileManager.CBUF_MAX_SIZE];
            int newReadOffset = 0;
            int numReadBytes;

            long loopOuterStartTime = System.nanoTime();
            // Read replay transaction section of file
            // the outer loop fills cbuf. cbuf may contain the start of a line which was not complete in the last cbuf
            while ((numReadBytes = replayInputStreamReader.read(cbuf, newReadOffset, ReplayFileManager.CBUF_MAX_SIZE - newReadOffset)) != -1) {
                assert(newReadOffset >= 0);
                assert(newReadOffset <= ReplayFileManager.CBUF_MAX_SIZE);
                assert(numReadBytes >= 0);
                // the inner loop loops through all completed lines in cbuf. if there is an incomplete line at the end
                // of cbuf, it copies it to the start of cbuf and sets newReadOffset accordingly
                int parseLineStartOffset = 0;
                int cbufSize = newReadOffset + numReadBytes;

                while (parseLineStartOffset < cbufSize) {
                    // try to parse the line starting from parseLineStartOffset
                    ReplayFileLine replayFileLine = parseReplayFileLine(cbuf, parseLineStartOffset, cbufSize);
                    if (replayFileLine == null) {
                        int numUnfinishedChars = cbufSize - parseLineStartOffset;
                        newReadOffset = numUnfinishedChars;
                        System.arraycopy(cbuf, parseLineStartOffset, cbuf, 0, numUnfinishedChars);
                        break;
                    }
                    parseLineStartOffset = replayFileLine.endParseOffset + 1;

                    if (replayFileLine.sqlStmtIDOrString.equals(BEGIN_STRING)) {
                        if (currentActiveTransaction != null) {
                            throw new RuntimeException("Found BEGIN when previous transaction hadn't ended yet");
                        }
                        currentActiveTransaction = new ReplayTransaction(replayFileLine.logTime, true);
                    } else if (replayFileLine.sqlStmtIDOrString.equals(COMMIT_STRING) || replayFileLine.sqlStmtIDOrString.equals(ROLLBACK_STRING)) {
                        if (currentActiveTransaction == null) {
                            throw new RuntimeException("Found COMMIT or ROLLBACK when there is no active transaction");
                        }
                        currentActiveTransaction.setShouldRollback(replayFileLine.sqlStmtIDOrString.equals(ROLLBACK_STRING));
                        replayTransactionQueue.add(currentActiveTransaction);
                        currentActiveTransaction = null;
                    } else {
                        int sqlStmtID = Integer.parseInt(replayFileLine.sqlStmtIDOrString);
                        if (!sqlStmtCache.containsKey(sqlStmtID)) {
                            // put a placeholder SQLStmt for now. later, when we read the sqlStmtCache section
                            // of the replay file, we'll fill in the strings in the sqlStmtCache
                            sqlStmtCache.put(sqlStmtID, new SQLStmt(""));
                        }
                        SQLStmt sqlStmt = sqlStmtCache.get(sqlStmtID);

                        if (currentActiveTransaction != null) {
                            currentActiveTransaction.addSQLStmtCall(sqlStmt, replayFileLine.params, replayFileLine.logTime);
                        } else {
                            // if there's isn't a current active transaction, it must be an implicit transaction
                            ReplayTransaction implicitReplayTransaction = new ReplayTransaction(replayFileLine.logTime, false);
                            implicitReplayTransaction.addSQLStmtCall(sqlStmt, replayFileLine.params, replayFileLine.logTime);
                            replayTransactionQueue.add(implicitReplayTransaction);
                        }
                    }
                }
            }
            System.out.printf("loadReplayFile: the whole loop took %.4fms\n", (double)(System.nanoTime() - loopOuterStartTime) / 1000000);
            System.out.printf("loadReplayFile: the whole loop added %d txns\n", replayTransactionQueue.size());

            // read SQL statement cache section of file
        //     String[] fields;
        //     while ((fields = replayCSVReader.readNext()) != null) {
        //         String sqlStmtIDString = fields[0];
        //         int sqlStmtID = Integer.parseInt(sqlStmtIDString);
        //         String sqlString = fields[1];

        //         if (!sqlStmtCache.containsKey(sqlStmtID)) {
        //             LOG.warn("Read SQL statement of ID " + sqlStmtID + ", except this ID didn't show up in the replay transactions");
        //         } else {
        //             SQLStmt sqlStmt = sqlStmtCache.get(sqlStmtID);
        //             sqlStmt.setSQL(sqlString);
        //         }
        //     }
            
        //     // perform validations
        //     for (Map.Entry<Integer, SQLStmt> entry : sqlStmtCache.entrySet()) {
        //         int sqlStmtID = entry.getKey();
        //         SQLStmt sqlStmt = entry.getValue();

        //         if (sqlStmt.getSQL().equals("")) {
        //             throw new RuntimeException("sqlStmtID " + sqlStmtID + " showed up in the replay transactions but not in the cache");
        //         }
        //     }

        //     hasSuccessfullyLoaded = true;
        // } catch (CsvValidationException e) {
        //     throw new RuntimeException("Replay file not in a valid CSV format");
        } catch (IOException e) {
            throw new RuntimeException("I/O exception " + e + " when reading replay file");
        }
    }

    // TODO: test that it parses the last line correctly (even though the last line may not have a '\n')
    /**
     * @brief Parses the next ReplayFileLine in cbuf if possible
     * @param cbuf A char buffer to parse from
     * @param startOffset The starting offset to parse from
     * @return The parsed ReplayFileLine, or null if the line wasn't complete
     */
    public static ReplayFileLine parseReplayFileLine(char[] cbuf, int startOffset, int cbufSize) {
        if (startOffset >= cbufSize) {
            return null;
        }

        // since a replay line always starts with a long, we know it can never start with the section delim (which will be set
        // to some char that never appears in a long)
        if (cbuf[startOffset] == ReplayFileManager.REPLAY_FILE_SECTION_DELIM) {
            return null;
        }

        int endOffsetsI = 0;
        int[] endOffsets = new int[3];

        for (int cbufI = startOffset; cbufI < cbufSize; cbufI++) {
            if (endOffsetsI < 2 && cbuf[cbufI] == ',') {
                endOffsets[endOffsetsI] = cbufI;
                endOffsetsI++;
            // the final param string may have commas in it, but the first two fields will not
            } else if (endOffsetsI == 2 && cbuf[cbufI] == '\n') {
                endOffsets[endOffsetsI] = cbufI;
                endOffsetsI++;
            }
        }

        if (endOffsetsI != 3) {
            return null;
        }

        long logTime = FastNumericParser.hexCBufToLong(cbuf, startOffset, endOffsets[0]);
        assert(cbuf[endOffsets[0] + 1] == '"' && cbuf[endOffsets[1] - 1] == '"');
        String sqlStmtIDOrString = ReplayFileManager.charBufToString(cbuf, endOffsets[0] + 2, endOffsets[1] - 1);
        assert(cbuf[endOffsets[1] + 1] == '"' && cbuf[endOffsets[2] - 1] == '"');
        String detailString = ReplayFileManager.charBufToString(cbuf, endOffsets[1] + 2, endOffsets[2] - 1);
        List<Object> params = PostgresLogFileParser.parseParamsFromDetail(detailString);
        return new ReplayFileLine(logTime, sqlStmtIDOrString, params, endOffsets[2] + 1);
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

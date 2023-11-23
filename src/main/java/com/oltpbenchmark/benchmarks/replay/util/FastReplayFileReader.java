package com.oltpbenchmark.benchmarks.replay.util;

import java.io.IOException;
import java.io.Reader;
import java.util.List;
import java.lang.AutoCloseable;

/**
 * @brief A class similar in spirit to BufferedReader which exposes an API to read the next line of a replay file
 * @author phw2
 */
public class FastReplayFileReader implements AutoCloseable {
    /**
     * @brief An object representation of a line in the replay file which is fully parsed but not yet processed
     */
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

    private Reader replayReader;
    private int cbufMaxSize;
    private char[] cbuf;
    private int cbufSize;
    private int parseLineStartOffset;
    private int newReadOffset = 0;

    public FastReplayFileReader(Reader reader, int cbufMaxSize) {
        this.replayReader = reader;

        this.cbufMaxSize = cbufMaxSize;
        this.cbuf = new char[this.cbufMaxSize];
        this.cbufSize = 0;
        this.parseLineStartOffset = 0;
        this.newReadOffset = 0;
    }

    /**
     * @brief Read the next line of the replay file
     * @return The read ReplayFileLine, or null if the last line was already read
     */
    public ReplayFileLine readLine() throws IOException {
        ReplayFileLine replayFileLine = parseReplayTxnLine(this.cbuf, this.parseLineStartOffset, this.cbufSize);

        // happy path: return the next replay file line in cbuf
        if (replayFileLine != null) {
            this.parseLineStartOffset = replayFileLine.endParseOffset + 1;
            return replayFileLine;
        }

        // move incomplete chars in cbuf over and load cbuf again
        while (true) {
            // move incomplete chars
            int numUnfinishedChars = this.cbufSize - this.parseLineStartOffset;
            assert(numUnfinishedChars >= 0); // should never be negative
            this.newReadOffset = numUnfinishedChars;
            System.arraycopy(cbuf, parseLineStartOffset, cbuf, 0, numUnfinishedChars);

            // load cbuf
            int numReadBytes = replayReader.read(cbuf, newReadOffset, this.cbufMaxSize - newReadOffset);
            // TODO: handle numReadBytes == 0
            // this means we've read the entire file
            if (numReadBytes == -1) {
                // there shouldn't be any unfinished chars here
                assert(numUnfinishedChars == 0);
                return null;
            }
            assert(newReadOffset >= 0);
            assert(newReadOffset <= this.cbufMaxSize);
            assert(numReadBytes >= 0);
            this.parseLineStartOffset = 0; // reset every time we reload cbuf
            this.cbufSize = newReadOffset + numReadBytes;

            // try to read again, returning if successful
            replayFileLine = parseReplayTxnLine(this.cbuf, this.parseLineStartOffset, this.cbufSize);
            if (replayFileLine != null) {
                this.parseLineStartOffset = replayFileLine.endParseOffset + 1;
                return replayFileLine;
            }
        }
    }

    @Override
    public void close() throws IOException {
        this.replayReader.close();
    }

    /**
     * @brief Parses the next ReplayFileLine in cbuf if possible
     * @param cbuf A char buffer to parse from
     * @param startOffset The starting offset to parse from
     * @return The parsed ReplayFileLine, or null if the line wasn't complete
     */
    public static ReplayFileLine parseReplayTxnLine(char[] cbuf, int startOffset, int cbufSize) {
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
                break; // break early here so we don't end up reading the entire cbuf
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
        return new ReplayFileLine(logTime, sqlStmtIDOrString, params, endOffsets[2]);
    }
}

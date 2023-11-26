package com.oltpbenchmark.benchmarks.replay.util;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import com.oltpbenchmark.benchmarks.replay.util.ReplayFileReader.ReplayFileLine;

import java.lang.AutoCloseable;

/**
 * @brief A class that implements the same API as opencsv.CSVReader but more efficiently and less robustly
 * @author phw2
 */
public class FastCSVReader implements AutoCloseable {
    private Reader reader;
    private int cbufMaxSize;
    private char[] cbuf;
    private int cbufSize;
    private int parseLineStartOffset;
    private int newReadOffset = 0;

    public FastCSVReader(Reader reader, int cbufMaxSize) {
        this.reader = reader;

        this.cbufMaxSize = cbufMaxSize;
        this.cbuf = new char[this.cbufMaxSize];
        this.cbufSize = 0;
        this.parseLineStartOffset = 0;
        this.newReadOffset = 0;
    }

    /**
     * @brief Read the next CSV line of the file
     * @return The next CSV line, or null if the last line was already read
     */
    public List<String> readNext() throws IOException {
        List<String> csvLine = parseCSVLine(this.cbuf, this.parseLineStartOffset, this.cbufSize);

        // happy path: return the next replay file line in cbuf
        if (csvLine != null) {
            this.parseLineStartOffset += getNumChars(csvLine);
            return csvLine;
        }

        // move incomplete chars in cbuf over and load cbuf again
        while (true) {
            // move incomplete chars
            int numUnfinishedChars = this.cbufSize - this.parseLineStartOffset;
            assert(numUnfinishedChars >= 0); // should never be negative
            this.newReadOffset = numUnfinishedChars;
            System.arraycopy(cbuf, parseLineStartOffset, cbuf, 0, numUnfinishedChars);

            // load cbuf
            int numReadBytes = reader.read(cbuf, newReadOffset, this.cbufMaxSize - newReadOffset);
            // TODO: handle numReadBytes == 0
            // this means we've read the entire file
            if (numReadBytes == -1) {
                // there shouldn't be any unfinished chars here
                assert numUnfinishedChars == 0 : "there shouldn't be any unfinished chars";
                return null;
            }
            assert(newReadOffset >= 0);
            assert(newReadOffset <= this.cbufMaxSize);
            assert(numReadBytes >= 0);
            this.parseLineStartOffset = 0; // reset every time we reload cbuf
            this.cbufSize = newReadOffset + numReadBytes;

            // try to read again, returning if successful
            csvLine = parseCSVLine(this.cbuf, this.parseLineStartOffset, this.cbufSize);
            if (csvLine != null) {
                this.parseLineStartOffset += getNumChars(csvLine);
                return csvLine;
            }
        }
    }

    public static ReplayFileLine csvLineToReplayFileLine(List<String> csvLine) {
        long logTime = Long.parseLong(csvLine.get(0));
        String sqlStmtIDOrString = csvLine.get(1);
        Object[] params = ReplayFileReader.replayStringToParams(csvLine.get(2));
        return new ReplayFileLine(logTime, sqlStmtIDOrString, params, 0);
    }

    public static ReplayFileLine fieldsToReplayFileLine(String[] fields) {
        long logTime = Long.parseLong(fields[0]);
        String sqlStmtIDOrString = fields[1];
        Object[] params = ReplayFileReader.replayStringToParams(fields[2]);
        return new ReplayFileLine(logTime, sqlStmtIDOrString, params, 0);
    }

    @Override
    public void close() throws IOException {
        this.reader.close();
    }

    public static List<String> parseCSVLine(char[] cbuf, int startOffset, int cbufSize) {
        if (startOffset >= cbufSize) {
            return null;
        }

        List<String> csvLine = new ArrayList<String>();
        int currStringStartI = startOffset;
        char nextDelimChar = ',';

        for (int i = startOffset; i < cbufSize; i++) {
            char c = cbuf[i];

            if (c == nextDelimChar) {
                int offset = currStringStartI;
                int count = i - currStringStartI;

                if (csvLine.size() > 0) {
                    offset++;
                    count -= 2; // subtract both the quotes
                }

                csvLine.add(String.valueOf(cbuf, offset, count));
                currStringStartI = i + 1;

                if (csvLine.size() == 2) {
                    nextDelimChar = '\n';
                } else if (csvLine.size() == 3) {
                    return csvLine;
                }
            }
        }

        return null;
    }

    public static int getNumChars(List<String> csvLine) {
        int numChars = 0;

        for (String csvField : csvLine) {
            numChars += csvField.length();
        }

        numChars += csvLine.size(); // adds chars for the ',' and '\n's
        numChars += 4; // add chars for the four missing quotes
        return numChars;
    }
}

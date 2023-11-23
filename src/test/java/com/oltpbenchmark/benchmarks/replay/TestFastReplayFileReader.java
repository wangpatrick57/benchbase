package com.oltpbenchmark.benchmarks.replay;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.Reader;
import java.util.Random;

import com.oltpbenchmark.benchmarks.replay.util.FastReplayFileReader;
import com.oltpbenchmark.benchmarks.replay.util.ReplayFileManager;
import com.oltpbenchmark.benchmarks.replay.util.FastReplayFileReader.ReplayFileLine;

public class TestFastReplayFileReader {
    /**
     * @brief A class to mock a reader reading from the contents of a file
     * 
     * All strings in returnStrings must be exact length matches to the calls of read,
     * except the last call to read. This helps ensure that you are indeed testing what
     * you intended to test.
     */
    private class MockReader extends Reader {
        private String[] returnStrings;
        private int returnStringsI;

        public MockReader(String[] returnStrings) {
            this.returnStrings = returnStrings;
            this.returnStringsI = 0;
        }

        @Override
        public int read(char[] cbuf, int off, int len) throws IOException {
            if (this.returnStringsI >= this.returnStrings.length) {
                return -1; // represents EOF
            }
            String returnString = returnStrings[returnStringsI];
            if (returnString.length() > len) {
                throw new IOException("return string longer than len");
            }
            if (returnStringsI != returnStrings.length - 1 && returnString.length() < len) {
                throw new IOException("return string shorter than len and this isn't last return string");
            }

            for (int i = 0; i < returnString.length(); i++) {
                cbuf[off + i] = returnString.charAt(i);
            }
            returnStringsI++;
            return returnString.length();
        }

        @Override
        public void close() throws IOException {}
    }

    /**
     * @brief A class representation of a transaction line string
     * 
     * Convenient for generating strings and comparing them against ReplayFileLines.
     * Also convenient because changing the format of a transaction line string only requires
     * changing this one class, instead of changing all tests one-by-one.
     */
    private static class TxnLineString {
        private static int DECIMAL_MAX = 10;
        private static int SQL_STMT_ID_MAX_LENGTH = 5;
        private static Random random = new Random();

        public static TxnLineString generate() {
            int sqlStmtIDLength = TxnLineString.random.nextInt(SQL_STMT_ID_MAX_LENGTH);
            return generate(sqlStmtIDLength);
        }

        public static TxnLineString generate(int sqlStmtIDLength) {
            long logTime = random.nextLong(Long.MAX_VALUE);
            String logTimeString = String.format("%016x", logTime);

            StringBuilder sqlStmtIDOrStringSB = new StringBuilder();
            while (sqlStmtIDOrStringSB.length() < sqlStmtIDLength) {
                sqlStmtIDOrStringSB.append(Integer.toString(random.nextInt(DECIMAL_MAX)));
            }
            String sqlStmtIDOrString = sqlStmtIDOrStringSB.toString();

            String paramsString = "";
            return new TxnLineString(logTimeString, sqlStmtIDOrString, paramsString);
        }

        private String logTimeString;
        private String sqlStmtIDOrString;
        private String paramsString;

        private TxnLineString(String logTimeString, String sqlStmtIDOrString, String paramsString) {
            this.logTimeString = logTimeString;
            this.sqlStmtIDOrString = sqlStmtIDOrString;
            this.paramsString = paramsString;
        }

        public String toString(boolean includeNewline) {
            String newlineString = includeNewline ? "\n" : "";
            return String.format("%s,\"%s\",\"%s\"%s", this.logTimeString, this.sqlStmtIDOrString, this.paramsString, newlineString);
        }

        public boolean equalsReplayFileLine(ReplayFileLine replayFileLine) {
            boolean logTimeEquals = replayFileLine.logTime == Long.decode("0x" + this.logTimeString);
            boolean sqlStmtIDOrStringEquals = replayFileLine.sqlStmtIDOrString.equals(this.sqlStmtIDOrString);
            return logTimeEquals && sqlStmtIDOrStringEquals;
        }
    }

    // TODO: check other fields in *Content tests
    /**
     * @section Testing parseReplayTxnLine
     */
    @Test
    public void testParseReplayTxnLineZeroStartContent() {
        TxnLineString txnLineString = TxnLineString.generate();
        String txnString = txnLineString.toString(true);
        char[] cbuf = txnString.toCharArray();
        ReplayFileLine replayFileLine = FastReplayFileReader.parseReplayTxnLine(cbuf, 0, txnString.length());
        assertTrue(txnLineString.equalsReplayFileLine(replayFileLine));
    }

    @Test
    public void testParseReplayTxnLineNonZeroStartContent() {
        TxnLineString txnLineString = TxnLineString.generate();
        String txnString = txnLineString.toString(true);
        String prefix = "hihi\n";
        String cbufString = String.format("%s%s", prefix, txnString);
        char[] cbuf = cbufString.toCharArray();
        ReplayFileLine replayFileLine = FastReplayFileReader.parseReplayTxnLine(cbuf, prefix.length(), cbufString.length());
        assertTrue(txnLineString.equalsReplayFileLine(replayFileLine));
    }

    @Test
    public void testParseReplayTxnLineExtraCharsAtEndContent() {
        String logTimeString = "179a0eb4b129eac0";
        String cbufString = String.format("%s,\"0\",\"\"\nfoo", logTimeString);
        char[] cbuf = cbufString.toCharArray();
        ReplayFileLine replayFileLine = FastReplayFileReader.parseReplayTxnLine(cbuf, 0, cbufString.length());
        assertEquals(replayFileLine.logTime, Long.decode("0x" + logTimeString).longValue());
        assertTrue(replayFileLine.sqlStmtIDOrString.equals("0"));
    }
    
    @Test
    public void testParseReplayTxnLineZeroStartEndParseOffset() {
        String cbufString = "179a0eb4af231e40,\"\",\"\"\n";
        char[] cbuf = cbufString.toCharArray();
        ReplayFileLine replayFileLine = FastReplayFileReader.parseReplayTxnLine(cbuf, 0, cbufString.length());
        assertEquals(replayFileLine.endParseOffset, cbufString.length() - 1);
    }
    
    @Test
    public void testParseReplayTxnLineNonZeroStartEndParseOffset() {
        String cbufString = "hello there\n179a0eb4af231e40,\"\",\"\"\n";
        char[] cbuf = cbufString.toCharArray();
        ReplayFileLine replayFileLine = FastReplayFileReader.parseReplayTxnLine(cbuf, 12, cbufString.length());
        assertEquals(replayFileLine.endParseOffset, cbufString.length() - 1);
    }
    
    @Test
    public void testParseReplayTxnLineExtraCharsAtEndEndParseOffset() {
        String extraString = "bar\n";
        String cbufString = String.format("179a0eb4af231e40,\"\",\"\"\n%s", extraString);
        char[] cbuf = cbufString.toCharArray();
        ReplayFileLine replayFileLine = FastReplayFileReader.parseReplayTxnLine(cbuf, 12, cbufString.length());
        assertEquals(replayFileLine.endParseOffset, cbufString.length() - 1 - extraString.length());
        assertEquals(cbuf[replayFileLine.endParseOffset], '\n');
    }

    @Test
    public void testParseReplayTxnLineAlmostCompleteJustMissingNewline() {
        String cbufString = "179a0eb4aef55780,\"12\",\"\"";
        char[] cbuf = cbufString.toCharArray();
        ReplayFileLine replayFileLine = FastReplayFileReader.parseReplayTxnLine(cbuf, 0, cbufString.length());
        assertEquals(replayFileLine, null);
    }

    @Test
    public void testParseReplayTxnLineStartEqualToSize() {
        String cbufString = "179a0eb4aef55780,\"12\",\"\"\n";
        char[] cbuf = cbufString.toCharArray();
        ReplayFileLine replayFileLine = FastReplayFileReader.parseReplayTxnLine(cbuf, cbufString.length(), cbufString.length());
        assertEquals(replayFileLine, null);
    }

    @Test
    public void testParseReplayTxnLineStartPastSize() {
        String cbufString = "179a0eb4aef55780,\"12\",\"\"\n";
        char[] cbuf = cbufString.toCharArray();
        ReplayFileLine replayFileLine = FastReplayFileReader.parseReplayTxnLine(cbuf, cbufString.length() + 5, cbufString.length());
        assertEquals(replayFileLine, null);
    }

    @Test
    public void testParseReplayTxnLineAtDelim() {
        String cbufString = String.format("%c", ReplayFileManager.REPLAY_FILE_SECTION_DELIM);
        char[] cbuf = cbufString.toCharArray();
        ReplayFileLine replayFileLine = FastReplayFileReader.parseReplayTxnLine(cbuf, 0, cbufString.length());
        assertEquals(replayFileLine, null);
    }

    /**
     * @section Testing readLine
     */
    @Test
    public void testReadLineOneCompleteTxnLine() throws IOException {
        String logTimeString = "179a0eb4af231e40";
        String txnString = String.format("%s,\"15\",\"\"\n", logTimeString);
        String[] returnStrings = {txnString};
        MockReader mockReader = new MockReader(returnStrings);
        FastReplayFileReader replayFileReader = new FastReplayFileReader(mockReader, txnString.length());

        // check content of first line
        ReplayFileLine replayFileLine = replayFileReader.readLine();
        assertEquals(replayFileLine.logTime, Long.decode("0x" + logTimeString).longValue());
        assertTrue(replayFileLine.sqlStmtIDOrString.equals("15"));

        // next line should be null
        replayFileLine = replayFileReader.readLine();
        assertEquals(replayFileLine, null);

        replayFileReader.close();
    }

    /**
     * @section Testing readLine
     */
    @Test
    public void testReadLineTwoCompleteTxnLinesInDifferentBlocks() throws IOException {
        String[] logTimeStrings = {"179a1ad83806a040", "179a1ad83815e280"};
        String[] txnStrings = {String.format("%s,\"4\",\"\"\n", logTimeStrings[0]), String.format("%s,\"8\",\"\"\n", logTimeStrings[1])};
        assertEquals(txnStrings[0].length(), txnStrings[1].length());
        MockReader mockReader = new MockReader(txnStrings);
        FastReplayFileReader replayFileReader = new FastReplayFileReader(mockReader, txnStrings[0].length());

        // check content of first line
        ReplayFileLine replayFileLine = replayFileReader.readLine();
        assertEquals(replayFileLine.logTime, Long.decode("0x" + logTimeStrings[0]).longValue());
        assertTrue(replayFileLine.sqlStmtIDOrString.equals("4"));

        // check content of second line
        replayFileLine = replayFileReader.readLine();
        assertEquals(replayFileLine.logTime, Long.decode("0x" + logTimeStrings[1]).longValue());
        assertTrue(replayFileLine.sqlStmtIDOrString.equals("8"));

        // next line should be null
        replayFileLine = replayFileReader.readLine();
        assertEquals(replayFileLine, null);

        replayFileReader.close();
    }

    /**
     * @section Testing readLine
     */
    @Test
    public void testReadLineWithTxnLineSpanningTwoBlocks() throws IOException {
        String[] logTimeStrings = {"179a0eb47a631700", "179a0eb483cdfa80"};
        String[] txnStrings = {String.format("%s,\"12\",\"\"\n", logTimeStrings[0]), String.format("%s,\"24\",\"\"\n", logTimeStrings[1])};
        assertEquals(txnStrings[0].length(), txnStrings[1].length());
        int numExtraChars = 3;
        String[] returnStrings = {txnStrings[0] + txnStrings[1].substring(0, numExtraChars), txnStrings[1].substring(numExtraChars)};
        MockReader mockReader = new MockReader(returnStrings);
        FastReplayFileReader replayFileReader = new FastReplayFileReader(mockReader, txnStrings[0].length() + numExtraChars);

        // check content of first line
        ReplayFileLine replayFileLine = replayFileReader.readLine();
        assertEquals(replayFileLine.logTime, Long.decode("0x" + logTimeStrings[0]).longValue());
        assertTrue(replayFileLine.sqlStmtIDOrString.equals("12"));

        // check content of second line
        replayFileLine = replayFileReader.readLine();
        assertEquals(replayFileLine.logTime, Long.decode("0x" + logTimeStrings[1]).longValue());
        assertTrue(replayFileLine.sqlStmtIDOrString.equals("24"));

        // next line should be null
        replayFileLine = replayFileReader.readLine();
        assertEquals(replayFileLine, null);

        replayFileReader.close();
    }
    // TODO: test that it parses the last line correctly (even though the last line may not have a '\n')
}

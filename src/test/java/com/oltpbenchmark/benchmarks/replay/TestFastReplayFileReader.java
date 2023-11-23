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
     * @brief An object representation of a transaction line string
     * 
     * Convenient for generating strings and comparing them against ReplayFileLines.
     * Also convenient because changing the format of a transaction line string only requires
     * changing this one class, instead of changing all tests one-by-one.
     */
    private static class TxnStringObj {
        private static int DECIMAL_MAX = 10;
        private static int SQL_STMT_ID_MAX_LENGTH = 5;
        private static Random random = new Random();

        public static TxnStringObj generate() {
            int sqlStmtIDLength = TxnStringObj.random.nextInt(SQL_STMT_ID_MAX_LENGTH);
            return generate(sqlStmtIDLength);
        }

        public static TxnStringObj generate(int sqlStmtIDLength) {
            long logTime = random.nextLong(Long.MAX_VALUE);
            String logTimeString = String.format("%016x", logTime);

            StringBuilder sqlStmtIDOrStringSB = new StringBuilder();
            while (sqlStmtIDOrStringSB.length() < sqlStmtIDLength) {
                sqlStmtIDOrStringSB.append(Integer.toString(random.nextInt(DECIMAL_MAX)));
            }
            String sqlStmtIDOrString = sqlStmtIDOrStringSB.toString();

            String paramsString = "";
            return new TxnStringObj(logTimeString, sqlStmtIDOrString, paramsString);
        }

        private String logTimeString;
        private String sqlStmtIDOrString;
        private String paramsString;

        private TxnStringObj(String logTimeString, String sqlStmtIDOrString, String paramsString) {
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
        TxnStringObj txnStringObj = TxnStringObj.generate();
        String txnString = txnStringObj.toString(true);
        char[] cbuf = txnString.toCharArray();
        ReplayFileLine replayFileLine = FastReplayFileReader.parseReplayTxnLine(cbuf, 0, txnString.length());
        assertTrue(txnStringObj.equalsReplayFileLine(replayFileLine));
    }

    @Test
    public void testParseReplayTxnLineNonZeroStartContent() {
        TxnStringObj txnStringObj = TxnStringObj.generate();
        String txnString = txnStringObj.toString(true);
        String extraChars = "hihi\n";
        String cbufString = String.format("%s%s", extraChars, txnString);
        char[] cbuf = cbufString.toCharArray();
        ReplayFileLine replayFileLine = FastReplayFileReader.parseReplayTxnLine(cbuf, extraChars.length(), cbufString.length());
        assertTrue(txnStringObj.equalsReplayFileLine(replayFileLine));
    }

    @Test
    public void testParseReplayTxnLineExtraCharsAtEndContent() {
        TxnStringObj txnStringObj = TxnStringObj.generate();
        String txnString = txnStringObj.toString(true);
        String extraChars = "foo";
        String cbufString = String.format("%s%s", txnString, extraChars);
        char[] cbuf = cbufString.toCharArray();
        ReplayFileLine replayFileLine = FastReplayFileReader.parseReplayTxnLine(cbuf, 0, cbufString.length());
        assertTrue(txnStringObj.equalsReplayFileLine(replayFileLine));
    }
    
    @Test
    public void testParseReplayTxnLineZeroStartEndParseOffset() {
        TxnStringObj txnStringObj = TxnStringObj.generate();
        String txnString = txnStringObj.toString(true);
        String cbufString = txnString;
        char[] cbuf = cbufString.toCharArray();
        ReplayFileLine replayFileLine = FastReplayFileReader.parseReplayTxnLine(cbuf, 0, cbufString.length());
        assertEquals(replayFileLine.endParseOffset, cbufString.length() - 1);
    }
    
    @Test
    public void testParseReplayTxnLineNonZeroStartEndParseOffset() {
        TxnStringObj txnStringObj = TxnStringObj.generate();
        String txnString = txnStringObj.toString(true);
        String extraChars = "hello there";
        String cbufString = String.format("%s%s", extraChars, txnString);
        char[] cbuf = cbufString.toCharArray();
        ReplayFileLine replayFileLine = FastReplayFileReader.parseReplayTxnLine(cbuf, extraChars.length(), cbufString.length());
        assertEquals(replayFileLine.endParseOffset, cbufString.length() - 1);
    }
    
    @Test
    public void testParseReplayTxnLineExtraCharsAtEndEndParseOffset() {
        TxnStringObj txnStringObj = TxnStringObj.generate();
        String txnString = txnStringObj.toString(true);
        String extraChars = "goodbye now";
        String cbufString = String.format("%s%s", txnString, extraChars);
        char[] cbuf = cbufString.toCharArray();
        ReplayFileLine replayFileLine = FastReplayFileReader.parseReplayTxnLine(cbuf, 0, cbufString.length());
        assertEquals(replayFileLine.endParseOffset, cbufString.length() - 1 - extraChars.length());
    }

    @Test
    public void testParseReplayTxnLineAlmostCompleteJustMissingNewline() {
        TxnStringObj txnStringObj = TxnStringObj.generate();
        String txnString = txnStringObj.toString(false);
        String cbufString = txnString;
        char[] cbuf = cbufString.toCharArray();
        ReplayFileLine replayFileLine = FastReplayFileReader.parseReplayTxnLine(cbuf, 0, cbufString.length());
        assertEquals(replayFileLine, null);
    }

    @Test
    public void testParseReplayTxnLineStartEqualToSize() {
        TxnStringObj txnStringObj = TxnStringObj.generate();
        String txnString = txnStringObj.toString(true);
        String cbufString = txnString;
        char[] cbuf = cbufString.toCharArray();
        ReplayFileLine replayFileLine = FastReplayFileReader.parseReplayTxnLine(cbuf, cbufString.length(), cbufString.length());
        assertEquals(replayFileLine, null);
    }

    @Test
    public void testParseReplayTxnLineStartPastSize() {
        TxnStringObj txnStringObj = TxnStringObj.generate();
        String txnString = txnStringObj.toString(true);
        String cbufString = txnString;
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
        TxnStringObj txnStringObj = TxnStringObj.generate();
        String txnString = txnStringObj.toString(true);
        String[] returnStrings = {txnString};
        MockReader mockReader = new MockReader(returnStrings);
        FastReplayFileReader replayFileReader = new FastReplayFileReader(mockReader, txnString.length());

        // check content of first line
        ReplayFileLine replayFileLine = replayFileReader.readLine();
        assertTrue(txnStringObj.equalsReplayFileLine(replayFileLine));

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
        int sqlStmtIDLength = 2; // must use this to ensure they're equal in length
        TxnStringObj[] txnStringObjs = {TxnStringObj.generate(sqlStmtIDLength), TxnStringObj.generate(sqlStmtIDLength)};
        String[] txnStrings = {txnStringObjs[0].toString(true), txnStringObjs[1].toString(true)};
        assertEquals(txnStrings[0].length(), txnStrings[1].length());
        MockReader mockReader = new MockReader(txnStrings);
        FastReplayFileReader replayFileReader = new FastReplayFileReader(mockReader, txnStrings[0].length());

        // check content of first line
        ReplayFileLine replayFileLine = replayFileReader.readLine();
        assertTrue(txnStringObjs[0].equalsReplayFileLine(replayFileLine));

        // check content of second line
        replayFileLine = replayFileReader.readLine();
        assertTrue(txnStringObjs[1].equalsReplayFileLine(replayFileLine));

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
        int sqlStmtIDLength = 2; // must use this to ensure they're equal in length
        TxnStringObj[] txnStringObjs = {TxnStringObj.generate(sqlStmtIDLength), TxnStringObj.generate(sqlStmtIDLength)};
        String[] txnStrings = {txnStringObjs[0].toString(true), txnStringObjs[1].toString(true)};
        assertEquals(txnStrings[0].length(), txnStrings[1].length());
        int numShiftChars = 3;
        String[] returnStrings = {txnStrings[0] + txnStrings[1].substring(0, numShiftChars), txnStrings[1].substring(numShiftChars)};
        MockReader mockReader = new MockReader(returnStrings);
        FastReplayFileReader replayFileReader = new FastReplayFileReader(mockReader, txnStrings[0].length() + numShiftChars);

        // check content of first line
        ReplayFileLine replayFileLine = replayFileReader.readLine();
        assertTrue(txnStringObjs[0].equalsReplayFileLine(replayFileLine));

        // check content of second line
        replayFileLine = replayFileReader.readLine();
        assertTrue(txnStringObjs[1].equalsReplayFileLine(replayFileLine));

        // next line should be null
        replayFileLine = replayFileReader.readLine();
        assertEquals(replayFileLine, null);

        replayFileReader.close();
    }
    // TODO: test that it parses the last line correctly (even though the last line may not have a '\n')
}

package com.oltpbenchmark.benchmarks.replay;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.Reader;

import com.oltpbenchmark.benchmarks.replay.util.FastReplayFileReader;
import com.oltpbenchmark.benchmarks.replay.util.ReplayFileManager;
import com.oltpbenchmark.benchmarks.replay.util.FastReplayFileReader.ReplayFileLine;

public class TestFastReplayFileReader {
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

    // TODO: check other fields in *Content tests
    /**
     * @section Testing parseReplayTxnLine
     */
    @Test
    public void testParseReplayTxnLineZeroStartContent() {
        String logTimeString = "179a0eb4aef55780";
        String cbufString = String.format("%s,\"12\",\"\"\n", logTimeString);
        char[] cbuf = cbufString.toCharArray();
        ReplayFileLine replayFileLine = FastReplayFileReader.parseReplayTxnLine(cbuf, 0, cbufString.length());
        assertEquals(replayFileLine.logTime, Long.decode("0x" + logTimeString).longValue());
        assertTrue(replayFileLine.sqlStmtIDOrString.equals("12"));
    }

    @Test
    public void testParseReplayTxnLineNonZeroStartContent() {
        String logTimeString = "179a0eb4af231e40";
        String cbufString = String.format("hihi\n%s,\"15\",\"\"\n", logTimeString);
        char[] cbuf = cbufString.toCharArray();
        ReplayFileLine replayFileLine = FastReplayFileReader.parseReplayTxnLine(cbuf, 5, cbufString.length());
        assertEquals(replayFileLine.logTime, Long.decode("0x" + logTimeString).longValue());
        assertTrue(replayFileLine.sqlStmtIDOrString.equals("15"));
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
    public void testReadLineOneTxnLine() throws IOException {
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
    // TODO: test that it parses the last line correctly (even though the last line may not have a '\n')
}

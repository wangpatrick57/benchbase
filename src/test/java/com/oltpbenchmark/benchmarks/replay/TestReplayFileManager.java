package com.oltpbenchmark.benchmarks.replay;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.oltpbenchmark.benchmarks.replay.util.ReplayFileManager;
import com.oltpbenchmark.benchmarks.replay.util.FastReplayFileReader.ReplayFileLine;

public class TestReplayFileManager {
    // TODO: check other fields in *Content tests
    @Test
    public void testParseReplayFileLineZeroStartContent() {
        String logTimeString = "179a0eb4aef55780";
        String cbufString = String.format("%s,\"12\",\"\"\n", logTimeString);
        char[] cbuf = cbufString.toCharArray();
        ReplayFileLine replayFileLine = ReplayFileManager.parseReplayFileLine(cbuf, 0, cbufString.length());
        assertEquals(replayFileLine.logTime, Long.decode("0x" + logTimeString).longValue());
        assertTrue(replayFileLine.sqlStmtIDOrString.equals("12"));
    }

    @Test
    public void testParseReplayFileLineNonZeroStartContent() {
        String logTimeString = "179a0eb4af231e40";
        String cbufString = String.format("hihi\n%s,\"15\",\"\"\n", logTimeString);
        char[] cbuf = cbufString.toCharArray();
        ReplayFileLine replayFileLine = ReplayFileManager.parseReplayFileLine(cbuf, 5, cbufString.length());
        assertEquals(replayFileLine.logTime, Long.decode("0x" + logTimeString).longValue());
        assertTrue(replayFileLine.sqlStmtIDOrString.equals("15"));
    }

    @Test
    public void testParseReplayFileLineExtraCharsAtEndContent() {
        String logTimeString = "179a0eb4b129eac0";
        String cbufString = String.format("%s,\"0\",\"\"\nfoo", logTimeString);
        char[] cbuf = cbufString.toCharArray();
        ReplayFileLine replayFileLine = ReplayFileManager.parseReplayFileLine(cbuf, 0, cbufString.length());
        assertEquals(replayFileLine.logTime, Long.decode("0x" + logTimeString).longValue());
        assertTrue(replayFileLine.sqlStmtIDOrString.equals("0"));
    }
    
    @Test
    public void testParseReplayFileLineZeroStartEndParseOffset() {
        String cbufString = "179a0eb4af231e40,\"\",\"\"\n";
        char[] cbuf = cbufString.toCharArray();
        ReplayFileLine replayFileLine = ReplayFileManager.parseReplayFileLine(cbuf, 0, cbufString.length());
        assertEquals(replayFileLine.endParseOffset, cbufString.length());
    }
    
    @Test
    public void testParseReplayFileLineNonZeroStartEndParseOffset() {
        String cbufString = "hello there\n179a0eb4af231e40,\"\",\"\"\n";
        char[] cbuf = cbufString.toCharArray();
        ReplayFileLine replayFileLine = ReplayFileManager.parseReplayFileLine(cbuf, 12, cbufString.length());
        assertEquals(replayFileLine.endParseOffset, cbufString.length());
    }
    
    @Test
    public void testParseReplayFileLineExtraCharsAtEndEndParseOffset() {
        String cbufString = "179a0eb4af231e40,\"\",\"\"\nbar\n";
        char[] cbuf = cbufString.toCharArray();
        ReplayFileLine replayFileLine = ReplayFileManager.parseReplayFileLine(cbuf, 12, cbufString.length());
        assertEquals(replayFileLine.endParseOffset, cbufString.length() - 4);
        assertEquals(cbuf[replayFileLine.endParseOffset], 'b');
    }

    @Test
    public void testParseReplayFileLineAlmostCompleteJustMissingNewline() {
        String cbufString = "179a0eb4aef55780,\"12\",\"\"";
        char[] cbuf = cbufString.toCharArray();
        ReplayFileLine replayFileLine = ReplayFileManager.parseReplayFileLine(cbuf, 0, cbufString.length());
        assertEquals(replayFileLine, null);
    }

    @Test
    public void testParseReplayFileLineStartEqualToSize() {
        String cbufString = "179a0eb4aef55780,\"12\",\"\"\n";
        char[] cbuf = cbufString.toCharArray();
        ReplayFileLine replayFileLine = ReplayFileManager.parseReplayFileLine(cbuf, cbufString.length(), cbufString.length());
        assertEquals(replayFileLine, null);
    }

    @Test
    public void testParseReplayFileLineStartPastSize() {
        String cbufString = "179a0eb4aef55780,\"12\",\"\"\n";
        char[] cbuf = cbufString.toCharArray();
        ReplayFileLine replayFileLine = ReplayFileManager.parseReplayFileLine(cbuf, cbufString.length() + 5, cbufString.length());
        assertEquals(replayFileLine, null);
    }

    @Test
    public void testParseReplayFileLineAtDelim() {
        String cbufString = String.format("%c", ReplayFileManager.REPLAY_FILE_SECTION_DELIM);
        char[] cbuf = cbufString.toCharArray();
        ReplayFileLine replayFileLine = ReplayFileManager.parseReplayFileLine(cbuf, 0, cbufString.length());
        assertEquals(replayFileLine, null);
    }
}

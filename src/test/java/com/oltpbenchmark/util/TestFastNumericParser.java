package com.oltpbenchmark.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.oltpbenchmark.benchmarks.replay.util.FastNumericParser;

public class TestFastNumericParser {
    @Test
    public void testHexCharToInt() throws Exception {
        assertEquals(FastNumericParser.hexCharToInt('0'), 0);
        assertEquals(FastNumericParser.hexCharToInt('2'), 2);
        assertEquals(FastNumericParser.hexCharToInt('9'), 9);
        assertEquals(FastNumericParser.hexCharToInt('a'), 10);
        assertEquals(FastNumericParser.hexCharToInt('b'), 11);
        assertEquals(FastNumericParser.hexCharToInt('f'), 15);
        assertEquals(FastNumericParser.hexCharToInt('A'), 10);
        assertEquals(FastNumericParser.hexCharToInt('D'), 13);
        assertEquals(FastNumericParser.hexCharToInt('F'), 15);
    }

    @Test
    public void testHexCBufToLong() throws Exception {
        String[] hexStrings = {"1a3f201", "0", "aBcDeF", "0001f"};

        for (String hexString : hexStrings) {
            char[] cbuf = hexString.toCharArray();
            assertEquals(FastNumericParser.hexCBufToLong(cbuf, 0, hexString.length()), Long.decode("0x" + hexString).longValue());
        }
    }
}

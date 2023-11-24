package com.oltpbenchmark.benchmarks.replay.util;

import java.util.Random;

public class PrivateBench {
    public static void run() {
        Random random = new Random();

        // hex longs
        int HEX_LONG_MAX_SIZE = 16;
        int HEX_LONG_BUF_SIZE = 1048576;
        char[] hexLongCBuf = new char[HEX_LONG_BUF_SIZE];
        long logTime = random.nextLong(Long.MAX_VALUE);
        for (int i = 0; i < HEX_LONG_BUF_SIZE; i += HEX_LONG_MAX_SIZE) {
            String logTimeString = String.format("%016x", logTime);
            for (int off = 0; off < HEX_LONG_MAX_SIZE; off++) {
                hexLongCBuf[i + off] = logTimeString.charAt(off);
            }
        }

        // hexCBufToLong
        long startTime = System.nanoTime();
        for (int i = 0; i < HEX_LONG_BUF_SIZE; i += HEX_LONG_MAX_SIZE) {
            FastNumericParser.hexCBufToLong(hexLongCBuf, i, i + HEX_LONG_MAX_SIZE);
        }
        System.out.printf("FastNumericParser.hexCBufToLong took %.4fms to parse %d hex longs\n", (double)(System.nanoTime() - startTime) / 1000000, HEX_LONG_BUF_SIZE / HEX_LONG_MAX_SIZE);

        // parse long for hex longs
        startTime = System.nanoTime();
        for (int i = 0; i < HEX_LONG_BUF_SIZE; i += HEX_LONG_MAX_SIZE) {
            String string = new String(hexLongCBuf, i, HEX_LONG_MAX_SIZE);
            Long.parseLong(string, 16);
        }
        System.out.printf("Long.parseLong took %.4fms to parse %d hex longs\n", (double)(System.nanoTime() - startTime) / 1000000, HEX_LONG_BUF_SIZE / HEX_LONG_MAX_SIZE);

        // decimal longs
        int DEC_LONG_MAX_SIZE = 19;
        int DEC_LONG_BUF_SIZE = 1245184;
        char[] decLongCBuf = new char[DEC_LONG_BUF_SIZE];
        logTime = random.nextLong(Long.MAX_VALUE);
        for (int i = 0; i < DEC_LONG_BUF_SIZE; i += DEC_LONG_MAX_SIZE) {
            String logTimeString = String.format("%019d", logTime);
            for (int off = 0; off < DEC_LONG_MAX_SIZE; off++) {
                decLongCBuf[i + off] = logTimeString.charAt(off);
            }
        }

        // parse long for decimal longs
        startTime = System.nanoTime();
        for (int i = 0; i < DEC_LONG_BUF_SIZE; i += DEC_LONG_MAX_SIZE) {
            String string = new String(decLongCBuf, i, DEC_LONG_MAX_SIZE);
            Long.parseLong(string);
        }
        System.out.printf("Long.parseLong took %.4fms to parse %d decimal longs\n", (double)(System.nanoTime() - startTime) / 1000000, DEC_LONG_BUF_SIZE / DEC_LONG_MAX_SIZE);

        throw new RuntimeException("private bench done");
    }
}

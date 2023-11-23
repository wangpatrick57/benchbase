package com.oltpbenchmark.benchmarks.replay.util;

import java.util.Random;

public class PrivateBench {
    public static void run() {
        int BUF_SIZE = 65536;
        int LONG_SIZE = 16;
        char[] cbuf = new char[BUF_SIZE];

        Random random = new Random();
        long logTime = random.nextLong(Long.MAX_VALUE);
        for (int i = 0; i < BUF_SIZE; i += LONG_SIZE) {
            String logTimeString = String.format("%016x", logTime);
            for (int off = 0; off < LONG_SIZE; off++) {
                cbuf[i + off] = logTimeString.charAt(off);
            }
        }

        long startTime = System.nanoTime();
        for (int i = 0; i < BUF_SIZE; i += LONG_SIZE) {
            FastNumericParser.hexCBufToLong(cbuf, i, i + LONG_SIZE);
        }
        System.out.printf("FastNumericParser.hexCBufToLong took %.4fms to parse %d longs\n", (double)(System.nanoTime() - startTime) / 1000000, BUF_SIZE / LONG_SIZE);

        startTime = System.nanoTime();
        for (int i = 0; i < BUF_SIZE; i += LONG_SIZE) {
            String string = new String(cbuf, i, LONG_SIZE);
            Long.parseLong(string, 16);
        }
        System.out.printf("Long.parseLong took %.4fms to parse %d longs\n", (double)(System.nanoTime() - startTime) / 1000000, BUF_SIZE / LONG_SIZE);

        throw new RuntimeException("private bench done");
    }
}

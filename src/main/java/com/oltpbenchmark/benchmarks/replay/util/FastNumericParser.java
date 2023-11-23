package com.oltpbenchmark.benchmarks.replay.util;

/**
 * @brief Fast parsing functions for numbers
 * 
 * Designed specifically for parsing timestamps in replay files and thus sacrifices
 * many features for efficiency, such as:
 *   1. Parsing negative numbers
 *   2. Handling bad inputs
 *   3. Handling long overflows
 *   4. Handling capital letters in addition to lowercase ones
 * 
 * @detail Hex was chosen because * 16 is the same as << 4, which is efficient
 * @author phw2
 */
public class FastNumericParser {
    private static final int HEX_BASE = 16;

    public static long hexCBufToLong(char[] cbuf, int start, int end) {
        long out = 0;

        for (int i = start; i < end; i++) {
            out *= FastNumericParser.HEX_BASE;
            out += FastNumericParser.hexCharToInt(cbuf[i]);
        }

        return out;
    }

    public static int hexCharToInt(char c) {
        if (c >= '0' && c <= '9') {
            return c - 48;
        } else if (c >= 'a' && c <= 'f') {
            return c - 97 + 10;
        } else if (c >= 'A' && c <= 'F') {
            return c - 65 + 10;
        } else {
            throw new RuntimeException(String.format("Invalid char (%c) passed to parseIntFromHexChar", c));
        }
    }
}

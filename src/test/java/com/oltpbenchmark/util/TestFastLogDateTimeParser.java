package com.oltpbenchmark.util;

import static org.junit.Assert.assertEquals;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

import org.junit.Test;

import com.oltpbenchmark.benchmarks.replay.util.FastLogDateTimeParser;

/**
 * @author phw2
 */
public class TestFastLogDateTimeParser {
    private static DateTimeFormatter GROUND_TRUTH_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS z", Locale.US);

    @Test
    public void testSingleParse() throws Exception {
        FastLogDateTimeParser fastDTParser = new FastLogDateTimeParser();
        String dtString = "2023-11-12 13:06:58.721 EST";
        assertEquals(fastDTParser.dtStringToNanoTime(dtString), groundTruthDTStringToNanoTime(dtString));
    }

    @Test
    public void testTwoParsesDifferentSecondSameEverythingElse() throws Exception {
        FastLogDateTimeParser fastDTParser = new FastLogDateTimeParser();
        String dtString = "2023-11-12 13:06:58.721 EST";
        assertEquals(fastDTParser.dtStringToNanoTime(dtString), groundTruthDTStringToNanoTime(dtString));
        dtString = "2023-11-12 13:06:59.154 EST";
        assertEquals(fastDTParser.dtStringToNanoTime(dtString), groundTruthDTStringToNanoTime(dtString));
    }

    @Test
    public void testTwoParsesDifferentMinuteSameEverythingElse() throws Exception {
        FastLogDateTimeParser fastDTParser = new FastLogDateTimeParser();
        String dtString = "2023-11-12 13:06:58.721 EST";
        assertEquals(fastDTParser.dtStringToNanoTime(dtString), groundTruthDTStringToNanoTime(dtString));
        dtString = "2023-11-12 13:07:58.721 EST";
        assertEquals(fastDTParser.dtStringToNanoTime(dtString), groundTruthDTStringToNanoTime(dtString));
    }

    @Test
    public void testTwoParsesDifferentHourSameEverythingElse() throws Exception {
        FastLogDateTimeParser fastDTParser = new FastLogDateTimeParser();
        String dtString = "2023-11-12 13:06:58.721 EST";
        assertEquals(fastDTParser.dtStringToNanoTime(dtString), groundTruthDTStringToNanoTime(dtString));
        dtString = "2023-11-12 15:06:58.721 EST";
        assertEquals(fastDTParser.dtStringToNanoTime(dtString), groundTruthDTStringToNanoTime(dtString));
    }

    @Test
    public void testTwoParsesDifferentDateSameEverythingElse() throws Exception {
        FastLogDateTimeParser fastDTParser = new FastLogDateTimeParser();
        String dtString = "2023-11-12 13:06:58.721 EST";
        assertEquals(fastDTParser.dtStringToNanoTime(dtString), groundTruthDTStringToNanoTime(dtString));
        dtString = "2024-05-03 13:06:58.721 EST";
        assertEquals(fastDTParser.dtStringToNanoTime(dtString), groundTruthDTStringToNanoTime(dtString));
    }

    @Test
    public void testTwoParsesDifferentTimezoneSameEverythingElse() throws Exception {
        FastLogDateTimeParser fastDTParser = new FastLogDateTimeParser();
        String dtString = "2023-11-12 13:06:58.721 EST";
        assertEquals(fastDTParser.dtStringToNanoTime(dtString), groundTruthDTStringToNanoTime(dtString));
        dtString = "2023-11-12 13:06:58.721 EDT";
        assertEquals(fastDTParser.dtStringToNanoTime(dtString), groundTruthDTStringToNanoTime(dtString));
    }

    @Test
    public void testTwoParsesDifferentEverything() throws Exception {
        FastLogDateTimeParser fastDTParser = new FastLogDateTimeParser();
        String dtString = "2023-11-12 13:06:58.721 EST";
        assertEquals(fastDTParser.dtStringToNanoTime(dtString), groundTruthDTStringToNanoTime(dtString));
        dtString = "2025-06-06 05:12:08.151 PST";
        assertEquals(fastDTParser.dtStringToNanoTime(dtString), groundTruthDTStringToNanoTime(dtString));
    }

    @Test
    public void testTwoParsesInDescendingOrder() throws Exception {
        // FastLogDateTimeParser is optimized for parsing timestamps in ascending order but should be correct even if this is not true
        FastLogDateTimeParser fastDTParser = new FastLogDateTimeParser();
        String dtString = "2023-11-12 13:06:58.721 EST";
        assertEquals(fastDTParser.dtStringToNanoTime(dtString), groundTruthDTStringToNanoTime(dtString));
        dtString = "2022-01-08 22:55:12.152 EST";
        assertEquals(fastDTParser.dtStringToNanoTime(dtString), groundTruthDTStringToNanoTime(dtString));
    }

    @Test
    public void testLotsOfParses() throws Exception {
        // FastLogDateTimeParser is optimized for parsing timestamps in ascending order but should be correct even if this is not true
        FastLogDateTimeParser fastDTParser = new FastLogDateTimeParser();
        String[] dtStrings = {
            "2023-11-12 13:06:58.721 EST",
            "2023-11-12 13:06:58.738 EST",
            "2023-11-12 13:06:58.926 EST",
            "2023-11-12 13:06:58.935 EST",
            "2023-11-12 14:06:58.935 EST",
            "2024-11-12 14:06:58.935 EST",
        };

        for (String dtString : dtStrings) {
            assertEquals(fastDTParser.dtStringToNanoTime(dtString), groundTruthDTStringToNanoTime(dtString));
        }
    }

    private long groundTruthDTStringToNanoTime(String dtString) {
        ZonedDateTime zdt = ZonedDateTime.parse(dtString, GROUND_TRUTH_FORMATTER);
        Instant instant = zdt.toInstant();
        long nanoseconds = instant.getEpochSecond() * 1_000_000_000L + instant.getNano();
        return nanoseconds;
    }
}

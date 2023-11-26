package com.oltpbenchmark.benchmarks.replay.util;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @author phw2
 * @brief A class to efficiently parse date time strings in log files
 * 
 * @detail
 * This class was created because date time parsing, even using the modern java.time package, took 50% of total compute time when reading a log file
 * It is only efficient assuming (1) it is asked to parse date time strings in ascending timestamp order and (2) the timestamps span a small number of unique dates
 * 
 * @usage
 * Do not instantiate the class each time you call dtStringToNanoTime(). Instantiate it once so that it can do caching
 * Multiple calls to dtStringToNanoTime() on the same instance are not thread-safe
 * Multiple calls to dtStringToNanoTime() on the different instances are thread-safe
 */
public class FastLogDateTimeParser {
    private String lastDateZeroTimeString;
    private long lastDateZeroTimeNanoseconds;
    private String lastHourString;
    private long lastHourNanoseconds;
    private String lastMinuteString;
    private long lastMinuteNanoseconds;
    private static DateTimeFormatter DATE_AND_ZONE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS z");

    public FastLogDateTimeParser() {
        this.lastDateZeroTimeString = null;
        this.lastHourString = null;
        this.lastMinuteString = null;
    }

    /**
     * @brief Efficiently parse a datetime string into its Unix epoch time in nanoseconds
     * @param dtString A date string in the format of Postgres' log files
     * @return Nanoseconds
     * 
     * @pre Is only efficient when parsing dtStrings in ascending time order, which happens
     *      to be the case for log files
     * @pre The timestamp must be valid. Timestamp validation takes a lot of time so it is not done at all
     * @post May modify the cached date string, date nanoseconds, etc.
     */
    public long dtStringToNanoTime(String dtString) {
        // process date and timezone (unlikely to change, can use the slow parsing functions in java.time which handle complex edge cases)
        String[] dtComponents = dtString.split(" ");
        String dateString = dtComponents[0];
        String zoneString = dtComponents[2];
        String dateZeroTimeString = dateString + " 00:00:00.000 " + zoneString;
        if (this.lastDateZeroTimeString == null || !this.lastDateZeroTimeString.equals(dateZeroTimeString)) {
            this.lastDateZeroTimeString = dateZeroTimeString;
            ZonedDateTime zdt = ZonedDateTime.parse(dateZeroTimeString, DATE_AND_ZONE_FORMATTER);
            Instant instant = zdt.toInstant();
            this.lastDateZeroTimeNanoseconds = instant.getEpochSecond() * 1_000_000_000L + instant.getNano();
        }

        // process time (likely to change, use fast custom parsing logic)
        String timeString = dtComponents[1];
        String[] timeComponents = timeString.split(":");
        String hourString = timeComponents[0];
        String minuteString = timeComponents[1];
        String secondString = timeComponents[2];
        if (this.lastHourString == null || !this.lastHourString.equals(hourString)) {
            this.lastHourString = hourString;
            long hour = Long.parseLong(hourString);
            this.lastHourNanoseconds = hour * 3600 * 1_000_000_000L;
        }
        if (this.lastMinuteString == null || !this.lastMinuteString.equals(minuteString)) {
            this.lastMinuteString = minuteString;
            long minute = Long.parseLong(minuteString);
            this.lastMinuteNanoseconds = minute * 60 * 1_000_000_000L;
        }
        double second = Double.parseDouble(secondString);
        long nanoOfDay = this.lastHourNanoseconds + this.lastMinuteNanoseconds + (long)(second * 1_000_000_000L);
        return this.lastDateZeroTimeNanoseconds + nanoOfDay;
    }
}

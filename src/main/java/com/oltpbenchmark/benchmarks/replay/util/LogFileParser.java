package com.oltpbenchmark.benchmarks.replay.util;

/**
 * @author phw2
 */
public interface LogFileParser {
    // I made this an instance method instead of a static method for two reasons:
    //   1. Some versions of Java don't allow you to override static methods
    //   2. This gives more flexibility to subclasses in case they want instance member variables
    public void convertLogFileToReplayFile(String logFilePath, String replayFilePath);
}

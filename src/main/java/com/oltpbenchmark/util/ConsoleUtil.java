package com.oltpbenchmark.util;

public class ConsoleUtil {
    public static int printProgressBar(long current, long total, int lastProgressPercent) {
        int progressBarWidth = 50; // Width of the progress bar

        // Calculate the number of characters to display
        int progressPercent = (int)(((double)current / total) * 100);
        int progressChars = (int) (((double)current / total) * progressBarWidth);

        if (progressPercent > lastProgressPercent) {
            // Building the progress bar
            StringBuilder bar = new StringBuilder("[");
            for (int i = 0; i < progressBarWidth; i++) {
                if (i < progressChars - 1) {
                    bar.append("=");
                } else if (i == progressChars - 1) {
                    bar.append(">");
                } else {
                    bar.append(" ");
                }
            }
            bar.append("] ").append(progressPercent).append("%");

            // Print the progress bar
            System.out.print("\r" + bar.toString()); // '\r' to return to the start of the line
        }

        return progressPercent;
    }
}

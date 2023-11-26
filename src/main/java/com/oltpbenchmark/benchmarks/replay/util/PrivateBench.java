package com.oltpbenchmark.benchmarks.replay.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Random;

import com.oltpbenchmark.benchmarks.replay.util.ReplayFileReader.ReplayFileLine;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

public class PrivateBench {
    public static void run(String logFilePath, String replayFilePath) {
        throw new RuntimeException("private bench done");
    }

    private static void numberParsing() {
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
    }

    private static void warmup(String filePath) {
        inputStreamReaderScan(filePath, false);
    }

    private static void inputStreamReaderScan(String filePath, boolean doPrint) {
        FileInputStream inputStream;
        try {
            inputStream = new FileInputStream(filePath);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("File " + filePath + " does not exist");
        }

        try (InputStreamReader inputStreamReader = new InputStreamReader(inputStream)) {
            char[] cbuf = new char[4096];
            int n;
            long loopOuterStartTime = System.nanoTime();
            while ((n = inputStreamReader.read(cbuf, 0, 4096)) == 4096) {}
            if (doPrint) { System.out.printf("inputStreamReaderScan(filePath=%s): the whole loop took %.4fms\n", filePath, (double)(System.nanoTime() - loopOuterStartTime) / 1000000); }
        } catch (IOException e) {
            throw new RuntimeException("I/O exception " + e + " when reading log file");
        }
    }

    private static void inputStreamReaderScanWithStringCopy(String filePath, boolean doPrint) {
        FileInputStream inputStream;
        try {
            inputStream = new FileInputStream(filePath);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("File " + filePath + " does not exist");
        }

        try (InputStreamReader inputStreamReader = new InputStreamReader(inputStream)) {
            char[] cbuf = new char[4096];
            int n;
            long loopOuterStartTime = System.nanoTime();
            while ((n = inputStreamReader.read(cbuf, 0, 4096)) == 4096) {
                String s = String.copyValueOf(cbuf, 0, 4096);
            }
            if (doPrint) { System.out.printf("inputStreamReaderScanWithStringCopy(filePath=%s): the whole loop took %.4fms\n", filePath, (double)(System.nanoTime() - loopOuterStartTime) / 1000000); }
        } catch (IOException e) {
            throw new RuntimeException("I/O exception " + e + " when reading log file");
        }
    }

    private static void csvReaderScan(String filePath, boolean doPrint) {
        FileInputStream inputStream;
        try {
            inputStream = new FileInputStream(filePath);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("File " + filePath + " does not exist");
        }

        try (CSVReader csvReader = new CSVReader(new InputStreamReader(inputStream))) {
            String[] fields;
            long loopOuterStartTime = System.nanoTime();
            while ((fields = csvReader.readNext()) != null) {}
            if (doPrint) { System.out.printf("csvReaderScan(filePath=%s): the whole loop took %.4fms\n", filePath, (double)(System.nanoTime() - loopOuterStartTime) / 1000000); }
        } catch (CsvValidationException e) {
            throw new RuntimeException("Log file not in a valid CSV format");
        } catch (IOException e) {
            throw new RuntimeException("I/O exception " + e + " when reading log file");
        }
    }

    private static void fastCSVReaderScan(String filePath, boolean doPrint) {
        FileInputStream inputStream;
        try {
            inputStream = new FileInputStream(filePath);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("File " + filePath + " does not exist");
        }

        try (FastCSVReader csvReader = new FastCSVReader(new InputStreamReader(inputStream), 4096)) {
            List<String> csvLine;
            long loopOuterStartTime = System.nanoTime();
            int n = 0;
            while ((csvLine = csvReader.readNext()) != null) {}
            if (doPrint) { System.out.printf("fastCSVReaderScan(filePath=%s): the whole loop took %.4fms\n", filePath, (double)(System.nanoTime() - loopOuterStartTime) / 1000000); }
        } catch (IOException e) {
            throw new RuntimeException("I/O exception " + e + " when reading log file");
        }
    }

    private static void replayFileReaderScan(String filePath, boolean doPrint) {
        FileInputStream inputStream;
        try {
            inputStream = new FileInputStream(filePath);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("File " + filePath + " does not exist");
        }

        try (ReplayFileReader csvReader = new ReplayFileReader(new InputStreamReader(inputStream), 4096)) {
            ReplayFileLine replayFileLine;
            long loopOuterStartTime = System.nanoTime();
            int n = 0;
            while ((replayFileLine = csvReader.readLine()) != null) {}
            if (doPrint) { System.out.printf("fastCSVReaderScan(filePath=%s): the whole loop took %.4fms\n", filePath, (double)(System.nanoTime() - loopOuterStartTime) / 1000000); }
        } catch (IOException e) {
            throw new RuntimeException("I/O exception " + e + " when reading log file");
        }
    }
}

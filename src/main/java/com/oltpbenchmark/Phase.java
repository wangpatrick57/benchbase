/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.oltpbenchmark;

import com.oltpbenchmark.benchmarks.replay.util.ReplayFileQueue;
import com.oltpbenchmark.benchmarks.replay.util.ReplayTransaction;
import com.oltpbenchmark.util.StringUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class Phase {
    private static final Logger LOG = LoggerFactory.getLogger(Phase.class);

    public enum Arrival {
        REGULAR, POISSON, REPLAY
    }

    private final Random gen = new Random();
    private final String benchmarkName;
    private final int id;
    private final int time;
    private final int warmupTime;
    private final double rate;
    private final double replaySpeedup;
    private final Arrival arrival;


    private final boolean rateLimited;
    private final boolean disabled;
    private final boolean serial;
    private final boolean replaySpeedupLimited;
    private final boolean replay;
    private final boolean timed;
    private final List<Double> weights;
    private final int weightCount;
    private final int activeTerminals;
    private int nextSerial;

    private ReplayFileQueue replayFileQueue;
    private long replayStartTime;
    private long firstFirstLogTime;


    Phase(String benchmarkName, int id, int t, int wt, double r, double replaySpeedup, List<Double> weights, boolean rateLimited, boolean disabled, boolean serial, boolean replaySpeedupLimited, boolean replay, boolean timed, int activeTerminals, Arrival a, String replayFilePath) {
        this.benchmarkName = benchmarkName;
        this.id = id;
        this.time = t;
        this.warmupTime = wt;
        this.rate = r;
        this.replaySpeedup = replaySpeedup;
        this.weights = weights;
        this.weightCount = this.weights.size();
        this.rateLimited = rateLimited;
        this.disabled = disabled;
        this.serial = serial;
        this.replaySpeedupLimited = replaySpeedupLimited;
        this.replay = replay;
        this.timed = timed;
        this.nextSerial = 1;
        this.activeTerminals = activeTerminals;
        this.arrival = a;

        if (this.isReplay()) {
            this.replayFileQueue = new ReplayFileQueue(replayFilePath);
        }
    }

    /**
     * Any functions which will be called when the phase starts
     */
    public void onStart() {
        this.resetSerial();
        if (this.isReplay()) {
            // mark the current time as the start of the replay, which is used to shift logged timestamps over
            this.replayStartTime = System.nanoTime();
            Optional<ReplayTransaction> replayTransaction = this.replayFileQueue.peek();
            
            if (replayTransaction.isEmpty()) {
                // this means the replay file is empty. we can just set firstFirstLogTime to an arbitrary value since this replay phase will be skipped anyways
                this.firstFirstLogTime = 0;
            } else {
                // it's the firstLogTime of the first ReplayTransaction, which is why there are two "firsts"
                this.firstFirstLogTime = replayTransaction.get().getFirstLogTime();
            }
        }
    }

    public boolean isRateLimited() {
        return rateLimited;
    }

    public boolean isDisabled() {
        return disabled;
    }

    public boolean isSerial() {
        return serial;
    }

    public boolean isReplaySpeedupLimited() {
        return replaySpeedupLimited;
    }

    public boolean isReplay() {
        return replay;
    }

    public boolean isTimed() {
        return timed;
    }

    public boolean isLatencyRun() {
        return !timed && serial;
    }

    public boolean isThroughputRun() {
        return !isLatencyRun();
    }

    public void resetSerial() {
        this.nextSerial = 1;
    }

    public int getActiveTerminals() {
        return activeTerminals;
    }

    public int getWeightCount() {
        return (this.weightCount);
    }

    public int getId() {
        return id;
    }

    public int getTime() {
        return time;
    }

    public int getWarmupTime() {
        return warmupTime;
    }

    public double getRate() {
        return rate;
    }

    public double getReplaySpeedup() {
        return replaySpeedup;
    }

    public Arrival getArrival() {
        return arrival;
    }

    public List<Double> getWeights() {
        return (this.weights);
    }

    /**
     * Computes the sum of weights. Usually needs to add up to 100%
     *
     * @return The total weight
     */
    public double totalWeight() {
        double total = 0.0;
        for (Double d : weights) {
            total += d;
        }
        return total;
    }

    /**
     * Check whether the next replay transaction is in the past.
     * May block if ReplayFileQueue blocks.
     * @return Whether the next replay transaction is in the past
     */
    public boolean isNextReplayTransactionInPast() {
        if (!this.isReplay()) {
            throw new RuntimeException("hasPastReplayProcedure should only be called for replay phases");
        }

        Optional<ReplayTransaction> replayTransactionOpt = this.replayFileQueue.peek();

        if (replayTransactionOpt.isEmpty()) {
            LOG.warn("In replay phases, isNextReplayTransactionInPast() should only be called if there are more replay transactions");
            // return false as a safe value
            return false;
        } else {
            long now = System.nanoTime();
            long replayTime = this.getReplayTime(replayTransactionOpt.get().getFirstLogTime());
            return replayTime <= now;
        }
    }

    /**
     * Check whether there is a next replay transaction
     * @return Whether there is a next replay transaction
     */
    public boolean existsNextReplayTransaction() {
        if (!this.isReplay()) {
            throw new RuntimeException("existsNextReplayTransaction should only be called for replay phases");
        }

        Optional<ReplayTransaction> replayTransactionOpt = this.replayFileQueue.peek();
        return !replayTransactionOpt.isEmpty();
    }

    /**
     * Get the timestamp the next ReplayTransaction should be dequeued at
     * @return The timestamp
     */
    public long getNextReplayTransactionTimestamp() {
        if (!this.isReplay()) {
            throw new RuntimeException("getNextReplayTransactionTimestamp should only be called for replay phases");
        }

        Optional<ReplayTransaction> replayTransaction = this.replayFileQueue.peek();

        if (replayTransaction.isEmpty()) {
            LOG.warn("In replay phases, getNextReplayTransactionTimestamp() should only be called if there are more replay transactions");
            // return the current time as a safe value
            // I add a 100ms wait so the while loop in ThreadBench doesn't get called too many times in quick succession
            return System.nanoTime() + 100000000;
        } else {
            return this.getReplayTime(replayTransaction.get().getFirstLogTime());
        }
    }

    /**
     * Given a log time, get the time the ReplayTransaction should be dequeued
     * @param logTime The time the first line of the ReplayTransaction in the log file
     * @return The shifted replay time
     */
    private long getReplayTime(long logTime) {
        return this.replayStartTime + (long)((logTime - firstFirstLogTime) / this.replaySpeedup);
    }

    /**
     * Generates the next submitted procedure for this phase
     * In replay phases, it may block if ReplayFileQueue blocks.
     * @return The next SubmittedProcedure to run for this phase
     */
    public SubmittedProcedure generateSubmittedProcedure() {
        return this.generateSubmittedProcedure(false);
    }

    public SubmittedProcedure generateSubmittedProcedure(boolean isColdQuery) {
        Optional<List<Object>> runArgs = Optional.empty();

        if (this.isReplay()) {
            synchronized (this) {
                // since peek() and remove() must be called atomically, this chunk of code needs to be synchronized
                Optional<ReplayTransaction> replayTransactionOpt = this.replayFileQueue.peek();
                if (replayTransactionOpt.isEmpty()) {
                    LOG.warn("In replay phases, generateSubmittedProcedure() should only be called if there are more replay transactions");
                    // runArgs being empty causes DynamicProcedure to just be a NOOP
                    runArgs = Optional.empty();
                } else {
                    this.replayFileQueue.remove();
                    runArgs = Optional.of(new ArrayList<Object>());
                    runArgs.get().add(replayTransactionOpt.get());
                    runArgs.get().add(this.replaySpeedupLimited);
                    runArgs.get().add(this.replaySpeedup);
                }
            }
        }

        return new SubmittedProcedure(this.chooseTransaction(isColdQuery), runArgs);
    }

    /**
     * This simply computes the next transaction by randomly selecting one based
     * on the weights of this phase.
     *
     * @return
     */
    private int chooseTransaction(boolean isColdQuery) {
        if (isDisabled()) {
            return -1;
        }

        if (isSerial()) {
            int ret;
            synchronized (this) {
                ret = this.nextSerial;

                // Serial runs should not execute queries with non-positive
                // weights.
                while (ret <= this.weightCount && weights.get(ret - 1) <= 0.0) {
                    ret = ++this.nextSerial;
                }

                // If it's a cold execution, then we don't want to advance yet,
                // since the hot run needs to execute the same query.
                if (!isColdQuery) {

                    // throughput) run, so we loop through the list multiple
                    // times. Note that we do the modulus before the increment
                    // so that we end up in the range [1,num_weights]
                    if (isTimed()) {

                        this.nextSerial %= this.weightCount;
                    }

                    ++this.nextSerial;
                }
            }
            return ret;
        } else {
            int randomPercentage = gen.nextInt((int) totalWeight()) + 1;
            double weight = 0.0;
            for (int i = 0; i < this.weightCount; i++) {
                weight += weights.get(i);
                if (randomPercentage <= weight) {
                    return i + 1;
                }
            }
        }

        return -1;
    }

    /**
     * Returns a string for logging purposes when entering the phase
     */
    public String currentPhaseString() {
        List<String> inner = new ArrayList<>();
        inner.add("[Workload=" + benchmarkName.toUpperCase() + "]");
        if (isDisabled()) {
            inner.add("[Disabled=true]");
        } else {
            if (isLatencyRun()) {
                inner.add("[Serial=true]");
                inner.add("[Time=n/a]");
            } else {
                inner.add("[Serial=" + isSerial() + "]");
                inner.add("[Time=" + time + "]");
            }
            inner.add("[WarmupTime=" + warmupTime + "]");
            inner.add("[Rate=" + (isRateLimited() ? rate : "unlimited") + "]");
            inner.add("[Arrival=" + arrival + "]");
            inner.add("[Ratios=" + getWeights() + "]");
            inner.add("[ActiveWorkers=" + getActiveTerminals() + "]");
        }

        return StringUtil.bold("PHASE START") + " :: " + StringUtil.join(" ", inner);
    }

}
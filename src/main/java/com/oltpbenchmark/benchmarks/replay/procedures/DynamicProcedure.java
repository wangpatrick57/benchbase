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
package com.oltpbenchmark.benchmarks.replay.procedures;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.replay.util.ReplayTransaction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oltpbenchmark.api.Procedure;

 /**
  * A Procedure which can represent an arbitrary list of SQLStmts which
  * is passed in as an argument at runtime.
  * 
  * The reason it takes in a list of SQLStmts is so that it can represent
  * multi-statement transactions.
  */
public class DynamicProcedure extends Procedure {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicProcedure.class);

    public void run(Connection conn, ReplayTransaction replayTransaction) throws SQLException {
        System.out.printf("Entering DynamicProcedure.run() for %d statements\n", replayTransaction.getSQLStmtCallCount());

        // I chose to replay statements within a transaction relative to when we entered _this function_
        // instead of relative to when the entire replay started. This way, if the ReplayTransaction was
        // delayed in the work queue, we avoid replaying a bunch of SQL statements in close succession
        // at the start.
        long transactionReplayStartTime = System.nanoTime();

        if (!replayTransaction.hasSQLStmtCall()) {
            LOG.warn("run() called with a replayTransaction with no SQL statement calls in it");
        }

        while (replayTransaction.hasSQLStmtCall()) {
            // sleep until the next SQLStmt call, which may be "don't sleep"
            long thisCallLogTime = replayTransaction.peekCallTime();
            long thisCallReplayTime = transactionReplayStartTime + (thisCallLogTime - replayTransaction.getFirstLogTime());
            long now = System.nanoTime();
            long diff = thisCallReplayTime - now;
            while (diff > 0) { // this can wake early: sleep multiple times to avoid that
                long ms = diff / 1000000;
                diff = diff % 1000000;
                try {
                    Thread.sleep(ms, (int) diff);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                now = System.nanoTime();
                diff = thisCallReplayTime - now;
            }

            SQLStmt sqlStmt = replayTransaction.peekSQLStmt();
            LocalDateTime nowDT = LocalDateTime.now();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            String timestampAsString = nowDT.format(formatter);
            System.out.printf("Executing %s at time %s\n", sqlStmt, timestampAsString);
            PreparedStatement preparedStatement = this.getPreparedStatement(conn, sqlStmt);
            preparedStatement.execute();
            replayTransaction.removeSQLStmtCall();
        }

        if (replayTransaction.getShouldAbort()) {
            throw new UserAbortException("This transaction aborted in the replay file.");
        }
    }
 }
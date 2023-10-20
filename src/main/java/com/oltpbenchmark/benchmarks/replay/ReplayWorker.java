package com.oltpbenchmark.benchmarks.replay;

import java.sql.Connection;
import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.benchmarks.replay.procedures.DynamicProcedure;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.types.TransactionStatus;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplayWorker extends Worker<ReplayBenchmark> {
    
    private static final Logger LOG = LoggerFactory.getLogger(ReplayWorker.class);

     public ReplayWorker(ReplayBenchmark benchmarkModule, int id) {
         super(benchmarkModule, id);
     }

     @Override
     protected TransactionStatus executeWork(Connection conn, TransactionType nextTransaction) throws UserAbortException, SQLException {
        try {
            DynamicProcedure proc = (DynamicProcedure) this.getProcedure(nextTransaction.getProcedureClass());
            List<Object> runArgs = new ArrayList<Object>();
            runArgs.add(new SQLStmt("SELECT * FROM customer LIMIT 10;"));
            proc.run(conn, runArgs);
        } catch (ClassCastException ex) {
            //fail gracefully
            LOG.error("We have been invoked with an INVALID transactionType?!", ex);
            throw new RuntimeException("Bad transaction type = " + nextTransaction);
        }
        return (TransactionStatus.SUCCESS);
     }
 }
package com.oltpbenchmark.benchmarks.replay;

import java.sql.Connection;
import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.benchmarks.replay.procedures.DynamicProcedure;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.types.TransactionStatus;

import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplayWorker extends Worker<ReplayBenchmark> {
    
    private static final Logger LOG = LoggerFactory.getLogger(ReplayWorker.class);

     public ReplayWorker(ReplayBenchmark benchmarkModule, int id) {
         super(benchmarkModule, id);
     }

    /**
     * If runArgs is empty this will be a NOOP. However, it is not recommended to call this with an empty runArgs since it incorrectly counts it as a successful transaction
    */
    @Override
    protected TransactionStatus executeWork(Connection conn, TransactionType nextTransaction, Optional<List<Object>> runArgs) throws UserAbortException, SQLException {
    try {
        DynamicProcedure proc = (DynamicProcedure) this.getProcedure(nextTransaction.getProcedureClass());
        if (runArgs.isEmpty()) {
            LOG.warn("ReplayWorker.executeWork(): runArgs is empty");
        } else {
            proc.run(conn, runArgs.get());
        }
    } catch (ClassCastException ex) {
        //fail gracefully
        LOG.error("We have been invoked with an INVALID transactionType?!", ex);
        throw new RuntimeException("Bad transaction type = " + nextTransaction);
    }
        return (TransactionStatus.SUCCESS);
    }
 }
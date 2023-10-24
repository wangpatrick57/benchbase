package com.oltpbenchmark.benchmarks.replay;

import java.sql.Connection;
import java.sql.SQLException;

import com.oltpbenchmark.DBWorkload;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.benchmarks.replay.procedures.DynamicProcedure;
import com.oltpbenchmark.benchmarks.replay.util.ReplayTransaction;
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
     * Execute DynamicProcedure.run() with the given runArgs
     * 
     * If runArgs is Optional.empty() this will be a NOOP. However, it is not recommended to call this
     * with an empty runArgs since it affects the metrics.
     * 
     * @param conn The connection to use
     * @param nextTransaction The transactionType
     * @param runArgs Should be a list containing a single ReplayTransaction
     * @pre The transactionType should be the one for a DynamicProcedure
    */
    @Override
    protected TransactionStatus executeWork(Connection conn, TransactionType nextTransaction, Optional<List<Object>> runArgs) throws UserAbortException, SQLException {
        if (DBWorkload.DEBUG) {
            System.out.printf("entering ReplayWorker.executeWork\n");
        }
        try {
            DynamicProcedure proc = (DynamicProcedure) this.getProcedure(nextTransaction.getProcedureClass());
            if (runArgs.isEmpty()) {
                LOG.warn("runArgs is empty");
                return (TransactionStatus.UNKNOWN);
            } else {
                ReplayTransaction replayTransaction = ReplayWorker.castArguments(runArgs.get());
                proc.run(conn, replayTransaction);
                return (TransactionStatus.SUCCESS);
            }
        } catch (ClassCastException ex) {
            //fail gracefully
            LOG.error("We have been invoked with an INVALID transactionType?!", ex);
            throw new RuntimeException("Bad transaction type = " + nextTransaction);
        }
    }

    private static ReplayTransaction castArguments(List<Object> runArgs) {
        if (runArgs.size() != 1) {
            throw new RuntimeException("Only one argument should be passed to DynamicProcedure");
        }
        return (ReplayTransaction)runArgs.get(0);
    }
 }
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
    protected TransactionStatus executeWork(Connection conn, TransactionType nextTransaction, Optional<List<Object>> runArgs) throws UserAbortException, SQLException, RuntimeException {
        if (DBWorkload.DEBUG) {
            System.out.printf("entering ReplayWorker.executeWork\n");
        }

        DynamicProcedure proc;
        try {
            proc = (DynamicProcedure) this.getProcedure(nextTransaction.getProcedureClass());
        } catch (ClassCastException ex) {
            LOG.error("We have been invoked with an INVALID transactionType?!", ex);
            throw new RuntimeException("Bad transaction type = " + nextTransaction);
        }

        if (runArgs.isEmpty()) {
            LOG.warn("runArgs is empty");
            return (TransactionStatus.UNKNOWN);
        } else {
            List<Object> runArgsList = runArgs.get();
            if (runArgsList.size() != 3) {
                LOG.error("Exactly three runArgs should be passed to DynamicProcedure");
                throw new RuntimeException("Exactly three runArgs should be passed to DynamicProcedure");
            }
            try {
                ReplayTransaction replayTransaction = (ReplayTransaction)runArgsList.get(0);
                boolean replaySpeedupLimited = (boolean)runArgsList.get(1);
                double replaySpeedup = (double)runArgsList.get(2);
                proc.run(conn, replayTransaction, replaySpeedupLimited, replaySpeedup);
                return (TransactionStatus.SUCCESS);
            } catch (ClassCastException ex) {
                LOG.error("runArgs not of the correct type");
                throw new RuntimeException("runArgs not of the correct type");
            }
        }
    }
 }
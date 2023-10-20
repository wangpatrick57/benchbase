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

public class ReplayWorker extends Worker<ReplayBenchmark> {
    public ReplayWorker(ReplayBenchmark benchmarkModule, int id) {
        super(benchmarkModule, id);
    }

    @Override
    protected TransactionStatus executeWork(Connection conn, TransactionType nextTransaction, Optional<List<Object>> runArgs) throws UserAbortException, SQLException {
        DynamicProcedure proc = (DynamicProcedure) this.getProcedure(nextTransaction.getProcedureClass());
        proc.run(conn, runArgs);
        return (TransactionStatus.SUCCESS);
    }
}

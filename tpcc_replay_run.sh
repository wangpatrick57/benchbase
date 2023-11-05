#!/bin/bash
. ./set_dblab_postgres_dpath.sh
GEN_LOG_CSV_SPATH="$DBLAB_POSTGRES_DPATH/postgresql.csv"
PERMANENT_LOG_CSV_FPATH="$DBLAB_POSTGRES_DPATH/tpcc_log.csv"

SKIP_BUILD=""

# generate log file
if true; then
  # this if block creates tpcc_log.csv
  SKIP_BUILD=$SKIP_BUILD ./run.sh tpcc-c-l
  SKIP_BUILD="1"
  rm "$(readlink $GEN_LOG_CSV_SPATH)"
  SKIP_BUILD=$SKIP_BUILD ./run.sh tpcc-exec
  mv "$(readlink $GEN_LOG_CSV_SPATH)" $PERMANENT_LOG_CSV_FPATH
fi

# [Invariant] tpcc_log.csv will have the correct values

# reset DB
if true; then
  SKIP_BUILD=$SKIP_BUILD ./run.sh tpcc-c-l # first, reset the database
  SKIP_BUILD="1" # if it was already 1, this is harmless
fi

# run replay
if true; then
  # clean log file before running the replay so that we can track that as well
  rm "$(readlink $GEN_LOG_CSV_SPATH)"

  # run replay
  SKIP_BUILD=$SKIP_BUILD ./run.sh tpcc-replay # then, run a replay
fi
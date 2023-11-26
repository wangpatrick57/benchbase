#!/bin/bash
. ./set_dblab_postgres_dpath.sh
RESET_FPATH="$DBLAB_POSTGRES_DPATH/reset.sh"
COMMANDS_FPATH="$DBLAB_POSTGRES_DPATH/commands.sql"
GEN_LOG_CSV_SPATH="$DBLAB_POSTGRES_DPATH/postgresql.csv"
PERMANENT_LOG_CSV_FPATH="$DBLAB_POSTGRES_DPATH/lab_log.csv"

# generate log file
if false; then
  "$RESET_FPATH"
  psql -d lab -a -f $COMMANDS_FPATH
  mv $(readlink $GEN_LOG_CSV_SPATH) $PERMANENT_LOG_CSV_FPATH
fi

# [Invariant] lab_log.csv will be correctly set

# reset DB
if false; then
  "$RESET_FPATH"
fi

# run replay
if true; then
  ./run.sh lab-replay
fi
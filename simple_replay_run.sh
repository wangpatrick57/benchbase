#!/bin/bash
DBLAB_POSTGRES_DPATH="$HOME/Documents/mastersStuff/dblab/postgres"
RESET_FPATH="$DBLAB_POSTGRES_DPATH/reset.sh"
COMMANDS_FPATH="$DBLAB_POSTGRES_DPATH/commands.sql"
GEN_LOG_CSV_SPATH="$DBLAB_POSTGRES_DPATH/postgresql.csv"
PERMANENT_LOG_CSV_FPATH="$DBLAB_POSTGRES_DPATH/simple_log.csv"

if false; then
  "$RESET_FPATH"
  psql -d lab -a -f $COMMANDS_FPATH
  psql -d lab -a -f $COMMANDS_FPATH
  mv $(readlink $GEN_LOG_CSV_SPATH) $PERMANENT_LOG_CSV_FPATH
fi

# [Invariant] simple_log.csv will be correctly set

"$RESET_FPATH" # reset the database
./run.sh replay # run a replay
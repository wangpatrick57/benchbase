#!/bin/bash
OS_NAME=$(uname -s)
if [ "$OS_NAME" == "Linux" ]; then
  DBLAB_POSTGRES_DPATH="$HOME/dblab/d7pg"
elif [ "$OS_NAME" == "Darwin" ]; then
  DBLAB_POSTGRES_DPATH="$HOME/dblab/m1pg"
else
  echo "OS_NAME $OS_NAME is unknown"
  exit 1
fi
TPCC_CONFIG_PATH="$DBLAB_POSTGRES_DPATH/tpcc_config.xml"
REPLAY_CONFIG_PATH="$DBLAB_POSTGRES_DPATH/replay_config.xml"

if [ "$#" -eq 0 ]; then
  echo "must pass at least one arg"
  exit 1
elif [ "$1" == "tpcc-c-l" ]; then
  EXEC_ARGS="-b tpcc -c $TPCC_CONFIG_PATH --create=true --load=true"
elif [ "$1" == "tpcc-exec" ]; then
  EXEC_ARGS="-b tpcc -c $TPCC_CONFIG_PATH --execute=true"
elif [ "$1" == "tpcc-c-l-e" ]; then
  EXEC_ARGS="-b tpcc -c $TPCC_CONFIG_PATH --create=true --load=true --execute=true"
elif [ "$1" == "replay" ]; then
  EXEC_ARGS="-b replay -c $REPLAY_CONFIG_PATH --execute=true"
else
  EXEC_ARGS="$@"
fi

if [ "$SKIP_BUILD" != "1" ]; then
  ./mvnw clean package -P postgres -Dmaven.test.skip=true
fi

cd target/benchbase-postgres/benchbase-postgres
java -jar benchbase.jar $EXEC_ARGS

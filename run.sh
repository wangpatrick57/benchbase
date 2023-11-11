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
TPCC_REPLAY_CONFIG_PATH="$DBLAB_POSTGRES_DPATH/tpcc_replay_config.xml"
LAB_REPLAY_CONFIG_PATH="$DBLAB_POSTGRES_DPATH/lab_replay_config.xml"

COMMON_ARGS=""
if [ "$#" -eq 0 ]; then
  echo "must pass at least one arg"
  exit 1
elif [ "$1" == "tpcc-c-l" ]; then
  EXEC_ARGS="-b tpcc -c $TPCC_CONFIG_PATH --create=true --load=true $COMMON_ARGS"
elif [ "$1" == "tpcc-exec" ]; then
  EXEC_ARGS="-b tpcc -c $TPCC_CONFIG_PATH --execute=true $COMMON_ARGS"
elif [ "$1" == "tpcc-c-l-e" ]; then
  EXEC_ARGS="-b tpcc -c $TPCC_CONFIG_PATH --create=true --load=true --execute=true $COMMON_ARGS"
elif [ "$1" == "tpcc-replay" ]; then
  EXEC_ARGS="-b replay -c $TPCC_REPLAY_CONFIG_PATH --execute=true $COMMON_ARGS"
elif [ "$1" == "lab-replay" ]; then
  EXEC_ARGS="-b replay -c $LAB_REPLAY_CONFIG_PATH --execute=true $COMMON_ARGS"
else
  EXEC_ARGS="$@"
fi

if [ "$SKIP_BUILD" != "1" ]; then
  ./mvnw clean package -P postgres -Dmaven.test.skip=true
fi

cd target/benchbase-postgres/benchbase-postgres
# java -jar -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 benchbase.jar $EXEC_ARGS
java -jar -Djava.util.logging.config.file=/home/phw2/benchbase/src/main/resources/logging.properties benchbase.jar $EXEC_ARGS
# java -jar benchbase.jar $EXEC_ARGS

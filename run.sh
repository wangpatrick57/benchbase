#!/bin/bash
if [ "$#" -eq 0 ]; then
  echo "must pass at least one arg"
  exit 1
elif [ "$1" == "tpcc-c-l" ]; then
  EXEC_ARGS="-b tpcc -c config/postgres/sample_tpcc_config.xml --create=true --load=true"
elif [ "$1" == "tpcc-exec" ]; then
  EXEC_ARGS="-b tpcc -c config/postgres/sample_tpcc_config.xml --execute=true -jh histograms.json"
elif [ "$1" == "tpcc-c-l-e" ]; then
  EXEC_ARGS="-b tpcc -c config/postgres/sample_tpcc_config.xml --create=true --load=true --execute=true -jh histograms.json"
elif [ "$1" == "replay" ]; then
  EXEC_ARGS="-b replay -c config/postgres/sample_replay_config.xml --execute=true -jh histograms.json"
else
  EXEC_ARGS="$@"
fi

if [ "$SKIP_BUILD" != "1" ]; then
  ./mvnw clean package -P postgres -Dmaven.test.skip=true
fi

cd target/benchbase-postgres/benchbase-postgres
java -jar benchbase.jar $EXEC_ARGS

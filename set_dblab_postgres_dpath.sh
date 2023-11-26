#!/bin/bash
OS_NAME=$(uname -s)
if [ "$OS_NAME" == "Linux" ]; then
  DBLAB_POSTGRES_DPATH="$HOME/dblab/d7pg"
elif [ "$OS_NAME" == "Darwin" ]; then
  DBLAB_POSTGRES_DPATH="$HOME/Documents/mastersStuff/dblab/m1pg"
else
  echo "OS_NAME $OS_NAME is unknown"
  exit 1
fi
#!/bin/bash

case "$1" in
  master)
    ${SPARK_HOME}/sbin/start-master.sh
    tail -f /dev/null  # Keep container running
    ;;
  worker)
    ${SPARK_HOME}/sbin/start-worker.sh spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT}
    tail -f /dev/null
    ;;
  history)
    ${SPARK_HOME}/sbin/start-history-server.sh
    tail -f /dev/null
    ;;
  *)
    exec "$@"
    ;;
esac
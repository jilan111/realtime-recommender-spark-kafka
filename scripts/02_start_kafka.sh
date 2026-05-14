#!/usr/bin/env bash
# Start Zookeeper and the Kafka broker in the foreground (split into two
# background processes so Ctrl-C cleanly stops both).
set -euo pipefail

KAFKA_HOME="${HOME}/kafka"
[ -d "${KAFKA_HOME}" ] || { echo "Run scripts/01_setup_vm.sh first."; exit 1; }

LOG_DIR="${HOME}/kafka-data/logs"
mkdir -p "${LOG_DIR}"

cleanup() {
  echo
  echo "[kafka] shutting down..."
  [ -n "${KAFKA_PID:-}" ] && kill -TERM "${KAFKA_PID}" 2>/dev/null || true
  sleep 2
  [ -n "${ZK_PID:-}" ] && kill -TERM "${ZK_PID}" 2>/dev/null || true
  wait 2>/dev/null || true
  echo "[kafka] stopped."
}
trap cleanup EXIT INT TERM

echo "[kafka] starting Zookeeper..."
"${KAFKA_HOME}/bin/zookeeper-server-start.sh" "${KAFKA_HOME}/config/zookeeper.properties" \
  >"${LOG_DIR}/zookeeper.log" 2>&1 &
ZK_PID=$!
sleep 5

echo "[kafka] starting Kafka broker..."
"${KAFKA_HOME}/bin/kafka-server-start.sh" "${KAFKA_HOME}/config/server.properties" \
  >"${LOG_DIR}/kafka.log" 2>&1 &
KAFKA_PID=$!

echo "[kafka] zookeeper pid=${ZK_PID}, broker pid=${KAFKA_PID}"
echo "[kafka] logs: ${LOG_DIR}/{zookeeper.log,kafka.log}"
echo "[kafka] tail with: tail -f ${LOG_DIR}/kafka.log"
echo "[kafka] press Ctrl-C to stop"
wait

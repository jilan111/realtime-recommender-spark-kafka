#!/usr/bin/env bash
# Stop a Kafka/Zookeeper pair started by scripts/02_start_kafka.sh.
# Use this when the start script was backgrounded with `&` or detached.
set -euo pipefail

KAFKA_HOME="${HOME}/kafka"
"${KAFKA_HOME}/bin/kafka-server-stop.sh" || true
sleep 2
"${KAFKA_HOME}/bin/zookeeper-server-stop.sh" || true
echo "[kafka] shutdown signalled."

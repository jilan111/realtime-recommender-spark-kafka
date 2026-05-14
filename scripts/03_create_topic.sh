#!/usr/bin/env bash
# Create the 'reviews' topic with 2 partitions and replication-factor 1.
# The 2-partition choice is justified in REPORT.md (partitioning strategy).
set -euo pipefail

KAFKA_HOME="${HOME}/kafka"
TOPIC="reviews"
PARTITIONS=2
RF=1
BOOTSTRAP="localhost:9092"

if "${KAFKA_HOME}/bin/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --list | grep -qx "${TOPIC}"; then
  echo "[topic] '${TOPIC}' already exists — describing:"
  "${KAFKA_HOME}/bin/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --describe --topic "${TOPIC}"
  exit 0
fi

"${KAFKA_HOME}/bin/kafka-topics.sh" \
  --bootstrap-server "${BOOTSTRAP}" \
  --create \
  --topic "${TOPIC}" \
  --partitions "${PARTITIONS}" \
  --replication-factor "${RF}"

echo "[topic] created '${TOPIC}' with ${PARTITIONS} partitions"
"${KAFKA_HOME}/bin/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --describe --topic "${TOPIC}"

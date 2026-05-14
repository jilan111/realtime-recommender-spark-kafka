#!/usr/bin/env bash
# Sanity check: produce one JSON event and read it back. Used right after
# the initial setup to confirm Kafka is wired up correctly.
set -euo pipefail

KAFKA_HOME="${HOME}/kafka"
TOPIC="reviews"
BOOTSTRAP="localhost:9092"
PAYLOAD='{"user_id": 1, "item_id": 99, "rating": 5.0, "timestamp": "2026-05-13T00:00:00"}'

echo "[smoke] sending one test message..."
echo "${PAYLOAD}" | "${KAFKA_HOME}/bin/kafka-console-producer.sh" \
  --bootstrap-server "${BOOTSTRAP}" --topic "${TOPIC}"

echo "[smoke] reading from --from-beginning (Ctrl-C to stop)..."
"${KAFKA_HOME}/bin/kafka-console-consumer.sh" \
  --bootstrap-server "${BOOTSTRAP}" --topic "${TOPIC}" \
  --from-beginning --max-messages 1

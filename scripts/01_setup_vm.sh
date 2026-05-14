#!/usr/bin/env bash
# One-time VM setup: downloads Apache Kafka 3.7.0 and stages its data dirs.
# Idempotent — safe to re-run.
set -euo pipefail

KAFKA_VERSION="3.7.0"
SCALA_VERSION="2.13"
TARBALL="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
URL="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${TARBALL}"

KAFKA_HOME="${HOME}/kafka"
KAFKA_DATA="${HOME}/kafka-data"

echo "[setup] target install dir: ${KAFKA_HOME}"

if [ ! -d "${KAFKA_HOME}" ]; then
  cd "${HOME}"
  if [ ! -f "${TARBALL}" ]; then
    echo "[setup] downloading ${URL}"
    curl -L --fail -o "${TARBALL}" "${URL}"
  fi
  echo "[setup] extracting ${TARBALL}"
  tar -xzf "${TARBALL}"
  mv "kafka_${SCALA_VERSION}-${KAFKA_VERSION}" "${KAFKA_HOME}"
  rm -f "${TARBALL}"
else
  echo "[setup] ${KAFKA_HOME} already exists, skipping download"
fi

mkdir -p "${KAFKA_DATA}/zookeeper" "${KAFKA_DATA}/kafka"

# Patch the bundled configs so log dirs live under ~/kafka-data (not /tmp,
# which Ubuntu wipes on reboot).
sed -i "s|^dataDir=.*|dataDir=${KAFKA_DATA}/zookeeper|" "${KAFKA_HOME}/config/zookeeper.properties"
sed -i "s|^log.dirs=.*|log.dirs=${KAFKA_DATA}/kafka|" "${KAFKA_HOME}/config/server.properties"

# Default num.partitions to 2 to satisfy the assignment.
if grep -q '^num.partitions=' "${KAFKA_HOME}/config/server.properties"; then
  sed -i 's|^num.partitions=.*|num.partitions=2|' "${KAFKA_HOME}/config/server.properties"
else
  echo 'num.partitions=2' >> "${KAFKA_HOME}/config/server.properties"
fi

echo "[setup] Kafka installed at ${KAFKA_HOME}"
echo "[setup] Data dirs:           ${KAFKA_DATA}/{zookeeper,kafka}"
echo "[setup] Java: $(java -version 2>&1 | head -n1)"
echo "[setup] DONE — next: bash scripts/02_start_kafka.sh"

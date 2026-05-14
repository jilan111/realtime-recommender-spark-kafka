#!/usr/bin/env bash
# One-time VM setup: prepares Kafka for the project.
#
# Two modes, auto-detected:
#   A) /opt/kafka already installed (e.g. apt or pre-baked image) →
#      symlink ~/kafka → /opt/kafka and patch its config in place.
#   B) /opt/kafka missing → download Apache Kafka 3.7.0 tarball into ~/kafka.
#
# In either mode the script ensures:
#   * ~/kafka                    points at a Kafka install
#   * ~/kafka-data/{zookeeper,kafka,logs}   exist
#   * zookeeper.properties:dataDir   → ~/kafka-data/zookeeper
#   * server.properties:log.dirs     → ~/kafka-data/kafka
#   * server.properties:num.partitions=2
#
# Idempotent — safe to re-run.
set -euo pipefail

KAFKA_VERSION="3.7.0"
SCALA_VERSION="2.13"
TARBALL="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
URL="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${TARBALL}"

KAFKA_HOME="${HOME}/kafka"
KAFKA_DATA="${HOME}/kafka-data"
SYSTEM_KAFKA="/opt/kafka"

echo "[setup] target Kafka home: ${KAFKA_HOME}"

if [ -d "${SYSTEM_KAFKA}" ] && [ -x "${SYSTEM_KAFKA}/bin/kafka-server-start.sh" ]; then
  echo "[setup] found system Kafka at ${SYSTEM_KAFKA}, symlinking ${KAFKA_HOME} -> ${SYSTEM_KAFKA}"
  # If ~/kafka is a real directory (old install), back it up rather than clobber
  if [ -d "${KAFKA_HOME}" ] && [ ! -L "${KAFKA_HOME}" ]; then
    mv "${KAFKA_HOME}" "${KAFKA_HOME}.bak.$(date +%s)"
  fi
  ln -sfn "${SYSTEM_KAFKA}" "${KAFKA_HOME}"
elif [ ! -d "${KAFKA_HOME}" ]; then
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
  echo "[setup] ${KAFKA_HOME} already exists, skipping install step"
fi

mkdir -p "${KAFKA_DATA}/zookeeper" "${KAFKA_DATA}/kafka" "${KAFKA_DATA}/logs"

# Config files may live under a root-owned /opt/kafka — use sudo only if needed.
ZK_CONF="${KAFKA_HOME}/config/zookeeper.properties"
SRV_CONF="${KAFKA_HOME}/config/server.properties"
if [ -w "${ZK_CONF}" ] && [ -w "${SRV_CONF}" ]; then
  SUDO=""
else
  SUDO="sudo"
  echo "[setup] config files are not writable as ${USER}; will use sudo for sed"
fi

${SUDO} sed -i "s|^dataDir=.*|dataDir=${KAFKA_DATA}/zookeeper|" "${ZK_CONF}"
${SUDO} sed -i "s|^log.dirs=.*|log.dirs=${KAFKA_DATA}/kafka|" "${SRV_CONF}"

if grep -q '^num.partitions=' "${SRV_CONF}"; then
  ${SUDO} sed -i 's|^num.partitions=.*|num.partitions=2|' "${SRV_CONF}"
else
  echo 'num.partitions=2' | ${SUDO} tee -a "${SRV_CONF}" >/dev/null
fi

echo "[setup] Kafka home:  ${KAFKA_HOME}  ($(readlink -f "${KAFKA_HOME}"))"
echo "[setup] Data dirs:   ${KAFKA_DATA}/{zookeeper,kafka,logs}"
echo "[setup] zookeeper.properties:"
grep -E '^(dataDir|clientPort)=' "${ZK_CONF}" || true
echo "[setup] server.properties:"
grep -E '^(log.dirs|num.partitions|listeners)=' "${SRV_CONF}" || true
echo "[setup] Java: $(java -version 2>&1 | head -n1)"
echo "[setup] DONE — next: bash scripts/02_start_kafka.sh"

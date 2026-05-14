#!/usr/bin/env bash
# One-command end-to-end demo runner.
#
# Open this file (or run `bash scripts/run_demo.sh`) on the UTM VM and it
# will:
#   1. activate the venv
#   2. download + preprocess the data (only first time)
#   3. train the ALS model     (only first time)
#   4. start Kafka in the background
#   5. create the 'reviews' topic if missing
#   6. start the Spark streaming consumer in the background
#   7. start the Kafka producer in the background (with --inject-spike so
#      the alert system is exercised)
#   8. keep running until you press Ctrl-C, then shut everything down
#      cleanly (Kafka, Zookeeper, streaming, producer)
#
# Logs are tailed live to per-process files under output/logs/.
set -euo pipefail

# ---- paths -----------------------------------------------------------------
ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
cd "${ROOT}"

LOG_DIR="${ROOT}/output/logs"
PID_DIR="${ROOT}/output/pids"
mkdir -p "${LOG_DIR}" "${PID_DIR}"

VENV="${ROOT}/.venv"
KAFKA_HOME="${HOME}/kafka"

# ---- prerequisites ---------------------------------------------------------
if [ ! -d "${VENV}" ]; then
  echo "[run_demo] creating venv with --system-site-packages (reuses pre-installed pyspark/streamlit/etc.)"
  python3 -m venv --system-site-packages "${VENV}"
fi
[ -d "${KAFKA_HOME}" ] || { echo "[run_demo] Kafka not installed. Run: bash scripts/01_setup_vm.sh"; exit 1; }

# shellcheck disable=SC1091
source "${VENV}/bin/activate"

# ---- shutdown handler ------------------------------------------------------
shutdown() {
  echo
  echo "[run_demo] shutting down..."
  for name in producer streaming kafka zookeeper; do
    pidfile="${PID_DIR}/${name}.pid"
    if [ -f "${pidfile}" ]; then
      pid=$(cat "${pidfile}")
      if kill -0 "${pid}" 2>/dev/null; then
        echo "[run_demo]   stopping ${name} (pid ${pid})..."
        kill -TERM "${pid}" 2>/dev/null || true
      fi
      rm -f "${pidfile}"
    fi
  done
  sleep 3
  # SIGKILL anything stubborn
  pkill -KILL -f 'kafka.Kafka' 2>/dev/null || true
  pkill -KILL -f 'QuorumPeerMain' 2>/dev/null || true
  echo "[run_demo] done."
}
trap shutdown EXIT INT TERM

start_bg() {
  local name="$1"; shift
  local log="${LOG_DIR}/${name}.log"
  echo "[run_demo] starting ${name}, log=${log}"
  ( "$@" ) >"${log}" 2>&1 &
  echo $! > "${PID_DIR}/${name}.pid"
}

wait_port() {
  local port="$1"; local secs="${2:-60}"
  for _ in $(seq 1 "${secs}"); do
    if (echo >/dev/tcp/127.0.0.1/"${port}") 2>/dev/null; then return 0; fi
    sleep 1
  done
  echo "[run_demo] port ${port} never came up"; return 1
}

# ---- step 1+2: data + model (skip if already present) ----------------------
if [ ! -f "${ROOT}/data/processed/ratings.parquet" ]; then
  echo "[run_demo] preparing data..."
  python src/download_data.py
else
  echo "[run_demo] data already prepared, skipping download."
fi

if [ ! -d "${ROOT}/models/als" ]; then
  echo "[run_demo] training ALS..."
  python src/train_als.py
else
  echo "[run_demo] ALS model already trained, skipping."
fi

# ---- step 3: Kafka + Zookeeper --------------------------------------------
start_bg zookeeper "${KAFKA_HOME}/bin/zookeeper-server-start.sh" \
  "${KAFKA_HOME}/config/zookeeper.properties"
wait_port 2181 60

start_bg kafka "${KAFKA_HOME}/bin/kafka-server-start.sh" \
  "${KAFKA_HOME}/config/server.properties"
wait_port 9092 60

# ---- step 4: topic ---------------------------------------------------------
bash "${ROOT}/scripts/03_create_topic.sh"

# ---- step 5: streaming consumer -------------------------------------------
start_bg streaming python -u src/spark_streaming.py
sleep 8  # give Spark a moment to register its queries

# ---- step 6: producer (with alert-triggering spike) ------------------------
start_bg producer python -u src/kafka_producer.py --rate 50 --inject-spike

# ---- run loop --------------------------------------------------------------
cat <<EOF

==============================================================================
 demo is now running. logs:
   tail -f ${LOG_DIR}/streaming.log
   tail -f ${LOG_DIR}/producer.log
   tail -f ${LOG_DIR}/kafka.log
 dashboard:
   streamlit run src/dashboard.py
 stop with Ctrl-C.
==============================================================================
EOF

# Block until Ctrl-C
while true; do
  sleep 30
  for name in zookeeper kafka streaming producer; do
    pid=$(cat "${PID_DIR}/${name}.pid" 2>/dev/null || echo "")
    if [ -n "${pid}" ] && ! kill -0 "${pid}" 2>/dev/null; then
      echo "[run_demo] WARNING: ${name} (pid ${pid}) has died — check ${LOG_DIR}/${name}.log"
    fi
  done
done

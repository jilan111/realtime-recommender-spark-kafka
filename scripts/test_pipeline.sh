#!/usr/bin/env bash
# End-to-end self-test. Runs the whole pipeline for 90 seconds, then checks
# that every part of the system produced the artefacts the assignment
# requires. Prints a PASS/FAIL summary at the end.
#
# Use this as a "did I really get everything working?" smoke check before
# the grading session. It is destructive only in the sense that it spawns
# (and kills) Kafka, the producer, and the streaming app — your trained
# model and Parquet data are not touched.
set -euo pipefail

ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
cd "${ROOT}"

DURATION_SECONDS="${DURATION_SECONDS:-90}"

LOG_DIR="${ROOT}/output/logs"
PID_DIR="${ROOT}/output/pids"
mkdir -p "${LOG_DIR}" "${PID_DIR}"

VENV="${ROOT}/.venv"
KAFKA_HOME="${HOME}/kafka"
KAFKA_DATA="${HOME}/kafka-data"

GREEN="\033[32m"; RED="\033[31m"; YELLOW="\033[33m"; NC="\033[0m"

pass=0; fail=0; warn=0
check() {
  local label="$1"; shift
  if "$@" >/dev/null 2>&1; then
    echo -e "  ${GREEN}PASS${NC}  ${label}"
    pass=$((pass+1))
  else
    echo -e "  ${RED}FAIL${NC}  ${label}"
    fail=$((fail+1))
  fi
}
warn_if() {
  local label="$1"; shift
  if "$@" >/dev/null 2>&1; then
    echo -e "  ${GREEN}PASS${NC}  ${label}"
    pass=$((pass+1))
  else
    echo -e "  ${YELLOW}WARN${NC}  ${label}"
    warn=$((warn+1))
  fi
}

if [ ! -d "${VENV}" ]; then
  echo "[test] creating venv with --system-site-packages (reuses pre-installed pyspark/streamlit/etc.)"
  python3 -m venv --system-site-packages "${VENV}"
fi
[ -d "${KAFKA_HOME}" ] || { echo "missing Kafka — run bash scripts/01_setup_vm.sh"; exit 2; }
# shellcheck disable=SC1091
source "${VENV}/bin/activate"

echo "==[ 0. preconditions ]======================================================"
check "config.py importable"                  python -c "import sys; sys.path.insert(0, 'src'); import config"
check "historical Parquet exists (>=500K rows)" python -c "
import sys, pandas as pd
sys.path.insert(0, 'src'); import config
df = pd.read_parquet(config.HISTORICAL_PARQUET, columns=['rating'])
assert len(df) >= 500_000, f'only {len(df)} rows'
"
check "ALS model directory present"           test -d models/als
check "ALS RMSE log meets target (<=1.5)"     python -c "
import json, pathlib
log = json.loads(pathlib.Path('models/tuning_log.json').read_text())
assert log['best_rmse'] <= log['target_rmse'], log
"

shutdown() {
  echo
  echo "[test] tearing down..."
  for name in producer streaming kafka zookeeper; do
    pidfile="${PID_DIR}/${name}.pid"
    [ -f "${pidfile}" ] && { kill -TERM "$(cat "${pidfile}")" 2>/dev/null || true; rm -f "${pidfile}"; }
  done
  sleep 3
  pkill -KILL -f 'kafka.Kafka' 2>/dev/null || true
  pkill -KILL -f 'QuorumPeerMain' 2>/dev/null || true
}
trap shutdown EXIT INT TERM

start_bg() {
  local name="$1"; shift
  ( "$@" ) >"${LOG_DIR}/${name}.log" 2>&1 &
  echo $! >"${PID_DIR}/${name}.pid"
}
wait_port() {
  local port="$1"; local secs="${2:-60}"
  for _ in $(seq 1 "${secs}"); do
    if (echo >/dev/tcp/127.0.0.1/"${port}") 2>/dev/null; then return 0; fi
    sleep 1
  done
  return 1
}

echo
echo "==[ 1. starting Kafka ]====================================================="
start_bg zookeeper "${KAFKA_HOME}/bin/zookeeper-server-start.sh" "${KAFKA_HOME}/config/zookeeper.properties"
check "Zookeeper port 2181 up"  wait_port 2181 60
start_bg kafka "${KAFKA_HOME}/bin/kafka-server-start.sh" "${KAFKA_HOME}/config/server.properties"
check "Kafka port 9092 up"      wait_port 9092 60

bash "${ROOT}/scripts/03_create_topic.sh" >/dev/null
check "topic 'reviews' has 2 partitions" bash -c "
  '${KAFKA_HOME}/bin/kafka-topics.sh' --bootstrap-server localhost:9092 --describe --topic reviews | grep -q 'PartitionCount: 2'
"

echo
echo "==[ 2. running pipeline for ${DURATION_SECONDS}s ]==========================="
# Clean previous output so we know everything below is fresh.
rm -rf output/window_metrics output/recommendations output/alerts output/checkpoints

start_bg streaming python -u src/spark_streaming.py
sleep 12  # let Spark register its queries

start_bg producer python -u src/kafka_producer.py --rate 80 --inject-spike --limit 6000

# Live progress dots
for _ in $(seq 1 "${DURATION_SECONDS}"); do printf '.'; sleep 1; done; echo

echo
echo "==[ 3. validating outputs ]================================================="
check "events landed in Kafka" bash -c "
  '${KAFKA_HOME}/bin/kafka-get-offsets.sh' \
    --bootstrap-server localhost:9092 --topic reviews --time -1 \
    | awk -F: '{s+=\$3} END{exit !(s>0)}'
"

check "item window metrics written" bash -c "ls output/window_metrics/items/*.parquet >/dev/null 2>&1"
check "user window metrics written" bash -c "ls output/window_metrics/users/*.parquet >/dev/null 2>&1"
check "recommendations written"     bash -c "ls output/recommendations/data/**/*.parquet >/dev/null 2>&1"
warn_if "alerts file present"       bash -c "find output/alerts -name '*.json' | head -1 | grep -q ."

check "average reco latency < 5s (bonus)" python -c "
import pandas as pd, glob
files = sorted(glob.glob('output/recommendations/data/**/*.parquet', recursive=True))
assert files, 'no reco files'
df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
mean_lat = df['latency_seconds'].mean()
print(f'mean latency = {mean_lat:.2f}s over {len(df)} recs')
assert mean_lat < 5.0
"

check "trending score has finite values" python -c "
import pandas as pd, glob, math
files = glob.glob('output/window_metrics/items/*.parquet')
assert files
df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
assert df['trending_score'].notna().any()
assert math.isfinite(df['trending_score'].max())
print('max trending_score =', df['trending_score'].max())
"

echo
echo "==========================================================================="
echo -e " ${GREEN}PASS${NC}: ${pass}    ${RED}FAIL${NC}: ${fail}    ${YELLOW}WARN${NC}: ${warn}"
echo "==========================================================================="

if [ "${fail}" -gt 0 ]; then
  echo "Some checks failed. Inspect logs under output/logs/."
  exit 1
fi
echo "All required checks passed."
exit 0

"""Shared configuration for every script in the project.

Keeping these constants in one place means the producer, the streaming
consumer, and the dashboard cannot drift out of sync on topic names or
window sizes.
"""
from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# ---- data ----------------------------------------------------------------
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
HISTORICAL_PARQUET = PROCESSED_DIR / "ratings.parquet"
STREAM_REPLAY_PARQUET = PROCESSED_DIR / "stream_replay.parquet"

# Mapping tables (string customer / product ids → ALS integer ids)
USER_INDEX_PARQUET = PROCESSED_DIR / "user_index.parquet"
ITEM_INDEX_PARQUET = PROCESSED_DIR / "item_index.parquet"

# Required dataset size for the assignment
MIN_ROWS = 500_000
# 1.5 M rows — comfortably above the 500 K requirement, fast enough to
# preprocess and ALS-train on a 4-core / 8 GB VM in ~10 minutes total.
SAMPLE_ROWS = 1_500_000

# ---- model ---------------------------------------------------------------
MODEL_DIR = PROJECT_ROOT / "models" / "als"
RMSE_TARGET = 1.5

# ---- kafka ---------------------------------------------------------------
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = "reviews"
KAFKA_PARTITIONS = 2

# ---- streaming -----------------------------------------------------------
WINDOW_DURATION = "30 seconds"
SLIDE_DURATION = "10 seconds"
WATERMARK_DELAY = "1 minute"  # events older than (max event time − 1 min) are dropped

# ---- alerts --------------------------------------------------------------
ALERT_RATING_THRESHOLD = 4.5
ALERT_MIN_INTERACTIONS = 5  # need at least 5 ratings in window before raising alert
ALERT_ACTIVITY_THRESHOLD = 8  # interactions/user/window that count as "spike"

# ---- outputs (streaming sinks) -------------------------------------------
OUTPUT_DIR = PROJECT_ROOT / "output"
WINDOW_METRICS_DIR = OUTPUT_DIR / "window_metrics"
RECOMMENDATIONS_DIR = OUTPUT_DIR / "recommendations"
ALERTS_DIR = OUTPUT_DIR / "alerts"
CHECKPOINT_DIR = OUTPUT_DIR / "checkpoints"

for d in (
    RAW_DIR,
    PROCESSED_DIR,
    MODEL_DIR.parent,
    OUTPUT_DIR,
    WINDOW_METRICS_DIR,
    RECOMMENDATIONS_DIR,
    ALERTS_DIR,
    CHECKPOINT_DIR,
):
    d.mkdir(parents=True, exist_ok=True)

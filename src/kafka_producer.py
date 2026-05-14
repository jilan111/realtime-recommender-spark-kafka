"""Replay Amazon reviews into Kafka as a live stream.

Reads `data/processed/stream_replay.parquet` (the most recent 10 % of rows
held out by download_data.py) and publishes one JSON event per row to the
Kafka topic configured in `config.py`.

Event format (matches the assignment spec):

    {
        "user_id": 10,
        "item_id": 200,
        "rating": 4.0,
        "timestamp": "2015-08-31T00:00:00"
    }

Two things worth knowing about the partitioning strategy:

* The topic is created with **2 partitions** (see scripts/03_create_topic.sh).
* We use `key=user_id` so that all events for the same user land on the same
  partition. This lets us compute per-user activity counters without needing
  cross-partition state, and keeps the ordering guarantees that ALS-style
  recommendation freshness depends on.

Run (in its own terminal):
    python src/kafka_producer.py                 # default rate = 50 msg/sec
    python src/kafka_producer.py --rate 200      # faster
    python src/kafka_producer.py --rate 0        # as-fast-as-possible
    python src/kafka_producer.py --inject-spike  # also inject a burst of
                                                 # high ratings on one item
                                                 # to demo the alert system
"""
from __future__ import annotations

import argparse
import json
import random
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from config import KAFKA_BOOTSTRAP, KAFKA_TOPIC, STREAM_REPLAY_PARQUET


def _make_producer() -> KafkaProducer:
    try:
        return KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: str(k).encode("utf-8"),
            acks="all",  # wait for both partitions' leaders → durable demo
            linger_ms=20,
        )
    except NoBrokersAvailable:
        sys.exit(
            f"Could not reach Kafka at {KAFKA_BOOTSTRAP}. "
            "Start it first with `bash scripts/02_start_kafka.sh`."
        )


def _iter_events(df: pd.DataFrame, *, loop: bool = True):
    """Yield event dicts forever (looping the dataframe)."""
    while True:
        for row in df.itertuples(index=False):
            yield {
                "user_id": int(row.als_user),
                "item_id": int(row.als_item),
                "rating": float(row.rating),
                "timestamp": pd.Timestamp(row.timestamp).isoformat(),
            }
        if not loop:
            return


def _spike_events(target_item: int, n: int = 30):
    """Generate a burst of 5-star ratings on one item to trip the alert."""
    now = datetime.now(timezone.utc).isoformat()
    for i in range(n):
        yield {
            "user_id": 10_000_000 + i,  # synthetic users, won't clash with index
            "item_id": int(target_item),
            "rating": 5.0,
            "timestamp": now,
        }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--rate",
        type=float,
        default=50.0,
        help="Messages per second (0 = unbounded). Default 50.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Stop after N events (0 = run forever).",
    )
    parser.add_argument(
        "--inject-spike",
        action="store_true",
        help="After 30 s, inject a burst of 5-star ratings on a single item.",
    )
    args = parser.parse_args()

    if not Path(STREAM_REPLAY_PARQUET).exists():
        sys.exit(
            f"{STREAM_REPLAY_PARQUET} not found. Run `python src/download_data.py` first."
        )

    df = pd.read_parquet(STREAM_REPLAY_PARQUET)
    # Shuffle to avoid a single item dominating the head of the stream.
    df = df.sample(frac=1, random_state=7).reset_index(drop=True)
    print(f"[producer] {len(df):,} events available for replay")

    producer = _make_producer()
    print(f"[producer] connected to {KAFKA_BOOTSTRAP}, topic={KAFKA_TOPIC}")

    sleep_s = (1.0 / args.rate) if args.rate > 0 else 0.0
    spike_target = int(df["als_item"].mode().iat[0])  # a popular item

    sent = 0
    spike_injected = False
    start = time.time()

    def _stop(signum, frame):  # noqa: ARG001
        print(f"\n[producer] received signal {signum}, flushing...")
        producer.flush(timeout=5)
        producer.close()
        print(f"[producer] sent {sent} events in {time.time() - start:.1f}s")
        sys.exit(0)

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    for event in _iter_events(df):
        # Replace the event timestamp with NOW so windowing works against
        # wall-clock time during the demo. (We keep the original timestamp
        # in the parquet for reproducibility.)
        event["timestamp"] = datetime.now(timezone.utc).isoformat()

        producer.send(KAFKA_TOPIC, key=event["user_id"], value=event)
        sent += 1

        if sent % 200 == 0:
            elapsed = time.time() - start
            print(f"[producer] sent={sent}  rate={sent / elapsed:.1f}/s")

        if args.inject_spike and not spike_injected and (time.time() - start) > 30:
            print(f"[producer] >>> injecting alert spike on item {spike_target}")
            for spike in _spike_events(spike_target):
                producer.send(KAFKA_TOPIC, key=spike["user_id"], value=spike)
                sent += 1
            spike_injected = True

        if args.limit and sent >= args.limit:
            break

        if sleep_s:
            # Add a touch of jitter so it does not look perfectly periodic.
            time.sleep(sleep_s * random.uniform(0.5, 1.5))

    producer.flush()
    producer.close()
    print(f"[producer] DONE — sent {sent} events")


if __name__ == "__main__":
    main()

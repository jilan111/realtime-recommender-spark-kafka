"""Download a slice of the Amazon US Customer Reviews dataset.

The full Kaggle dataset (`cynthiarempel/amazon-us-customer-reviews-dataset`) is
many GB across 40+ TSV files split by product category. For the mini project
we only need >= 500 K rows in the canonical (user, item, rating, timestamp)
shape, so this script:

1. Downloads the dataset via `kagglehub`.
2. Picks one category file that is comfortably > 500 K rows (Electronics).
3. Streams it in pandas chunks (the file is multi-GB, do not load whole).
4. Filters to the four columns we need + renames them to project schema.
5. Samples 800 K rows so the 80/20 split still leaves >= 500 K for training.
6. Builds integer index tables for ALS (it needs int user/item IDs).
7. Saves everything as Parquet under data/processed/.

Fallback:
    If `kagglehub` cannot reach Kaggle (no internet / no API token), drop a
    file called `amazon_reviews_us_Electronics_v1_00.tsv` (gzipped or plain)
    into data/raw/ manually and re-run; the script will find it and skip
    the network step.

Run:
    python src/download_data.py
"""
from __future__ import annotations

import gzip
import sys
from pathlib import Path

import pandas as pd

from config import (
    HISTORICAL_PARQUET,
    ITEM_INDEX_PARQUET,
    MIN_ROWS,
    PROCESSED_DIR,
    RAW_DIR,
    SAMPLE_ROWS,
    STREAM_REPLAY_PARQUET,
    USER_INDEX_PARQUET,
)

# We pick Electronics because it is a single ~1.7 GB file with ~3M reviews —
# plenty above 500 K, but not the multi-tens-of-millions Books category that
# would push the VM disk over the edge.
CATEGORY_FILE = "amazon_reviews_us_Electronics_v1_00.tsv"

# Map from Amazon column → assignment schema column
COLS = {
    "customer_id": "user_id",
    "product_id": "item_id",
    "star_rating": "rating",
    "review_date": "timestamp",
}


def _locate_tsv() -> Path:
    """Find the Electronics TSV, downloading via kagglehub if necessary."""
    # 1. Already in data/raw/?
    candidates = list(RAW_DIR.glob(f"{CATEGORY_FILE}*"))
    if candidates:
        print(f"[download_data] using local file {candidates[0]}")
        return candidates[0]

    # 2. Try kagglehub
    try:
        import kagglehub  # noqa: WPS433 — lazy so the fallback path stays usable
    except ImportError:
        sys.exit(
            "kagglehub not installed and no local TSV in data/raw/. "
            "Either `pip install kagglehub` or drop the Electronics TSV into data/raw/."
        )

    print("[download_data] downloading via kagglehub — this is large, be patient...")
    path = Path(
        kagglehub.dataset_download("cynthiarempel/amazon-us-customer-reviews-dataset")
    )
    print(f"[download_data] kagglehub cache at {path}")

    # Find the Electronics file inside the cache
    matches = list(path.rglob(f"{CATEGORY_FILE}*"))
    if not matches:
        sys.exit(
            f"Could not find {CATEGORY_FILE} under {path}. "
            "Inspect the directory and copy the desired TSV into data/raw/."
        )
    return matches[0]


def _read_in_chunks(tsv: Path, rows_needed: int) -> pd.DataFrame:
    """Stream the TSV in 200K-row chunks until we have rows_needed clean rows."""
    opener = gzip.open if tsv.suffix == ".gz" else open
    parts: list[pd.DataFrame] = []
    collected = 0
    # `on_bad_lines='skip'` is essential — the Amazon TSVs contain stray quotes.
    reader = pd.read_csv(
        tsv,
        sep="\t",
        usecols=list(COLS.keys()),
        dtype={"customer_id": "string", "product_id": "string", "star_rating": "float32"},
        parse_dates=["review_date"],
        chunksize=200_000,
        on_bad_lines="skip",
        quoting=3,  # csv.QUOTE_NONE
        engine="c",
    )
    for chunk in reader:
        chunk = chunk.dropna()
        chunk = chunk[(chunk["star_rating"] >= 1) & (chunk["star_rating"] <= 5)]
        parts.append(chunk)
        collected += len(chunk)
        print(f"[download_data]   collected {collected:,} clean rows...")
        if collected >= rows_needed:
            break
    df = pd.concat(parts, ignore_index=True)
    df = df.rename(columns=COLS)
    # Some weird customer_id values are non-numeric strings — keep them; we
    # build our own integer index below regardless.
    return df.head(rows_needed)


def _build_indexes(df: pd.DataFrame) -> pd.DataFrame:
    """Add integer als_user / als_item columns and persist the index tables."""
    user_index = (
        df["user_id"].drop_duplicates().reset_index(drop=True).rename("user_id").to_frame()
    )
    user_index["als_user"] = user_index.index.astype("int32")

    item_index = (
        df["item_id"].drop_duplicates().reset_index(drop=True).rename("item_id").to_frame()
    )
    item_index["als_item"] = item_index.index.astype("int32")

    df = df.merge(user_index, on="user_id", how="left")
    df = df.merge(item_index, on="item_id", how="left")

    user_index.to_parquet(USER_INDEX_PARQUET, index=False)
    item_index.to_parquet(ITEM_INDEX_PARQUET, index=False)
    print(
        f"[download_data] index tables saved: "
        f"{len(user_index):,} users, {len(item_index):,} items"
    )
    return df


def main() -> None:
    tsv = _locate_tsv()
    print(f"[download_data] reading {tsv} ...")

    df = _read_in_chunks(tsv, SAMPLE_ROWS)
    if len(df) < MIN_ROWS:
        sys.exit(
            f"Only collected {len(df):,} rows, need >= {MIN_ROWS:,}. "
            "Try a larger category file."
        )

    # Order chronologically — we will use the last 10% as a 'stream replay'
    # source so the live demo feels like recent events.
    df = df.sort_values("timestamp").reset_index(drop=True)

    df = _build_indexes(df)

    # Split: oldest 90 % for historical training, newest 10 % for streaming replay.
    split = int(len(df) * 0.9)
    historical, stream_replay = df.iloc[:split], df.iloc[split:]

    historical.to_parquet(HISTORICAL_PARQUET, index=False)
    stream_replay.to_parquet(STREAM_REPLAY_PARQUET, index=False)

    print(f"[download_data] historical → {HISTORICAL_PARQUET}  ({len(historical):,} rows)")
    print(f"[download_data] stream replay → {STREAM_REPLAY_PARQUET}  ({len(stream_replay):,} rows)")
    print(f"[download_data] processed dir: {PROCESSED_DIR}")


if __name__ == "__main__":
    main()

"""Real-time Streamlit dashboard for the streaming pipeline (BONUS).

Reads the parquet/JSON files that `spark_streaming.py` keeps writing under
`output/` and refreshes every few seconds so you can watch the system
breathe.

What it shows (>= 3 panels per the assignment bonus spec):
  1. **Top trending items**  — by trending_score over the latest window
  2. **Most active users**    — interactions per user in the latest window
  3. **Recent alerts**        — both item-trending and user-spike alerts
  4. **Latest recommendations** — sample of top-5 served to active users
  5. **Throughput**           — events / window (10s granularity)

Run (in its own terminal):
    streamlit run src/dashboard.py
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

# Hive-style partition: ".../batch_id=42/part-*.parquet"
_PARTITION_RE = re.compile(r"([^/=]+)=([^/]+)")

from config import (
    ALERTS_DIR,
    RECOMMENDATIONS_DIR,
    WINDOW_METRICS_DIR,
)

st.set_page_config(
    page_title="Real-Time Amazon Recommender",
    page_icon=":bar_chart:",
    layout="wide",
)

REFRESH_SECONDS = 5


@st.cache_data(ttl=REFRESH_SECONDS)
def _read_parquet_dir(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    files = list(path.rglob("*.parquet"))
    if not files:
        return pd.DataFrame()
    # Read newest 20 files to keep this responsive even after long runs
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    dfs = []
    for f in files[:20]:
        try:
            df = pd.read_parquet(f)
        except Exception:  # noqa: BLE001 — file may be mid-write
            continue
        # Recover Hive-style partition columns (e.g. batch_id=N) from the path.
        for col, val in _PARTITION_RE.findall(str(f)):
            if col not in df.columns:
                try:
                    df[col] = int(val)
                except ValueError:
                    df[col] = val
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


@st.cache_data(ttl=REFRESH_SECONDS)
def _read_json_dir(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    rows = []
    for f in path.rglob("*.json"):
        try:
            for line in f.read_text().splitlines():
                if line.strip():
                    rows.append(json.loads(line))
        except Exception:  # noqa: BLE001
            continue
    return pd.DataFrame(rows)


def _latest_window(df: pd.DataFrame, col: str = "window_end") -> pd.DataFrame:
    if df.empty or col not in df.columns:
        return df
    df[col] = pd.to_datetime(df[col])
    cutoff = df[col].max()
    return df[df[col] == cutoff]


# --------------------------------------------------------------------------- #
st.title("Real-Time Amazon Recommender — live dashboard")
st.caption(
    "Reads parquet/JSON sinks written by `src/spark_streaming.py`. "
    f"Auto-refreshes every {REFRESH_SECONDS}s."
)


@st.fragment(run_every=REFRESH_SECONDS)
def live_panels() -> None:
    item_metrics = _read_parquet_dir(WINDOW_METRICS_DIR / "items")
    user_metrics = _read_parquet_dir(WINDOW_METRICS_DIR / "users")
    recs = _read_parquet_dir(RECOMMENDATIONS_DIR / "data")
    item_alerts = _read_json_dir(ALERTS_DIR / "item")
    user_alerts = _read_json_dir(ALERTS_DIR / "user")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Item windows seen", f"{len(item_metrics):,}")
    c2.metric("User windows seen", f"{len(user_metrics):,}")
    c3.metric("Recommendations served", f"{len(recs):,}")
    c4.metric(
        "Alerts",
        f"{len(item_alerts) + len(user_alerts):,}",
        delta=f"{len(item_alerts)} item · {len(user_alerts)} user",
    )

    st.subheader("Top trending items — latest window")
    latest_items = _latest_window(item_metrics)
    if not latest_items.empty:
        top = latest_items.sort_values("trending_score", ascending=False).head(15)
        fig = px.bar(
            top,
            x="item_id",
            y="trending_score",
            hover_data=["interactions", "avg_rating"],
            color="avg_rating",
            color_continuous_scale="viridis",
        )
        fig.update_layout(xaxis_type="category", height=350)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Waiting for first window metrics ...")

    left, right = st.columns(2)
    with left:
        st.subheader("Most active users — latest window")
        latest_users = _latest_window(user_metrics)
        if not latest_users.empty:
            top_u = latest_users.sort_values("interactions", ascending=False).head(10)
            st.dataframe(top_u, use_container_width=True, hide_index=True)
        else:
            st.info("Waiting for user windows ...")
    with right:
        st.subheader("Recent recommendations")
        if not recs.empty and "batch_id" in recs.columns:
            latest_batch = recs[recs["batch_id"] == recs["batch_id"].max()]
            sample_user = latest_batch["user_id"].drop_duplicates().head(5).tolist()
            shown = latest_batch[latest_batch["user_id"].isin(sample_user)]
            st.dataframe(
                shown[["user_id", "item_id", "als_score", "trending_score", "blended_score", "rank"]]
                .sort_values(["user_id", "rank"]),
                use_container_width=True,
                hide_index=True,
            )
            st.caption(f"avg latency: {latest_batch['latency_seconds'].mean():.2f}s")
        else:
            st.info("Waiting for first recommendation batch ...")

    st.subheader("Recent alerts")
    alerts = pd.concat([item_alerts, user_alerts], ignore_index=True)
    if alerts.empty:
        st.info("No alerts yet — try running the producer with `--inject-spike`.")
    else:
        alerts["window_end"] = pd.to_datetime(alerts.get("window_end"))
        st.dataframe(
            alerts.sort_values("window_end", ascending=False).head(20),
            use_container_width=True,
            hide_index=True,
        )

    st.subheader("Throughput per window")
    if not item_metrics.empty:
        tput = (
            item_metrics.groupby("window_end")["interactions"]
            .sum()
            .reset_index()
            .sort_values("window_end")
            .tail(120)
        )
        fig = px.line(tput, x="window_end", y="interactions", markers=True)
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)


live_panels()

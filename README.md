# Mini Project 3 — Real-Time Recommendation System

**Big Data Analytics** · Zewail City University

| Field | Value |
| --- | --- |
| Domain | **E-commerce products** (Amazon US Customer Reviews) |
| Focus | **Real-Time Intelligence** (trending items, rating spikes, anomaly alerts) |
| Stack | Apache Spark 3.5, Spark MLlib (ALS), Kafka 3.7, Python 3.10, Streamlit |
| Target VM | Ubuntu 22.04 LTS (works on UTM/ARM and VirtualBox/x86) |

---

## What This Project Does

```
   Amazon reviews              Kafka (topic: reviews, 2 partitions)
   ─────────────────► Producer ─────────────────────────────────────┐
                                                                    ▼
                                          ┌──────────────────────────────────────┐
                                          │  Spark Structured Streaming          │
   Historical ratings                     │  • parse + watermark                 │
   ────────► train_als.py ──► ALS model ──┤  • 30s window / 10s slide           │
                                          │  • avg rating, interactions, trend   │
                                          │  • top-5 recommendations / user      │
                                          │  • alerts (rating, activity spikes)  │
                                          └──────────────┬───────────────────────┘
                                                         ▼
                                          parquet  +  JSON files  +  console
                                                         │
                                                         ▼
                                                  Streamlit dashboard
```

A team of two students will:
1. Download a slice of the Amazon US Customer Reviews dataset (≥ 500 K rows).
2. Train an ALS model on 80 / 20 split; tune until **RMSE ≤ 1.5**.
3. Stream events through Kafka into Spark Structured Streaming.
4. Compute windowed metrics + a **custom Trending Score**.
5. Combine ALS history with live activity to produce **top-5** recommendations per user (< 5 s latency).
6. Emit alerts for hot items and user activity spikes.
7. View everything in a real-time Streamlit dashboard.

---

## Quick Start (after setup)

Two single-command entry points — open one of these on the UTM VM and the
whole project runs itself:

```bash
# ----------------------------------------------------------------------
#  A) Full live demo (runs until you press Ctrl-C)
# ----------------------------------------------------------------------
cd ~/big-data-mini-project-3
bash scripts/run_demo.sh

# Then, in a second terminal (optional, bonus):
source .venv/bin/activate
streamlit run src/dashboard.py


# ----------------------------------------------------------------------
#  B) Self-test (~5 min — verifies the whole assignment in one go)
# ----------------------------------------------------------------------
bash scripts/test_pipeline.sh
```

Both scripts handle: download (1.5 M rows), ALS training, Kafka + Zookeeper
bring-up, topic creation, streaming consumer, producer (with an alert-triggering
spike), and clean shutdown on Ctrl-C. The self-test additionally validates that
every assignment artefact is produced and that the recommendation latency stays
below the 5 s bonus threshold.

If you prefer to run each stage by hand, the four-terminal workflow is:

```bash
source .venv/bin/activate
python src/download_data.py        # one-time
python src/train_als.py            # one-time
bash   scripts/02_start_kafka.sh   # terminal A
bash   scripts/03_create_topic.sh  # terminal B (once)
python src/spark_streaming.py      # terminal C
python src/kafka_producer.py       # terminal D
streamlit run src/dashboard.py     # terminal E (bonus)
```

Full setup instructions for a fresh VM are in [SETUP.md](SETUP.md).
The deliverable report is in [REPORT.md](REPORT.md).

---

## Repository Layout

```
.
├── README.md                # this file
├── SETUP.md                 # fresh-VM setup walkthrough
├── REPORT.md                # final project report (deliverable)
├── requirements.txt         # python deps
├── docker-compose.yml       # optional: Kafka via docker
├── scripts/
│   ├── 01_setup_vm.sh       # apt + java + kafka tarball
│   ├── 02_start_kafka.sh    # zookeeper + broker
│   ├── 03_create_topic.sh   # creates 'reviews' topic w/ 2 partitions
│   ├── 04_stop_kafka.sh
│   └── 05_smoke_test.sh     # send & consume one message
├── src/
│   ├── config.py            # shared settings
│   ├── download_data.py     # kagglehub → samples 500 K rows
│   ├── train_als.py         # batch ML: preprocess, train, eval, tune
│   ├── kafka_producer.py    # JSON event producer
│   ├── spark_streaming.py   # full streaming pipeline
│   └── dashboard.py         # Streamlit visualisation (bonus)
└── notebooks/
    └── main_notebook.ipynb  # end-to-end walkthrough notebook
```

---

## Team Customisation

We did **not** pick the default movie scenario; this project differs from a baseline submission in three concrete ways:

1. **Amazon reviews instead of MovieLens** — exposes a heavier long-tail item distribution and noisier ratings (1-vote items are common).
2. **Custom Trending Score** — we combine count, recency, and rating mean inside the sliding window instead of using simple counts.
3. **Hybrid recommendations** — ALS predictions for known users are re-ranked using live windowed popularity for cold-start mitigation.

See [REPORT.md](REPORT.md) for full discussion.

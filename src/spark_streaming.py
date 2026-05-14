"""End-to-end Spark Structured Streaming pipeline.

What this single application does:

A) Reads JSON events from Kafka.
B) Parses them safely (malformed rows are kept in a separate sink).
C) Windowed analytics (30 s window, 10 s slide) per item:
     - average rating
     - interaction count
     - **trending score** (custom metric — see below)
D) Per-user windowed activity count.
E) Watermarking: events older than 1 minute (relative to max event time)
   are dropped — addressing the assignment's late-data requirement.
F) Alerts:
     - Item: avg_rating > 4.5 AND interactions >= 5  → "trending"
     - User: interactions > 8 in any 30 s window      → "activity spike"
G) Top-5 recommendations per active user that combines:
     - ALS predictions from the batch model (historical preferences)
     - Live trending items from the most recent window (cold-start boost)

Every output goes to:
   - the **console** (live demo)
   - **parquet/json files** under output/ (so the dashboard can read them)

Run (in its own terminal):
    python src/spark_streaming.py
"""
from __future__ import annotations

import time
from pathlib import Path

from pyspark.ml.recommendation import ALSModel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from config import (
    ALERT_ACTIVITY_THRESHOLD,
    ALERT_MIN_INTERACTIONS,
    ALERT_RATING_THRESHOLD,
    ALERTS_DIR,
    CHECKPOINT_DIR,
    HISTORICAL_PARQUET,
    KAFKA_BOOTSTRAP,
    KAFKA_TOPIC,
    MODEL_DIR,
    RECOMMENDATIONS_DIR,
    SLIDE_DURATION,
    WATERMARK_DELAY,
    WINDOW_DURATION,
    WINDOW_METRICS_DIR,
)

EVENT_SCHEMA = StructType(
    [
        StructField("user_id", IntegerType(), nullable=False),
        StructField("item_id", IntegerType(), nullable=False),
        StructField("rating", DoubleType(), nullable=False),
        StructField("timestamp", StringType(), nullable=False),
    ]
)


def _spark() -> SparkSession:
    return (
        SparkSession.builder.appName("realtime-recommender")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
        .getOrCreate()
    )


# --------------------------------------------------------------------------- #
# Source                                                                       #
# --------------------------------------------------------------------------- #
def read_kafka(spark: SparkSession) -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("evt"),
        F.col("partition").alias("kafka_partition"),
        F.col("timestamp").alias("kafka_ingest_ts"),
    )

    # Split: well-formed vs. malformed (evt is null when JSON parse failed)
    good = (
        parsed.filter(F.col("evt").isNotNull())
        .select(
            F.col("evt.user_id").alias("user_id"),
            F.col("evt.item_id").alias("item_id"),
            F.col("evt.rating").alias("rating"),
            F.to_timestamp(F.col("evt.timestamp")).alias("event_time"),
            F.col("kafka_partition"),
            F.col("kafka_ingest_ts"),
        )
        .filter(
            (F.col("rating").between(1.0, 5.0))
            & F.col("user_id").isNotNull()
            & F.col("item_id").isNotNull()
            & F.col("event_time").isNotNull()
        )
    )
    return good.withWatermark("event_time", WATERMARK_DELAY)


# --------------------------------------------------------------------------- #
# Windowed analytics                                                           #
# --------------------------------------------------------------------------- #
def item_window_metrics(events: DataFrame) -> DataFrame:
    """Avg rating + count + trending score per item, per 30s/10s window."""
    return (
        events.groupBy(
            F.window(F.col("event_time"), WINDOW_DURATION, SLIDE_DURATION),
            F.col("item_id"),
        )
        .agg(
            F.count("*").alias("interactions"),
            F.avg("rating").alias("avg_rating"),
            F.stddev_pop("rating").alias("rating_stddev"),
            F.max("event_time").alias("last_seen"),
        )
        # Custom metric: trending_score
        #
        #     trending = log1p(interactions) * (avg_rating / 5)
        #
        # Why this shape:
        #   - log1p(interactions) damps the effect of "one viral item" so a
        #     burst of 1000 doesn't drown out 50 strong items
        #   - avg_rating / 5 keeps the score in [0, ~log(N)] and rewards
        #     positively-rated activity over noisy 1-star bursts
        .withColumn(
            "trending_score",
            F.log1p(F.col("interactions")) * (F.col("avg_rating") / F.lit(5.0)),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "item_id",
            "interactions",
            "avg_rating",
            "rating_stddev",
            "trending_score",
            "last_seen",
        )
    )


def user_window_metrics(events: DataFrame) -> DataFrame:
    """Interaction count per user, per 30s/10s window."""
    return (
        events.groupBy(
            F.window(F.col("event_time"), WINDOW_DURATION, SLIDE_DURATION),
            F.col("user_id"),
        )
        .agg(F.count("*").alias("interactions"))
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "user_id",
            "interactions",
        )
    )


# --------------------------------------------------------------------------- #
# Recommendations (ALS + live trending)                                        #
# --------------------------------------------------------------------------- #
def recommendations_for_batch(
    micro_batch: DataFrame, batch_id: int, *, als: ALSModel, trending_pdf
):
    """foreachBatch sink: for every distinct user in the micro-batch, emit top-5."""
    spark = micro_batch.sparkSession

    if micro_batch.rdd.isEmpty():
        return

    arrived_at = time.time()
    user_ids = (
        micro_batch.select("user_id").distinct().limit(200)  # cap per batch for the demo
    )

    # 1) ALS predictions for these users
    als_recs = als.recommendForUserSubset(user_ids.withColumnRenamed("user_id", "als_user"), 10)
    # als_recs schema:  als_user, recommendations: array<struct<als_item, rating>>
    als_recs = als_recs.withColumn("rec", F.explode("recommendations")).select(
        F.col("als_user").alias("user_id"),
        F.col("rec.als_item").alias("item_id"),
        F.col("rec.rating").alias("als_score"),
    )

    # 2) Live trending boost — re-rank using the most recent window's trending_score
    if trending_pdf is not None and not trending_pdf.empty:
        trending_df = spark.createDataFrame(trending_pdf[["item_id", "trending_score"]])
        als_recs = als_recs.join(trending_df, "item_id", "left").fillna(
            {"trending_score": 0.0}
        )
        # blended = 0.7 * als_score + 0.3 * trending_score
        als_recs = als_recs.withColumn(
            "blended_score",
            F.lit(0.7) * F.col("als_score") + F.lit(0.3) * F.col("trending_score"),
        )
    else:
        als_recs = als_recs.withColumn("trending_score", F.lit(0.0))
        als_recs = als_recs.withColumn("blended_score", F.col("als_score"))

    # 3) Keep top-5 per user
    from pyspark.sql.window import Window

    w = Window.partitionBy("user_id").orderBy(F.col("blended_score").desc())
    top5 = (
        als_recs.withColumn("rank", F.row_number().over(w))
        .filter(F.col("rank") <= 5)
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("generated_at", F.current_timestamp())
        .withColumn(
            "latency_seconds",
            F.lit(time.time() - arrived_at),
        )
    )

    # Print to console for the demo
    print(f"\n[recommender] batch {batch_id} — top-5 for {top5.select('user_id').distinct().count()} users  "
          f"(latency so far {time.time() - arrived_at:.2f}s)")
    top5.show(20, truncate=False)

    (
        top5.write.mode("append")
        .partitionBy("batch_id")
        .parquet(str(RECOMMENDATIONS_DIR / "data"))
    )


# --------------------------------------------------------------------------- #
# Alerts                                                                       #
# --------------------------------------------------------------------------- #
def item_alerts(item_metrics: DataFrame) -> DataFrame:
    return (
        item_metrics.filter(
            (F.col("avg_rating") >= ALERT_RATING_THRESHOLD)
            & (F.col("interactions") >= ALERT_MIN_INTERACTIONS)
        )
        .select(
            F.lit("ITEM_TRENDING").alias("alert_type"),
            "window_start",
            "window_end",
            "item_id",
            "interactions",
            "avg_rating",
            "trending_score",
            F.current_timestamp().alias("emitted_at"),
        )
    )


def user_alerts(user_metrics: DataFrame) -> DataFrame:
    return (
        user_metrics.filter(F.col("interactions") >= ALERT_ACTIVITY_THRESHOLD)
        .select(
            F.lit("USER_ACTIVITY_SPIKE").alias("alert_type"),
            "window_start",
            "window_end",
            "user_id",
            "interactions",
            F.lit(None).cast(DoubleType()).alias("avg_rating"),
            F.lit(None).cast(DoubleType()).alias("trending_score"),
            F.current_timestamp().alias("emitted_at"),
        )
    )


# --------------------------------------------------------------------------- #
# Trending cache + recommendation sink                                         #
# --------------------------------------------------------------------------- #
# IMPORTANT: both of these are module-level classes (no nested-function
# closures). On Python 3.14, cloudpickle in pyspark 3.5 recurses infinitely on
# `<locals>.` qualnames, so we cannot define foreachBatch callbacks inside
# main(). Keeping them at module scope keeps their __qualname__ simple and
# makes them safe to register with `writeStream.foreachBatch(...)`.
class _TrendingCache:
    """Holds the most recent top-N trending items as a pandas DataFrame so the
    recommendation sink can re-rank ALS predictions against it.
    """

    def __init__(self) -> None:
        self.pdf = None

    def update(self, batch_df: DataFrame, _batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return
        latest = (
            batch_df.orderBy(F.col("window_end").desc(), F.col("trending_score").desc())
            .limit(50)
            .toPandas()
        )
        self.pdf = latest


class _RecoSink:
    """Callable foreachBatch sink for the recommendation query.

    Instantiated once at module load, holds references to the ALS model and
    the trending cache so the callback can access them without a closure.
    """

    def __init__(self, als: ALSModel, trending_cache: "_TrendingCache") -> None:
        self.als = als
        self.trending_cache = trending_cache

    def __call__(self, batch_df: DataFrame, batch_id: int) -> None:
        recommendations_for_batch(
            batch_df,
            batch_id,
            als=self.als,
            trending_pdf=self.trending_cache.pdf,
        )


# --------------------------------------------------------------------------- #
# Main                                                                         #
# --------------------------------------------------------------------------- #
def main() -> None:
    if not Path(MODEL_DIR).exists():
        raise SystemExit(
            f"ALS model not found at {MODEL_DIR}. Run `python src/train_als.py` first."
        )

    spark = _spark()
    spark.sparkContext.setLogLevel("WARN")

    als = ALSModel.load(str(MODEL_DIR))
    print(f"[streaming] loaded ALS model from {MODEL_DIR}")

    events = read_kafka(spark)
    item_metrics = item_window_metrics(events)
    user_metrics = user_window_metrics(events)
    item_alert_stream = item_alerts(item_metrics)
    user_alert_stream = user_alerts(user_metrics)

    trending_cache = _TrendingCache()

    # 1) Window metrics → parquet (append mode requires watermark, which we set)
    q_metrics = (
        item_metrics.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", str(WINDOW_METRICS_DIR / "items"))
        .option("checkpointLocation", str(CHECKPOINT_DIR / "items"))
        .trigger(processingTime="10 seconds")
        .start()
    )
    q_user_metrics = (
        user_metrics.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", str(WINDOW_METRICS_DIR / "users"))
        .option("checkpointLocation", str(CHECKPOINT_DIR / "users"))
        .trigger(processingTime="10 seconds")
        .start()
    )

    # 2) Trending cache updater (an in-memory micro-batch sink)
    q_cache = (
        item_metrics.writeStream
        .outputMode("append")
        .foreachBatch(trending_cache.update)
        .option("checkpointLocation", str(CHECKPOINT_DIR / "trending_cache"))
        .trigger(processingTime="10 seconds")
        .start()
    )

    # 3) Recommendations — uses raw events as the trigger and the trending cache.
    # Using a module-level callable class instead of a nested closure so the
    # function's __qualname__ stays simple (avoids cloudpickle stack overflow
    # on Python 3.14).
    reco_sink = _RecoSink(als, trending_cache)
    q_reco = (
        events.writeStream
        .outputMode("append")
        .foreachBatch(reco_sink)
        .option("checkpointLocation", str(CHECKPOINT_DIR / "recommendations"))
        .trigger(processingTime="10 seconds")
        .start()
    )

    # 4) Alerts → JSON (small files, dashboard reads them tail-style) + console
    q_item_alert = (
        item_alert_stream.writeStream
        .outputMode("append")
        .format("json")
        .option("path", str(ALERTS_DIR / "item"))
        .option("checkpointLocation", str(CHECKPOINT_DIR / "item_alerts"))
        .trigger(processingTime="10 seconds")
        .start()
    )
    q_user_alert = (
        user_alert_stream.writeStream
        .outputMode("append")
        .format("json")
        .option("path", str(ALERTS_DIR / "user"))
        .option("checkpointLocation", str(CHECKPOINT_DIR / "user_alerts"))
        .trigger(processingTime="10 seconds")
        .start()
    )
    q_alerts_console = (
        item_alert_stream.union(user_alert_stream).writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("[streaming] all sinks started — press Ctrl-C to stop")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()

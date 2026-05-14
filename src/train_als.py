"""Batch ML: train an ALS model on the historical Amazon ratings.

What this script does, in order:

1. Loads `data/processed/ratings.parquet` (produced by download_data.py).
2. Splits 80 / 20 train / test.
3. Trains an ALS model with sensible defaults (rank=10, regParam=0.1).
4. Evaluates RMSE on the test set.
5. If RMSE > 1.5, performs a small grid search over rank, regParam, maxIter
   and keeps the best model — satisfies the assignment's tuning requirement.
6. Saves the final model under models/als/ along with a tuning log.

Run:
    python src/train_als.py
"""
from __future__ import annotations

import json
import shutil
import time
from pathlib import Path

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession

from config import HISTORICAL_PARQUET, MODEL_DIR, RMSE_TARGET


def _spark() -> SparkSession:
    return (
        SparkSession.builder.appName("als-batch-training")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "3g")
        .getOrCreate()
    )


def _train_one(train_df, test_df, *, rank: int, reg_param: float, max_iter: int):
    als = ALS(
        userCol="als_user",
        itemCol="als_item",
        ratingCol="rating",
        rank=rank,
        regParam=reg_param,
        maxIter=max_iter,
        coldStartStrategy="drop",  # drops NaN preds from new users/items in test
        nonnegative=True,
        seed=42,
    )
    t0 = time.time()
    model = als.fit(train_df)
    train_seconds = time.time() - t0

    predictions = model.transform(test_df)
    evaluator = RegressionEvaluator(
        metricName="rmse", labelCol="rating", predictionCol="prediction"
    )
    rmse = evaluator.evaluate(predictions)
    return model, rmse, train_seconds


def main() -> None:
    if not Path(HISTORICAL_PARQUET).exists():
        raise SystemExit(
            f"{HISTORICAL_PARQUET} not found. Run `python src/download_data.py` first."
        )

    spark = _spark()
    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.read.parquet(str(HISTORICAL_PARQUET))
        .select("als_user", "als_item", "rating")
        .repartition(8)
        .cache()
    )
    total = df.count()
    print(f"[train_als] loaded {total:,} ratings")

    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    train_df.cache(); test_df.cache()
    print(f"[train_als] split → train {train_df.count():,} / test {test_df.count():,}")

    # ---- 1) baseline -----------------------------------------------------
    print("[train_als] training baseline (rank=10, regParam=0.1, maxIter=10) ...")
    best_model, best_rmse, t = _train_one(train_df, test_df, rank=10, reg_param=0.1, max_iter=10)
    print(f"[train_als] baseline RMSE = {best_rmse:.4f}  (fit took {t:.1f}s)")

    tuning_log = [
        {"rank": 10, "regParam": 0.1, "maxIter": 10, "rmse": best_rmse, "fit_seconds": t}
    ]
    best_params = {"rank": 10, "regParam": 0.1, "maxIter": 10}

    # ---- 2) tune if needed ----------------------------------------------
    if best_rmse > RMSE_TARGET:
        print(
            f"[train_als] baseline RMSE > {RMSE_TARGET}, running grid search ..."
        )
        grid = [
            {"rank": 20, "regParam": 0.1, "maxIter": 15},
            {"rank": 30, "regParam": 0.1, "maxIter": 15},
            {"rank": 20, "regParam": 0.05, "maxIter": 15},
            {"rank": 30, "regParam": 0.2, "maxIter": 20},
        ]
        for params in grid:
            print(f"[train_als]   trying {params} ...")
            model, rmse, t = _train_one(train_df, test_df, **{k: params[k] for k in ("rank", "regParam", "maxIter")})  # noqa: E501
            params_with_metric = {**params, "rmse": rmse, "fit_seconds": t}
            print(f"[train_als]   → RMSE = {rmse:.4f}")
            tuning_log.append(params_with_metric)
            if rmse < best_rmse:
                best_model, best_rmse, best_params = model, rmse, params
            if best_rmse <= RMSE_TARGET:
                print(f"[train_als]   target RMSE reached, stopping early.")
                break
    else:
        print(f"[train_als] baseline already <= {RMSE_TARGET}, skipping tuning.")

    # ---- 3) save model + log --------------------------------------------
    if MODEL_DIR.exists():
        shutil.rmtree(MODEL_DIR)
    best_model.save(str(MODEL_DIR))
    log_path = MODEL_DIR.parent / "tuning_log.json"
    log_payload = {
        "best_rmse": best_rmse,
        "best_params": best_params,
        "target_rmse": RMSE_TARGET,
        "rows_train": train_df.count(),
        "rows_test": test_df.count(),
        "trials": tuning_log,
    }
    log_path.write_text(json.dumps(log_payload, indent=2))

    print()
    print(f"[train_als] DONE — best RMSE = {best_rmse:.4f}")
    print(f"[train_als] best params  = {best_params}")
    print(f"[train_als] model saved   → {MODEL_DIR}")
    print(f"[train_als] tuning log    → {log_path}")

    spark.stop()


if __name__ == "__main__":
    main()

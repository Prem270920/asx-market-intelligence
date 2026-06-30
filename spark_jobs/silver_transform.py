"""
ASX Market Intelligence Pipeline
Silver Layer — PySpark Window Function Transformations

Reads validated Bronze data and computes:
  1. Daily log returns
  2. Simple Moving Averages (SMA20, SMA50)
  3. Bollinger Bands (upper, lower, width)
  4. RSI (14-day Relative Strength Index)
  5. 30-day annualised volatility

All metrics use PySpark Window functions partitioned by ticker
and ordered by date — the standard pattern for time-series data.

"""

import sys
import argparse
import logging
import math
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("silver_transform")

def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("ASX-Silver-Transform")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


# ── Window definitions ─────────────────────────────────────────────
def get_windows():
    """Return all window specs used in this job."""
    base = Window.partitionBy("ticker").orderBy("Date")

    return {
        "base": base,
        "w20":  base.rowsBetween(-19, 0),   # 20-day window
        "w50":  base.rowsBetween(-49, 0),   # 50-day window
        "w14":  base.rowsBetween(-13, 0),   # 14-day window (RSI)
        "w30":  base.rowsBetween(-29, 0),   # 30-day window (volatility)
    }

# ── Transformation 1: Log returns ──────────────────────────────────
def add_log_returns(df: DataFrame, w) -> DataFrame:
    log.info("  → log_return")
    return df.withColumn(
        "log_return",
        F.log(F.col("Close") / F.lag("Close", 1).over(w["base"]))
    )


# ── Transformation 2: Simple Moving Averages ───────────────────────
def add_sma(df: DataFrame, w) -> DataFrame:
    log.info("  → sma_20, sma_50")
    return (
        df
        .withColumn("sma_20", F.avg("Close").over(w["w20"]))
        .withColumn("sma_50", F.avg("Close").over(w["w50"]))
    )


# ── Transformation 3: Bollinger Bands ──────────────────────────────
def add_bollinger_bands(df: DataFrame, w) -> DataFrame:
    log.info("  → bb_upper, bb_lower, bb_width")
    return (
        df
        .withColumn("bb_stddev", F.stddev("Close").over(w["w20"]))
        .withColumn("bb_upper", F.col("sma_20") + (F.lit(2.0) * F.col("bb_stddev")))
        .withColumn("bb_lower", F.col("sma_20") - (F.lit(2.0) * F.col("bb_stddev")))
        .withColumn("bb_width", F.col("bb_upper") - F.col("bb_lower"))
        .drop("bb_stddev")
    )


# ── Transformation 4: RSI (Relative Strength Index) ────────────────
def add_rsi(df: DataFrame, w) -> DataFrame:
    log.info("  → rsi_14")
    return (
        df
        .withColumn("price_change",
                    F.col("Close") - F.lag("Close", 1).over(w["base"]))
        .withColumn("gain",
                    F.when(F.col("price_change") > 0, F.col("price_change"))
                     .otherwise(F.lit(0.0)))
        .withColumn("loss",
                    F.when(F.col("price_change") < 0, -F.col("price_change"))
                     .otherwise(F.lit(0.0)))
        .withColumn("avg_gain", F.avg("gain").over(w["w14"]))
        .withColumn("avg_loss", F.avg("loss").over(w["w14"]))
        .withColumn(
            "rsi_14",
            F.when(F.col("avg_loss") == 0, F.lit(100.0))
             .otherwise(
                 F.lit(100.0) - (F.lit(100.0) / (F.lit(1.0) + F.col("avg_gain") / F.col("avg_loss")))
             )
        )
        .drop("price_change", "gain", "loss", "avg_gain", "avg_loss")
    )

# ── Transformation 5: Annualised Volatility ────────────────────────
def add_volatility(df: DataFrame, w) -> DataFrame:
    log.info("  → volatility_30d")
    sqrt_252 = math.sqrt(252)
    return df.withColumn(
        "volatility_30d",
        F.stddev("log_return").over(w["w30"]) * F.lit(sqrt_252)
    )

# ── Main pipeline ──────────────────────────────────────────────────
def transform(df: DataFrame) -> DataFrame:
    log.info("Computing window-based metrics:")
    w = get_windows()

    df = add_log_returns(df, w)
    df = add_sma(df, w)
    df = add_bollinger_bands(df, w)
    df = add_rsi(df, w)
    df = add_volatility(df, w)

    return (
        df
        .withColumn("log_return",      F.round("log_return", 6))
        .withColumn("sma_20",          F.round("sma_20", 4))
        .withColumn("sma_50",          F.round("sma_50", 4))
        .withColumn("bb_upper",        F.round("bb_upper", 4))
        .withColumn("bb_lower",        F.round("bb_lower", 4))
        .withColumn("bb_width",        F.round("bb_width", 4))
        .withColumn("rsi_14",          F.round("rsi_14", 2))
        .withColumn("volatility_30d",  F.round("volatility_30d", 4))
    )


def write_silver(df: DataFrame, output_dir: str):
    log.info(f"Writing Silver to: {output_dir}")
    (
        df
        .repartition(F.col("ticker"))
        .write
        .mode("overwrite")
        .partitionBy("ticker")
        .parquet(output_dir)
    )
    files = list(Path(output_dir).rglob("*.parquet"))
    log.info(f"  ✓ Written: {len(files)} partition files")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",  default="data/bronze/validated")
    parser.add_argument("--output", default="data/silver")
    args = parser.parse_args()

    log.info("ASX Silver Transform — starting")
    log.info(f"  Input:  {args.input}")
    log.info(f"  Output: {args.output}")

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(args.input)
    log.info(f"  Read: {df.count():,} rows across {df.select('ticker').distinct().count()} tickers")

    df_silver = transform(df)

    log.info("\nSample BHP rows after transformation:")
    df_silver.filter(F.col("ticker") == "BHP") \
             .orderBy(F.col("Date").desc()) \
             .select("Date", "Close", "sma_20", "sma_50", "rsi_14",
                     "bb_upper", "bb_lower", "volatility_30d", "log_return") \
             .show(5, truncate=False)

    write_silver(df_silver, args.output)

    spark.stop()
    log.info("✓ Silver transform complete.")


if __name__ == "__main__":
    main()
"""
ASX Market Intelligence Pipeline
Bronze Layer — PySpark Ingestion Job

What this does:
  1. Reads raw Parquet files using a strict StructType schema
  2. Runs 10 data quality checks (nulls, price sanity, OHLC integrity, etc.)
  3. Deduplicates on (ticker, Date) using a Window function
  4. Writes validated output partitioned by ticker
"""

import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DateType, DoubleType, LongType, TimestampNTZType
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("bronze_ingest")

# Schema
# TimestampNTZType = pandas saves dates as "timestamp without timezone" in Parquet

BRONZE_SCHEMA = StructType([
    StructField("Date",   TimestampNTZType(), nullable=False),
    StructField("Open",   DoubleType(),       nullable=True),
    StructField("High",   DoubleType(),       nullable=True),
    StructField("Low",    DoubleType(),       nullable=True),
    StructField("Close",  DoubleType(),       nullable=False),
    StructField("Volume", LongType(),         nullable=True),
    StructField("ticker", StringType(),       nullable=False),
    StructField("sector", StringType(),       nullable=True),
])

# Data quality thresholds 
DQ_CONFIG = {
    "max_null_rate_close":  0.01,    # Allow at most 1% null Close values
    "max_null_rate_volume": 0.05,    # Allow at most 5% null Volume values
    "min_close":  0.001,             # Minimum valid price (not $0 or negative)
    "max_close":  5000.0,            # Maximum valid price (CSL ~$285, MQG ~$190)
    "min_date":   "2020-01-01",      # No data before 2020
    "max_date":   datetime.today().strftime("%Y-%m-%d"),
    "min_rows_per_ticker": 200,      # At least ~1 year of trading data
}

def get_spark() -> SparkSession:
    """
    local[*] = use all available CPU cores.
    """
    return (
        SparkSession.builder
        .appName("ASX-Bronze-Ingest")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8") 
        .config("spark.driver.memory", "2g")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

def read_raw_parquet(spark: SparkSession, input_dir: str) -> DataFrame:
    """Read all Parquet files and enforce schema."""
    log.info(f"Reading from: {input_dir}")
    df = (
        spark.read
        .option("mergeSchema", "false")
        .schema(BRONZE_SCHEMA)
        .parquet(input_dir)
    )
    total   = df.count()
    tickers = df.select("ticker").distinct().count()
    log.info(f"  Loaded: {total:,} rows across {tickers} tickers")
    return df


def enforce_types(df: DataFrame) -> DataFrame:
    """
    Enforce strict data types and convert Date to pure calendar date.
    """
    return (
        df
        .withColumn("Date",   F.to_date(F.col("Date")))
        .withColumn("Open",   F.col("Open").cast(DoubleType()))
        .withColumn("High",   F.col("High").cast(DoubleType()))
        .withColumn("Low",    F.col("Low").cast(DoubleType()))
        .withColumn("Close",  F.col("Close").cast(DoubleType()))
        .withColumn("Volume", F.col("Volume").cast(LongType()))
        .withColumn("ticker", F.trim(F.col("ticker")))
        .withColumn("sector", F.trim(F.col("sector")))
        # Fix yfinance adjusted-price rounding artefacts
        .withColumn("High", F.when(F.col("High") < F.col("Close"), F.col("Close"))
                             .otherwise(F.col("High")))
        .withColumn("Low",  F.when(F.col("Low") > F.col("Close"), F.col("Close"))
                             .otherwise(F.col("Low")))

    )

def run_dq_checks(df: DataFrame, cfg: dict):
    """
    Data Quality checks. Each check produces a PASS or FAIL.
    """
    total   = df.count()
    passed  = []
    failed  = []

    def check(name, ok, detail):
        status = "PASS" if ok else "FAIL"
        log.info(f"  {status}  {name}: {detail}")
        (passed if ok else failed).append(name)

    log.info("----- DATA QUALITY REPORT -----")

    # 1. Any rows at all?
    check("row_count", total > 0, f"{total:,} total rows")

    # 2. Null rate in Close
    null_close     = df.filter(F.col("Close").isNull()).count()
    null_rate_c    = null_close / total
    check("null_close", null_rate_c <= cfg["max_null_rate_close"],
          f"{null_rate_c:.2%} null (max: {cfg['max_null_rate_close']:.0%})")

    # 3. Null rate in Volume
    null_vol    = df.filter(F.col("Volume").isNull()).count()
    null_rate_v = null_vol / total
    check("null_volume", null_rate_v <= cfg["max_null_rate_volume"],
          f"{null_rate_v:.2%} null (max: {cfg['max_null_rate_volume']:.0%})")

    # 4. Close price in valid range
    bad_price = df.filter(
        (F.col("Close") <= cfg["min_close"]) |
        (F.col("Close") >= cfg["max_close"])
    ).count()
    check("price_range", bad_price == 0,
          f"{bad_price} rows outside [{cfg['min_close']}, {cfg['max_close']}]")

    # 5. OHLC: High must be ≥ Low
    h_lt_l = df.filter(F.col("High") < F.col("Low")).count()
    check("high_gte_low", h_lt_l == 0, f"{h_lt_l} rows where High < Low")

    # 6. OHLC: High must be ≥ Close
    h_lt_c = df.filter(
    F.col("High").isNotNull() & (F.col("High") < F.col("Close") - 0.01)).count()
    check("high_gte_close", h_lt_c == 0, f"{h_lt_c} rows where High < Close")

    # 7. Date range
    dates = df.agg(F.min("Date").alias("mn"), F.max("Date").alias("mx")).collect()[0]
    check("date_range", str(dates["mn"]) >= cfg["min_date"],
          f"min={dates['mn']}  max={dates['mx']}")

    # 8. Minimum rows per ticker
    thin = (
        df.groupBy("ticker").count()
          .filter(F.col("count") < cfg["min_rows_per_ticker"])
          .count()
    )
    check("min_rows_per_ticker", thin == 0,
          f"{thin} tickers below {cfg['min_rows_per_ticker']}-row minimum")

    # 9. No future dates
    future = df.filter(F.col("Date") > F.lit(cfg["max_date"])).count()
    check("no_future_dates", future == 0, f"{future} rows with future Date")

    # 10. Volume non-negative
    neg_vol = df.filter(F.col("Volume").isNotNull() & (F.col("Volume") < 0)).count()
    check("volume_positive", neg_vol == 0, f"{neg_vol} rows with negative Volume")

    if failed:
        log.warning(f"FAILED checks: {failed}")
    else:
        log.info("All DQ checks passed ✓")

    return len(failed) == 0


def deduplicate(df: DataFrame) -> DataFrame:
    """
    Remove duplicate (ticker, Date) pairs using a Window function.

    Window functions are a key PySpark concept — they operate over
    a "window" of rows (like all rows for ticker='BHP') without
    collapsing them into a single row (unlike groupBy).

    Here: for each (ticker, Date), keep the row with highest Volume.
    """
    log.info("Deduplicating on (ticker, Date)...")
    before = df.count()

    # Window = "for each ticker+date, ordered by Volume descending"
    w = Window.partitionBy("ticker", "Date").orderBy(F.col("Volume").desc())

    df_clean = (
        df
        .withColumn("_rank", F.row_number().over(w))
        .filter(F.col("_rank") == 1)     # keep only the top-ranked row
        .drop("_rank")
    )

    after   = df_clean.count()
    removed = before - after
    log.info(f"  {before:,} → {after:,} rows  (removed {removed:,} duplicates)")
    return df_clean

def write_bronze(df: DataFrame, output_dir: str):
    """
    Write validated data partitioned by ticker.

    This is crucial for performance — when Silver reads only BHP data,
    Spark skips all other ticker folders entirely (partition pruning)
    """
    log.info(f"Writing partitioned Parquet to: {output_dir}")
    (
        df
        .repartition(F.col("ticker"))   # one partition file per ticker
        .write
        .mode("overwrite")
        .partitionBy("ticker")
        .parquet(output_dir)
    )
    files = list(Path(output_dir).rglob("*.parquet"))
    log.info(f"  ✓ Written: {len(files)} partition files")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",  default="data/bronze/raw")
    parser.add_argument("--output", default="data/bronze/validated")
    args = parser.parse_args()

    log.info("ASX Bronze Ingest — starting")
    log.info(f"  Input:  {args.input}")
    log.info(f"  Output: {args.output}")

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Step 1: Read
    df = read_raw_parquet(spark, args.input)
    df.printSchema()

    # Step 2: Fix types
    df = enforce_types(df)

    # Step 3: Data quality
    all_passed = run_dq_checks(df, DQ_CONFIG)

    # Step 4: Deduplicate
    df = deduplicate(df)

    # Step 5: Add metadata columns (useful for debugging in production)
    df = (
        df
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source",      F.lit("bronze_ingest_v1"))
    )

    # Step 6: Write
    write_bronze(df, args.output)

    # Step 7: Quick summary
    log.info("\n=== Sector Summary ===")
    df.groupBy("sector").agg(
        F.countDistinct("ticker").alias("tickers"),
        F.count("*").alias("rows"),
        F.round(F.avg("Close"), 2).alias("avg_close_AUD"),
    ).orderBy(F.col("rows").desc()).show(truncate=False)

    spark.stop()
    log.info("✓ Bronze ingest complete.")


if __name__ == "__main__":
    main()
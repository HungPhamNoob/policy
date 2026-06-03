#!/usr/bin/env python3
"""
processing/spark_batch.py
Spark Silver-to-Gold batch job.

Reads all Flink-generated feature JSONL files from GCS Silver, applies
schema validation, deduplication, and null-filling, then writes the
ML-ready dataset to GCS Gold as partitioned Parquet and flat CSV.

Data flow:
    GCS Silver (flink_features JSONL)
        -> schema enforcement + cleaning + deduplication
        -> GCS Gold (partitioned Parquet + CSV)
        -> H2O retraining input

This job is triggered by the Airflow model_retrain_hourly DAG.
"""

import logging
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# Path configuration (resolved from environment variables set by Airflow/Docker)
# ---------------------------------------------------------------------------

SILVER_PATH = os.getenv(
    "SILVER_FEATURES_PATH",
    "gs://big-data-group-4-silver/process/flink_features",
)

GOLD_RETRAIN_PATH = os.getenv(
    "GOLD_RETRAIN_PATH",
    "gs://big-data-group-4-gold/features/retrain",
)
GOLD_RETRAIN_PARQUET_PATH = os.getenv(
    "GOLD_RETRAIN_PARQUET_PATH",
    f"{GOLD_RETRAIN_PATH.rstrip('/')}/parquet",
)
GOLD_RETRAIN_CSV_PATH = os.getenv(
    "GOLD_RETRAIN_CSV_PATH",
    f"{GOLD_RETRAIN_PATH.rstrip('/')}/csv",
)

WRITE_PARTITIONS = int(os.getenv("SPARK_WRITE_PARTITIONS", "4"))
READ_PARTITIONS = int(os.getenv("SPARK_READ_PARTITIONS", "64"))
EXISTING_GOLD_PARQUET_PATH = os.getenv(
    "EXISTING_GOLD_PARQUET_PATH",
    GOLD_RETRAIN_PARQUET_PATH,
)
SILVER_DELTA_MANIFEST_PATH = os.getenv("SILVER_DELTA_MANIFEST_PATH", "")
SPARK_INCREMENTAL_MAX_FILES = int(os.getenv("SPARK_INCREMENTAL_MAX_FILES", "10000"))

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("spark-silver-to-gold")

# ---------------------------------------------------------------------------
# Schema – must match the output of processing.feature_engineering.build_features()
# ---------------------------------------------------------------------------

FEATURE_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("event_year", IntegerType(), nullable=False),
        StructField("event_time", StringType(), nullable=True),
        StructField("true_severity", IntegerType(), nullable=True),
        StructField("lat", DoubleType(), nullable=False),
        StructField("lon", DoubleType(), nullable=False),
        StructField("hour", IntegerType(), nullable=True),
        StructField("day_of_week", IntegerType(), nullable=True),
        StructField("is_weekend", IntegerType(), nullable=True),
        StructField("is_rush_hour", IntegerType(), nullable=True),
        StructField("weather_code", IntegerType(), nullable=True),
        StructField("temperature_f", FloatType(), nullable=True),
        StructField("humidity", FloatType(), nullable=True),
        StructField("wind_speed_mph", FloatType(), nullable=True),
        StructField("visibility_mi", FloatType(), nullable=True),
        StructField("road_type_code", IntegerType(), nullable=True),
        StructField("is_junction", IntegerType(), nullable=True),
        StructField("has_traffic_signal", IntegerType(), nullable=True),
        StructField("is_crossing", IntegerType(), nullable=True),
        StructField("is_roundabout", IntegerType(), nullable=True),
        StructField("is_stop", IntegerType(), nullable=True),
        StructField("is_station", IntegerType(), nullable=True),
        StructField("is_railway", IntegerType(), nullable=True),
        StructField("is_night", IntegerType(), nullable=True),
    ]
)

# Rows missing any of these fields are discarded before writing Gold.
REQUIRED_COLUMNS = [
    "event_id",
    "event_year",
    "true_severity",
    "lat",
    "lon",
]

# Sensible defaults for optional numeric feature columns.
FILL_DEFAULTS = {
    "hour": 0,
    "day_of_week": 0,
    "is_weekend": 0,
    "is_rush_hour": 0,
    "weather_code": 0,
    "temperature_f": 0.0,
    "humidity": 0.0,
    "wind_speed_mph": 0.0,
    "visibility_mi": 0.0,
    "road_type_code": 0,
    "is_junction": 0,
    "has_traffic_signal": 0,
    "is_crossing": 0,
    "is_roundabout": 0,
    "is_stop": 0,
    "is_station": 0,
    "is_railway": 0,
    "is_night": 0,
}

FEATURE_COLUMNS = [field.name for field in FEATURE_SCHEMA]


# ---------------------------------------------------------------------------
# Main job
# ---------------------------------------------------------------------------


def build_clean_feature_df(spark: SparkSession, input_paths=None):
    reader = (
        spark.read.option("recursiveFileLookup", "true")
        .schema(FEATURE_SCHEMA)
        .option("mode", "PERMISSIVE")
    )

    if input_paths:
        raw_df = reader.json(input_paths)
    else:
        raw_df = reader.json(SILVER_PATH)

    if READ_PARTITIONS > 0:
        raw_df = raw_df.coalesce(READ_PARTITIONS)

    if not raw_df.take(1):
        logger.warning("No Silver data found for this Spark run.")
        return raw_df, 0, 0

    logger.info("Dropping rows with missing required fields ...")
    clean_df = raw_df.dropna(subset=REQUIRED_COLUMNS)

    logger.info("Filtering to valid severity (1-4) and coordinate ranges ...")
    clean_df = (
        clean_df.filter(F.col("true_severity").between(1, 4))
        .filter(F.col("lat").between(-90, 90))
        .filter(F.col("lon").between(-180, 180))
    )

    logger.info("Filling null feature columns with sensible defaults ...")
    clean_df = clean_df.fillna(FILL_DEFAULTS)

    logger.info("Deduplicating by event_id ...")
    clean_df = clean_df.dropDuplicates(["event_id"])

    return clean_df, -1, -1


def load_delta_paths():
    if not SILVER_DELTA_MANIFEST_PATH:
        return []

    manifest_path = Path(SILVER_DELTA_MANIFEST_PATH)
    if not manifest_path.exists():
        logger.info("Silver delta manifest does not exist: %s", manifest_path)
        return []

    delta_paths = [
        line.strip()
        for line in manifest_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    logger.info("Silver delta manifest contains %s candidate files.", len(delta_paths))
    return delta_paths


def should_run_incremental(delta_paths):
    if not delta_paths:
        logger.info("Incremental Spark disabled because no delta manifest was provided.")
        return False

    if len(delta_paths) > SPARK_INCREMENTAL_MAX_FILES:
        logger.info(
            "Incremental Spark disabled because %s delta files exceed the limit of %s.",
            len(delta_paths),
            SPARK_INCREMENTAL_MAX_FILES,
        )
        return False

    gold_path = Path(EXISTING_GOLD_PARQUET_PATH)
    if not gold_path.exists():
        logger.info(
            "Incremental Spark disabled because the existing Gold Parquet path does not exist: %s",
            EXISTING_GOLD_PARQUET_PATH,
        )
        return False

    if not any(gold_path.rglob("*.parquet")):
        logger.info(
            "Incremental Spark disabled because no Parquet files were found under %s",
            EXISTING_GOLD_PARQUET_PATH,
        )
        return False

    return True


def main() -> None:
    logger.info("=" * 80)
    logger.info("Spark Silver -> Gold Parquet Job")
    logger.info("Silver path:       %s", SILVER_PATH)
    logger.info("Gold Parquet path: %s", GOLD_RETRAIN_PARQUET_PATH)
    logger.info("Gold CSV path:     %s", GOLD_RETRAIN_CSV_PATH)
    logger.info("Existing Gold:     %s", EXISTING_GOLD_PARQUET_PATH)
    logger.info("Silver delta file: %s", SILVER_DELTA_MANIFEST_PATH or "(not set)")
    logger.info("Read partitions:   %s", READ_PARTITIONS)
    logger.info("Write partitions:  %s", WRITE_PARTITIONS)
    logger.info("Incremental limit: %s files", SPARK_INCREMENTAL_MAX_FILES)
    logger.info("=" * 80)

    spark = (
        SparkSession.builder.appName("SilverToGoldRetrainDataset")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .config("spark.sql.files.ignoreMissingFiles", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        delta_paths = load_delta_paths()
        incremental_mode = should_run_incremental(delta_paths)

        if incremental_mode:
            logger.info("Running incremental Gold rebuild from existing Gold + new Silver delta.")
            existing_gold_df = spark.read.parquet(EXISTING_GOLD_PARQUET_PATH).select(
                *FEATURE_COLUMNS
            )
            existing_gold_df = existing_gold_df.cache()
            existing_gold_count = existing_gold_df.count()
            logger.info("Existing Gold rows available locally: %s", f"{existing_gold_count:,}")

            delta_df, total_raw, delta_clean_count = build_clean_feature_df(spark, delta_paths)
            if total_raw == 0:
                logger.warning(
                    "Delta Silver files produced no valid rows. Keeping the existing Gold snapshot."
                )
                clean_df = existing_gold_df
                final_count = existing_gold_count
            else:
                logger.info("Unioning existing Gold rows with the new Silver delta.")
                clean_df = (
                    existing_gold_df.unionByName(delta_df.select(*FEATURE_COLUMNS))
                    .dropDuplicates(["event_id"])
                    .cache()
                )
                final_count = clean_df.count()
                logger.info("Final cumulative Gold rows: %s", f"{final_count:,}")
        else:
            logger.info("Running full Gold rebuild from the complete Silver snapshot.")
            clean_df, total_raw, final_count = build_clean_feature_df(spark)

            if total_raw == 0:
                logger.warning("No Silver data found. Exiting without writing Gold output.")
                return

        logger.info("Writing Gold Parquet and CSV outputs ...")
        partitioned_df = clean_df.repartition(WRITE_PARTITIONS, "event_year").cache()
        partitioned_count = partitioned_df.count()
        logger.info("Rows ready to write: %s", f"{partitioned_count:,}")

        if partitioned_count == 0:
            logger.warning("No valid rows remain after cleaning. Exiting.")
            return

        partitioned_df.write.mode("overwrite").partitionBy("event_year").parquet(
            GOLD_RETRAIN_PARQUET_PATH
        )
        partitioned_df.write.mode("overwrite").option("header", "true").csv(
            GOLD_RETRAIN_CSV_PATH
        )

        logger.info("Gold Parquet written to: %s", GOLD_RETRAIN_PARQUET_PATH)
        logger.info("Gold CSV written to:     %s", GOLD_RETRAIN_CSV_PATH)

    finally:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
import argparse
import json
import os
import glob
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Transform raw telemetry to Silver and Gold with quality metrics")
    parser.add_argument("--process_date", required=True, help="Date to process (YYYY-MM-DD)")
    parser.add_argument("--raw_base", default=str(Path("data/raw")))
    parser.add_argument("--silver_base", default=str(Path("data/silver")))
    parser.add_argument("--gold_base", default=str(Path("data/gold")))
    parser.add_argument("--devices", default=str(Path("data/reference/devices.csv")))
    parser.add_argument("--metrics_dir", default=str(Path("data/metrics")))
    return parser.parse_args()


def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder.appName("energy-telemetry-transform")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    return spark


def telemetry_schema() -> T.StructType:
    return T.StructType(
        [
            T.StructField("ts_utc", T.StringType(), True),
            T.StructField("device_id", T.StringType(), True),
            T.StructField("site_id", T.StringType(), True),
            T.StructField("power_w", T.DoubleType(), True),
            T.StructField("energy_wh", T.DoubleType(), True),
            T.StructField("voltage_v", T.DoubleType(), True),
            T.StructField("current_a", T.DoubleType(), True),
            T.StructField("temperature_c", T.DoubleType(), True),
            T.StructField("soc_percent", T.DoubleType(), True),
            T.StructField("status", T.StringType(), True),
        ]
    )


def read_raw_for_date(spark: SparkSession, base: str, process_date: str) -> DataFrame:
    yyyy, mm, dd = process_date.split("-")
    path_glob = str(Path(base) / yyyy / mm / dd / "*" / "*.jsonl")
    paths = sorted(glob.glob(path_glob))
    if not paths:
        # Return empty DF with schema if no inputs, so downstream can handle gracefully
        return spark.createDataFrame(spark.sparkContext.emptyRDD(), telemetry_schema())
    return spark.read.schema(telemetry_schema()).json(paths)


def clean_and_enrich(df: DataFrame, spark: SparkSession, devices_csv: str) -> tuple[DataFrame, int]:
    # Convert and add event_date (handle Z timezone explicitly)
    df_with_ts = df.withColumn("ts_utc", F.col("ts_utc").cast(T.StringType()))
    df_cast = df_with_ts.withColumn("ts", F.to_timestamp("ts_utc", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    df_cast = df_cast.withColumn("event_date", F.to_date(F.col("ts")))

    # Duplicate counting BEFORE dropping
    total_rows = df_cast.count()
    distinct_keys = df_cast.select("device_id", "ts").distinct().count()
    duplicate_count = max(0, total_rows - distinct_keys)

    # Drop duplicate keys
    df_dedup = df_cast.dropDuplicates(["device_id", "ts"]).dropna(subset=["ts"])  # drop rows where ts couldn't parse

    # Cleaning rules
    df_clean = df_dedup.withColumn("power_w", F.when(F.col("power_w") < 0, F.lit(0.0)).otherwise(F.col("power_w")))
    df_clean = df_clean.withColumn(
        "soc_percent",
        F.when((F.col("soc_percent") < 0) | (F.col("soc_percent") > 100), F.lit(None)).otherwise(F.col("soc_percent")),
    )
    df_clean = df_clean.withColumn(
        "temperature_c",
        F.when((F.col("temperature_c") < -20) | (F.col("temperature_c") > 80), F.lit(None)).otherwise(F.col("temperature_c")),
    )

    # Enrichment from devices.csv
    devices = (
        spark.read.option("header", True)
        .csv(devices_csv)
        .withColumn("capacity_kwh", F.col("capacity_kwh").cast(T.DoubleType()))
        .select("device_id", "capacity_kwh")
    )
    df_enriched = df_clean.join(F.broadcast(devices), on="device_id", how="left")

    return df_enriched, duplicate_count


def write_silver(df: DataFrame, out_base: str) -> None:
    (
        df.repartition("event_date", "site_id")
        .write.mode("overwrite")
        .partitionBy("event_date", "site_id")
        .parquet(out_base)
    )


def write_gold(df: DataFrame, out_base: str) -> None:
    agg = (
        df.groupBy("event_date", "site_id")
        .agg(
            F.sum("energy_wh").alias("energy_wh_total"),
            F.max("power_w").alias("peak_power_w"),
            F.avg("temperature_c").alias("avg_temp_c"),
            (F.sum(F.when(F.col("status") == F.lit("OK"), F.lit(1)).otherwise(F.lit(0))) / F.count(F.lit(1)) * 100.0).alias(
                "uptime_percent"
            ),
            F.sum(F.when(F.col("status") != F.lit("OK"), F.lit(1)).otherwise(F.lit(0))).alias("anomaly_count"),
        )
    )

    (
        agg.repartition("event_date", "site_id")
        .write.mode("overwrite")
        .partitionBy("event_date", "site_id")
        .parquet(out_base)
    )


def write_metrics(metrics_dir: str, process_date: str, rows_total: int, duplicate_keys: int, null_power: int, null_soc: int) -> None:
    os.makedirs(metrics_dir, exist_ok=True)
    out_path = Path(metrics_dir) / f"quality_{process_date}.json"
    payload = {
        "process_date": process_date,
        "rows_total": int(rows_total),
        "duplicate_keys": int(duplicate_keys),
        "null_power": int(null_power),
        "null_soc": int(null_soc),
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def main() -> None:
    args = parse_args()

    spark = build_spark()

    try:
        raw_df = read_raw_for_date(spark, args.raw_base, args.process_date)

        if raw_df.rdd.isEmpty():
            print(f"No raw files found for {args.process_date} under {args.raw_base}. Nothing to do.")
            return

        df_silver, duplicate_count = clean_and_enrich(raw_df, spark, args.devices)

        # Compute metrics
        rows_total = raw_df.count()
        null_power = df_silver.filter(F.col("power_w").isNull()).count()
        null_soc = df_silver.filter(F.col("soc_percent").isNull()).count()

        # Write Silver and Gold
        write_silver(df_silver, args.silver_base)
        write_gold(df_silver, args.gold_base)

        # Quality metrics
        write_metrics(args.metrics_dir, args.process_date, rows_total, duplicate_count, null_power, null_soc)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

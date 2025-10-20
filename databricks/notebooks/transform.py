#!/usr/bin/env python3
# Databricks Runtime 14.x / PySpark 3.5+
import argparse
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark(app_name: str = "aero-transform"):
    return (
        SparkSession.builder.appName(app_name)
        # sensible defaults; Databricks will override many
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )


def parse_args():
    p = argparse.ArgumentParser(
        description="Aero Spark - Transform flight delays CSV to partitioned Parquet"
    )
    p.add_argument(
        "--input_path", required=True, help="Raw CSV path (local or s3://...)"
    )
    p.add_argument(
        "--output_path",
        required=True,
        help="Processed Parquet base path (local or s3://...)",
    )
    p.add_argument(
        "--run_date",
        required=True,
        help="Processing date YYYY-MM-DD (controls partition fields)",
    )
    p.add_argument(
        "--dry_run", action="store_true", help="Read & transform but do not write"
    )
    return p.parse_args()


def delay_bucket_expr(col):
    # Assumes arrival delay minutes; negative treated as on_time
    return (
        F.when(col <= 0, F.lit("on_time"))
        .when((col > 0) & (col <= 15), F.lit("0-15"))
        .when((col > 15) & (col <= 60), F.lit("16-60"))
        .otherwise(F.lit("60+"))
    )


def load_csv(spark: SparkSession, input_path: str):
    # Step 2 will enforce the 27-col schema. For now, infer to unblock dev.
    # Keep options consistent with expected raw files.
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("multiLine", "false")
        .option("mode", "DROPMALFORMED")
        .option("encoding", "UTF-8")
        .option("inferSchema", "true")  # TODO: replace with explicit schema in Step 2
        .load(input_path)
    )
    return df


def transform(df, run_date_str: str):
    # Basic cleaning: trim all string columns
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, F.trim(F.col(c)))

    # Canonicalize expected columns (case-insensitive search)
    def find(col_name, candidates):
        # find first match in df columns by case-insensitive equality
        lower = {c.lower(): c for c in df.columns}
        for cand in candidates:
            if cand.lower() in lower:
                return lower[cand.lower()]
        return None

    origin_col = find("origin", ["origin", "Origin", "ORIGIN"])
    dest_col = find("dest", ["dest", "Dest", "DEST", "destination", "DESTINATION"])
    arrdelay = find(
        "arr_delay",
        ["arr_delay", "ArrDelay", "ARR_DELAY", "ARR_DELAY_NEW", "arrival_delay"],
    )

    # Ensure canonical column names used later
    if origin_col != "origin":
        df = df.withColumnRenamed(origin_col, "origin")
        origin_col = "origin"
    if dest_col != "dest":
        df = df.withColumnRenamed(dest_col, "dest")
        dest_col = "dest"

    if origin_col is None or dest_col is None:
        raise ValueError(
            "Could not locate ORIGIN/DEST columns in input CSV. Confirm Phase 1 schema and headers."
        )

    # Route and delay_bucket
    df = df.withColumn("route", F.concat_ws("→", F.col(origin_col), F.col(dest_col)))

    if arrdelay is None:
        # if missing, assume 0 for bucket (we'll enforce schema in Step 2)
        df = df.withColumn("delay_bucket", F.lit("on_time"))
    else:
        # cast to integer minutes; nulls become on_time
        df = df.withColumn(arrdelay, F.col(arrdelay).cast(T.IntegerType()))
        df = df.withColumn(
            "delay_bucket", delay_bucket_expr(F.coalesce(F.col(arrdelay), F.lit(0)))
        )

    # Partition fields from run_date
    run_dt = datetime.strptime(run_date_str, "%Y-%m-%d")
    df = (
        df.withColumn("year", F.lit(run_dt.year).cast(T.IntegerType()))
        .withColumn("month", F.lit(run_dt.month).cast(T.IntegerType()))
        .withColumn("day", F.lit(run_dt.day).cast(T.IntegerType()))
    )

    return df


def write_parquet(df, output_path: str):
    (
        df.write.mode("overwrite")
        .partitionBy("year", "month", "day", "origin")  # origin must exist per schema
        .format("parquet")
        .save(output_path)
    )


def main():
    args = parse_args()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(
        f"[INFO] Starting transform | input={args.input_path} | output={args.output_path} | run_date={args.run_date} | dry_run={args.dry_run}"
    )

    df_raw = load_csv(spark, args.input_path)
    print(f"[INFO] Read rows={df_raw.count()} cols={len(df_raw.columns)}")
    print("[INFO] Columns:", df_raw.columns)

    df_tr = transform(df_raw, args.run_date)

    # Validate required columns for partitioning
    req = ["origin", "year", "month", "day", "route", "delay_bucket"]

    missing = [c for c in req if c not in df_tr.columns]
    if missing:
        raise ValueError(f"Transformed DataFrame missing required columns: {missing}")

    print("[INFO] Transformed schema:")
    print(df_tr.printSchema())

    if args.dry_run:
        print("[INFO] Dry run enabled; skipping write.")
    else:
        write_parquet(df_tr, args.output_path)
        print(f"[INFO] Wrote Parquet to {args.output_path}")

    spark.stop()
    print("[INFO] Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
# Databricks Runtime 14.x / PySpark 3.5+
import argparse
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from schema import SCHEMA, SCHEMA_COLUMNS

try:
    from pyspark.errors import PySparkAttributeError
except Exception:

    class PySparkAttributeError(Exception): ...


# ----------------------------
# Cleaning helpers (Step 3)
# ----------------------------
NULL_STRINGS = {"", "na", "n/a", "null", "none", "nan", "NAN"}


def to_null_if_blank(col):
    # trim, lower, and map known null-likes to actual null
    return F.when(
        F.lower(F.trim(col)).isin([s.lower() for s in NULL_STRINGS]), F.lit(None)
    ).otherwise(F.trim(col))


def safe_int(col):
    # Remove commas/whitespace, cast to int
    return F.regexp_replace(F.trim(col), r"[,\s]", "").cast(T.IntegerType())


def safe_double(col):
    # Remove commas/whitespace, cast to double
    c = F.regexp_replace(F.trim(col), r"[,\s]", "")
    return c.cast(T.DoubleType())


def safe_date(col):
    # Try common patterns: yyyy-MM-dd, yyyyMMdd, MM/dd/yyyy
    return F.coalesce(
        F.to_date(col, "yyyy-MM-dd"),
        F.to_date(col, "yyyyMMdd"),
        F.to_date(col, "MM/dd/yyyy"),
    )


def safe_ts(col):
    # Try common timestamp patterns
    return F.coalesce(
        F.to_timestamp(col, "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(col, "yyyy-MM-dd'T'HH:mm:ss"),
        F.to_timestamp(col, "MM/dd/yyyy HH:mm"),
    )


def safe_bool(col):
    # Accept 1/0, true/false, yes/no, y/n (case-insensitive); otherwise cast
    s = F.lower(F.trim(col.cast(T.StringType())))
    return (
        F.when(s.isin("1", "true", "t", "yes", "y"), F.lit(True))
        .when(s.isin("0", "false", "f", "no", "n"), F.lit(False))
        .otherwise(col.cast(T.BooleanType()))
    )


# ----------------------------


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
    # Strict, schema-enforced read
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("multiLine", "false")
        .option("mode", "FAILFAST")  # be strict now
        .option("encoding", "UTF-8")
        .schema(SCHEMA)  # enforce the 27-col schema
        .load(input_path)
    )
    # Keep only expected columns and order (guards against extras)
    df = df.select(*SCHEMA_COLUMNS)

    # (Optional guard) detect unexpected columns that slipped through
    extra = [c for c in df.columns if c not in SCHEMA_COLUMNS]
    if extra:
        raise ValueError(f"Unexpected columns in raw file: {extra}")

    return df


def transform(df, run_date_str: str):
    # 1) String normalization for all string columns
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, to_null_if_blank(F.col(c)))

    # 2) Standardize key categorical fields
    if "origin" in df.columns:
        df = df.withColumn("origin", F.upper(F.col("origin")))
    if "dest" in df.columns:
        df = df.withColumn("dest", F.upper(F.col("dest")))
    if "airline_code" in df.columns:
        df = df.withColumn("airline_code", F.upper(F.col("airline_code")))

    # 3) Type-safe casts for common numeric/date/time columns (match schema.md)
    cast_plan_int = [
        "dep_delay_minutes",
        "arr_delay_minutes",
        "taxi_out_minutes",
        "taxi_in_minutes",
        "air_time_minutes",
        "distance_miles",
    ]
    for col_name in cast_plan_int:
        if col_name in df.columns:
            df = df.withColumn(col_name, safe_int(F.col(col_name)))

    cast_plan_ts = [
        "scheduled_dep",
        "dep_time",
        "scheduled_arr",
        "arr_time",
        "wheels_off",
        "wheels_on",
        "ingest_ts",
    ]
    for col_name in cast_plan_ts:
        if col_name in df.columns:
            df = df.withColumn(col_name, safe_ts(F.col(col_name)))

    if "flight_date" in df.columns:
        df = df.withColumn("flight_date", safe_date(F.col("flight_date")))

    # booleans
    for bcol in ["cancelled", "diverted"]:
        if bcol in df.columns:
            df = df.withColumn(bcol, safe_bool(F.col(bcol)))

    # 4) Ensure required route columns exist
    if "origin" not in df.columns or "dest" not in df.columns:
        raise ValueError("Schema must contain 'origin' and 'dest' columns.")

    # Route
    df = df.withColumn("route", F.concat_ws("→", F.col("origin"), F.col("dest")))

    # 5) Delay bucket using canonical schema column
    delay_col = "arr_delay_minutes"
    if delay_col in df.columns:
        df = df.withColumn(delay_col, F.col(delay_col).cast(T.IntegerType()))
        df = df.withColumn(
            "delay_bucket",
            delay_bucket_expr(F.coalesce(F.col(delay_col), F.lit(0))),
        )
    else:
        df = df.withColumn("delay_bucket", F.lit("on_time"))

    # 6) Partition fields from run_date
    run_dt = datetime.strptime(run_date_str, "%Y-%m-%d")
    df = (
        df.withColumn("year", F.lit(run_dt.year).cast(T.IntegerType()))
        .withColumn("month", F.lit(run_dt.month).cast(T.IntegerType()))
        .withColumn("day", F.lit(run_dt.day).cast(T.IntegerType()))
    )

    # 7) Ensure non-null origin for partitioning (sentinel)
    df = df.withColumn("origin", F.coalesce(F.col("origin"), F.lit("UNK")))

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
    try:
        # Accessing sparkContext directly is blocked on serverless; use a guard.
        sc = object.__getattribute__(spark, "sparkContext")
        sc.setLogLevel("WARN")
    except PySparkAttributeError:
        pass
    except Exception:
        # Any other environment that blocks JVM access—just continue.
        pass

    print(
        f"[INFO] Starting transform | input={args.input_path} | output={args.output_path} | run_date={args.run_date} | dry_run={args.dry_run}"
    )

    df_raw = load_csv(spark, args.input_path)
    # Safer logging than a full count on large datasets
    print(f"[INFO] Columns={len(df_raw.columns)} -> {df_raw.columns}")
    print("[INFO] Sample rows (up to 10):")
    for r in df_raw.take(10):
        print(r)

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

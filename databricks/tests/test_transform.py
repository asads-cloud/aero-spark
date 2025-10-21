import os
import sys
from datetime import date

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

# Make job modules importable when running from repo root
sys.path.insert(0, os.path.abspath("databricks/notebooks"))
from schema import SCHEMA, SCHEMA_COLUMNS
from transform import transform


@pytest.fixture(scope="session")
def spark():
    py = os.environ.get("PYSPARK_PYTHON", sys.executable)

    spark = (
        SparkSession.builder.appName("aero-transform-tests")
        .master("local[*]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.pyspark.python", py)
        .config("spark.pyspark.driver.python", py)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    # IMPORTANT: yield (not return)
    yield spark
    spark.stop()


def mk_df(spark):
    """
    Build a tiny in-memory DataFrame using the canonical schema column names.
    Values match the Python types required by SCHEMA.
    """
    rows = [
        # on-time (<=0), lower-case origin/dest to test upper-casing
        Row(
            flight_date=date(2025, 10, 20),
            origin="jfk",
            dest="lhr",
            arr_delay_minutes=0,
            dep_delay_minutes=5,
            distance_miles=3450,
        ),
        # small delay (0-15)
        Row(
            flight_date=date(2025, 10, 20),
            origin="LAX",
            dest="SFO",
            arr_delay_minutes=7,
            dep_delay_minutes=9,
            distance_miles=337,
        ),
        # medium delay (16-60)
        Row(
            flight_date=date(2025, 10, 20),
            origin="SEA",
            dest="DEN",
            arr_delay_minutes=45,
            dep_delay_minutes=50,
            distance_miles=1024,
        ),
        # big delay (60+)
        Row(
            flight_date=date(2025, 10, 20),
            origin="BOS",
            dest="MIA",
            arr_delay_minutes=120,
            dep_delay_minutes=130,
            distance_miles=1258,
        ),
        # missing origin -> should become UNK
        Row(
            flight_date=date(2025, 10, 20),
            origin=None,
            dest="ORD",
            arr_delay_minutes=-3,
            dep_delay_minutes=0,
            distance_miles=720,
        ),
        # blanks that should become null (None is fine for typed schema)
        Row(
            flight_date=date(2025, 10, 20),
            origin="  ",
            dest="  na  ",
            arr_delay_minutes=None,
            dep_delay_minutes=None,
            distance_miles=None,
        ),
    ]

    # Expand each Row to full 27-column shape in the same order as SCHEMA_COLUMNS
    def to_full(row):
        base = row.asDict()
        return Row(**{c: base.get(c) for c in SCHEMA_COLUMNS})

    return spark.createDataFrame([to_full(r) for r in rows], schema=SCHEMA)


def test_transform_basics(spark, tmp_path):
    run_date = "2025-10-21"
    df_raw = mk_df(spark)

    df_tr = transform(df_raw, run_date)

    # --- schema presence ---
    for col in ["route", "delay_bucket", "year", "month", "day"]:
        assert col in df_tr.columns
    assert set(SCHEMA_COLUMNS).issubset(set(df_tr.columns))

    # --- partition fields from run_date ---
    parts = df_tr.select("year", "month", "day").distinct().collect()
    assert len(parts) == 1
    (p,) = parts
    assert p["year"] == 2025 and p["month"] == 10 and p["day"] == 21

    # --- route construction & upper-casing ---
    routes = {r["route"] for r in df_tr.select("route").distinct().collect()}
    assert "JFK→LHR" in routes
    assert "LAX→SFO" in routes

    # --- delay_bucket bins ---
    delay_col = "arr_delay_minutes"
    buckets = dict(
        df_tr.select(delay_col, "delay_bucket")
        .na.fill(-999, [delay_col])
        .rdd.map(lambda r: (r[delay_col], r["delay_bucket"]))
        .collect()
    )
    assert buckets[0] == "on_time"
    assert buckets[7] == "0-15"
    assert buckets[45] == "16-60"
    assert buckets[120] == "60+"
    # null/negative treated as on_time
    assert buckets[-999] == "on_time"
    assert buckets[-3] == "on_time"

    # --- origin non-null for partitioning (UNK sentinel allowed) ---
    assert df_tr.filter(F.col("origin").isNull()).count() == 0
    origins = {r["origin"] for r in df_tr.select("origin").distinct().collect()}
    assert "UNK" in origins


def test_idempotent_write_read(spark, tmp_path):
    """Write partitioned output locally and read it back."""
    outp = tmp_path / "processed" / "flight_delays"
    df_raw = mk_df(spark)
    df_tr = transform(df_raw, "2025-10-21")
    (
        df_tr.write.mode("overwrite")
        .partitionBy("year", "month", "day", "origin")
        .parquet(str(outp))
    )

    df_back = spark.read.parquet(str(outp))
    assert df_back.count() == df_tr.count()
    assert df_back.select("year", "month", "day", "origin").distinct().count() >= 1

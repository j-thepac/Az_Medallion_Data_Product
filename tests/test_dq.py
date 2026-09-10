from datetime import datetime, timezone

from pyspark.sql import SparkSession, types as T

from src.dq import classify_quality


def test_coordinate_quarantine_and_city_warning():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("test-dq")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        schema = T.StructType(
            [
                T.StructField("chapter_id", T.StringType()),
                T.StructField("chapter_name", T.StringType()),
                T.StructField("city", T.StringType()),
                T.StructField("state", T.StringType()),
                T.StructField("longitude", T.StringType()),
                T.StructField("latitude", T.StringType()),
                T.StructField("ingest_run_id", T.StringType()),
                T.StructField("ingested_at", T.TimestampType()),
                T.StructField("raw_response", T.StringType()),
            ]
        )
        rows = [
            ("CA-1", "Clean", "Austin", "CA", "-120", "35", "test", datetime.now(timezone.utc), "{}"),
            ("OR-1", "Warn", "UNKNOWN", "OR", "-120", "45", "test", datetime.now(timezone.utc), "{}"),
            ("WA-1", "Bad", "Seattle", "WA", "181", "47", "test", datetime.now(timezone.utc), "{}"),
        ]
        quarantine, silver = classify_quality(spark, spark.createDataFrame(rows, schema))
        assert quarantine.select("chapter_id").collect()[0][0] == "WA-1"
        assert silver.where("dq_status = 'WARNING'").count() == 1
        assert silver.where("dq_status = 'OK'").count() == 1
    finally:
        spark.stop()

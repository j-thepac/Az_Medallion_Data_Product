import json
import shutil
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, functions as F, types as T

try:
    from src.settings import (
        BRONZE_ROOT,
        FIXTURE_PATH,
        GOLD_ROOT,
        QUARANTINE_ROOT,
        SILVER_ROOT,
    )
    from src.dq import classify_quality
except ModuleNotFoundError:
    from settings import (
        BRONZE_ROOT,
        FIXTURE_PATH,
        GOLD_ROOT,
        QUARANTINE_ROOT,
        SILVER_ROOT,
    )
    from dq import classify_quality

TRANSFORM_SCHEMA = T.StructType(
    [
        T.StructField("chapter_id", T.StringType(), True),
        T.StructField("chapter_name", T.StringType(), True),
        T.StructField("city", T.StringType(), True),
        T.StructField("state", T.StringType(), True),
        T.StructField("longitude", T.StringType(), True),
        T.StructField("latitude", T.StringType(), True),
        T.StructField("ingest_run_id", T.StringType(), False),
        T.StructField("ingested_at", T.TimestampType(), False),
        T.StructField("raw_response", T.StringType(), False),
    ]
)


def create_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("university-chapters-medallion")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def load_features(response_path: Path, run_id: str, ingested_at: datetime) -> list[dict]:
    response_data = json.loads(response_path.read_text(encoding="utf-8"))
    return [
        {
            "chapter_id": feature.get("attributes", {}).get("ChapterID"),
            "chapter_name": feature.get("attributes", {}).get("University_Chapter"),
            "city": feature.get("attributes", {}).get("City"),
            "state": feature.get("attributes", {}).get("State"),
            "longitude": str(feature.get("geometry", {}).get("x"))
            if feature.get("geometry", {}).get("x") is not None
            else None,
            "latitude": str(feature.get("geometry", {}).get("y"))
            if feature.get("geometry", {}).get("y") is not None
            else None,
            "ingest_run_id": run_id,
            "ingested_at": ingested_at,
            "raw_response": json.dumps(feature, sort_keys=True),
        }
        for feature in response_data.get("features", [])
    ]


def latest_bronze_run() -> tuple[Path, str, datetime]:
    runs = [path for path in BRONZE_ROOT.glob("*") if path.is_dir()]
    if not runs:
        raise RuntimeError("No Bronze run found. Run python -m src.ingest first.")

    latest_run = None
    for run_path in runs:
        metadata_path = run_path / "metadata.json"
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        if latest_run is None or metadata["ingested_at"] > latest_run[1]["ingested_at"]:
            latest_run = (run_path, metadata)

    run_path, metadata = latest_run
    ingested_at = datetime.fromisoformat(metadata["ingested_at"])
    return run_path / "payload.json", metadata["ingest_run_id"], ingested_at


def seed_fixture() -> tuple[Path, str, datetime]:
    run_id = "fixture-" + uuid.uuid4().hex[:8]
    ingested_at = datetime.now(timezone.utc)
    run_path = BRONZE_ROOT / run_id
    run_path.mkdir(parents=True, exist_ok=False)
    shutil.copyfile(FIXTURE_PATH, run_path / "payload.json")
    metadata = {
        "ingest_run_id": run_id,
        "ingested_at": ingested_at.isoformat(),
        "source_url": "fixture://dq_test_data.json",
        "rows_in": len(json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))["features"]),
    }
    (run_path / "metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    return run_path / "payload.json", run_id, ingested_at


def calculate_metrics(quarantine_df, silver_df, rows_in: int) -> dict[str, int]:
    quarantined_count = quarantine_df.count()
    warned_count = silver_df.where(F.col("dq_status") == "WARNING").count()
    ok_count = silver_df.where(F.col("dq_status") == "OK").count()
    return {
        "rows_in": rows_in,
        "rows_quarantined": quarantined_count,
        "rows_warned": warned_count,
        "rows_ok": ok_count,
    }


def write_quarantine(quarantine_df, run_id: str) -> None:
    if quarantine_df.count():
        quarantine_df.write.mode("overwrite").parquet(str(QUARANTINE_ROOT / run_id))


def run(use_fixture: bool = False) -> dict[str, int]:
    selected_run = seed_fixture() if use_fixture else latest_bronze_run()
    response_path, run_id, ingested_at = selected_run
    spark = create_spark()
    try:
        rows = load_features(response_path, run_id, ingested_at)
        if not rows:
            raise RuntimeError("Bronze batch is empty; Gold was not published")
        bronze_df = spark.createDataFrame(rows, schema=TRANSFORM_SCHEMA)\
            .withColumn("city", F.trim(F.col("city")))
        rows_in = bronze_df.count()
        quarantine_df, silver_df = classify_quality(spark, bronze_df)
        metrics = calculate_metrics(quarantine_df, silver_df, rows_in)
        write_quarantine(quarantine_df, run_id)
        silver_df.write.mode("overwrite").parquet(str(SILVER_ROOT))
        gold_df = silver_df.select(
            "chapter_id",
            "chapter_name",
            "city",
            "state",
            "longitude",
            "latitude",
            "dq_status",
            "dq_warnings",
        )
        gold_df.write.mode("overwrite").parquet(str(GOLD_ROOT))
        print(json.dumps(metrics, sort_keys=True))
        return metrics
    finally:
        spark.stop()


def main() -> None:
    use_fixture = "--fixture" in sys.argv
    run(use_fixture=use_fixture)


if __name__ == "__main__":
    main()

from pathlib import Path

from src import transform


def test_fixture_pipeline(tmp_path, monkeypatch):
    data_root = tmp_path / "data"
    monkeypatch.setattr(transform, "BRONZE_ROOT", data_root / "bronze" / "university_chapters")
    monkeypatch.setattr(transform, "SILVER_ROOT", data_root / "silver" / "university_chapters")
    monkeypatch.setattr(transform, "GOLD_ROOT", data_root / "gold" / "university_chapters" / "v1")
    monkeypatch.setattr(transform, "QUARANTINE_ROOT", data_root / "quarantine" / "university_chapters")
    monkeypatch.setattr(transform, "FIXTURE_PATH", Path(__file__).parents[1] / "fixtures" / "dq_test_data.json")

    metrics = transform.run(use_fixture=True)

    assert metrics == {"rows_in": 3, "rows_quarantined": 1, "rows_warned": 1, "rows_ok": 1}
    gold_path = data_root / "gold" / "university_chapters" / "v1"
    quarantine_root = data_root / "quarantine" / "university_chapters"
    spark = transform.create_spark()
    try:
        gold = spark.read.parquet(str(gold_path))
        assert gold.where("chapter_id = 'WA-0001'").count() == 0
        assert gold.where("dq_status = 'WARNING'").count() == 1
        assert set(gold.columns) == {
            "chapter_id",
            "chapter_name",
            "city",
            "state",
            "longitude",
            "latitude",
            "dq_status",
            "dq_warnings",
        }
        assert list(quarantine_root.glob("fixture-*/part-*.parquet"))
    finally:
        spark.stop()

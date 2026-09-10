from pyspark.sql import DataFrame, SparkSession, Window, functions as F


def classify_quality(spark: SparkSession, bronze_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    classified = (
        bronze_df
        .withColumn("city", F.when(F.col("city") == "", None).otherwise(F.col("city")))
        .withColumn("longitude", F.expr("try_cast(longitude AS DOUBLE)"))
        .withColumn("latitude", F.expr("try_cast(latitude AS DOUBLE)"))
        .withColumn(
            "quarantine_reason",
            F.when(
                F.col("longitude").isNull()| F.col("latitude").isNull()| ~F.col("longitude").between(-180, 180)| ~F.col("latitude").between(-90, 90),
                F.lit("INVALID_COORDINATES"),
            ),
        )
        .withColumn(
            "warning_reason",
            F.when(
                F.col("city").isNull() | (F.upper(F.col("city")) == "UNKNOWN"),
                F.lit("MISSING_OR_UNKNOWN_CITY"),
            ),
        )
        .withColumn(
            "record_rank",
            F.row_number().over(
                Window.partitionBy("chapter_id").orderBy(F.col("ingested_at").desc())
            ),
        )
        .where(F.col("record_rank") == 1)
        .drop("record_rank")
    )

    quarantine_df = classified.where("quarantine_reason IS NOT NULL").select(
        "chapter_id",
        "chapter_name",
        "city",
        "state",
        "longitude",
        "latitude",
        "quarantine_reason",
        "ingest_run_id",
        "ingested_at",
        "raw_response",
    )
    silver_df = (
        classified.where("quarantine_reason IS NULL")
        .withColumn(
            "dq_status",
            F.when(F.col("warning_reason").isNull(), "OK").otherwise("WARNING"),
        )
        .withColumn(
            "dq_warnings",
            F.when(
                F.col("warning_reason").isNull(),
                F.array(),
            ).otherwise(F.array(F.col("warning_reason"))),
        )
        .select(
            "chapter_id",
            "chapter_name",
            "city",
            "state",
            "longitude",
            "latitude",
            "dq_status",
            "dq_warnings",
            "ingest_run_id",
            "ingested_at",
        )
    )
    return quarantine_df, silver_df

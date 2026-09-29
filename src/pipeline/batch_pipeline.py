from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F  # pyright: ignore[reportMissingImports]

from ..ingestion.tlc_reader import read_tlc_parquet
from ..quality.metrics import build_quality_metrics
from ..validation.rules import apply_quality_rules
from ..validation.schema import normalize_column_names


def run_batch_pipeline(
    spark: SparkSession,
    input_path: str,
    source_file: str,
    batch_id: str,
    schema_version: str,
    curated_output: str,
    quarantine_output: str,
    metrics_output: str,
) -> dict:
    """
    Execute one TransitFlow batch.

    Returns summary counts for the run ledger.
    """

    trips = read_tlc_parquet(
        spark=spark,
        input_path=input_path,
    )

    trips = normalize_column_names(trips)
    trips = apply_quality_rules(trips)

    ingested_at = datetime.now(timezone.utc).isoformat()

    trips = (
        trips
        .withColumn("source_file", F.lit(source_file))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("schema_version", F.lit(schema_version))
        .withColumn("ingested_at", F.lit(ingested_at))
    )

    print("\n=== TRANSITFLOW INGESTION ===")
    print(f"Rows read: {trips.count():,}")

    print("\n=== QUALITY SUMMARY ===")

    (
        trips.groupBy("validation_status")
        .count()
        .orderBy("validation_status")
        .show(truncate=False)
    )

    quality_metrics = build_quality_metrics(trips)

    print("\n=== QUALITY METRICS ===")
    quality_metrics.show(truncate=False)

    curated = trips.filter(
        F.col("validation_status").isin("VALID", "WARNING")
    )

    quarantine = trips.filter(
        F.col("validation_status") == "QUARANTINE"
    )

    print("\n=== WRITING OUTPUTS ===")

    curated.write.mode("overwrite").parquet(
        curated_output
    )

    quarantine.write.mode("overwrite").parquet(
        quarantine_output
    )

    quality_metrics.write.mode("overwrite").json(
        metrics_output
    )

    print(f"Curated output written to: {curated_output}")
    print(f"Quarantine output written to: {quarantine_output}")
    print(f"Metrics output written to: {metrics_output}")

    status_counts = {
        row["validation_status"]: row["count"]
        for row in (
            trips.groupBy("validation_status")
            .count()
            .collect()
        )
    }

    return {
        "rows_read": sum(status_counts.values()),
        "valid_records": status_counts.get("VALID", 0),
        "warning_records": status_counts.get("WARNING", 0),
        "quarantined_records": status_counts.get(
            "QUARANTINE",
            0,
        ),
    }
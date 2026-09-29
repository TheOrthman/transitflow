from importlib import import_module
from typing import Any


def build_quality_metrics(df: Any) -> Any:
    """
    Build summary quality metrics for a validated TransitFlow batch.

    Expected input columns:
        validation_status
        quarantine_reasons
        warning_reasons
    """

    F = import_module("pyspark.sql.functions")

    return (
        df.agg(
            F.count("*").alias("total_records"),

            F.sum(
                F.when(
                    F.col("validation_status") == "VALID",
                    1,
                ).otherwise(0)
            ).alias("valid_records"),

            F.sum(
                F.when(
                    F.col("validation_status") == "WARNING",
                    1,
                ).otherwise(0)
            ).alias("warning_records"),

            F.sum(
                F.when(
                    F.col("validation_status") == "QUARANTINE",
                    1,
                ).otherwise(0)
            ).alias("quarantined_records"),
        )
        .withColumn(
            "quality_pass_rate",
            F.round(
                (
                    F.col("valid_records")
                    + F.col("warning_records")
                )
                / F.col("total_records")
                * 100,
                4,
            ),
        )
        .withColumn(
            "quarantine_rate",
            F.round(
                F.col("quarantined_records")
                / F.col("total_records")
                * 100,
                4,
            ),
        )
    )
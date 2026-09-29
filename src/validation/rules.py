from pyspark.sql import DataFrame  # pyright: ignore[reportMissingImports]
from pyspark.sql import functions as F  # pyright: ignore[reportMissingImports]


def apply_quality_rules(df: DataFrame) -> DataFrame:
    """
    Apply TransitFlow data-quality rules.

    QUARANTINE:
        Records that violate core structural/business requirements.

    WARNING:
        Records that may be unusual but could still be legitimate.

    VALID:
        Records with no detected issues.
    """

    df = df.withColumn(
        "quarantine_reasons",
        F.array(
            F.when(
                F.col("pickup_datetime").isNull(),
                F.lit("MISSING_PICKUP_DATETIME"),
            ),
            F.when(
                F.col("dropoff_datetime").isNull(),
                F.lit("MISSING_DROPOFF_DATETIME"),
            ),
            F.when(
                F.col("dropoff_datetime") < F.col("pickup_datetime"),
                F.lit("INVALID_TIME_RANGE"),
            ),
            F.when(
                F.col("trip_distance") < 0,
                F.lit("NEGATIVE_TRIP_DISTANCE"),
            ),
            F.when(
                F.col("pickup_location_id").isNull(),
                F.lit("MISSING_PICKUP_LOCATION"),
            ),
            F.when(
                F.col("dropoff_location_id").isNull(),
                F.lit("MISSING_DROPOFF_LOCATION"),
            ),
        ),
    )

    df = df.withColumn(
        "warning_reasons",
        F.array(
            F.when(
                F.col("trip_distance") == 0,
                F.lit("ZERO_TRIP_DISTANCE"),
            ),
            F.when(
                F.col("fare_amount") < 0,
                F.lit("NEGATIVE_FARE_AMOUNT"),
            ),
            F.when(
                F.col("total_amount") < 0,
                F.lit("NEGATIVE_TOTAL_AMOUNT"),
            ),
            F.when(
                (
                    F.unix_timestamp("dropoff_datetime")
                    - F.unix_timestamp("pickup_datetime")
                ) > 24 * 60 * 60,
                F.lit("TRIP_OVER_24_HOURS"),
            ),
        ),
    )

    # Remove NULL entries created by rules that did not fire.
    df = df.withColumn(
        "quarantine_reasons",
        F.expr(
            "filter(quarantine_reasons, x -> x is not null)"
        ),
    )

    df = df.withColumn(
        "warning_reasons",
        F.expr(
            "filter(warning_reasons, x -> x is not null)"
        ),
    )

    df = df.withColumn(
        "validation_status",
        F.when(
            F.size("quarantine_reasons") > 0,
            F.lit("QUARANTINE"),
        )
        .when(
            F.size("warning_reasons") > 0,
            F.lit("WARNING"),
        )
        .otherwise(F.lit("VALID")),
    )

    return df
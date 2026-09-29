from datetime import datetime
import sys
from pathlib import Path

import pytest  # type: ignore[import-not-found]
from pyspark.sql import SparkSession  # type: ignore[import-not-found]

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.validation.rules import apply_quality_rules  # type: ignore[import-not-found]


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("TransitFlowQualityRuleTests")
        .getOrCreate()
    )

    yield session

    session.stop()


def build_df(spark, rows):
    schema = """
        pickup_datetime TIMESTAMP,
        dropoff_datetime TIMESTAMP,
        trip_distance DOUBLE,
        pickup_location_id INT,
        dropoff_location_id INT,
        fare_amount DOUBLE,
        total_amount DOUBLE
    """

    return spark.createDataFrame(rows, schema=schema)


def test_valid_trip_is_classified_valid(spark):
    df = build_df(
        spark,
        [
            (
                datetime(2025, 8, 1, 10, 0),
                datetime(2025, 8, 1, 10, 20),
                3.5,
                100,
                200,
                15.0,
                20.0,
            )
        ],
    )

    result = apply_quality_rules(df).collect()[0]

    assert result.validation_status == "VALID"
    assert result.quarantine_reasons == []
    assert result.warning_reasons == []


def test_negative_trip_distance_is_quarantined(spark):
    df = build_df(
        spark,
        [
            (
                datetime(2025, 8, 1, 10, 0),
                datetime(2025, 8, 1, 10, 20),
                -1.0,
                100,
                200,
                15.0,
                20.0,
            )
        ],
    )

    result = apply_quality_rules(df).collect()[0]

    assert result.validation_status == "QUARANTINE"
    assert "NEGATIVE_TRIP_DISTANCE" in result.quarantine_reasons


def test_invalid_time_range_is_quarantined(spark):
    df = build_df(
        spark,
        [
            (
                datetime(2025, 8, 1, 11, 0),
                datetime(2025, 8, 1, 10, 0),
                2.0,
                100,
                200,
                10.0,
                12.0,
            )
        ],
    )

    result = apply_quality_rules(df).collect()[0]

    assert result.validation_status == "QUARANTINE"
    assert "INVALID_TIME_RANGE" in result.quarantine_reasons


def test_zero_distance_is_warning(spark):
    df = build_df(
        spark,
        [
            (
                datetime(2025, 8, 1, 10, 0),
                datetime(2025, 8, 1, 10, 10),
                0.0,
                100,
                200,
                10.0,
                12.0,
            )
        ],
    )

    result = apply_quality_rules(df).collect()[0]

    assert result.validation_status == "WARNING"
    assert "ZERO_TRIP_DISTANCE" in result.warning_reasons
    assert result.quarantine_reasons == []


def test_negative_fare_is_warning_not_quarantine(spark):
    df = build_df(
        spark,
        [
            (
                datetime(2025, 8, 1, 10, 0),
                datetime(2025, 8, 1, 10, 10),
                2.0,
                100,
                200,
                -5.0,
                12.0,
            )
        ],
    )

    result = apply_quality_rules(df).collect()[0]

    assert result.validation_status == "WARNING"
    assert "NEGATIVE_FARE_AMOUNT" in result.warning_reasons
    assert result.quarantine_reasons == []


def test_quarantine_takes_priority_over_warning(spark):
    df = build_df(
        spark,
        [
            (
                datetime(2025, 8, 1, 11, 0),
                datetime(2025, 8, 1, 10, 0),
                0.0,
                100,
                200,
                -5.0,
                12.0,
            )
        ],
    )

    result = apply_quality_rules(df).collect()[0]

    assert result.validation_status == "QUARANTINE"

    assert "INVALID_TIME_RANGE" in result.quarantine_reasons
    assert "ZERO_TRIP_DISTANCE" in result.warning_reasons
    assert "NEGATIVE_FARE_AMOUNT" in result.warning_reasons
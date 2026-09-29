from datetime import datetime
from pathlib import Path

import json
import pytest
from pyspark.sql import SparkSession

from src.pipeline.batch_pipeline import run_batch_pipeline


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("TransitFlowIntegrationTests")
        .getOrCreate()
    )

    yield session

    session.stop()


def test_batch_pipeline_end_to_end(tmp_path, spark):
    input_path = tmp_path / "input"
    curated_output = tmp_path / "curated"
    quarantine_output = tmp_path / "quarantine"
    metrics_output = tmp_path / "metrics"

    input_path.mkdir()

    rows = [
        # VALID
        (
            1,
            datetime(2025, 8, 1, 10, 0),
            datetime(2025, 8, 1, 10, 20),
            1,
            3.5,
            1,
            "N",
            100,
            200,
            1,
            15.0,
            1.0,
            0.5,
            3.0,
            0.0,
            0.3,
            19.8,
            2.5,
            0.0,
            0.0,
        ),

        # WARNING: zero trip distance
        (
            1,
            datetime(2025, 8, 1, 11, 0),
            datetime(2025, 8, 1, 11, 10),
            1,
            0.0,
            1,
            "N",
            100,
            200,
            1,
            10.0,
            1.0,
            0.5,
            2.0,
            0.0,
            0.3,
            13.8,
            2.5,
            0.0,
            0.0,
        ),

        # QUARANTINE: negative trip distance
        (
            1,
            datetime(2025, 8, 1, 12, 0),
            datetime(2025, 8, 1, 12, 20),
            1,
            -2.0,
            1,
            "N",
            100,
            200,
            1,
            12.0,
            1.0,
            0.5,
            2.0,
            0.0,
            0.3,
            15.8,
            2.5,
            0.0,
            0.0,
        ),
    ]

    schema = """
        VendorID INT,
        tpep_pickup_datetime TIMESTAMP,
        tpep_dropoff_datetime TIMESTAMP,
        passenger_count INT,
        trip_distance DOUBLE,
        RatecodeID INT,
        store_and_fwd_flag STRING,
        PULocationID INT,
        DOLocationID INT,
        payment_type INT,
        fare_amount DOUBLE,
        extra DOUBLE,
        mta_tax DOUBLE,
        tip_amount DOUBLE,
        tolls_amount DOUBLE,
        improvement_surcharge DOUBLE,
        total_amount DOUBLE,
        congestion_surcharge DOUBLE,
        Airport_fee DOUBLE,
        cbd_congestion_fee DOUBLE
    """

    df = spark.createDataFrame(rows, schema=schema)

    df.write.mode("overwrite").parquet(str(input_path))

    summary = run_batch_pipeline(
        spark=spark,
        input_path=str(input_path),
        source_file="yellow_tripdata_2025-08.parquet",
        batch_id="yellow_taxi_2025_08_test",
        schema_version="1.0",
        curated_output=str(curated_output),
        quarantine_output=str(quarantine_output),
        metrics_output=str(metrics_output),
    )

    assert summary["rows_read"] == 3
    assert summary["valid_records"] == 1
    assert summary["warning_records"] == 1
    assert summary["quarantined_records"] == 1

    curated_df = spark.read.parquet(str(curated_output))
    quarantine_df = spark.read.parquet(str(quarantine_output))

    assert curated_df.count() == 2
    assert quarantine_df.count() == 1

    curated_statuses = {
        row.validation_status
        for row in curated_df.select(
            "validation_status"
        ).collect()
    }

    assert curated_statuses == {
        "VALID",
        "WARNING",
    }

    quarantined = quarantine_df.collect()[0]

    assert quarantined.validation_status == "QUARANTINE"
    assert (
        "NEGATIVE_TRIP_DISTANCE"
        in quarantined.quarantine_reasons
    )

    metrics_files = list(
        Path(metrics_output).glob("*.json")
    )

    assert metrics_files

    metrics = []

    for file_path in metrics_files:
        with file_path.open(
            "r",
            encoding="utf-8",
        ) as file:
            for line in file:
                line = line.strip()

                if line:
                    metrics.append(
                        json.loads(line)
                    )

    assert metrics

    metric = metrics[0]

    assert metric["total_records"] == 3
    assert metric["valid_records"] == 1
    assert metric["warning_records"] == 1
    assert metric["quarantined_records"] == 1

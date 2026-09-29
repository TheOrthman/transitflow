import sys
from pathlib import Path

from pyspark.sql import SparkSession  # type: ignore[reportMissingImports]


PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.config import BatchConfig
from src.common.run_context import (
    calculate_file_checksum,
    generate_run_id,
    successful_batch_exists,
    utc_now,
    write_run_record,
)
from src.pipeline.batch_pipeline import (  # type: ignore[reportMissingImports]
    run_batch_pipeline,
)


CONFIG = BatchConfig(
    dataset="yellow_taxi",
    source_prefix="yellow_tripdata",
    year=2025,
    month=2,
)


def main() -> None:
    run_id = generate_run_id()
    started_at = utc_now()

    source_checksum = calculate_file_checksum(
        CONFIG.input_path
    )

    print("=== TRANSITFLOW RUN ===")
    print(f"Run ID: {run_id}")
    print(f"Batch ID: {CONFIG.batch_id}")
    print(f"Source file: {CONFIG.source_file}")
    print(f"Source checksum: {source_checksum}")

    if successful_batch_exists(
        batch_id=CONFIG.batch_id,
        source_checksum=source_checksum,
    ):
        print(
            "\nSKIPPED: This exact source batch has already "
            "completed successfully."
        )
        return

    spark = None

    try:
        spark = (
            SparkSession.builder
            .master("local[*]")
            .appName("TransitFlowIngestion")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")

        summary = run_batch_pipeline(
            spark=spark,
            input_path=CONFIG.input_path,
            source_file=CONFIG.source_file,
            batch_id=CONFIG.batch_id,
            schema_version=CONFIG.schema_version,
            curated_output=CONFIG.curated_output,
            quarantine_output=CONFIG.quarantine_output,
            metrics_output=CONFIG.metrics_output,
        )

        write_run_record(
            {
                "run_id": run_id,
                "batch_id": CONFIG.batch_id,
                "source_file": CONFIG.source_file,
                "source_checksum": source_checksum,
                "schema_version": CONFIG.schema_version,
                "started_at": started_at,
                "completed_at": utc_now(),
                "status": "SUCCESS",
                **summary,
            }
        )

        print(f"\nRun ledger updated for: {run_id}")

    except Exception as exc:
        write_run_record(
            {
                "run_id": run_id,
                "batch_id": CONFIG.batch_id,
                "source_file": CONFIG.source_file,
                "source_checksum": source_checksum,
                "schema_version": CONFIG.schema_version,
                "started_at": started_at,
                "completed_at": utc_now(),
                "status": "FAILED",
                "error": str(exc),
            }
        )

        raise

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
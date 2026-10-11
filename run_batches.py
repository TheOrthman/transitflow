from __future__ import annotations

import sys
from importlib import import_module
from pathlib import Path
from typing import Any

# The storage modules are imported dynamically below.  Use editor-safe type
# aliases here so static analysis does not require resolving the local package.
GlueCatalogType = Any
S3StorageType = Any

# -------------------------------------------------------------------
# Project root / imports
# -------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.config import BatchConfig
from src.common.run_context import (
    calculate_file_checksum,
    generate_run_id,
    get_processed_counts,
    successful_batch_exists,
    utc_now,
    write_run_record,
)
from src.reconciliation.athena_reconciler import AthenaReconciler
from src.ingestion.tlc_downloader import download_tlc_file
from src.pipeline.batch_pipeline import run_batch_pipeline
batch_publisher = import_module("src.storage.batch_publisher")
batch_is_published_to_s3 = batch_publisher.batch_is_published_to_s3
publish_batch_to_s3 = batch_publisher.publish_batch_to_s3
GlueCatalog = import_module("src.storage.glue_catalog").GlueCatalog
S3Storage = import_module("src.storage.s3_storage").S3Storage


# -------------------------------------------------------------------
# Runtime configuration
# -------------------------------------------------------------------

ENABLE_S3_PUBLISH = True
ENABLE_GLUE_REGISTRATION = True
ENABLE_ATHENA_RECONCILIATION = True

AWS_PROFILE = "transitflow-lp"
AWS_REGION = "eu-west-1"

S3_BUCKET = "transitflow-data-1789925359"

GLUE_DATABASE = "transitflow"
GLUE_TABLE = "curated_yellow_taxi"
ATHENA_OUTPUT_LOCATION = "s3://transitflow-data-1789925359/athena-results/"


BATCHES = [
    BatchConfig(
        dataset="yellow_taxi",
        source_prefix="yellow_tripdata",
        year=2025,
        month=month,
    )
    for month in range(1, 9)
]


# -------------------------------------------------------------------
# AWS clients
# -------------------------------------------------------------------

def build_s3_storage() -> S3StorageType | None:
    """
    Build the TransitFlow S3 storage client.

    Local development uses the AWS SSO profile.
    In Glue/EMR later, profile_name can be omitted so boto3 uses
    the attached IAM role.
    """
    if not ENABLE_S3_PUBLISH:
        return None

    return S3Storage(
        bucket=S3_BUCKET,
        profile_name=AWS_PROFILE,
        region_name=AWS_REGION,
    )


def build_glue_catalog() -> GlueCatalogType | None:
    """
    Build the Glue catalog client used to register curated partitions.
    """
    if not ENABLE_S3_PUBLISH:
        return None

    if not ENABLE_GLUE_REGISTRATION:
        return None

    return GlueCatalog(
        database=GLUE_DATABASE,
        table=GLUE_TABLE,
        bucket=S3_BUCKET,
        profile_name=AWS_PROFILE,
        region_name=AWS_REGION,
    )


def build_athena_reconciler() -> AthenaReconciler | None:
    if not (ENABLE_S3_PUBLISH and ENABLE_GLUE_REGISTRATION
            and ENABLE_ATHENA_RECONCILIATION):
        return None
    return AthenaReconciler(
        database=GLUE_DATABASE,
        table=GLUE_TABLE,
        output_location=ATHENA_OUTPUT_LOCATION,
        profile_name=AWS_PROFILE,
        region_name=AWS_REGION,
    )


def reconcile_batch(
    reconciler: AthenaReconciler | None,
    config: BatchConfig,
    run_record: dict[str, Any],
) -> None:
    """Persist the comparison before raising on mismatch or query failure."""
    if reconciler is None or run_record.get("glue_partition_status") not in {
        "CREATED", "EXISTS"
    }:
        run_record["reconciliation_status"] = "DISABLED"
        return
    try:
        if "valid_records" not in run_record:
            run_record.update(get_processed_counts(
                config.batch_id, run_record["source_checksum"]
            ))
        result = reconciler.reconcile_partition(
            year=config.year,
            month=config.month,
            expected_records=(run_record["valid_records"]
                              + run_record["warning_records"]),
        )
        run_record.update(result)
        if result["reconciliation_status"] not in {"MATCH", "MISMATCH"}:
            raise RuntimeError("Unexpected Athena reconciliation status")
    except Exception:
        run_record["reconciliation_status"] = "FAILED"
        raise
    if result["reconciliation_status"] == "MISMATCH":
        raise RuntimeError(
            f"Athena reconciliation MISMATCH for {config.batch_id}: "
            f"expected {result['expected_records']}, "
            f"found {result['athena_records']}"
        )
    print(f"Athena reconciliation: MATCH ({result['athena_records']} records)")


def ensure_glue_partition(
    catalog: GlueCatalogType | None,
    config: BatchConfig,
) -> str:
    """
    Ensure the curated partition is registered in Glue.

    Returns:
        DISABLED
        CREATED
        EXISTS
    """
    if catalog is None:
        print("Glue registration disabled.")
        return "DISABLED"

    print("Checking Glue partition state...")

    created = catalog.register_partition(config)

    if created:
        print("Glue partition check: CREATED")
        print(
            f"Registered partition: "
            f"year={config.year}/month={config.month_string}"
        )
        return "CREATED"

    print("Glue partition check: EXISTS")
    print("Glue partition already registered.")

    return "EXISTS"


# -------------------------------------------------------------------
# Batch processing
# -------------------------------------------------------------------

def process_batch(
    spark: Any,
    config: BatchConfig,
    storage: S3StorageType | None,
    catalog: GlueCatalogType | None,
    reconciler: AthenaReconciler | None = None,
) -> None:
    run_id = generate_run_id()
    started_at = utc_now()

    source_path = Path(config.input_path)

    print("\n" + "=" * 72)
    print(f"Batch: {config.batch_id}")
    print(f"Source: {source_path}")
    print(f"Schema version: {config.schema_version}")
    print("=" * 72)

    run_record = {
        "run_id": run_id,
        "batch_id": config.batch_id,
        "dataset": config.dataset,
        "period": config.period,
        "source_file": config.source_file,
        "source_path": str(source_path),
        "schema_version": config.schema_version,
        "started_at": started_at,
        "status": "STARTED",
        "reconciliation_status": "NOT_RUN",
        "athena_reconciliation_enabled": reconciler is not None,
        "s3_publish_enabled": ENABLE_S3_PUBLISH,
        "glue_registration_enabled": ENABLE_GLUE_REGISTRATION,
    }

    try:
        # -----------------------------------------------------------
        # 1. Ensure raw source exists locally
        # -----------------------------------------------------------

        if not source_path.exists():
            print("Source file missing locally.")
            print("Attempting download...")

            downloaded = download_tlc_file(
                source_file=config.source_file,
                destination_path=source_path,
            )

            if not downloaded:
                raise RuntimeError(
                    f"Failed to download source file: "
                    f"{config.source_file}"
                )

        # -----------------------------------------------------------
        # 2. Calculate source checksum
        # -----------------------------------------------------------

        source_checksum = calculate_file_checksum(source_path)

        run_record["source_checksum"] = source_checksum

        print(f"Source checksum: {source_checksum}")

        # -----------------------------------------------------------
        # 3. Processing idempotency
        # -----------------------------------------------------------

        already_processed = successful_batch_exists(
            batch_id=config.batch_id,
            source_checksum=source_checksum,
        )

        # ===========================================================
        # EXISTING PROCESSED BATCH
        # ===========================================================

        if already_processed:
            print("Idempotency check: MATCH FOUND")
            print("Batch already processed successfully.")
            print("Skipping Spark processing.")

            run_record["processing_status"] = "SKIPPED_ALREADY_PROCESSED"

            # -------------------------------------------------------
            # 3a. S3 disabled
            # -------------------------------------------------------

            if storage is None:
                print("S3 publishing disabled.")
                return

            # -------------------------------------------------------
            # 3b. Check S3 independently
            # -------------------------------------------------------

            print("Checking S3 publication state...")

            already_published = batch_is_published_to_s3(
                storage=storage,
                config=config,
                source_checksum=source_checksum,
            )

            publish_summary = None

            if already_published:
                print("S3 publication check: COMPLETE")
                print("Batch already published successfully.")

                s3_status = "EXISTS"

            else:
                print("S3 publication check: INCOMPLETE")
                print(
                    "Publishing existing local outputs to S3..."
                )

                publish_summary = publish_batch_to_s3(
                    storage=storage,
                    config=config,
                )

                s3_status = "BACKFILLED"

                print(
                    f"S3 backfill complete: "
                    f"{config.batch_id}"
                )

            # -------------------------------------------------------
            # 3c. Check Glue independently
            # -------------------------------------------------------

            run_record["s3_publish_status"] = s3_status
            run_record["s3_bucket"] = storage.bucket

            glue_status = ensure_glue_partition(
                catalog=catalog,
                config=config,
            )

            run_record.update(
                s3_publish_status=s3_status,
                s3_bucket=storage.bucket,
                glue_partition_status=glue_status,
            )
            if publish_summary is not None:
                for key in ("curated_object_count", "quarantine_object_count",
                            "quality_object_count"):
                    run_record[f"s3_{key}"] = publish_summary[key]
            reconcile_batch(reconciler, config, run_record)
            if (s3_status == "BACKFILLED" or glue_status == "CREATED"
                    or run_record["reconciliation_status"] == "MATCH"):
                run_record.update(status="SUCCESS", completed_at=utc_now())
                write_run_record(run_record)

            return

        # ===========================================================
        # NEW BATCH
        # ===========================================================

        print("Idempotency check: NEW BATCH")

        # -----------------------------------------------------------
        # 4. Run Spark pipeline
        # -----------------------------------------------------------

        summary = run_batch_pipeline(
            spark=spark,
            input_path=config.input_path,
            source_file=config.source_file,
            batch_id=config.batch_id,
            schema_version=config.schema_version,
            curated_output=config.curated_output,
            quarantine_output=config.quarantine_output,
            metrics_output=config.metrics_output,
        )

        run_record["rows_read"] = summary["rows_read"]
        run_record["valid_records"] = summary[
            "valid_records"
        ]
        run_record["warning_records"] = summary[
            "warning_records"
        ]
        run_record["quarantined_records"] = summary[
            "quarantined_records"
        ]

        run_record["processing_status"] = "SUCCESS"

        # -----------------------------------------------------------
        # 5. Publish new batch to S3
        # -----------------------------------------------------------

        if storage is not None:
            print(
                "\nPublishing successful batch outputs to S3..."
            )

            publish_summary = publish_batch_to_s3(
                storage=storage,
                config=config,
            )

            run_record["s3_publish_status"] = "SUCCESS"
            run_record["s3_bucket"] = storage.bucket

            run_record[
                "s3_curated_object_count"
            ] = publish_summary[
                "curated_object_count"
            ]

            run_record[
                "s3_quarantine_object_count"
            ] = publish_summary[
                "quarantine_object_count"
            ]

            run_record[
                "s3_quality_object_count"
            ] = publish_summary[
                "quality_object_count"
            ]

            # -------------------------------------------------------
            # 6. Register Glue partition
            # -------------------------------------------------------

            glue_status = ensure_glue_partition(
                catalog=catalog,
                config=config,
            )

            run_record[
                "glue_partition_status"
            ] = glue_status

        else:
            run_record["s3_publish_status"] = "DISABLED"
            run_record[
                "glue_partition_status"
            ] = "DISABLED"

        # -----------------------------------------------------------
        # 7. Reconcile after Glue registration, then mark complete
        # -----------------------------------------------------------

        reconcile_batch(reconciler, config, run_record)

        run_record["status"] = "SUCCESS"
        run_record["completed_at"] = utc_now()

        write_run_record(run_record)

        print(f"\nBatch complete: {config.batch_id}")

    except Exception as exc:
        run_record["status"] = "FAILED"
        run_record["completed_at"] = utc_now()
        run_record["error_type"] = type(exc).__name__
        run_record["error_message"] = str(exc)

        if (
            ENABLE_S3_PUBLISH
            and "s3_publish_status" not in run_record
        ):
            run_record["s3_publish_status"] = "FAILED"

        if (
            ENABLE_GLUE_REGISTRATION
            and "glue_partition_status"
            not in run_record
        ):
            run_record["glue_partition_status"] = "FAILED"

        write_run_record(run_record)

        print(f"\nBatch failed: {config.batch_id}")
        print(f"{type(exc).__name__}: {exc}")

        raise


# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------

def main() -> None:
    print("TransitFlow multi-batch pipeline")
    print(
        f"S3 publishing enabled: "
        f"{ENABLE_S3_PUBLISH}"
    )
    print(
        f"Glue registration enabled: "
        f"{ENABLE_GLUE_REGISTRATION}"
    )

    storage = build_s3_storage()
    catalog = build_glue_catalog()
    reconciler = build_athena_reconciler()

    spark_session = import_module("pyspark.sql").SparkSession
    spark = (
        spark_session.builder
        .appName("TransitFlowBatchPipeline")
        .getOrCreate()
    )

    try:
        for config in BATCHES:
            process_batch(
                spark=spark,
                config=config,
                storage=storage,
                catalog=catalog,
                reconciler=reconciler,
            )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
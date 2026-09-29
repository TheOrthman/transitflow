from __future__ import annotations

from typing import Any

from ..common.config import BatchConfig
from .s3_storage import S3Storage


def curated_partition_prefix(config: BatchConfig) -> str:
    """
    Return the Hive-style S3 prefix for a curated batch.

    Example:
    curated/yellow_taxi/year=2025/month=08
    """
    return (
        f"curated/{config.dataset}/"
        f"year={config.year}/"
        f"month={config.month_string}"
    )


def publish_batch_to_s3(
    storage: S3Storage,
    config: BatchConfig,
) -> dict[str, Any]:
    """
    Publish one successfully processed TransitFlow batch to S3.

    Layout:
      raw/<dataset>/<source-file>
      curated/<dataset>/year=YYYY/month=MM/
      quarantine/<dataset>/YYYY-MM/
      metrics/quality/<dataset>/YYYY-MM/
    """

    print(f"\nPublishing batch to S3: {config.batch_id}")

    # -----------------------------------------------------------
    # Raw source
    # -----------------------------------------------------------

    raw_key = (
        f"raw/{config.dataset}/"
        f"{config.source_file}"
    )

    raw_result = storage.upload_file(
        local_path=config.input_path,
        key=raw_key,
    )

    print(
        f"Uploaded raw: "
        f"s3://{storage.bucket}/{raw_key}"
    )

    # -----------------------------------------------------------
    # Curated data - Hive-style partitioned layout
    # -----------------------------------------------------------

    curated_prefix = curated_partition_prefix(config)

    curated_results = storage.upload_directory(
        local_directory=config.curated_output,
        s3_prefix=curated_prefix,
    )

    print(
        f"Uploaded curated objects: "
        f"{len(curated_results)}"
    )

    print(
        f"Curated partition: "
        f"s3://{storage.bucket}/{curated_prefix}/"
    )

    # -----------------------------------------------------------
    # Quarantine data
    # -----------------------------------------------------------

    quarantine_prefix = (
        f"quarantine/{config.dataset}/{config.period}"
    )

    quarantine_results = storage.upload_directory(
        local_directory=config.quarantine_output,
        s3_prefix=quarantine_prefix,
    )

    print(
        f"Uploaded quarantine objects: "
        f"{len(quarantine_results)}"
    )

    # -----------------------------------------------------------
    # Quality metrics
    # -----------------------------------------------------------

    quality_prefix = (
        f"metrics/quality/"
        f"{config.dataset}/{config.period}"
    )

    quality_results = storage.upload_directory(
        local_directory=config.metrics_output,
        s3_prefix=quality_prefix,
    )

    print(
        f"Uploaded quality metric objects: "
        f"{len(quality_results)}"
    )

    return {
        "batch_id": config.batch_id,
        "raw": raw_result,
        "curated_prefix": curated_prefix,
        "curated_object_count": len(curated_results),
        "quarantine_object_count": len(
            quarantine_results
        ),
        "quality_object_count": len(
            quality_results
        ),
    }


def batch_is_published_to_s3(
    storage: S3Storage,
    config: BatchConfig,
    source_checksum: str,
) -> bool:
    """
    Verify that a batch has been completely published.

    Checks:
      - raw object exists
      - raw SHA-256 matches local source
      - curated partition has Spark _SUCCESS
      - quarantine output has Spark _SUCCESS
      - quality output has Spark _SUCCESS
    """

    # -----------------------------------------------------------
    # Verify raw source
    # -----------------------------------------------------------

    raw_key = (
        f"raw/{config.dataset}/"
        f"{config.source_file}"
    )

    if not storage.object_exists(raw_key):
        return False

    raw_info = storage.head_object(raw_key)

    remote_checksum = raw_info[
        "metadata"
    ].get("sha256")

    if remote_checksum != source_checksum:
        return False

    # -----------------------------------------------------------
    # Verify generated artifacts
    # -----------------------------------------------------------

    curated_prefix = curated_partition_prefix(config)

    required_markers = [
        f"{curated_prefix}/_SUCCESS",
        (
            f"quarantine/{config.dataset}/"
            f"{config.period}/_SUCCESS"
        ),
        (
            f"metrics/quality/{config.dataset}/"
            f"{config.period}/_SUCCESS"
        ),
    ]

    for key in required_markers:
        if not storage.object_exists(key):
            return False

    return True
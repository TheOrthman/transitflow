try:
    from src.common.config import BatchConfig  # type: ignore[import-not-found]
    from src.storage.batch_publisher import curated_partition_prefix  # type: ignore[import-not-found]
except ModuleNotFoundError:
    from common.config import BatchConfig  # type: ignore[import-not-found]
    from storage.batch_publisher import curated_partition_prefix  # type: ignore[import-not-found]


def test_curated_partition_prefix():
    config = BatchConfig(
        dataset="yellow_taxi",
        source_prefix="yellow_tripdata",
        year=2025,
        month=8,
    )

    assert curated_partition_prefix(config) == (
        "curated/yellow_taxi/year=2025/month=08"
    )
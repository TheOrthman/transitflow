from unittest.mock import Mock

from src.common.config import BatchConfig
from src.storage.batch_publisher import (
    batch_is_published_to_s3,
    curated_partition_prefix,
)


def make_config():
    return BatchConfig(
        dataset="yellow_taxi",
        source_prefix="yellow_tripdata",
        year=2025,
        month=8,
    )


def test_batch_is_published_when_all_checks_pass():
    config = make_config()

    storage = Mock()
    storage.object_exists.return_value = True
    storage.head_object.return_value = {
        "metadata": {
            "sha256": "abc123",
        }
    }

    result = batch_is_published_to_s3(
        storage=storage,
        config=config,
        source_checksum="abc123",
    )

    assert result is True


def test_batch_is_not_published_when_raw_missing():
    config = make_config()

    storage = Mock()
    storage.object_exists.return_value = False

    result = batch_is_published_to_s3(
        storage=storage,
        config=config,
        source_checksum="abc123",
    )

    assert result is False


def test_batch_is_not_published_when_checksum_differs():
    config = make_config()

    storage = Mock()
    storage.object_exists.return_value = True
    storage.head_object.return_value = {
        "metadata": {
            "sha256": "wrong-checksum",
        }
    }

    result = batch_is_published_to_s3(
        storage=storage,
        config=config,
        source_checksum="abc123",
    )

    assert result is False


def test_curated_partition_prefix_uses_zero_padded_month():
    config = make_config()

    assert curated_partition_prefix(config) == (
        "curated/yellow_taxi/year=2025/month=08"
    )
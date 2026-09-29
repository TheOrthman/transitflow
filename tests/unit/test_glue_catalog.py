from unittest.mock import Mock

from botocore.exceptions import ClientError

from src.common.config import BatchConfig
from src.storage.glue_catalog import GlueCatalog


def make_config():
    return BatchConfig(
        dataset="yellow_taxi",
        source_prefix="yellow_tripdata",
        year=2025,
        month=8,
    )


def make_catalog():
    catalog = GlueCatalog.__new__(GlueCatalog)

    catalog.database = "transitflow"
    catalog.table = "curated_yellow_taxi"
    catalog.bucket = "transitflow-data-1789925359"
    catalog.client = Mock()

    return catalog


def test_partition_values_are_zero_padded():
    catalog = make_catalog()
    config = make_config()

    assert catalog.partition_values(config) == [
        "2025",
        "08",
    ]


def test_partition_location_is_canonical():
    catalog = make_catalog()
    config = make_config()

    assert catalog.partition_location(config) == (
        "s3://transitflow-data-1789925359/"
        "curated/yellow_taxi/"
        "year=2025/month=08/"
    )


def test_partition_exists_returns_true():
    catalog = make_catalog()
    config = make_config()

    catalog.client.get_partition.return_value = {
        "Partition": {}
    }

    result = catalog.partition_exists(config)

    assert result is True

    catalog.client.get_partition.assert_called_once_with(
        DatabaseName="transitflow",
        TableName="curated_yellow_taxi",
        PartitionValues=["2025", "08"],
    )


def test_partition_exists_returns_false_when_missing():
    catalog = make_catalog()
    config = make_config()

    error_response = {
        "Error": {
            "Code": "EntityNotFoundException",
            "Message": "Partition not found",
        }
    }

    catalog.client.get_partition.side_effect = ClientError(
        error_response,
        "GetPartition",
    )

    result = catalog.partition_exists(config)

    assert result is False


def test_partition_is_correct_when_location_matches():
    catalog = make_catalog()
    config = make_config()

    catalog.client.get_partition.return_value = {
        "Partition": {
            "StorageDescriptor": {
                "Location": (
                    "s3://transitflow-data-1789925359/"
                    "curated/yellow_taxi/"
                    "year=2025/month=08/"
                )
            }
        }
    }

    assert catalog.partition_is_correct(config) is True


def test_partition_is_incorrect_when_location_differs():
    catalog = make_catalog()
    config = make_config()

    catalog.client.get_partition.return_value = {
        "Partition": {
            "StorageDescriptor": {
                "Location": (
                    "s3://transitflow-data-1789925359/"
                    "curated/year=2025/month=8/"
                )
            }
        }
    }

    assert catalog.partition_is_correct(config) is False


def test_register_partition_does_nothing_when_correct():
    catalog = make_catalog()
    config = make_config()

    catalog.partition_is_correct = Mock(
        return_value=True
    )

    result = catalog.register_partition(config)

    assert result is False

    catalog.client.delete_partition.assert_not_called()
    catalog.client.create_partition.assert_not_called()


def test_register_partition_replaces_wrong_partition():
    catalog = make_catalog()
    config = make_config()

    catalog.partition_is_correct = Mock(
        return_value=False
    )

    catalog.partition_exists = Mock(
        return_value=True
    )

    catalog.client.get_table.return_value = {
        "Table": {
            "StorageDescriptor": {
                "Columns": [
                    {
                        "Name": "vendor_id",
                        "Type": "int",
                    }
                ],
                "InputFormat": (
                    "org.apache.hadoop.hive.ql.io.parquet."
                    "MapredParquetInputFormat"
                ),
                "OutputFormat": (
                    "org.apache.hadoop.hive.ql.io.parquet."
                    "MapredParquetOutputFormat"
                ),
                "SerdeInfo": {
                    "SerializationLibrary": (
                        "org.apache.hadoop.hive.ql.io.parquet.serde."
                        "ParquetHiveSerDe"
                    )
                },
                "Location": (
                    "s3://transitflow-data-1789925359/"
                    "curated/yellow_taxi/"
                ),
            }
        }
    }

    result = catalog.register_partition(config)

    assert result is True

    catalog.client.delete_partition.assert_called_once_with(
        DatabaseName="transitflow",
        TableName="curated_yellow_taxi",
        PartitionValues=["2025", "08"],
    )

    catalog.client.create_partition.assert_called_once()

    call_args = (
        catalog.client.create_partition.call_args.kwargs
    )

    assert call_args["PartitionInput"]["Values"] == [
        "2025",
        "08",
    ]

    assert (
        call_args["PartitionInput"]
        ["StorageDescriptor"]
        ["Location"]
        == (
            "s3://transitflow-data-1789925359/"
            "curated/yellow_taxi/"
            "year=2025/month=08/"
        )
    )
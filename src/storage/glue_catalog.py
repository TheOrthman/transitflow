from __future__ import annotations

from copy import deepcopy

import boto3  # pyright: ignore[reportMissingImports]
from botocore.exceptions import ClientError  # pyright: ignore[reportMissingImports]

from ..common.config import BatchConfig


def curated_partition_prefix(config: BatchConfig) -> str:
    """Return the canonical prefix for a curated batch partition."""
    return (
        f"curated/{config.dataset}/"
        f"year={config.year}/month={config.month_string}"
    )


class GlueCatalog:
    def __init__(
        self,
        database: str,
        table: str,
        bucket: str,
        profile_name: str | None = None,
        region_name: str | None = None,
    ) -> None:
        self.database = database
        self.table = table
        self.bucket = bucket

        session_kwargs: dict[str, str] = {}

        if profile_name:
            session_kwargs["profile_name"] = profile_name

        if region_name:
            session_kwargs["region_name"] = region_name

        session = boto3.Session(**session_kwargs)
        self.client = session.client("glue")

    def partition_values(
        self,
        config: BatchConfig,
    ) -> list[str]:
        """
        Return canonical Glue partition values.

        Example:
        ["2025", "08"]
        """
        return [
            str(config.year),
            config.month_string,
        ]

    def partition_location(
        self,
        config: BatchConfig,
    ) -> str:
        """
        Return the canonical S3 location for the curated partition.

        Example:
        s3://bucket/curated/yellow_taxi/year=2025/month=08/
        """
        prefix = curated_partition_prefix(config)

        return f"s3://{self.bucket}/{prefix}/"

    def partition_exists(
        self,
        config: BatchConfig,
    ) -> bool:
        try:
            self.client.get_partition(
                DatabaseName=self.database,
                TableName=self.table,
                PartitionValues=self.partition_values(config),
            )
            return True

        except ClientError as exc:
            code = exc.response.get(
                "Error", {}
            ).get("Code")

            if code == "EntityNotFoundException":
                return False

            raise

    def get_partition(
        self,
        config: BatchConfig,
    ) -> dict | None:
        try:
            response = self.client.get_partition(
                DatabaseName=self.database,
                TableName=self.table,
                PartitionValues=self.partition_values(config),
            )

            return response["Partition"]

        except ClientError as exc:
            code = exc.response.get(
                "Error", {}
            ).get("Code")

            if code == "EntityNotFoundException":
                return None

            raise

    def partition_is_correct(
        self,
        config: BatchConfig,
    ) -> bool:
        """
        Check both partition existence and its S3 location.
        """
        partition = self.get_partition(config)

        if partition is None:
            return False

        actual_location = partition[
            "StorageDescriptor"
        ].get("Location")

        expected_location = self.partition_location(
            config
        )

        return actual_location == expected_location

    def register_partition(
        self,
        config: BatchConfig,
    ) -> bool:
        """
        Create the partition if it does not exist.

        Returns True when created.
        Returns False when an already-correct partition exists.
        """

        if self.partition_is_correct(config):
            return False

        # Remove an existing partition if its location is wrong.
        if self.partition_exists(config):
            self.client.delete_partition(
                DatabaseName=self.database,
                TableName=self.table,
                PartitionValues=self.partition_values(config),
            )

        table_response = self.client.get_table(
            DatabaseName=self.database,
            Name=self.table,
        )

        table = table_response["Table"]

        storage_descriptor = deepcopy(
            table["StorageDescriptor"]
        )

        storage_descriptor["Location"] = (
            self.partition_location(config)
        )

        self.client.create_partition(
            DatabaseName=self.database,
            TableName=self.table,
            PartitionInput={
                "Values": self.partition_values(config),
                "StorageDescriptor": storage_descriptor,
            },
        )

        return True
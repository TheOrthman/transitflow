from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import boto3  # type: ignore[reportMissingImports]


class S3Storage:
    """
    Small S3 abstraction for TransitFlow.

    Local development can use an AWS SSO profile such as "transitflow".
    In AWS-managed environments such as Glue or EMR, profile_name can be
    omitted so boto3 uses the attached IAM role automatically.
    """

    def __init__(
        self,
        bucket: str,
        profile_name: str | None = None,
        region_name: str | None = None,
    ) -> None:
        self.bucket = bucket

        session_kwargs: dict[str, str] = {}

        if profile_name:
            session_kwargs["profile_name"] = profile_name

        if region_name:
            session_kwargs["region_name"] = region_name

        self.session = boto3.Session(**session_kwargs)
        self.client = self.session.client("s3")

    @staticmethod
    def calculate_sha256(
        file_path: str | Path,
        chunk_size: int = 1024 * 1024,
    ) -> str:
        """
        Calculate a SHA-256 checksum without loading the entire file
        into memory.
        """
        path = Path(file_path)

        if not path.is_file():
            raise FileNotFoundError(f"File does not exist: {path}")

        sha256 = hashlib.sha256()

        with path.open("rb") as file_obj:
            while chunk := file_obj.read(chunk_size):
                sha256.update(chunk)

        return sha256.hexdigest()

    def object_exists(self, key: str) -> bool:
        """
        Return True if an object exists in the configured bucket.
        """
        try:
            self.client.head_object(
                Bucket=self.bucket,
                Key=key,
            )
            return True

        except Exception as exc:
            response = getattr(exc, "response", {})
            error_code = response.get("Error", {}).get("Code")

            if error_code in {"404", "NoSuchKey", "NotFound"}:
                return False

            raise

    def head_object(self, key: str) -> dict[str, Any]:
        """
        Return useful metadata for an S3 object.
        """
        response = self.client.head_object(
            Bucket=self.bucket,
            Key=key,
        )

        return {
            "bucket": self.bucket,
            "key": key,
            "size_bytes": response.get("ContentLength"),
            "etag": response.get("ETag", "").strip('"'),
            "last_modified": response.get("LastModified"),
            "content_type": response.get("ContentType"),
            "metadata": response.get("Metadata", {}),
        }

    def upload_file(
        self,
        local_path: str | Path,
        key: str,
        calculate_checksum: bool = True,
    ) -> dict[str, Any]:
        """
        Upload a local file to S3.

        When calculate_checksum=True, the local SHA-256 checksum is stored
        as S3 user-defined object metadata.
        """
        path = Path(local_path)

        if not path.is_file():
            raise FileNotFoundError(f"File does not exist: {path}")

        metadata: dict[str, str] = {}

        if calculate_checksum:
            metadata["sha256"] = self.calculate_sha256(path)

        extra_args: dict[str, Any] = {}

        if metadata:
            extra_args["Metadata"] = metadata

        self.client.upload_file(
            Filename=str(path),
            Bucket=self.bucket,
            Key=key,
            ExtraArgs=extra_args,
        )

        remote = self.head_object(key)

        return {
            "bucket": self.bucket,
            "key": key,
            "local_path": str(path),
            "size_bytes": remote["size_bytes"],
            "sha256": metadata.get("sha256"),
            "remote_metadata": remote["metadata"],
        }

    def download_file(
        self,
        key: str,
        destination_path: str | Path,
    ) -> Path:
        """
        Download an S3 object to a local destination.
        """
        destination = Path(destination_path)
        destination.parent.mkdir(parents=True, exist_ok=True)

        self.client.download_file(
            Bucket=self.bucket,
            Key=key,
            Filename=str(destination),
        )

        return destination

    def upload_directory(
        self,
        local_directory: str | Path,
        s3_prefix: str,
        skip_hidden: bool = True,
    ) -> list[dict[str, Any]]:
        """
        Recursively upload a local directory to an S3 prefix.

        Each uploaded file receives its SHA-256 checksum as object metadata.
        Hadoop/Spark CRC sidecar files are ignored.
        """
        directory = Path(local_directory)

        if not directory.is_dir():
            raise NotADirectoryError(
                f"Directory does not exist: {directory}"
            )

        uploaded: list[dict[str, Any]] = []

        for path in sorted(directory.rglob("*")):
            if not path.is_file():
                continue

            # Skip local Hadoop checksum sidecar files.
            if path.name.endswith(".crc"):
                continue

            if skip_hidden and path.name.startswith("."):
                continue

            relative_path = path.relative_to(directory)

            key = (
                f"{s3_prefix.rstrip('/')}/"
                f"{relative_path.as_posix()}"
            )

            result = self.upload_file(
                local_path=path,
                key=key,
                calculate_checksum=True,
            )

            uploaded.append(result)


        return uploaded

    def prefix_has_objects(self, prefix: str) -> bool:
        """
        Return True when at least one object exists under an S3 prefix.
        """
        response = self.client.list_objects_v2(
            Bucket=self.bucket,
            Prefix=prefix.rstrip("/") + "/",
            MaxKeys=1,
        )

        return response.get("KeyCount", 0) > 0
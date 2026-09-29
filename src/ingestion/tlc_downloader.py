from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from ..common.run_context import calculate_file_checksum


TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

DOWNLOAD_METADATA_DIR = Path(
    "data/metrics/downloads"
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_tlc_url(source_file: str) -> str:
    return f"{TLC_BASE_URL}/{source_file}"


def write_download_metadata(
    source_file: str,
    metadata: dict,
) -> None:
    DOWNLOAD_METADATA_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    metadata_path = (
        DOWNLOAD_METADATA_DIR
        / f"{source_file}.json"
    )

    metadata_path.write_text(
        json.dumps(
            metadata,
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )


def download_tlc_file(
    source_file: str,
    destination_path: str,
    chunk_size: int = 1024 * 1024,
    max_retries: int = 3,
) -> bool:
    """
    Download a TLC Parquet file safely.

    Features:
        - skips existing files
        - downloads to a temporary .part file
        - retries transient failures
        - exponential backoff
        - atomic rename after success
        - writes download metadata
        - calculates SHA-256 checksum
    """

    destination = Path(destination_path)
    partial_path = destination.with_suffix(
        destination.suffix + ".part"
    )

    if destination.exists():
        print(
            f"Source already exists: "
            f"{destination}"
        )
        return True

    destination.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    url = build_tlc_url(source_file)

    for attempt in range(1, max_retries + 1):
        started_at = utc_now()

        print(
            f"Download attempt {attempt}/{max_retries}: "
            f"{source_file}"
        )

        try:
            if partial_path.exists():
                partial_path.unlink()

            bytes_written = 0

            with urlopen(
                url,
                timeout=60,
            ) as response:
                with partial_path.open("wb") as output_file:
                    while True:
                        chunk = response.read(chunk_size)

                        if not chunk:
                            break

                        output_file.write(chunk)
                        bytes_written += len(chunk)

            # Atomic promotion:
            # only make the final file visible after
            # the download completes successfully.
            partial_path.replace(destination)

            checksum = calculate_file_checksum(
                str(destination)
            )

            completed_at = utc_now()

            metadata = {
                "source_file": source_file,
                "source_url": url,
                "destination_path": str(destination),
                "status": "SUCCESS",
                "attempt": attempt,
                "started_at": started_at,
                "completed_at": completed_at,
                "file_size_bytes": bytes_written,
                "sha256": checksum,
            }

            write_download_metadata(
                source_file=source_file,
                metadata=metadata,
            )

            print(
                f"Downloaded successfully: "
                f"{destination}"
            )
            print(
                f"Size: {bytes_written:,} bytes"
            )
            print(
                f"SHA-256: {checksum}"
            )

            return True

        except (
            HTTPError,
            URLError,
            TimeoutError,
            OSError,
        ) as exc:
            print(
                f"Download attempt {attempt} failed: "
                f"{exc}"
            )

            if partial_path.exists():
                partial_path.unlink()

            if attempt < max_retries:
                wait_seconds = 2 ** (attempt - 1)

                print(
                    f"Retrying in "
                    f"{wait_seconds} second(s)..."
                )

                time.sleep(wait_seconds)

            else:
                write_download_metadata(
                    source_file=source_file,
                    metadata={
                        "source_file": source_file,
                        "source_url": url,
                        "destination_path": str(
                            destination
                        ),
                        "status": "FAILED",
                        "attempt": attempt,
                        "started_at": started_at,
                        "completed_at": utc_now(),
                        "error": str(exc),
                    },
                )

                print(
                    f"Download failed permanently: "
                    f"{source_file}"
                )

                return False

    return False
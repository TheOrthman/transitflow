from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


RUN_LEDGER_PATH = Path(
    "data/metrics/pipeline_runs/run_ledger.jsonl"
)


def utc_now() -> str:
    """Return the current UTC timestamp."""
    return datetime.now(timezone.utc).isoformat()


def generate_run_id() -> str:
    """Generate a unique identifier for a pipeline execution."""
    return str(uuid.uuid4())


def calculate_file_checksum(
    file_path: str,
    chunk_size: int = 1024 * 1024,
) -> str:
    """
    Calculate the SHA-256 checksum of a source file.

    The file is read incrementally so large source files
    do not need to be loaded completely into memory.
    """
    sha256 = hashlib.sha256()

    with open(file_path, "rb") as source_file:
        while True:
            chunk = source_file.read(chunk_size)

            if not chunk:
                break

            sha256.update(chunk)

    return sha256.hexdigest()


def write_run_record(record: dict[str, Any]) -> None:
    """
    Append one pipeline execution record to the local run ledger.
    """
    RUN_LEDGER_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with RUN_LEDGER_PATH.open(
        "a",
        encoding="utf-8",
    ) as ledger:
        ledger.write(
            json.dumps(record, default=str) + "\n"
        )


def successful_batch_exists(
    batch_id: str,
    source_checksum: str,
) -> bool:
    """
    Return True when the same logical batch and source
    checksum have already completed successfully.
    """
    if not RUN_LEDGER_PATH.exists():
        return False

    with RUN_LEDGER_PATH.open(
        "r",
        encoding="utf-8",
    ) as ledger:
        for line in ledger:
            if not line.strip():
                continue

            record = json.loads(line)

            if (
                record.get("batch_id") == batch_id
                and record.get("source_checksum")
                == source_checksum
                and record.get("status") == "SUCCESS"
            ):
                return True

    return False
import json
from src.common import run_context  # pyright: ignore[reportMissingImports]


def test_successful_batch_exists_returns_true_for_matching_success(
    tmp_path,
    monkeypatch,
):
    ledger = tmp_path / "run_ledger.jsonl"

    record = {
        "batch_id": "yellow_taxi_2025_08",
        "source_checksum": "abc123",
        "status": "SUCCESS",
    }

    ledger.write_text(
        json.dumps(record) + "\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        run_context,
        "RUN_LEDGER_PATH",
        ledger,
    )

    assert run_context.successful_batch_exists(
        "yellow_taxi_2025_08",
        "abc123",
    ) is True


def test_successful_batch_exists_ignores_failed_run(
    tmp_path,
    monkeypatch,
):
    ledger = tmp_path / "run_ledger.jsonl"

    record = {
        "batch_id": "yellow_taxi_2025_08",
        "source_checksum": "abc123",
        "status": "FAILED",
    }

    ledger.write_text(
        json.dumps(record) + "\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        run_context,
        "RUN_LEDGER_PATH",
        ledger,
    )

    assert run_context.successful_batch_exists(
        "yellow_taxi_2025_08",
        "abc123",
    ) is False


def test_successful_batch_exists_rejects_checksum_mismatch(
    tmp_path,
    monkeypatch,
):
    ledger = tmp_path / "run_ledger.jsonl"

    record = {
        "batch_id": "yellow_taxi_2025_08",
        "source_checksum": "old-checksum",
        "status": "SUCCESS",
    }

    ledger.write_text(
        json.dumps(record) + "\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        run_context,
        "RUN_LEDGER_PATH",
        ledger,
    )

    assert run_context.successful_batch_exists(
        "yellow_taxi_2025_08",
        "new-checksum",
    ) is False
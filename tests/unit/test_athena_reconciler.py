from unittest.mock import Mock

import pytest

from src.reconciliation.athena_reconciler import (
    AthenaQueryError,
    AthenaQueryTimeoutError,
    AthenaReconciler,
)


def build_reconciler(client=None):
    return AthenaReconciler(
        database="transitflow",
        table="curated_yellow_taxi",
        output_location="s3://test-bucket/athena-results/",
        athena_client=client or Mock(),
    )


def test_build_count_query_is_partition_scoped():
    reconciler = build_reconciler()

    query = reconciler.build_count_query(year=2025, month=8)

    assert '"transitflow"."curated_yellow_taxi"' in query
    assert "year = 2025" in query
    assert "month = 8" in query
    assert "COUNT(*)" in query


def test_count_partition_returns_athena_count():
    client = Mock()

    client.start_query_execution.return_value = {
        "QueryExecutionId": "query-123"
    }

    client.get_query_execution.return_value = {
        "QueryExecution": {
            "Status": {
                "State": "SUCCEEDED"
            }
        }
    }

    client.get_query_results.return_value = {
        "ResultSet": {
            "Rows": [
                {
                    "Data": [
                        {"VarCharValue": "record_count"}
                    ]
                },
                {
                    "Data": [
                        {"VarCharValue": "3574089"}
                    ]
                },
            ]
        }
    }

    reconciler = build_reconciler(client)

    result = reconciler.count_partition(
        year=2025,
        month=8,
    )

    assert result["query_execution_id"] == "query-123"
    assert result["year"] == 2025
    assert result["month"] == 8
    assert result["athena_records"] == 3574089


def test_reconcile_partition_returns_match():
    reconciler = build_reconciler()

    reconciler.count_partition = Mock(
        return_value={
            "query_execution_id": "query-123",
            "year": 2025,
            "month": 8,
            "athena_records": 3574089,
        }
    )

    result = reconciler.reconcile_partition(
        year=2025,
        month=8,
        expected_records=3574089,
    )

    assert result["expected_records"] == 3574089
    assert result["athena_records"] == 3574089
    assert result["difference"] == 0
    assert result["reconciliation_status"] == "MATCH"


def test_reconcile_partition_returns_mismatch():
    reconciler = build_reconciler()

    reconciler.count_partition = Mock(
        return_value={
            "query_execution_id": "query-456",
            "year": 2025,
            "month": 8,
            "athena_records": 3574000,
        }
    )

    result = reconciler.reconcile_partition(
        year=2025,
        month=8,
        expected_records=3574089,
    )

    assert result["expected_records"] == 3574089
    assert result["athena_records"] == 3574000
    assert result["difference"] == -89
    assert result["reconciliation_status"] == "MISMATCH"


def test_wait_for_query_raises_on_failed_query():
    client = Mock()

    client.get_query_execution.return_value = {
        "QueryExecution": {
            "Status": {
                "State": "FAILED",
                "StateChangeReason": "Test failure",
            }
        }
    }

    reconciler = build_reconciler(client)

    with pytest.raises(AthenaQueryError, match="Test failure"):
        reconciler.wait_for_query("query-failed")


def test_wait_for_query_times_out():
    client = Mock()

    client.get_query_execution.return_value = {
        "QueryExecution": {
            "Status": {
                "State": "RUNNING"
            }
        }
    }

    reconciler = build_reconciler(client)

    with pytest.raises(AthenaQueryTimeoutError):
        reconciler.wait_for_query(
            "query-running",
            timeout_seconds=0,
            poll_interval_seconds=0,
        )

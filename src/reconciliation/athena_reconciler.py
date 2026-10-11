from __future__ import annotations

import time
from typing import Any

import boto3


class AthenaQueryError(RuntimeError):
    """Raised when an Athena query fails or is cancelled."""


class AthenaQueryTimeoutError(TimeoutError):
    """Raised when an Athena query does not finish within the timeout."""


class AthenaReconciler:
    """
    Reconcile Spark-produced record counts against Athena.

    The reconciler runs a partition-scoped COUNT(*) query against the
    curated Glue/Athena table and compares the result with the expected
    record count produced by the Spark batch pipeline.
    """

    TERMINAL_STATES = {"SUCCEEDED", "FAILED", "CANCELLED"}

    def __init__(
        self,
        database: str,
        table: str,
        output_location: str,
        profile_name: str | None = None,
        region_name: str | None = None,
        athena_client: Any | None = None,
    ) -> None:
        self.database = database
        self.table = table
        self.output_location = output_location.rstrip("/") + "/"
        self.profile_name = profile_name
        self.region_name = region_name

        if athena_client is not None:
            self.client = athena_client
        else:
            session = boto3.Session(
                profile_name=profile_name,
                region_name=region_name,
            )
            self.client = session.client("athena")

    def build_count_query(self, year: int, month: int) -> str:
        """
        Build a partition-scoped Athena count query.

        Partition filtering keeps the reconciliation query targeted to the
        batch being validated instead of scanning unrelated partitions.
        """
        return (
            f'SELECT COUNT(*) AS record_count '
            f'FROM "{self.database}"."{self.table}" '
            f"WHERE year = {int(year)} AND month = {int(month)}"
        )

    def start_query(self, query: str) -> str:
        """Submit a query to Athena and return its execution ID."""
        response = self.client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                "Database": self.database,
            },
            ResultConfiguration={
                "OutputLocation": self.output_location,
            },
        )

        return response["QueryExecutionId"]

    def wait_for_query(
        self,
        query_execution_id: str,
        timeout_seconds: int = 120,
        poll_interval_seconds: float = 1.0,
    ) -> dict[str, Any]:
        """
        Poll Athena until the query reaches a terminal state.
        """
        started_at = time.monotonic()

        while True:
            response = self.client.get_query_execution(
                QueryExecutionId=query_execution_id
            )

            execution = response["QueryExecution"]
            status = execution["Status"]
            state = status["State"]

            if state == "SUCCEEDED":
                return execution

            if state in {"FAILED", "CANCELLED"}:
                reason = status.get(
                    "StateChangeReason",
                    "No failure reason returned by Athena.",
                )
                raise AthenaQueryError(
                    f"Athena query {query_execution_id} "
                    f"ended with state {state}: {reason}"
                )

            elapsed = time.monotonic() - started_at

            if elapsed >= timeout_seconds:
                raise AthenaQueryTimeoutError(
                    f"Athena query {query_execution_id} "
                    f"did not finish within {timeout_seconds} seconds."
                )

            time.sleep(poll_interval_seconds)

    def get_count_result(self, query_execution_id: str) -> int:
        """
        Read the COUNT(*) value returned by Athena.
        """
        response = self.client.get_query_results(
            QueryExecutionId=query_execution_id
        )

        rows = response.get("ResultSet", {}).get("Rows", [])

        if len(rows) < 2:
            raise AthenaQueryError(
                f"Athena query {query_execution_id} "
                "returned no count result."
            )

        data = rows[1].get("Data", [])

        if not data or "VarCharValue" not in data[0]:
            raise AthenaQueryError(
                f"Athena query {query_execution_id} "
                "returned an invalid count result."
            )

        try:
            return int(data[0]["VarCharValue"])
        except (TypeError, ValueError) as exc:
            raise AthenaQueryError(
                f"Athena query {query_execution_id} "
                "returned a non-integer count."
            ) from exc

    def count_partition(self, year: int, month: int) -> dict[str, Any]:
        """
        Run an Athena COUNT(*) query for one year/month partition.
        """
        query = self.build_count_query(year=year, month=month)

        query_execution_id = self.start_query(query)

        self.wait_for_query(query_execution_id)

        record_count = self.get_count_result(query_execution_id)

        return {
            "query_execution_id": query_execution_id,
            "year": int(year),
            "month": int(month),
            "athena_records": record_count,
        }

    def reconcile_partition(
        self,
        year: int,
        month: int,
        expected_records: int,
    ) -> dict[str, Any]:
        """
        Compare the Spark curated count with Athena's partition count.
        """
        result = self.count_partition(
            year=year,
            month=month,
        )

        athena_records = result["athena_records"]
        expected_records = int(expected_records)

        difference = athena_records - expected_records
        status = "MATCH" if difference == 0 else "MISMATCH"

        return {
            **result,
            "expected_records": expected_records,
            "difference": difference,
            "reconciliation_status": status,
        }

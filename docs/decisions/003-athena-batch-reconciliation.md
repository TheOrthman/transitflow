# ADR 003: Reconcile curated batches after Glue registration

## Decision

The batch runner builds AthenaReconciler with profile `transitflow-lp`, region
`eu-west-1`, database `transitflow`, table `curated_yellow_taxi`, and results
location `s3://transitflow-data-1789925359/athena-results/`.

Both new and previously processed batches reconcile after Glue returns CREATED
or EXISTS. The expected curated count is valid_records + warning_records;
quarantined rows are excluded. Existing batches recover counts from the run
ledger using batch ID and source checksum, skipping historical count-less
backfill records. Missing counts fail explicitly without issuing an Athena query.

Every completed comparison is appended to the ledger with query_execution_id,
year, month, expected_records, athena_records, difference, and
reconciliation_status. MATCH permits SUCCESS. MISMATCH records FAILED and raises,
stopping the batch loop. Query errors/timeouts record reconciliation_status FAILED
plus error_type/error_message and propagate. Earlier-stage failures leave the
comparison NOT_RUN. Disabled reconciliation records DISABLED on recorded runs.

Completed Spark processing remains reusable after an Athena mismatch or error.
Retries still check S3 publication independently, backfill only when necessary,
ensure the Glue partition, and run a fresh Athena comparison. This intentionally
adds one audit record/query on each enabled rerun, even when cloud state is intact.
Disabling Athena preserves the previous no-op rerun behavior.

ENABLE_ATHENA_RECONCILIATION controls client construction; S3 and Glue must also
be enabled. No IAM changes or additional AWS API permissions are introduced.
The existing AthenaReconciler implementation is reused unchanged.

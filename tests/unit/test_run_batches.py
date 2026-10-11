import json
from pathlib import Path
from unittest.mock import Mock

import pytest
import run_batches as runner
from src.common import run_context
from src.common.config import BatchConfig


@pytest.fixture
def batch(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(run_context, "RUN_LEDGER_PATH", tmp_path / "ledger.jsonl")
    config = BatchConfig("yellow_taxi", "yellow_tripdata", 2025, 8)
    source = Path(config.input_path)
    source.parent.mkdir(parents=True)
    source.write_bytes(b"source")
    pipeline = Mock(return_value=dict(rows_read=12, valid_records=8,
                                     warning_records=2, quarantined_records=2))
    publish = Mock(return_value=dict(curated_object_count=1,
                                    quarantine_object_count=1, quality_object_count=1))
    monkeypatch.setattr(runner, "run_batch_pipeline", pipeline)
    monkeypatch.setattr(runner, "publish_batch_to_s3", publish)
    monkeypatch.setattr(runner, "batch_is_published_to_s3", Mock(return_value=True))
    storage = Mock(bucket="test-bucket")
    catalog = Mock()
    catalog.register_partition.return_value = True
    reconciler = Mock()
    reconciler.reconcile_partition.return_value = dict(
        query_execution_id="query-123", year=2025, month=8,
        expected_records=10, athena_records=10, difference=0,
        reconciliation_status="MATCH")
    return config, storage, catalog, reconciler, pipeline, publish


def records():
    return [json.loads(line) for line in run_context.RUN_LEDGER_PATH.read_text().splitlines()]


def run(batch):
    runner.process_batch(None, *batch[:4])


def test_new_batch_order_counts_and_ledger(batch):
    config, storage, catalog, reconciler, pipeline, publish = batch
    calls = Mock()
    for name, child in [("pipeline", pipeline), ("publish", publish),
                        ("glue", catalog.register_partition),
                        ("athena", reconciler.reconcile_partition)]:
        calls.attach_mock(child, name)
    run(batch)
    assert [call[0] for call in calls.mock_calls] == ["pipeline", "publish", "glue", "athena"]
    reconciler.reconcile_partition.assert_called_once_with(year=2025, month=8, expected_records=10)
    record = records()[-1]
    assert record["status"] == "SUCCESS"
    assert record["reconciliation_status"] == "MATCH"
    assert record["query_execution_id"] == "query-123"
    assert record["difference"] == 0


@pytest.mark.parametrize("published,created", [(True, False), (False, False), (True, True)])
def test_existing_batch_reconciles_without_reprocessing(batch, monkeypatch, published, created):
    run(batch)
    runner.batch_is_published_to_s3.return_value = published
    batch[2].register_partition.return_value = created
    run(batch)
    batch[4].assert_called_once()
    assert batch[5].call_count == (1 if published else 2)
    assert batch[3].reconcile_partition.call_count == 2
    assert records()[-1]["processing_status"] == "SKIPPED_ALREADY_PROCESSED"
    assert records()[-1]["s3_publish_status"] == ("EXISTS" if published else "BACKFILLED")


@pytest.mark.parametrize("failure", ["MISMATCH", "error"])
def test_failure_recorded_and_retry_skips_spark(batch, failure):
    reconciler = batch[3]
    if failure == "MISMATCH":
        reconciler.reconcile_partition.return_value.update(
            reconciliation_status="MISMATCH", athena_records=9, difference=-1)
    else:
        reconciler.reconcile_partition.side_effect = TimeoutError("query timed out")
    with pytest.raises((RuntimeError, TimeoutError)):
        run(batch)
    record = records()[-1]
    assert record["status"] == "FAILED"
    assert record["reconciliation_status"] == ("MISMATCH" if failure == "MISMATCH" else "FAILED")
    assert record["s3_publish_status"] == "SUCCESS"
    assert record["glue_partition_status"] == "CREATED"
    if failure == "MISMATCH":
        assert record["difference"] == -1
        assert record["query_execution_id"] == "query-123"
    reconciler.reconcile_partition.side_effect = None
    reconciler.reconcile_partition.return_value.update(
        reconciliation_status="MATCH", athena_records=10, difference=0)
    run(batch)
    batch[4].assert_called_once()
    batch[5].assert_called_once()
    assert records()[-1]["status"] == "SUCCESS"


@pytest.mark.parametrize("disabled", ["storage", "catalog", "reconciler"])
def test_disabled_dependencies_skip_athena(batch, disabled):
    args = list(batch)
    args[{"storage": 1, "catalog": 2, "reconciler": 3}[disabled]] = None
    run(args)
    batch[3].reconcile_partition.assert_not_called()
    assert records()[-1]["reconciliation_status"] == "DISABLED"


def test_glue_failure_prevents_athena(batch):
    batch[2].register_partition.side_effect = RuntimeError("Glue unavailable")
    with pytest.raises(RuntimeError, match="Glue unavailable"):
        run(batch)
    batch[3].reconcile_partition.assert_not_called()
    assert records()[-1]["reconciliation_status"] == "NOT_RUN"


def test_legacy_counts_survive_countless_backfill(batch):
    config = batch[0]
    checksum = run_context.calculate_file_checksum(config.input_path)
    base = dict(batch_id=config.batch_id, source_checksum=checksum, status="SUCCESS")
    run_context.write_run_record(dict(base, valid_records=8, warning_records=2))
    run_context.write_run_record(base)
    run(batch)
    batch[4].assert_not_called()
    batch[3].reconcile_partition.assert_called_once_with(year=2025, month=8, expected_records=10)


def test_missing_historical_counts_fails_without_query(batch):
    config = batch[0]
    run_context.write_run_record(dict(batch_id=config.batch_id,
        source_checksum=run_context.calculate_file_checksum(config.input_path), status="SUCCESS"))
    with pytest.raises(RuntimeError, match="No processed counts"):
        run(batch)
    batch[4].assert_not_called()
    batch[3].reconcile_partition.assert_not_called()
    assert records()[-1]["reconciliation_status"] == "FAILED"


def test_reconciler_configuration(monkeypatch):
    factory = Mock()
    monkeypatch.setattr(runner, "AthenaReconciler", factory)
    assert runner.build_athena_reconciler() is factory.return_value
    factory.assert_called_once_with(database="transitflow", table="curated_yellow_taxi",
        output_location="s3://transitflow-data-1789925359/athena-results/",
        profile_name="transitflow-lp", region_name="eu-west-1")


@pytest.mark.parametrize("flag", ["ENABLE_S3_PUBLISH", "ENABLE_GLUE_REGISTRATION", "ENABLE_ATHENA_RECONCILIATION"])
def test_disabled_configuration_does_not_build_client(monkeypatch, flag):
    factory = Mock()
    monkeypatch.setattr(runner, "AthenaReconciler", factory)
    monkeypatch.setattr(runner, flag, False)
    assert runner.build_athena_reconciler() is None
    factory.assert_not_called()


def test_changed_source_is_reprocessed_after_mismatch(batch):
    batch[3].reconcile_partition.return_value.update(reconciliation_status="MISMATCH")
    with pytest.raises(RuntimeError, match="MISMATCH"):
        run(batch)
    Path(batch[0].input_path).write_bytes(b"changed source")
    batch[3].reconcile_partition.return_value.update(reconciliation_status="MATCH")
    run(batch)
    assert batch[4].call_count == 2


def test_disabled_reconciliation_preserves_noop_rerun(batch):
    args = list(batch)
    args[3] = None
    run(args)
    batch[2].register_partition.return_value = False
    run(args)
    batch[4].assert_called_once()
    batch[5].assert_called_once()
    assert len(records()) == 1


def test_existing_batch_mismatch_is_logged(batch):
    run(batch)
    batch[3].reconcile_partition.return_value.update(
        reconciliation_status="MISMATCH", athena_records=11, difference=1)
    with pytest.raises(RuntimeError, match="MISMATCH"):
        run(batch)
    assert len(records()) == 2
    assert records()[-1]["status"] == "FAILED"
    assert records()[-1]["difference"] == 1
    assert records()[-1]["processing_status"] == "SKIPPED_ALREADY_PROCESSED"
    batch[4].assert_called_once()


def test_main_passes_reconciler_and_stops_spark(monkeypatch):
    spark = Mock()
    session = Mock()
    session.builder.appName.return_value.getOrCreate.return_value = spark
    monkeypatch.setattr(runner, "import_module", Mock(return_value=Mock(SparkSession=session)))
    reconciler = Mock()
    monkeypatch.setattr(runner, "build_s3_storage", Mock(return_value=None))
    monkeypatch.setattr(runner, "build_glue_catalog", Mock(return_value=None))
    monkeypatch.setattr(runner, "build_athena_reconciler", Mock(return_value=reconciler))
    process = Mock(side_effect=RuntimeError("MISMATCH"))
    monkeypatch.setattr(runner, "process_batch", process)
    with pytest.raises(RuntimeError, match="MISMATCH"):
        runner.main()
    assert process.call_count == 1
    assert process.call_args.kwargs["reconciler"] is reconciler
    spark.stop.assert_called_once()

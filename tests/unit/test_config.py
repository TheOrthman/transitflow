import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

try:
    from common.config import BatchConfig  # type: ignore[reportMissingImports]
except ModuleNotFoundError:
    BatchConfig = None  # type: ignore[assignment]


def test_batch_config_builds_expected_values():
    assert BatchConfig is not None
    config = BatchConfig(
        dataset="yellow_taxi",
        source_prefix="yellow_tripdata",
        year=2025,
        month=8,
    )

    assert config.month_string == "08"
    assert config.period == "2025-08"
    assert config.batch_id == "yellow_taxi_2025_08"
    assert config.source_file == "yellow_tripdata_2025-08.parquet"

    assert config.input_path == (
        "data/raw/yellow_taxi/"
        "yellow_tripdata_2025-08.parquet"
    )

    assert config.curated_output == (
        "data/curated/yellow_taxi/2025-08"
    )

    assert config.quarantine_output == (
        "data/quarantine/yellow_taxi/2025-08"
    )

    assert config.metrics_output == (
        "data/metrics/yellow_taxi/2025-08"
    )
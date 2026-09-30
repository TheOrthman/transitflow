from pathlib import Path

import pandas as pd


DATA_PATH = (
    Path(__file__).resolve().parents[2]
    / "app"
    / "data"
    / "batch_metrics.csv"
)


def load_metrics():
    return pd.read_csv(DATA_PATH)


def test_batch_metrics_file_exists():
    assert DATA_PATH.exists()


def test_source_equals_curated_plus_quarantine():
    df = load_metrics()

    assert (
        df["source_records"]
        == df["curated_records"]
        + df["quarantined_records"]
    ).all()


def test_curated_equals_valid_plus_warning():
    df = load_metrics()

    assert (
        df["curated_records"]
        == df["valid_records"]
        + df["warning_records"]
    ).all()


def test_athena_matches_curated():
    df = load_metrics()

    assert (
        df["athena_records"]
        == df["curated_records"]
    ).all()


def test_all_counts_are_non_negative():
    df = load_metrics()

    count_columns = [
        "source_records",
        "valid_records",
        "warning_records",
        "quarantined_records",
        "curated_records",
        "athena_records",
    ]

    assert (df[count_columns] >= 0).all().all()


def test_expected_month_count():
    df = load_metrics()

    assert len(df) == 8

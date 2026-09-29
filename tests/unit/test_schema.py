from importlib import import_module

pytest = import_module("pytest")
import sys
from pathlib import Path
SparkSession = import_module("pyspark.sql").SparkSession

# Ensure the project root is importable when tests are run from another directory.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

try:
    schema_module = import_module("src.validation.schema")
except ModuleNotFoundError:
    import importlib.util

    schema_path = PROJECT_ROOT / "src" / "validation" / "schema.py"
    spec = importlib.util.spec_from_file_location("schema", schema_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load schema module from {schema_path}")
    schema_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(schema_module)

normalize_column_names = schema_module.normalize_column_names
validate_required_columns = schema_module.validate_required_columns


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("TransitFlowUnitTests")
        .getOrCreate()
    )

    yield session

    session.stop()


def test_normalize_column_names(spark):
    df = spark.createDataFrame(
        [
            (
                1,
                "2025-08-01 10:00:00",
                "2025-08-01 10:15:00",
                2.5,
                100,
                200,
                10.0,
                15.0,
            )
        ],
        [
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "trip_distance",
            "PULocationID",
            "DOLocationID",
            "fare_amount",
            "total_amount",
        ],
    )

    normalized = normalize_column_names(df)

    assert "vendor_id" in normalized.columns
    assert "pickup_datetime" in normalized.columns
    assert "dropoff_datetime" in normalized.columns
    assert "pickup_location_id" in normalized.columns
    assert "dropoff_location_id" in normalized.columns

    assert "VendorID" not in normalized.columns
    assert "PULocationID" not in normalized.columns


def test_validate_required_columns_accepts_valid_schema(spark):
    df = spark.createDataFrame(
        [],
        schema="""
            VendorID INT,
            tpep_pickup_datetime TIMESTAMP,
            tpep_dropoff_datetime TIMESTAMP,
            trip_distance DOUBLE,
            PULocationID INT,
            DOLocationID INT,
            fare_amount DOUBLE,
            total_amount DOUBLE
        """,
    )

    validate_required_columns(df)


def test_validate_required_columns_rejects_missing_column(spark):
    df = spark.createDataFrame(
        [],
        schema="""
            VendorID INT,
            tpep_pickup_datetime TIMESTAMP,
            tpep_dropoff_datetime TIMESTAMP,
            trip_distance DOUBLE,
            PULocationID INT,
            fare_amount DOUBLE,
            total_amount DOUBLE
        """,
    )

    with pytest.raises(ValueError):
        validate_required_columns(df)
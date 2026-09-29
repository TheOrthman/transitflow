from typing import Any


# Keep validation importable when PySpark is not installed in the current
# development environment.
DataFrame = Any


COLUMN_MAPPING = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "RatecodeID": "rate_code_id",
    "store_and_fwd_flag": "store_and_forward_flag",
    "PULocationID": "pickup_location_id",
    "DOLocationID": "dropoff_location_id",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "total_amount": "total_amount",
    "congestion_surcharge": "congestion_surcharge",
    "Airport_fee": "airport_fee",
    "cbd_congestion_fee": "cbd_congestion_fee",
}


REQUIRED_COLUMNS = {
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
    "total_amount",
}


def validate_required_columns(df: DataFrame) -> None:
    """
    Validate that required source columns exist.

    Raises
    ------
    ValueError
        If one or more required columns are missing.
    """
    available_columns = set(df.columns)

    missing_columns = REQUIRED_COLUMNS - available_columns

    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise ValueError(
            f"Source dataset is missing required columns: {missing}"
        )


def normalize_column_names(df: DataFrame) -> DataFrame:
    """
    Rename NYC TLC source columns into the TransitFlow
    canonical snake_case schema.
    """
    validate_required_columns(df)

    normalized = df

    for source_name, target_name in COLUMN_MAPPING.items():
        if source_name in normalized.columns:
            normalized = normalized.withColumnRenamed(
                source_name,
                target_name,
            )

    return normalized
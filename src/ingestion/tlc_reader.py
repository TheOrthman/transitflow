from pathlib import Path
from typing import Any


def read_tlc_parquet(
    spark: Any,
    input_path: str,
) -> Any:
    """
    Read an NYC TLC Parquet file into a Spark DataFrame.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    input_path : str
        Path to the source Parquet file.

    Returns
    -------
    DataFrame
        Raw TLC trip records.
    """
    path = Path(input_path)

    if not path.exists():
        raise FileNotFoundError(f"Input file does not exist: {input_path}")

    return spark.read.parquet(str(path))
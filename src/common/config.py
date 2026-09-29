from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class BatchConfig:
    dataset: str
    source_prefix: str
    year: int
    month: int
    schema_version: str = "1.0"

    @property
    def month_string(self) -> str:
        return f"{self.month:02d}"

    @property
    def period(self) -> str:
        return f"{self.year}-{self.month_string}"

    @property
    def batch_id(self) -> str:
        return (
            f"{self.dataset}_"
            f"{self.year}_"
            f"{self.month_string}"
        )

    @property
    def source_file(self) -> str:
        return (
            f"{self.source_prefix}_"
            f"{self.year}-{self.month_string}.parquet"
        )

    @property
    def input_path(self) -> str:
        return str(
            Path("data")
            / "raw"
            / self.dataset
            / self.source_file
        )

    @property
    def curated_output(self) -> str:
        return str(
            Path("data")
            / "curated"
            / self.dataset
            / self.period
        )

    @property
    def quarantine_output(self) -> str:
        return str(
            Path("data")
            / "quarantine"
            / self.dataset
            / self.period
        )

    @property
    def metrics_output(self) -> str:
        return str(
            Path("data")
            / "metrics"
            / self.dataset
            / self.period
        )
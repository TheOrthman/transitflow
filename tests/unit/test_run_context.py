from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parents[2] / "src"))

from common.run_context import calculate_file_checksum  # type: ignore[import-not-found]


def test_calculate_file_checksum(tmp_path: Path):
    file_path = tmp_path / "sample.txt"
    file_path.write_text("TransitFlow", encoding="utf-8")

    checksum = calculate_file_checksum(file_path)

    assert checksum == (
        "ccf00a82bde4065e1e3bc46a507fecd894ee47b9b3b0832156f9a5c4dfe511cc"
    )


def test_calculate_file_checksum_is_deterministic(
    tmp_path: Path,
):
    file_path = tmp_path / "sample.txt"
    file_path.write_text(
        "same content",
        encoding="utf-8",
    )

    first = calculate_file_checksum(file_path)
    second = calculate_file_checksum(file_path)

    assert first == second
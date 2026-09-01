from __future__ import annotations

from pathlib import Path

from generator import generate_mock_logs
from main import run_pipeline


def test_pipeline_end_to_end(tmp_path: Path) -> None:
    raw = tmp_path / "server.log"
    processed = tmp_path / "logs_lake"
    quarantine = tmp_path / "quarantine"

    generate_mock_logs(raw, lines=1000, days_spread=5)

    result = run_pipeline(
        raw_path=raw,
        processed_dir=processed,
        quarantine_dir=quarantine,
        generate_if_missing=False,
    )

    assert result["metrics"]["total_input"] == 1000
    assert result["metrics"]["valid_count"] > 0
    partitions = list(processed.glob("dt_partition=*"))
    assert len(partitions) > 0

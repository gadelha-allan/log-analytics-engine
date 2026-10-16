from __future__ import annotations

from pathlib import Path

import polars as pl

from src.generator import generate_mock_logs
from src.main import run_pipeline


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
    assert set(result) == {"lake_path", "quarantine_path", "metrics"}
    assert result["lake_path"] == str(processed)
    assert result["quarantine_path"] == str(quarantine)
    lake = pl.read_parquet(list(Path(result["lake_path"]).rglob("*.parquet")))
    quarantine_files = list(Path(result["quarantine_path"]).glob("*.parquet"))
    rejected_count = pl.read_parquet(quarantine_files).height if quarantine_files else 0
    assert lake.height == result["metrics"]["valid_count"]
    assert rejected_count == result["metrics"]["quarantine_count"]
    assert lake.height + rejected_count == 1000

from __future__ import annotations

from collections import Counter
from pathlib import Path

import polars as pl
import pytest

from src import processor
from src.main import run_pipeline


@pytest.mark.parametrize("scenario", ["validos", "rejeitados", "mistos"])
def test_pipeline_retorna_caminhos_sem_materializar_dataset(
    scenario: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    valid_line = (
        '10.0.0.1 - - [27/Jul/2026:12:00:00 +0000] "GET / HTTP/1.1" 200 3000000000'
    )
    rejected_line = '  linha, com "aspas"  '
    lines = {
        "validos": [valid_line] * 5,
        "rejeitados": [rejected_line] * 5,
        "mistos": [valid_line, rejected_line, valid_line, rejected_line, rejected_line],
    }[scenario]
    source = tmp_path / "input.log"
    source.write_text("\n".join(lines) + "\n")
    lake_dir, quarantine_dir = tmp_path / "lake", tmp_path / "quarantine"
    collect = pl.LazyFrame.collect
    iter_batches = processor.pq.ParquetFile.iter_batches
    batch_sizes = []

    def forbid_full_read(*args, **kwargs):
        pytest.fail("O pipeline tentou carregar um dataset completo em memoria")

    def collect_metrics(lf: pl.LazyFrame, *args, **kwargs):
        frame = collect(lf, *args, **kwargs)
        assert set(frame.columns) <= {"len", "rejection_reason", "count"}
        assert frame.height <= 4
        return frame

    def read_batches(parquet, *args, **kwargs):
        assert kwargs["batch_size"] == 2
        for batch in iter_batches(parquet, *args, **kwargs):
            assert batch.num_rows <= 2
            batch_sizes.append(batch.num_rows)
            yield batch

    with monkeypatch.context() as streaming:
        streaming.setattr(pl, "read_parquet", forbid_full_read)
        streaming.setattr(pl, "read_csv", forbid_full_read)
        streaming.setattr(processor.pq, "read_table", forbid_full_read)
        streaming.setattr(processor.pq.ParquetFile, "read", forbid_full_read)
        streaming.setattr(pl.LazyFrame, "collect", collect_metrics)
        streaming.setattr(processor.pq.ParquetFile, "iter_batches", read_batches)
        streaming.setattr(processor, "_VALIDATION_BATCH_SIZE", 2)
        publication = run_pipeline(source, lake_dir, quarantine_dir)

    assert set(publication) == {"lake_path", "quarantine_path", "metrics"}
    assert publication["lake_path"] == str(lake_dir)
    assert publication["quarantine_path"] == str(quarantine_dir)
    assert lake_dir.is_dir() and quarantine_dir.is_dir()
    assert sum(batch_sizes) == len(lines)
    assert len(batch_sizes) >= 3
    valid_count, rejected_count = lines.count(valid_line), lines.count(rejected_line)
    assert publication["metrics"] == {
        "total_input": len(lines),
        "valid_count": valid_count,
        "quarantine_count": rejected_count,
        "rejection_rate": rejected_count / len(lines),
        "rejection_breakdown": (
            [{"rejection_reason": "regex_mismatch", "count": rejected_count}]
            if rejected_count
            else []
        ),
    }
    lake = pl.read_parquet(list(Path(publication["lake_path"]).rglob("*.parquet")))
    assert lake["raw"].to_list() == [valid_line] * valid_count
    assert lake["size"].to_list() == [3_000_000_000] * valid_count
    assert lake.schema["size"] == pl.Int64
    if rejected_count:
        rejected = pl.read_parquet(
            Path(publication["quarantine_path"]) / "quarantine.parquet"
        )
        assert (
            rejected.select("raw", "rejection_reason").rows()
            == [(rejected_line, "regex_mismatch")] * rejected_count
        )
        assert Counter(lake["raw"].to_list() + rejected["raw"].to_list()) == Counter(
            lines
        )
    else:
        assert not list(quarantine_dir.glob("*.parquet"))


def test_validacao_detecta_pagina_corrompida_com_schema_intacto(tmp_path: Path) -> None:
    parquet_path = tmp_path / "data.parquet"
    schema = pl.Schema({"raw": pl.String})
    pl.DataFrame({"raw": ["a", "b", "c"]}).write_parquet(
        parquet_path, compression="uncompressed", statistics=False
    )
    with processor.pq.ParquetFile(parquet_path) as parquet:
        offset = parquet.metadata.row_group(0).column(0).data_page_offset
    contents = bytearray(parquet_path.read_bytes())
    contents[offset : offset + 20] = b"\xff" * 20
    parquet_path.write_bytes(contents)
    assert pl.scan_parquet(parquet_path).collect_schema() == schema

    with pytest.raises(OSError):
        processor._validate_prepared_output(tmp_path, schema, 3)

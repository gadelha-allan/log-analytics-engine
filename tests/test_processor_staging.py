from __future__ import annotations

from collections import Counter
from pathlib import Path

import polars as pl
import pytest
from polars.exceptions import ComputeError
from polars.testing import assert_frame_equal

from src import processor


def _published_files(*directories: Path) -> dict[Path, tuple[bytes, int]]:
    return {
        path: (path.read_bytes(), path.stat().st_mtime_ns)
        for directory in directories
        for path in directory.rglob("*")
        if path.is_file()
    }


def _assert_no_staging(*directories: Path) -> None:
    for directory in directories:
        assert not list(directory.parent.glob(f".{directory.name}.staging-*"))


@pytest.mark.parametrize("published", [False, True])
@pytest.mark.parametrize(
    ("failure", "exception", "message"),
    [
        ("parsing", ComputeError, "parsing interrompido"),
        ("lake_write", OSError, "escrita interrompida"),
        ("quarantine_write", OSError, "escrita interrompida"),
        ("lake_schema", ValueError, "Schema invalido"),
        ("quarantine_schema", ValueError, "Schema invalido"),
        ("lake_count", ValueError, "Contagem invalida"),
        ("quarantine_count", ValueError, "Contagem invalida"),
        ("corrupt_parquet", ComputeError, "parquet"),
        ("readback", OSError, "leitura interrompida"),
        ("accounting", ValueError, "Contagem inconsistente"),
    ],
)
def test_falha_na_preparacao_preserva_publicacao_e_limpa_temporarios(
    failure: str,
    exception: type[Exception],
    message: str,
    published: bool,
    tmp_path: Path,
    mock_log_file: Path,
    temp_output_dirs: tuple[Path, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output, quarantine = temp_output_dirs
    if published:
        processor.process_logs(mock_log_file, output, quarantine)
    before = _published_files(output, quarantine)
    replacement = tmp_path / "replacement.log"
    replacement.write_text(
        '10.0.0.1 - - [28/Jul/2026:14:32:10 +0000] "GET /new HTTP/1.1" 200 100\n'
        '10.0.0.1 - - [29/Jul/2026:14:32:10 +0000] "GET /new HTTP/1.1" 200 200\n'
        'linha, "invalida"\n'
        "outra linha invalida\n"
    )

    def fail_parsing(lf: pl.LazyFrame) -> pl.LazyFrame:
        assert _published_files(output, quarantine) == before
        raise ComputeError("parsing interrompido")

    apply_rules = processor._apply_quality_rules

    def drop_valid_rows(lf: pl.LazyFrame) -> tuple[pl.LazyFrame, pl.LazyFrame]:
        valid, rejected = apply_rules(lf)
        return valid.limit(0), rejected

    sink_parquet = pl.LazyFrame.sink_parquet
    write_count = 0

    def write_prepared(lf: pl.LazyFrame, destination, **kwargs):
        nonlocal write_count
        write_count += 1
        assert _published_files(output, quarantine) == before
        written = sink_parquet(lf, destination, **kwargs)
        if (failure == "lake_write" and write_count == 1) or (
            failure == "quarantine_write" and write_count == 2
        ):
            raise OSError("escrita interrompida")

        if write_count == 2 and failure in {
            "lake_schema",
            "quarantine_schema",
            "lake_count",
            "quarantine_count",
            "corrupt_parquet",
        }:
            if failure.startswith("quarantine"):
                parquet_file = Path(destination)
            else:
                lake_stage = next(output.parent.glob(f".{output.name}.staging-*"))
                parquet_file = sorted(lake_stage.rglob("*.parquet"))[-1]
            if failure.endswith("schema"):
                pl.read_parquet(parquet_file).with_columns(
                    pl.col("size").cast(pl.Int32)
                ).write_parquet(parquet_file)
            elif failure.endswith("count"):
                pl.read_parquet(parquet_file).head(0).write_parquet(parquet_file)
            else:
                parquet_file.write_bytes(b"broken parquet")
        assert _published_files(output, quarantine) == before
        return written

    def fail_readback(*args, **kwargs):
        assert _published_files(output, quarantine) == before
        raise OSError("leitura interrompida")

    monkeypatch.setattr(pl.LazyFrame, "sink_parquet", write_prepared)
    if failure == "parsing":
        monkeypatch.setattr(processor, "_extract_and_type", fail_parsing)
    elif failure == "accounting":
        monkeypatch.setattr(processor, "_apply_quality_rules", drop_valid_rows)
    elif failure == "readback":
        monkeypatch.setattr(pl, "read_parquet", fail_readback)

    with pytest.raises(exception, match=message):
        processor.process_logs(replacement, output, quarantine, full_refresh=True)

    assert _published_files(output, quarantine) == before
    if not published:
        assert not output.exists()
        assert not quarantine.exists()
    _assert_no_staging(output, quarantine)


def test_publicacao_aguarda_validacao_dos_dois_diretorios(
    mock_log_file: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "lake-volume" / "logs"
    quarantine = tmp_path / "quarantine-volume" / "rejected"
    processor.process_logs(mock_log_file, output, quarantine)
    before = _published_files(output, quarantine)
    replacement = tmp_path / "replacement.log"
    replacement.write_text(mock_log_file.read_text().replace("27/Jul", "28/Jul"))
    validate = processor._validate_prepared_output
    prepared_outputs = []

    def validate_while_published(directory, schema, count):
        assert _published_files(output, quarantine) == before
        target = (output, quarantine)[len(prepared_outputs)]
        assert directory != target
        assert directory.parent == target.parent
        prepared = validate(directory, schema, count)
        prepared_outputs.append(prepared)
        assert _published_files(output, quarantine) == before
        return prepared

    monkeypatch.setattr(
        processor, "_validate_prepared_output", validate_while_published
    )
    processed = processor.process_logs(
        replacement, output, quarantine, full_refresh=True
    )

    assert len(prepared_outputs) == 2
    assert_frame_equal(processed["valid"], prepared_outputs[0])
    assert_frame_equal(processed["quarantine"], prepared_outputs[1])
    assert_frame_equal(
        pl.read_parquet(list(output.rglob("*.parquet"))), processed["valid"]
    )
    assert_frame_equal(
        pl.read_parquet(quarantine / "quarantine.parquet"), processed["quarantine"]
    )
    assert not (output / "dt_partition=2026-07-27").exists()
    assert processed["metrics"]["total_input"] == (
        processed["valid"].height + processed["quarantine"].height
    )
    _assert_no_staging(output, quarantine)


@pytest.mark.parametrize("published", [False, True])
def test_entrada_vazia_recusada_sem_substituir_publicacao(
    published: bool,
    tmp_path: Path,
    mock_log_file: Path,
    temp_output_dirs: tuple[Path, Path],
) -> None:
    output, quarantine = temp_output_dirs
    if published:
        processor.process_logs(mock_log_file, output, quarantine)
    before = _published_files(output, quarantine)
    empty = tmp_path / "empty.log"
    empty.write_bytes(b"")

    with pytest.raises(ValueError, match="Arquivo de entrada vazio"):
        processor.process_logs(empty, output, quarantine, full_refresh=published)

    assert _published_files(output, quarantine) == before
    if not published:
        assert not output.exists()
        assert not quarantine.exists()
    _assert_no_staging(output, quarantine)


def test_entrada_inteiramente_rejeitada_publica_lake_vazio_com_schema(
    mock_log_file: Path,
    tmp_path: Path,
    temp_output_dirs: tuple[Path, Path],
) -> None:
    output, quarantine = temp_output_dirs
    previous = processor.process_logs(mock_log_file, output, quarantine)
    rejected_lines = ['  linha, com "aspas"  ', "", "outra linha", "outra linha"]
    rejected_input = tmp_path / "rejected.log"
    rejected_input.write_text("\n".join(rejected_lines) + "\n")

    processed = processor.process_logs(
        rejected_input, output, quarantine, full_refresh=True
    )

    lake = pl.read_parquet(list(output.rglob("*.parquet")))
    assert lake.is_empty()
    assert lake.schema == previous["valid"].schema
    assert_frame_equal(lake, processed["valid"])
    assert not list(output.glob("dt_partition=*"))
    rejected = pl.read_parquet(quarantine / "quarantine.parquet")
    assert Counter(rejected.select("raw", "rejection_reason").rows()) == Counter(
        (line, "regex_mismatch") for line in rejected_lines
    )
    assert processed["metrics"] == {
        "total_input": 4,
        "valid_count": 0,
        "quarantine_count": 4,
        "rejection_rate": 1.0,
        "rejection_breakdown": [{"rejection_reason": "regex_mismatch", "count": 4}],
    }
    _assert_no_staging(output, quarantine)


@pytest.mark.parametrize("layout", ["same", "quarantine_inside", "lake_inside"])
def test_diretorios_sobrepostos_recusados_antes_de_escrever(
    layout: str, mock_log_file: Path, tmp_path: Path
) -> None:
    output = tmp_path / "published"
    quarantine = {
        "same": output,
        "quarantine_inside": output / "quarantine",
        "lake_inside": tmp_path,
    }[layout]
    output.mkdir()
    (output / "evidence.txt").write_text("dados publicados")
    before = _published_files(output)

    with pytest.raises(ValueError, match="sem sobreposicao"):
        processor.process_logs(mock_log_file, output, quarantine, full_refresh=True)

    assert _published_files(output) == before
    _assert_no_staging(output, quarantine)

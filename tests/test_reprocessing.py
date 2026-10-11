from __future__ import annotations

import subprocess
import sys
from collections import Counter
from datetime import date
from pathlib import Path

import polars as pl
import pytest
from polars.exceptions import ComputeError

from src.main import main

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ValidRow = tuple[str, str, int, int, date, bool]

OLD_VALID: list[ValidRow] = [
    (
        '10.0.0.1 - - [27/Jul/2026:12:00:00 +0000] "GET /old HTTP/1.1" 200 100',
        "/old",
        200,
        100,
        date(2026, 7, 27),
        False,
    ),
    (
        '10.0.0.1 - - [28/Jul/2026:12:00:00 +0000] "GET /retained HTTP/1.1" 404 3000000000',
        "/retained",
        404,
        3_000_000_000,
        date(2026, 7, 28),
        True,
    ),
]
OLD_VALID.append(OLD_VALID[0])
OLD_REJECTED = [
    ('  original, "sem formato"  ', "regex_mismatch"),
    (
        '10.0.0.1 - - [28/Jul/2026:12:00:00 +0000] "GET /old HTTP/1.1" 200 -1',
        "negative_size",
    ),
]
NEW_VALID: list[ValidRow] = [
    OLD_VALID[1],
    OLD_VALID[1],
    (
        '10.0.0.1 - - [29/Jul/2026:12:00:00 +0000] "GET /new HTTP/1.1" 500 42',
        "/new",
        500,
        42,
        date(2026, 7, 29),
        True,
    ),
]
NEW_REJECTED = [
    (
        '10.0.0.1 - - [29/Jul/2026:12:00:00 +0000] "GET /new HTTP/1.1" 900 5',
        "invalid_status",
    ),
    (
        '10.0.0.1 - - [31/Feb/2026:12:00:00 +0000] "GET /new HTTP/1.1" 200 5',
        "invalid_date",
    ),
]


@pytest.fixture
def reprocessing_inputs(tmp_path: Path) -> tuple[Path, Path]:
    old_input, new_input = tmp_path / "old.log", tmp_path / "complete.log"
    for path, valid, rejected in (
        (old_input, OLD_VALID, OLD_REJECTED),
        (new_input, NEW_VALID, NEW_REJECTED),
    ):
        path.write_text("\n".join(row[0] for row in [*valid, *rejected]) + "\n")
    return old_input, new_input


def _cli_command(
    raw: Path, output: Path, quarantine: Path, full_refresh: bool
) -> list[str]:
    command = [
        sys.executable,
        "-m",
        "src.main",
        "--raw",
        str(raw),
        "--output",
        str(output),
        "--quarantine",
        str(quarantine),
    ]
    if full_refresh:
        command.append("--full-refresh")
    return command


def _run_cli(
    raw: Path, output: Path, quarantine: Path, *, full_refresh: bool = False
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        _cli_command(raw, output, quarantine, full_refresh),
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )


def _assert_final_output(
    output: Path,
    quarantine: Path,
    expected_valid: list[ValidRow],
    expected_rejected: list[tuple[str, str]],
) -> None:
    lake = pl.read_parquet(sorted(output.rglob("*.parquet")), hive_partitioning=False)
    assert lake.schema == pl.Schema(
        {
            "raw": pl.String,
            "ip": pl.String,
            "date": pl.String,
            "method": pl.String,
            "endpoint": pl.String,
            "status": pl.Int32,
            "size": pl.Int64,
            "dt_partition": pl.Date,
            "is_error": pl.Boolean,
        }
    )
    assert Counter(
        lake.select(
            "raw", "endpoint", "status", "size", "dt_partition", "is_error"
        ).rows()
    ) == Counter(expected_valid)
    assert lake["ip"].to_list() == ["10.0.0.1"] * len(expected_valid)
    assert lake["method"].to_list() == ["GET"] * len(expected_valid)
    assert {path.name for path in output.glob("dt_partition=*")} == {
        f"dt_partition={row[4].isoformat()}" for row in expected_valid
    }

    rejected_raw: list[str] = []
    if expected_rejected:
        rejected = pl.read_parquet(sorted(quarantine.rglob("*.parquet")))
        assert rejected.schema == pl.Schema(
            {**lake.schema, "rejection_reason": pl.String()}
        )
        assert Counter(rejected.select("raw", "rejection_reason").rows()) == Counter(
            expected_rejected
        )
        rejected_raw = rejected["raw"].to_list()
    else:
        assert list(quarantine.iterdir()) == []

    assert lake.height + len(rejected_raw) == len(expected_valid) + len(
        expected_rejected
    )
    assert Counter(lake["raw"].to_list() + rejected_raw) == Counter(
        row[0] for row in [*expected_valid, *expected_rejected]
    )


def _snapshot(*directories: Path) -> dict[Path, tuple[bytes, int]]:
    return {
        path: (path.read_bytes(), path.stat().st_mtime_ns)
        for directory in directories
        for path in directory.rglob("*")
        if path.is_file()
    }


def _assert_no_leftovers(*directories: Path) -> None:
    for directory in directories:
        for suffix in ("staging", "backup"):
            assert not list(directory.parent.glob(f".{directory.name}.{suffix}-*"))


def test_full_refresh_repetido_nao_acrescenta_copias(
    reprocessing_inputs: tuple[Path, Path], temp_output_dirs: tuple[Path, Path]
) -> None:
    old_input, _ = reprocessing_inputs
    output, quarantine = temp_output_dirs
    for full_refresh in (False, True, True):
        completed = _run_cli(old_input, output, quarantine, full_refresh=full_refresh)
        assert completed.returncode == 0, completed.stderr
        _assert_final_output(output, quarantine, OLD_VALID, OLD_REJECTED)
        _assert_no_leftovers(output, quarantine)


def test_cli_sem_flag_preserva_arquivos_e_registros_anteriores(
    reprocessing_inputs: tuple[Path, Path], temp_output_dirs: tuple[Path, Path]
) -> None:
    old_input, new_input = reprocessing_inputs
    output, quarantine = temp_output_dirs
    first = _run_cli(old_input, output, quarantine)
    assert first.returncode == 0, first.stderr
    before = _snapshot(output, quarantine)

    refused = _run_cli(new_input, output, quarantine)

    assert refused.returncode == 1
    assert "Ja existem dados publicados" in refused.stderr
    assert "--full-refresh" in refused.stderr
    assert "Traceback" not in refused.stderr
    assert _snapshot(output, quarantine) == before
    _assert_final_output(output, quarantine, OLD_VALID, OLD_REJECTED)
    _assert_no_leftovers(output, quarantine)


@pytest.mark.parametrize("rejected", [False, True])
def test_cli_reconstroi_exatamente_o_novo_conjunto_completo(
    rejected: bool,
    reprocessing_inputs: tuple[Path, Path],
    temp_output_dirs: tuple[Path, Path],
) -> None:
    old_input, new_input = reprocessing_inputs
    output, quarantine = temp_output_dirs
    first = _run_cli(old_input, output, quarantine)
    assert first.returncode == 0, first.stderr
    if not rejected:
        new_input.write_text("\n".join(row[0] for row in NEW_VALID) + "\n")

    refreshed = _run_cli(new_input, output, quarantine, full_refresh=True)

    assert refreshed.returncode == 0, refreshed.stderr
    _assert_final_output(
        output, quarantine, NEW_VALID, NEW_REJECTED if rejected else []
    )
    _assert_no_leftovers(output, quarantine)


@pytest.mark.parametrize(
    ("failure", "error"),
    [
        ("parsing", ComputeError),
        ("writing", OSError),
        ("validation", ValueError),
        ("backup", OSError),
        ("publication", OSError),
    ],
)
def test_falha_no_reprocessamento_mantem_o_par_anterior_e_permite_nova_tentativa(
    failure: str,
    error: type[Exception],
    reprocessing_inputs: tuple[Path, Path],
    temp_output_dirs: tuple[Path, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    old_input, new_input = reprocessing_inputs
    output, quarantine = temp_output_dirs
    first = _run_cli(old_input, output, quarantine)
    assert first.returncode == 0, first.stderr
    before = _snapshot(output, quarantine)
    complete_input = new_input.read_bytes()
    sink_parquet = pl.LazyFrame.sink_parquet
    rename = Path.rename

    def fail_preparation(lf: pl.LazyFrame, destination, **kwargs):
        written = sink_parquet(lf, destination, **kwargs)
        if isinstance(destination, Path) and destination.name == "quarantine.parquet":
            if failure == "writing":
                raise OSError("quarantine write failed")
            pl.read_parquet(destination).drop("raw").write_parquet(destination)
        return written

    def fail_replacement(source: Path, target: Path) -> Path:
        if (failure == "backup" and source == quarantine) or (
            failure == "publication"
            and Path(target) == quarantine
            and ".staging-" in source.name
        ):
            raise OSError("quarantine replacement failed")
        return rename(source, target)

    with monkeypatch.context() as fault:
        fault.setattr(
            sys, "argv", _cli_command(new_input, output, quarantine, True)[2:]
        )
        if failure == "parsing":
            new_input.write_bytes(b"\xff\n")
        elif failure in {"writing", "validation"}:
            fault.setattr(pl.LazyFrame, "sink_parquet", fail_preparation)
        else:
            fault.setattr(Path, "rename", fail_replacement)
        with pytest.raises(error):
            main()

    assert _snapshot(output, quarantine) == before
    _assert_final_output(output, quarantine, OLD_VALID, OLD_REJECTED)
    _assert_no_leftovers(output, quarantine)
    new_input.write_bytes(complete_input)

    retried = _run_cli(new_input, output, quarantine, full_refresh=True)
    assert retried.returncode == 0, retried.stderr
    _assert_final_output(output, quarantine, NEW_VALID, NEW_REJECTED)
    _assert_no_leftovers(output, quarantine)

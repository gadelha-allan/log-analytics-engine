from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_cli_help_descreve_full_refresh() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "src.main", "--help"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert "--full-refresh" in result.stdout
    assert "reconstrucao completa" in result.stdout


def test_cli_exige_full_refresh_e_preserva_publicacao(
    mock_log_file: Path, tmp_path: Path, temp_output_dirs: tuple[Path, Path]
) -> None:
    output, quarantine = temp_output_dirs
    command = [
        sys.executable,
        "-m",
        "src.main",
        "--raw",
        str(mock_log_file),
        "--output",
        str(output),
        "--quarantine",
        str(quarantine),
    ]
    first = subprocess.run(
        command, cwd=PROJECT_ROOT, capture_output=True, text=True, check=False
    )
    assert first.returncode == 0, first.stderr
    before = {
        path: (path.read_bytes(), path.stat().st_mtime_ns)
        for directory in (output, quarantine)
        for path in directory.rglob("*")
        if path.is_file()
    }
    assert before
    replacement_line = (
        '10.0.0.1 - - [28/Jul/2026:12:00:00 +0000] "GET /replacement HTTP/1.1" 200 100'
    )
    replacement_input = tmp_path / "replacement.log"
    replacement_input.write_text(replacement_line + "\n")
    command[command.index("--raw") + 1] = str(replacement_input)

    refused = subprocess.run(
        command, cwd=PROJECT_ROOT, capture_output=True, text=True, check=False
    )
    assert refused.returncode == 1
    assert "Ja existem dados publicados" in refused.stderr
    assert "--full-refresh" in refused.stderr
    assert "reconstrucao completa" in refused.stderr
    assert "Traceback" not in refused.stderr
    after = {
        path: (path.read_bytes(), path.stat().st_mtime_ns)
        for directory in (output, quarantine)
        for path in directory.rglob("*")
        if path.is_file()
    }
    assert after == before

    refreshed = subprocess.run(
        [*command, "--full-refresh"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert refreshed.returncode == 0, refreshed.stderr
    persisted = pl.read_parquet(list(output.rglob("*.parquet")))
    assert persisted["raw"].to_list() == [replacement_line]
    assert not (output / "dt_partition=2026-07-27").exists()
    assert not (quarantine / "quarantine.parquet").exists()

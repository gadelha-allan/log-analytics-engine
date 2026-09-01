from __future__ import annotations

from pathlib import Path

import polars as pl

from processor import process_logs


def test_extracao_regex_campos_corretos(
    mock_log_file: Path, temp_output_dirs: tuple
) -> None:
    output, quarantine = temp_output_dirs
    result = process_logs(mock_log_file, output, quarantine)
    df = result["valid"]
    row = df.row(0, named=True)
    assert row["ip"] == "192.168.0.1"
    assert row["endpoint"] == "/api/v1/users"
    assert row["status"] == 200
    assert row["size"] == 3421
    assert row["method"] == "GET"


def test_descarte_linhas_invalidas(
    mock_log_file: Path, temp_output_dirs: tuple
) -> None:
    output, quarantine = temp_output_dirs
    result = process_logs(mock_log_file, output, quarantine)
    assert result["metrics"]["total_input"] == 5
    assert result["metrics"]["valid_count"] == 2
    assert result["metrics"]["quarantine_count"] == 3


def test_quarantine_tem_rejection_reason(
    mock_log_file: Path, temp_output_dirs: tuple
) -> None:
    output, quarantine = temp_output_dirs
    result = process_logs(mock_log_file, output, quarantine)
    df_q = result["quarantine"]
    assert df_q is not None
    assert "rejection_reason" in df_q.columns
    reasons = set(df_q["rejection_reason"].to_list())
    assert reasons == {"regex_mismatch", "invalid_status"}


def test_regra_is_error(mock_log_file: Path, temp_output_dirs: tuple) -> None:
    output, quarantine = temp_output_dirs
    result = process_logs(mock_log_file, output, quarantine)
    df = result["valid"]
    assert df.filter(pl.col("status") == 200)["is_error"][0] is False


def test_tipagem_colunas(mock_log_file: Path, temp_output_dirs: tuple) -> None:
    output, quarantine = temp_output_dirs
    result = process_logs(mock_log_file, output, quarantine)
    df = result["valid"]
    assert df.schema["status"] == pl.Int32
    assert df.schema["size"] == pl.Int32
    assert df.schema["dt_partition"] == pl.Date
    assert df.schema["is_error"] == pl.Boolean


def test_pipeline_idempotente(mock_log_file: Path, temp_output_dirs: tuple) -> None:
    output, quarantine = temp_output_dirs
    result1 = process_logs(mock_log_file, output, quarantine)
    result2 = process_logs(mock_log_file, output, quarantine)
    assert result1["metrics"]["valid_count"] == result2["metrics"]["valid_count"]

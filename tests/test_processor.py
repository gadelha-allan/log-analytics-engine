from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from src.processor import process_logs


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
    assert row["date"] == "27/Jul/2026:14:32:10 +0000"
    assert row["dt_partition"] == date(2026, 7, 27)


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
    assert df_q.filter(pl.col("rejection_reason") == "regex_mismatch")[
        "ip"
    ].to_list() == [
        None,
        None,
    ]
    invalid_status = df_q.filter(pl.col("rejection_reason") == "invalid_status")
    assert invalid_status.select("ip", "status", "size").to_dicts() == [
        {"ip": "192.168.0.1", "status": 900, "size": 3421}
    ]


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
    assert df.schema["size"] == pl.Int64
    assert df.schema["dt_partition"] == pl.Date
    assert df.schema["is_error"] == pl.Boolean


def test_pipeline_idempotente(mock_log_file: Path, temp_output_dirs: tuple) -> None:
    output, quarantine = temp_output_dirs
    result1 = process_logs(mock_log_file, output, quarantine)
    result2 = process_logs(mock_log_file, output, quarantine)
    assert result1["metrics"]["valid_count"] == result2["metrics"]["valid_count"]


@pytest.mark.parametrize(
    "timestamp",
    [
        "data-invalida",
        "31/Feb/2026:14:32:10 +0000",
        "27/Jul/2026:25:32:10 +0000",
        "27/Jul/2026:14:32:10 +9999",
    ],
)
def test_data_invalida_rejeitada(
    timestamp: str, tmp_path: Path, temp_output_dirs: tuple[Path, Path]
) -> None:
    log_file = tmp_path / "invalid_date.log"
    log_file.write_text(
        f'192.168.0.1 - - [{timestamp}] "GET /invalid-date HTTP/1.1" 200 3421\n'
        '10.0.0.5 - - [27/Jul/2026:15:00:00 +0000] "GET /valid HTTP/1.1" 200 500\n'
    )
    output, quarantine = temp_output_dirs
    result = process_logs(log_file, output, quarantine)

    assert result["metrics"]["total_input"] == 2
    assert result["metrics"]["valid_count"] == 1
    assert result["metrics"]["quarantine_count"] == 1
    assert result["metrics"]["rejection_breakdown"] == [
        {"rejection_reason": "invalid_date", "count": 1}
    ]
    assert result["valid"]["dt_partition"].to_list() == [date(2026, 7, 27)]
    assert result["valid"]["endpoint"].to_list() == ["/valid"]
    rejected = result["quarantine"].row(0, named=True)
    assert rejected["date"] == timestamp
    assert rejected["dt_partition"] is None
    assert rejected["size"] == 3421
    assert rejected["status"] == 200
    assert rejected["rejection_reason"] == "invalid_date"
    persisted = pl.read_parquet(quarantine / "quarantine.parquet")
    assert persisted.to_dicts() == result["quarantine"].to_dicts()


@pytest.mark.parametrize("size", [-1, -3_000_000_000])
def test_tamanho_negativo_rejeitado(
    size: int, tmp_path: Path, temp_output_dirs: tuple[Path, Path]
) -> None:
    log_file = tmp_path / "negative_size.log"
    log_file.write_text(
        f'192.168.0.1 - - [27/Jul/2026:14:32:10 +0000] "GET / HTTP/1.1" 200 {size}\n'
    )
    output, quarantine = temp_output_dirs
    result = process_logs(log_file, output, quarantine)

    assert result["valid"].is_empty()
    assert result["metrics"]["total_input"] == 1
    assert result["metrics"]["quarantine_count"] == 1
    assert result["metrics"]["rejection_breakdown"] == [
        {"rejection_reason": "negative_size", "count": 1}
    ]
    rejected = result["quarantine"].row(0, named=True)
    assert rejected["ip"] == "192.168.0.1"
    assert rejected["status"] == 200
    assert rejected["size"] == size
    assert rejected["dt_partition"] == date(2026, 7, 27)
    assert rejected["rejection_reason"] == "negative_size"
    assert result["quarantine"].schema["size"] == pl.Int64


@pytest.mark.parametrize("size", [0, 2**31, 3_000_000_000, 2**63 - 1])
def test_tamanho_int64_preservado(
    size: int, tmp_path: Path, temp_output_dirs: tuple[Path, Path]
) -> None:
    log_file = tmp_path / "response_size.log"
    log_file.write_text(
        f'192.168.0.1 - - [27/Jul/2026:14:32:10 +0000] "GET / HTTP/1.1" 200 {size}\n'
    )
    output, quarantine = temp_output_dirs
    result = process_logs(log_file, output, quarantine)

    assert result["metrics"]["valid_count"] == 1
    assert result["metrics"]["quarantine_count"] == 0
    assert result["metrics"]["rejection_breakdown"] == []
    assert result["quarantine"] is None
    assert result["valid"]["size"].to_list() == [size]
    assert result["valid"].schema["size"] == pl.Int64
    persisted = pl.read_parquet(list(output.rglob("*.parquet")))
    assert persisted["size"].to_list() == [size]
    assert persisted.schema["size"] == pl.Int64


@pytest.mark.parametrize(
    "ip", ["256.168.0.1", "1192.168.0.1", "999.168.0.1", "1.192.168.0.1"]
)
def test_ip_invalido_nao_aceita_trecho_da_regex(
    ip: str, tmp_path: Path, temp_output_dirs: tuple[Path, Path]
) -> None:
    log_file = tmp_path / "invalid_ip.log"
    log_file.write_text(
        f'{ip} - - [27/Jul/2026:14:32:10 +0000] "GET / HTTP/1.1" 200 3421\n'
    )
    output, quarantine = temp_output_dirs
    result = process_logs(log_file, output, quarantine)

    assert result["valid"].is_empty()
    assert result["metrics"]["total_input"] == 1
    assert result["metrics"]["quarantine_count"] == 1
    assert result["metrics"]["rejection_breakdown"] == [
        {"rejection_reason": "regex_mismatch", "count": 1}
    ]
    rejected = result["quarantine"].row(0, named=True)
    assert rejected["ip"] is None
    assert rejected["size"] is None
    assert rejected["status"] is None
    assert rejected["rejection_reason"] == "regex_mismatch"

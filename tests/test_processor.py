from __future__ import annotations

from collections import Counter
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from src.processor import FullRefreshRequiredError, process_logs


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
    assert row["raw"] == mock_log_file.read_text().splitlines()[0]


def test_descarte_linhas_invalidas(
    mock_log_file: Path, temp_output_dirs: tuple
) -> None:
    output, quarantine = temp_output_dirs
    result = process_logs(mock_log_file, output, quarantine)
    assert result["metrics"]["total_input"] == 5
    assert result["metrics"]["valid_count"] == 2
    assert result["metrics"]["quarantine_count"] == 3
    assert result["metrics"]["total_input"] == (
        result["metrics"]["valid_count"] + result["metrics"]["quarantine_count"]
    )


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
    assert df.schema["raw"] == pl.String


def test_pipeline_idempotente(mock_log_file: Path, temp_output_dirs: tuple) -> None:
    output, quarantine = temp_output_dirs
    result1 = process_logs(mock_log_file, output, quarantine)
    result2 = process_logs(mock_log_file, output, quarantine, full_refresh=True)
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


@pytest.mark.parametrize("scenario", ["validos", "rejeitados", "mistos"])
def test_quarentena_preserva_raw_e_conserva_registros(
    scenario: str, tmp_path: Path, temp_output_dirs: tuple[Path, Path]
) -> None:
    valid_line = (
        "192.168.0.1 - - [27/Jul/2026:14:32:10 +0000] "
        '"GET /search?q=a,b HTTP/1.1" 200 3421'
    )
    rejected_records = [
        ('  texto, com "aspas", acentuação e espaços  ', "regex_mismatch"),
        ('"linha inteira entre aspas"', "regex_mismatch"),
        ("12345", "regex_mismatch"),
        ("", "regex_mismatch"),
        (valid_line.replace("192.168.0.1", "256.168.0.1"), "regex_mismatch"),
        (valid_line.replace("200 3421", "900 3421"), "invalid_status"),
        (valid_line.replace("200 3421", "200 -1"), "negative_size"),
        (valid_line.replace("27/Jul/2026", "31/Feb/2026"), "invalid_date"),
        (valid_line.replace("200 3421", "900 -1"), "invalid_status"),
    ]
    # Repeated lines must remain separate records in the output.
    rejected_records.append(rejected_records[0])
    expected_valid = [valid_line, valid_line] if scenario != "rejeitados" else []
    expected_rejected = rejected_records if scenario != "validos" else []
    input_lines = expected_valid + [raw for raw, _ in expected_rejected]
    log_file = tmp_path / "evidence.log"
    log_file.write_text("\n".join(input_lines) + "\n", encoding="utf-8")
    output, quarantine = temp_output_dirs

    result = process_logs(log_file, output, quarantine)
    metrics = result["metrics"]
    assert metrics["total_input"] == len(input_lines)
    assert metrics["valid_count"] == len(expected_valid) == result["valid"].height
    assert metrics["quarantine_count"] == len(expected_rejected)
    assert (
        metrics["total_input"] == metrics["valid_count"] + metrics["quarantine_count"]
    )
    assert Counter(result["valid"]["raw"].to_list()) == Counter(expected_valid)
    assert {
        row["rejection_reason"]: row["count"] for row in metrics["rejection_breakdown"]
    } == (dict(Counter(reason for _, reason in expected_rejected)))

    if expected_rejected:
        persisted = pl.read_parquet(quarantine / "quarantine.parquet")
        assert persisted.schema["raw"] == pl.String
        assert persisted.height == result["quarantine"].height == len(expected_rejected)
        assert Counter(persisted.select("raw", "rejection_reason").rows()) == Counter(
            expected_rejected
        )
        assert Counter(input_lines) == Counter(
            result["valid"]["raw"].to_list()
        ) + Counter(persisted["raw"].to_list())
    else:
        assert result["quarantine"] is None
        assert not (quarantine / "quarantine.parquet").exists()

    if expected_valid:
        persisted_valid = pl.read_parquet(list(output.rglob("*.parquet")))
        assert Counter(persisted_valid["raw"].to_list()) == Counter(expected_valid)


@pytest.mark.parametrize("create_empty_dirs", [False, True])
def test_primeira_execucao_permitida_sem_full_refresh(
    create_empty_dirs: bool, mock_log_file: Path, temp_output_dirs: tuple[Path, Path]
) -> None:
    output, quarantine = temp_output_dirs
    if create_empty_dirs:
        (output / "dt_partition=2026-07-27").mkdir(parents=True)
        quarantine.mkdir()

    result = process_logs(mock_log_file, output, quarantine)

    assert result["metrics"]["valid_count"] == 2
    assert result["metrics"]["quarantine_count"] == 3
    assert pl.read_parquet(list(output.rglob("*.parquet"))).height == 2
    assert pl.read_parquet(quarantine / "quarantine.parquet").height == 3


@pytest.mark.parametrize("published_output", ["lake", "quarantine", "both"])
def test_recusa_reprocessamento_preserva_arquivos(
    published_output: str, tmp_path: Path, temp_output_dirs: tuple[Path, Path]
) -> None:
    output, quarantine = temp_output_dirs
    valid_line = (
        '192.168.0.1 - - [27/Jul/2026:14:32:10 +0000] "GET / HTTP/1.1" 200 100\n'
    )
    initial_input = tmp_path / "initial.log"
    initial_input.write_text(
        (valid_line if published_output != "quarantine" else "")
        + ("linha invalida\n" if published_output != "lake" else "")
    )
    process_logs(initial_input, output, quarantine)
    before = {
        path: (path.read_bytes(), path.stat().st_mtime_ns)
        for directory in (output, quarantine)
        for path in directory.rglob("*")
        if path.is_file()
    }
    assert before
    replacement_input = tmp_path / "replacement.log"
    replacement_input.write_text(valid_line.replace("27/Jul", "28/Jul"))

    with pytest.raises(FullRefreshRequiredError, match="--full-refresh") as error:
        process_logs(replacement_input, output, quarantine)

    assert "Ja existem dados publicados" in str(error.value)
    assert "reconstrucao completa" in str(error.value)
    after = {
        path: (path.read_bytes(), path.stat().st_mtime_ns)
        for directory in (output, quarantine)
        for path in directory.rglob("*")
        if path.is_file()
    }
    assert after == before


def test_full_refresh_substitui_conjunto_completo(
    mock_log_file: Path, tmp_path: Path, temp_output_dirs: tuple[Path, Path]
) -> None:
    output, quarantine = temp_output_dirs
    process_logs(mock_log_file, output, quarantine)
    replacement_line = (
        '10.0.0.1 - - [28/Jul/2026:12:00:00 +0000] "GET /new HTTP/1.1" 200 100'
    )
    replacement_input = tmp_path / "replacement.log"
    replacement_input.write_text(replacement_line + "\n")

    result = process_logs(replacement_input, output, quarantine, full_refresh=True)

    assert result["metrics"]["total_input"] == result["metrics"]["valid_count"] == 1
    assert result["metrics"]["quarantine_count"] == 0
    assert result["valid"]["raw"].to_list() == [replacement_line]
    persisted = pl.read_parquet(list(output.rglob("*.parquet")))
    assert persisted["raw"].to_list() == [replacement_line]
    assert persisted["dt_partition"].to_list() == [date(2026, 7, 28)]
    assert not (output / "dt_partition=2026-07-27").exists()
    assert not (quarantine / "quarantine.parquet").exists()

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from generator import generate_mock_logs
from processor import process_logs
from query_lake import discover_queries, load_query, run_analytics_queries


@pytest.fixture
def analytical_lake(tmp_path: Path) -> str:
    raw_path = tmp_path / "server.log"
    lake_path = tmp_path / "logs_lake"
    generate_mock_logs(raw_path, lines=500, days_spread=10)
    process_logs(raw_path, lake_path, tmp_path / "quarantine")
    return str(lake_path / "**" / "*.parquet")


def test_catalog_has_ten_executable_queries(analytical_lake: str) -> None:
    queries = discover_queries()

    assert len(queries) == 10
    with duckdb.connect() as connection:
        for query_path in queries.values():
            connection.execute(load_query(query_path, analytical_lake)).fetchall()


def test_catalog_includes_required_window_functions() -> None:
    sql = "\n".join(
        path.read_text(encoding="utf-8").upper() for path in discover_queries().values()
    )

    for function in ("ROW_NUMBER(", "RANK(", "LAG(", "LEAD("):
        assert function in sql


def test_runner_can_select_queries(analytical_lake: str) -> None:
    results = run_analytics_queries(
        analytical_lake,
        ["01_daily_traffic", "05_response_size_percentiles"],
    )

    assert set(results) == {"01_daily_traffic", "05_response_size_percentiles"}
    assert not results["01_daily_traffic"].empty


def test_runner_rejects_unknown_query(analytical_lake: str) -> None:
    with pytest.raises(ValueError, match="Unknown queries"):
        run_analytics_queries(analytical_lake, ["does_not_exist"])

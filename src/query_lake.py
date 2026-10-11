from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import duckdb

DEFAULT_LAKE_PATH = "data/processed/logs_lake/**/*.parquet"
DEFAULT_SQL_DIR = Path(__file__).resolve().parents[1] / "sql"
LAKE_PATH_TOKEN = "{{lake_path}}"


def discover_queries(sql_dir: Path = DEFAULT_SQL_DIR) -> dict[str, Path]:
    return {path.stem: path for path in sorted(sql_dir.glob("[0-9][0-9]_*.sql"))}


def load_query(query_path: Path, lake_path: str = DEFAULT_LAKE_PATH) -> str:
    # The glob replaces a SQL string literal, so embedded quotes must be escaped.
    escaped_path = lake_path.replace("'", "''")
    return query_path.read_text(encoding="utf-8").replace(LAKE_PATH_TOKEN, escaped_path)


def run_analytics_queries(
    lake_path: str = DEFAULT_LAKE_PATH,
    query_names: Sequence[str] | None = None,
    sql_dir: Path = DEFAULT_SQL_DIR,
) -> dict[str, Any]:
    query_files = discover_queries(sql_dir)
    selected_names = list(query_names) if query_names else list(query_files)
    unknown = sorted(set(selected_names) - set(query_files))
    if unknown:
        available = ", ".join(query_files)
        raise ValueError(
            f"Unknown queries: {', '.join(unknown)}. Available: {available}"
        )

    query_results: dict[str, Any] = {}
    with duckdb.connect() as connection:
        for name in selected_names:
            query_results[name] = connection.execute(
                load_query(query_files[name], lake_path)
            ).df()
    return query_results


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run DuckDB queries against the Parquet log lake."
    )
    parser.add_argument(
        "--lake-path",
        default=DEFAULT_LAKE_PATH,
        help="Parquet path or glob (default: %(default)s)",
    )
    parser.add_argument(
        "--query",
        action="append",
        dest="queries",
        help="Query filename stem; repeat to run multiple queries.",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available queries without running them.",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    query_files = discover_queries()
    if args.list:
        print("\n".join(query_files))
        return

    lake_root = args.lake_path.split("*")[0]
    if not Path(lake_root).exists():
        raise SystemExit(
            f"Data lake not found for {args.lake_path!r}. "
            "Run `python -m src.main --generate` first."
        )

    for name, query_result in run_analytics_queries(
        args.lake_path, args.queries
    ).items():
        print(f"\n{name}")
        print(query_result.to_string(index=False))


if __name__ == "__main__":
    main()

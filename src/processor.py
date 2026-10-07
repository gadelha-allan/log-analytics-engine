from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
from polars.exceptions import ComputeError

if TYPE_CHECKING:
    from polars import LazyFrame

logger = logging.getLogger(__name__)


class FullRefreshRequiredError(FileExistsError):
    """Published output cannot be replaced without explicit full-refresh intent."""


LOG_PATTERN = (
    r"^(?P<ip>(?:25[0-5]|2[0-4]\d|[01]?\d\d?)(?:\.(?:25[0-5]|2[0-4]\d|[01]?\d\d?)){3})"
    r" - - \[(?P<date>.*?)\] "
    r'"(?P<method>\S+) (?P<endpoint>.*?) HTTP/\S+" '
    r"(?P<status>\d{3}) "
    r"(?P<size>-?\d+)$"
)


def _extract_and_type(lf: LazyFrame) -> LazyFrame:
    return (
        lf.select("raw", pl.col("raw").str.extract_groups(LOG_PATTERN).alias("parsed"))
        .unnest("parsed")
        .with_columns(
            pl.col("status").cast(pl.Int32, strict=False),
            pl.col("size").cast(pl.Int64, strict=False),
            pl.col("date")
            .str.strptime(pl.Datetime, "%d/%b/%Y:%H:%M:%S %z", strict=False)
            .dt.date()
            .alias("dt_partition"),
        )
        .with_columns((pl.col("status") >= 400).alias("is_error"))
    )


def _apply_quality_rules(lf: LazyFrame) -> tuple[LazyFrame, LazyFrame]:
    unmatched = lf.filter(
        pl.col("ip").is_null() | pl.col("status").is_null() | pl.col("size").is_null()
    ).with_columns(pl.lit("regex_mismatch").alias("rejection_reason"))

    matched = lf.filter(
        pl.col("ip").is_not_null()
        & pl.col("status").is_not_null()
        & pl.col("size").is_not_null()
    )

    invalid_status = matched.filter(
        ~pl.col("status").is_between(100, 599)
    ).with_columns(pl.lit("invalid_status").alias("rejection_reason"))

    valid_status = matched.filter(pl.col("status").is_between(100, 599))

    invalid_size = valid_status.filter(pl.col("size") < 0).with_columns(
        pl.lit("negative_size").alias("rejection_reason")
    )

    valid_size = valid_status.filter(pl.col("size") >= 0)

    invalid_date = valid_size.filter(pl.col("dt_partition").is_null()).with_columns(
        pl.lit("invalid_date").alias("rejection_reason")
    )

    valid = valid_size.filter(pl.col("dt_partition").is_not_null())
    quarantine = pl.concat(
        [unmatched, invalid_status, invalid_size, invalid_date], how="diagonal"
    )

    return valid, quarantine


def process_logs(
    file_path: str | Path,
    output_dir: str | Path = "data/processed/logs_lake",
    quarantine_dir: str | Path = "data/processed/quarantine",
    full_refresh: bool = False,
) -> dict[str, Any]:
    file_path = Path(file_path)
    output_dir = Path(output_dir)
    quarantine_dir = Path(quarantine_dir)

    if not file_path.exists():
        raise FileNotFoundError(f"Arquivo nao encontrado: {file_path}")

    if not full_refresh:
        for directory in (output_dir, quarantine_dir):
            if directory.exists() and any(
                path.is_file() for path in directory.rglob("*")
            ):
                raise FullRefreshRequiredError(
                    f"Ja existem dados publicados em {directory}. "
                    "Nenhum arquivo foi substituido. Use --full-refresh para autorizar "
                    "a reconstrucao completa; a entrada deve conter todo o conjunto "
                    "que deseja publicar."
                )

    logger.info("Iniciando pipeline: %s", file_path)

    try:
        # CSV defaults would strip quotes or split log lines containing commas.
        lf_raw = pl.scan_csv(
            file_path,
            has_header=False,
            new_columns=["raw"],
            separator="\n",
            quote_char=None,
            infer_schema=False,
        ).with_columns(pl.col("raw").fill_null(""))
        total_input = lf_raw.select(pl.len()).collect().item()
        logger.info("Total de linhas de entrada: %s", f"{total_input:,}")

        lf_typed = _extract_and_type(lf_raw)
        lf_valid, lf_quarantine = _apply_quality_rules(lf_typed)

        valid_count = lf_valid.select(pl.len()).collect().item()
        quarantine_count = lf_quarantine.select(pl.len()).collect().item()
        rejection_breakdown = (
            lf_quarantine.group_by("rejection_reason")
            .agg(pl.len().alias("count"))
            .collect()
        )

        logger.info(
            "Qualidade - Validos: %s | Rejeitados: %s | Taxa: %.2f%%",
            f"{valid_count:,}",
            f"{quarantine_count:,}",
            (quarantine_count / total_input * 100) if total_input else 0,
        )
        for row in rejection_breakdown.iter_rows(named=True):
            logger.info(
                "  -> %s: %s registros", row["rejection_reason"], f'{row["count"]:,}'
            )

        output_dir.mkdir(parents=True, exist_ok=True)
        quarantine_dir.mkdir(parents=True, exist_ok=True)

        for subdir in output_dir.iterdir():
            if subdir.is_dir():
                shutil.rmtree(subdir)
        for f in quarantine_dir.glob("*.parquet"):
            f.unlink()

        logger.info("Gravando Parquet particionado (streaming)...")
        lf_valid.sink_parquet(
            pl.PartitionBy(output_dir, key="dt_partition"),
            mkdir=True,
        )

        if quarantine_count > 0:
            logger.info("Gravando quarentena...")
            lf_quarantine.sink_parquet(quarantine_dir / "quarantine.parquet")

        return {
            "valid": lf_valid.collect(),
            "quarantine": lf_quarantine.collect() if quarantine_count > 0 else None,
            "metrics": {
                "total_input": total_input,
                "valid_count": valid_count,
                "quarantine_count": quarantine_count,
                "rejection_rate": quarantine_count / total_input if total_input else 0,
                "rejection_breakdown": rejection_breakdown.to_dicts(),
            },
        }

    except ComputeError as e:
        logger.error("Polars nao conseguiu processar %s: %s", file_path, e)
        raise
    except Exception:
        logger.exception("Falha ao processar %s", file_path)
        raise

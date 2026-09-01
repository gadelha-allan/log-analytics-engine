from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

from generator import generate_mock_logs
from processor import process_logs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def run_pipeline(
    raw_path: str | Path = "data/raw/server.log",
    processed_dir: str | Path = "data/processed/logs_lake",
    quarantine_dir: str | Path = "data/processed/quarantine",
    generate_if_missing: bool = False,
    lines: int = 5_000_000,
) -> dict:
    raw_path = Path(raw_path)
    processed_dir = Path(processed_dir)
    quarantine_dir = Path(quarantine_dir)

    if not raw_path.exists():
        if generate_if_missing:
            logger.warning("Arquivo nao encontrado. Gerando dados sinteticos...")
            generate_mock_logs(raw_path, lines=lines)
        else:
            logger.error("Arquivo nao encontrado: %s", raw_path)
            logger.error("Use --generate para criar dados sinteticos.")
            sys.exit(1)

    start_time = time.perf_counter()

    try:
        logger.info("Iniciando pipeline ETL")
        result = process_logs(
            file_path=raw_path,
            output_dir=processed_dir,
            quarantine_dir=quarantine_dir,
        )

        elapsed = time.perf_counter() - start_time
        metrics = result["metrics"]
        throughput = metrics["valid_count"] / elapsed if elapsed > 0 else 0

        logger.info("=" * 60)
        logger.info("PIPELINE CONCLUIDO")
        logger.info("   Tempo total:       %.2f s", elapsed)
        logger.info("   Linhas de entrada: %,d", metrics["total_input"])
        logger.info("   Linhas validas:    %,d", metrics["valid_count"])
        logger.info(
            "   Rejeitadas:        %,d (%.2f%%)",
            metrics["quarantine_count"],
            metrics["rejection_rate"] * 100,
        )
        logger.info("   Throughput:        %,.0f linhas/s", throughput)
        logger.info("=" * 60)

        return result

    except FileNotFoundError:
        logger.error("Arquivo nao encontrado: %s", raw_path)
        raise
    except Exception:
        logger.exception("Pipeline falhou")
        raise


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pipeline ETL para processamento de logs de servidor"
    )
    parser.add_argument(
        "--raw", default="data/raw/server.log", help="Caminho do arquivo .log"
    )
    parser.add_argument(
        "--output", default="data/processed/logs_lake", help="Diretorio de saida"
    )
    parser.add_argument(
        "--quarantine",
        default="data/processed/quarantine",
        help="Diretorio de quarentena",
    )
    parser.add_argument(
        "--generate", action="store_true", help="Gera logs se arquivo nao existir"
    )
    parser.add_argument(
        "--lines", type=int, default=5_000_000, help="Numero de linhas a gerar"
    )
    args = parser.parse_args()

    run_pipeline(
        raw_path=args.raw,
        processed_dir=args.output,
        quarantine_dir=args.quarantine,
        generate_if_missing=args.generate,
        lines=args.lines,
    )


if __name__ == "__main__":
    main()

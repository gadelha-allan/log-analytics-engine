import os
import time
import logging
from polars.exceptions import PolarsError
from generator import generate_mock_logs
from processor import process_logs

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_pipeline():
    raw_path = "data/raw/server.log"
    processed_dir = "data/processed/logs_lake"

    if not os.path.exists(raw_path):
        logging.info("Gerando 5 milhões de linhas de log...")
        generate_mock_logs(raw_path, lines=5_000_000)

    start_time = time.perf_counter()

    try:
        logging.info("Iniciando processamento vetorial de logs...")
        df_final = process_logs(raw_path)

        if df_final.is_empty():
            logging.warning("Nenhum dado foi processado. Dataframe vazio.")
            return

        if os.path.exists(processed_dir):
            import shutil
            shutil.rmtree(processed_dir)

        os.makedirs(processed_dir, exist_ok=True)

        logging.info("Gravando Parquet particionado...")
        df_final.write_parquet(
            processed_dir,
            use_pyarrow=True,
            pyarrow_options={"partition_cols": ["dt_partition"]}
        )

        elapsed = time.perf_counter() - start_time
        row_count = df_final.height
        logging.info(f"Pipeline concluído em {elapsed:.2f}s — {row_count:,} linhas processadas ({row_count/elapsed:,.0f} linhas/s)")

    except FileNotFoundError:
        logging.error(f"Arquivo não encontrado: {raw_path}")
        raise
    except PolarsError as e:
        logging.error(f"Erro no Polars: {e}")
        raise
    except Exception as e:
        logging.exception(f"Erro inesperado: {e}")
        raise

if __name__ == "__main__":
    run_pipeline()

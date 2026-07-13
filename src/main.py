import os
import logging
from generator import generate_mock_logs
from processor import parse_logs, transform_logs


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_pipeline():
    raw_path = "data/raw/server.log"
    processed_path = "data/processed/logs_analyzed.parquet"
    
    if not os.path.exists(raw_path):
        generate_mock_logs(raw_path, lines=5000)

    try:
        logging.info("Iniciando extração...")
        raw_data = parse_logs(raw_path)

        logging.info("Iniciando transformação...")
        df_final = transform_logs(raw_data)

        if not df_final.is_empty():
            os.makedirs("data/processed", exist_ok=True)
            df_final.write_parquet(processed_path)
            logging.info(f"Dados salvos com sucesso em {processed_path}")
            

            raw_size = os.path.getsize(raw_path) / 1024
            parquet_size = os.path.getsize(processed_path) / 1024
            logging.info(f"Redução de armazenamento: {(1 - parquet_size/raw_size)*100:.2f}%")

    except Exception as e:
        logging.error(f"Falha crítica no pipeline: {e}")

if __name__ == "__main__":
    run_pipeline()
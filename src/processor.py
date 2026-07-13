import re
import polars as pl
import logging

# Configuração do log
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def parse_logs(file_path):
    regex = r'(?P<ip>\S+) - - \[(?P<date>.*?)\] "GET (?P<endpoint>.*?) .*?" (?P<status>\d+) (?P<size>\d+)'
    extracted_data = []
    corrupted_rows = 0

    try:
        with open(file_path, "r") as f:
            for line in f:
                match = re.search(regex, line)
                if match:
                    extracted_data.append(match.groupdict())
                else:
                    corrupted_rows += 1
        
        if corrupted_rows > 0:
            logger.warning(f"{corrupted_rows} linhas ignoradas por falha no formato.")
            
        return extracted_data
    except FileNotFoundError:
        logger.error(f"Arquivo {file_path} não encontrado.")
        return []

def transform_logs(data_list):
    if not data_list:
        logger.warning("Nenhum dado para transformar.")
        return pl.DataFrame()

    df = pl.DataFrame(data_list)


    required_cols = {"ip", "status", "size", "endpoint"}
    if not required_cols.issubset(set(df.columns)):
        logger.error("Esquema de dados inválido. Colunas faltando.")
        raise ValueError("Dados corrompidos: colunas necessárias ausentes.")

   
    df = df.with_columns([
        pl.col("status").cast(pl.Int32),
        pl.col("size").cast(pl.Int32),
        (pl.col("status") >= 400).alias("is_error")
    ])

    logger.info("Transformação concluída com sucesso.")
    return df
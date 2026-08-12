import polars as pl
from polars.exceptions import ComputeError
import logging

logger = logging.getLogger(__name__)

def process_logs(file_path: str) -> pl.DataFrame:
    regex = r'(?P<ip>\S+) - - \[(?P<date>.*?)\] "GET (?P<endpoint>.*?) .*?" (?P<status>\d+) (?P<size>\d+)'
    
    try:
        df = pl.scan_csv(file_path, has_header=False, new_columns=["raw"])
        
        df_parsed = df.select(
            pl.col("raw").str.extract_groups(regex).alias("parsed")
        ).unnest("parsed")

        # 1. Primeiro convertemos as colunas para seus tipos numéricos/datas
        df_typed = df_parsed.with_columns([
            pl.col("status").cast(pl.Int32),
            pl.col("size").cast(pl.Int32),
            pl.col("date").str.strptime(pl.Datetime, "%d/%b/%Y:%H:%M:%S %z").dt.date().alias("dt_partition")
        ])

        # 2. Agora que status é Int32, podemos avaliar 'is_error' e aplicar drop_nulls
        df_transformed = df_typed.with_columns([
            (pl.col("status") >= 400).alias("is_error")
        ]).drop_nulls()
        
        # 3. Filtro de Qualidade de Dados
        df_filtered = df_transformed.filter(
            pl.col("status").is_between(100, 599) &
            pl.col("ip").str.contains(r"^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$")
        )
        
        return df_filtered.collect() 
        
    except ComputeError as e:
        logger.error(f"Erro de computação no motor do Polars: {e}")
        raise
    except Exception as e:
        logger.error(f"Falha inesperada no processamento: {e}")
        raise
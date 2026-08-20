import polars as pl
from polars.exceptions import ComputeError
import logging

logger = logging.getLogger(__name__)

def process_logs(file_path: str) -> pl.DataFrame:
    regex = r'(?P<ip>\S+) - - \[(?P<date>.*?)\] "(?P<method>\S+) (?P<endpoint>.*?) HTTP/\S+" (?P<status>\d+) (?P<size>\d+)'

    try:
        df = pl.scan_csv(file_path, has_header=False, new_columns=["raw"])

        df_parsed = df.select(
            pl.col("raw").str.extract_groups(regex).alias("parsed")
        ).unnest("parsed")

        df_typed = df_parsed.with_columns([
            pl.col("status").cast(pl.Int32, strict=False),
            pl.col("size").cast(pl.Int32, strict=False),
            pl.col("date").str.strptime(
                pl.Datetime,
                "%d/%b/%Y:%H:%M:%S %z",
                strict=False
            ).dt.date().alias("dt_partition")
        ])

        df_transformed = df_typed.with_columns([
            (pl.col("status") >= 400).alias("is_error")
        ]).drop_nulls()

        df_filtered = df_transformed.filter(
            pl.col("status").is_between(100, 599) &
            (
                pl.col("ip").str.split(".").list.len() == 4
            ) &
            pl.col("ip").str.split(".").list.eval(
                pl.element().cast(pl.Int16, strict=False).is_between(0, 255)
            ).list.all()
        )

        return df_filtered.collect()

    except ComputeError as e:
        logger.error(f"Erro de computação no motor do Polars: {e}")
        raise
    except Exception as e:
        logger.error(f"Falha inesperada no processamento: {e}")
        raise

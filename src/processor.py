import re
import polars as pl

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
            print(f"⚠️ Aviso: {corrupted_rows} linhas ignoradas por falha no Regex.")
            
        return extracted_data
    except FileNotFoundError:
        print(f"❌ Erro: Arquivo {file_path} não encontrado.")
        return []
    
def transform_logs(data_list):
    if not data_list:
        return pl.DataFrame()

    # Converter para DataFrame
    df = pl.DataFrame(data_list)

    df = df.with_columns([
        pl.col("status").cast(pl.Int32),
        pl.col("size").cast(pl.Int32),
        (pl.col("status") >= 400).alias("is_error")
    ])

    top_ips = df.group_by("ip").count().sort("count", descending=True).head(5)
    print("\n--- Top 5 IPs ---")
    print(top_ips)

    top_errors = df.filter(pl.col("is_error") == True).group_by("endpoint").count().sort("count", descending=True)
    print("\n--- Endpoints com Erros ---")
    print(top_errors)

    return df
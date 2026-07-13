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
import os
from generator import generate_mock_logs
from processor import parse_logs, transform_logs

def run_pipeline():
    raw_path = "data/raw/server.log"
    processed_path = "data/processed/logs_analyzed.parquet"
    
    if not os.path.exists(raw_path):
        generate_mock_logs(raw_path, lines=5000)


    print("🚀 Iniciando Extração...")
    raw_data = parse_logs(raw_path)

    print("⚙️ Iniciando Transformação com Polars...")
    df_final = transform_logs(raw_data)

    if not df_final.is_empty():
        os.makedirs("data/processed", exist_ok=True)
        df_final.write_parquet(processed_path)
        

        raw_size = os.path.getsize(raw_path) / 1024
        parquet_size = os.path.getsize(processed_path) / 1024
        print(f"\n✅ ETL Finalizado com Sucesso!")
        print(f"Raw Log: {raw_size:.2f} KB")
        print(f"Parquet: {parquet_size:.2f} KB")
        print(f"Redução de: {(1 - parquet_size/raw_size)*100:.2f}%")

if __name__ == "__main__":
    run_pipeline()
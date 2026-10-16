# Log Analytics Engine

[English](README.md)

Processa logs de acesso HTTP com Polars e grava Parquet particionado por data. Registros rejeitados ficam na quarentena com a linha original e o motivo. DuckDB executa as consultas SQL, e dbt constrói os modelos Gold usados pelo dashboard Streamlit.

## Fluxo

```mermaid
flowchart LR
    A[server.log] --> B[Parsing e validação com Polars]
    B --> C[Parquet particionado por data]
    B --> D[Quarentena]
    C --> E[Consultas DuckDB]
    C --> F[Modelos Gold com dbt]
    F --> G[Dashboard Streamlit]
```

`src/generator.py` gera logs sintéticos. `src/processor.py` extrai os campos, valida datas, IPs, status e tamanhos e grava o lake e a quarentena. `src/main.py` expõe a CLI.

| Pasta | Conteúdo |
|---|---|
| `src/` | Pipeline, gerador, consultas e dashboard |
| `tests/` | Testes do parser, da CLI, de integração e das consultas SQL |
| `sql/` | Dez consultas DuckDB |
| `dbt/` | Staging, dimensões, fatos e agregações Gold |
| `docs/` | Modelo de dados e planos de execução |
| `data/` | Logs e dados produzidos localmente, fora do Git |

## Execução local

Requer Python 3.10 ou superior. Na raiz do repositório:

```bash
git clone https://github.com/gadelha-allan/log-analytics-engine.git
cd log-analytics-engine
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
python -m src.main --generate --lines 10000
```

A primeira execução gera a entrada quando ela não existe e grava o lake e a quarentena em `data/processed/`. `--generate` não regrava uma entrada que já existe.

## Docker

```bash
docker compose up --build
```

O serviço `log-engine` executa o pipeline com cinco milhões de linhas como padrão de geração. O `dashboard` atende em `http://localhost:8501`. O volume `./data:/app/data` mantém os dados no host.

Se já houver dados publicados, o pipeline exige `--full-refresh`, também no container:

```bash
docker compose run --rm log-engine python -m src.main --raw data/raw/server.log --full-refresh
```

## CLI

```bash
python -m src.main --help
```

| Opção | Padrão | Uso |
|---|---|---|
| `--raw` | `data/raw/server.log` | Arquivo de entrada |
| `--output` | `data/processed/logs_lake` | Parquet particionado |
| `--quarantine` | `data/processed/quarantine` | Registros rejeitados |
| `--generate` | `False` | Gera a entrada se ela não existir |
| `--lines` | `5_000_000` | Quantidade de linhas para geração |
| `--full-refresh` | `False` | Autoriza substituir o conjunto publicado |

A primeira execução dispensa a flag quando o lake e a quarentena não existem ou não contêm arquivos. Se algum dos destinos já tiver arquivos, a reconstrução exige `--full-refresh`; sem ela, a CLI retorna erro e mantém as duas saídas anteriores intactas.

Cada reconstrução exige uma entrada completa. Registros ausentes dela deixam de ser publicados, incluindo partições antigas e rejeitados. Um arquivo com apenas um dia substitui todo o conjunto; não há atualização incremental nem acréscimo à saída anterior. Repetir a mesma entrada com a flag produz os mesmos registros e contagens. Duplicatas que já existem na entrada continuam como registros separados.

```bash
# Primeira publicação, usando um arquivo de entrada existente
python -m src.main --raw data/raw/server.log

# Reconstrução após atualizar esse arquivo com todo o conjunto desejado
python -m src.main --raw data/raw/server.log --full-refresh
```

O lake e a quarentena são preparados em diretórios temporários ao lado dos destinos. Todos os Parquets são relidos em lotes para conferir schema e contagem antes da substituição. Falhas de parsing, escrita ou validação preservam os arquivos publicados e removem os temporários. Os destinos precisam ser separados, sem um dentro do outro.

Durante a publicação, os diretórios anteriores são movidos para backups ao lado dos destinos. As duas saídas preparadas vêm da mesma execução, incluindo uma quarentena vazia quando não há rejeitados. Uma falha na publicação restaura os diretórios anteriores; os backups são removidos após concluir as duas substituições ou a recuperação. Se a recuperação falhar, o erro informa os caminhos dos backups preservados. Falhas ao apagar um backup após a publicação são registradas sem desfazer a nova saída. Após uma recuperação bem-sucedida, corrija a causa e tente novamente com a entrada completa e `--full-refresh`.

O mecanismo atende à execução local, com um processo por vez. A troca de vários diretórios não é uma transação atômica para leitores concorrentes: durante a substituição, eles podem encontrar diretórios ausentes ou saídas de execuções diferentes. Não há recuperação automática após encerramento forçado do processo ou queda de energia.

Entrada vazia é recusada, inclusive com `--full-refresh`. Se todas as linhas forem rejeitadas, a execução publica todas na quarentena e um lake vazio com schema em `_empty.parquet`; com `--full-refresh`, isso substitui o conjunto anterior.

Em Python, `process_logs()` e `run_pipeline()` retornam `lake_path`, `quarantine_path` e `metrics`. Os caminhos são strings dos diretórios publicados; a quarentena fica vazia quando não há rejeitados. Para montar o retorno, o pipeline mantém em memória apenas contagens e motivos de rejeição agregados. Leia os Parquets publicados quando precisar dos registros; o retorno não contém mais DataFrames em `valid` ou `quarantine`.

## Dados processados

O lake fica em `data/processed/logs_lake/dt_partition=AAAA-MM-DD/*.parquet`.

| Coluna | Tipo | Conteúdo |
|---|---|---|
| `raw` | String | Linha original, sem o terminador de linha |
| `ip` | String | IPv4 do cliente |
| `date` | String | Timestamp extraído do log |
| `method` | String | Método HTTP |
| `endpoint` | String | Rota e query string |
| `status` | Int32 | Status HTTP entre 100 e 599 |
| `size` | Int64 | Tamanho da resposta em bytes, maior ou igual a zero |
| `dt_partition` | Date | Data derivada de `date`, usada no particionamento |
| `is_error` | Boolean | `true` quando `status >= 400` |

## Quarentena

`data/processed/quarantine/quarantine.parquet` é gravado quando há rejeições. Contém `raw`, os campos extraídos, que podem ser nulos, e `rejection_reason`.

| Motivo | Condição |
|---|---|
| `regex_mismatch` | A linha não corresponde ao formato do log ou IP, status ou tamanho ficam nulos |
| `invalid_status` | Status HTTP fora de 100 a 599 |
| `negative_size` | Tamanho negativo |
| `invalid_date` | Data que não pode ser convertida |

`raw` conserva espaços, vírgulas e aspas. Cada linha aparece uma vez no lake ou na quarentena, incluindo duplicatas: `total_input = valid_count + quarantine_count`.

## Consultas SQL

Veja o catálogo em [`sql/README.md`](sql/README.md). Com o lake gerado:

```bash
python -m src.query_lake --list
python -m src.query_lake
python -m src.query_lake --query 03_daily_change --query 05_response_size_percentiles
python -m src.query_lake --lake-path '/tmp/logs_lake/**/*.parquet'
```

As consultas leem o Parquet com `hive_partitioning = true`. Filtros em `dt_partition` podem reduzir os arquivos lidos. Há exemplos de `EXPLAIN` e `EXPLAIN ANALYZE` em [`docs/sql-performance.md`](docs/sql-performance.md).

## Modelos dbt

Depois de gerar o lake, crie o perfil local e construa os modelos:

```bash
cp dbt/profiles.yml.example dbt/profiles.yml
dbt build --project-dir dbt --profiles-dir dbt
dbt docs generate --project-dir dbt --profiles-dir dbt
```

O perfil padrão grava `data/log_analytics.duckdb`. O Gold contém `fact_requests`, `dim_date`, `dim_endpoint`, `mart_daily_traffic` e `mart_endpoint_health`.

Veja [`docs/dbt-medallion.md`](docs/dbt-medallion.md) e [`docs/gold-star-schema.md`](docs/gold-star-schema.md) para os caminhos, relações e granularidade dos modelos.

## Dashboard

```bash
streamlit run src/dashboard.py
```

O dashboard consulta as agregações Gold quando disponíveis e usa o lake Silver nos demais casos. Mostra volume de requisições, erros HTTP, tamanho médio, endpoints, série diária e rejeições por motivo.

## Testes

```bash
pip install -r requirements-dev.txt
python -m pytest
python -m pytest tests/test_processor.py -v
python -m pytest tests/test_reprocessing.py -v
ruff check src/ tests/
black --check src/ tests/
mypy src/
```

Os testes usam diretórios temporários. Conferem campos extraídos, tipos, motivos de rejeição, conteúdo da quarentena, conservação das contagens e recusa de reprocessamento sem `--full-refresh`.

`tests/test_reprocessing.py` executa a CLI e lê os Parquets finais do lake e da quarentena. Confere reconstruções repetidas, recusa sem a flag, substituição completa e consistência após falhas de parsing, escrita, validação, backup e publicação. Verifica valores, motivos de rejeição, tipos e contagens, além de repetir cada reconstrução que falhou.

A CI executa lint, formatação, Mypy e testes em Python 3.10, 3.11 e 3.12. Também constrói a imagem Docker e executa o pipeline com mil registros.

## Medição de desempenho

Uma execução anterior com cinco milhões de linhas gerou 18,3 MB de Parquet a partir de 369,5 MB de log, em 11,6 segundos de processamento. A medição usou Linux, Python 3.12, Polars 1.12.0 e PyArrow 17.0.0. Volume, schema, versões e hardware afetam esses resultados.

## Licença

[MIT](LICENSE).

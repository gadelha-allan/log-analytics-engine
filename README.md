# 🚀 Log Analytics Engine

Projeto de Engenharia de Dados focado no processamento de logs de servidor não estruturados. O objetivo é converter dados brutos (logs de servidor) em informações analíticas valiosas, utilizando técnicas de extração de padrões e armazenamento colunar otimizado.

## 🛠 Tecnologias Utilizadas

- **Linguagem:** Python 3.9+
- **Processamento:** Polars (Framework de alta performance para manipulação de dados)
- **Extração:** Expressões Regulares (Regex) para parsing de logs
- **Armazenamento:** Apache Parquet (Formato colunar para alta compressão e velocidade)
- **Monitoramento:** Logging estruturado para rastreabilidade

## ⚙️ Arquitetura do Pipeline

1. **Extract:** Leitura sequencial de logs brutos, com extração de campos (IP, Data, Endpoint, Status) através de Regex.
2. **Transform:** Processamento via Polars para limpeza, tipagem de dados e geração de métricas (flag de erro).
3. **Load:** Conversão para formato Parquet, reduzindo significativamente o volume de armazenamento.
4. **Validação:** Verificação de integridade de esquema antes do processamento.

## 🚀 Como Utilizar

### Pré-requisitos

- Python 3.9+
- Bibliotecas: polars, pyarrow

### Execução

1. Clone o repositório:
   ```bash
   git clone https://github.com/seu-usuario/log-analytics-engine.git
   cd log-analytics-engine
```

2. Instale as dependências:
   ```bash
   pip install polars pyarrow
   ```
3. Execute o pipeline:
   ```bash
   python main.py
   ```

📊 Análise de Performance

O uso do formato Parquet neste projeto demonstra um ganho expressivo em eficiência. Em testes realizados com 5.000 linhas de log, observou-se uma redução de armazenamento de mais de 90% em comparação ao arquivo .log original, mantendo a performance de leitura otimizada para consultas analíticas.

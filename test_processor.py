import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))
import os
import tempfile
import datetime
import pytest
import polars as pl
from processor import process_logs

LOG_VALIDO = '192.168.0.1 - - [27/Jul/2026:14:32:10 +0000] "GET /api/v1/users HTTP/1.1" 200 3421\n'
LOG_METODO_INVALIDO = '10.0.0.5 - - [27/Jul/2026:15:00:00 +0000] "POST /login HTTP/1.1" 201 500\n'
LOG_LIXO = 'isso é um texto aleatorio que quebrou no servidor\n'


@pytest.fixture
def arquivo_mock():
    fd, path = tempfile.mkstemp()
    with os.fdopen(fd, 'w') as f:
        f.write(LOG_VALIDO)
        f.write(LOG_METODO_INVALIDO)
        f.write(LOG_LIXO)
    yield path
    os.remove(path)


def test_extracao_regex_campos_corretos(arquivo_mock):
    df = process_logs(arquivo_mock)
    resultado = df.row(0, named=True)
    
    assert resultado["ip"] == "192.168.0.1", "Falha ao extrair o IP"
    assert resultado["endpoint"] == "/api/v1/users", "Falha ao extrair o endpoint da rota"
    assert resultado["status"] == 200, "Falha ao extrair e converter o status HTTP"
    assert resultado["size"] == 3421, "Falha ao extrair e converter o tamanho (size)"


def test_descarte_linhas_invalidas_com_drop_nulls(arquivo_mock):
    df = process_logs(arquivo_mock)
    
    assert df.height == 1, "O DataFrame deveria conter apenas 1 linha válida após o drop_nulls()"


def test_regra_is_error(arquivo_mock):
    df = process_logs(arquivo_mock)
    resultado = df.row(0, named=True)
    
    assert resultado["is_error"] is False, "is_error deveria ser False para status 200"


def test_tipagem_colunas(arquivo_mock):
    df = process_logs(arquivo_mock)
    
    assert df.schema["status"] == pl.Int32, "A coluna 'status' deve ser do tipo Int32"
    assert df.schema["size"] == pl.Int32, "A coluna 'size' deve ser do tipo Int32"
    assert df.schema["dt_partition"] == pl.Date, "A coluna 'dt_partition' deve ser do tipo Date"
    assert df.schema["is_error"] == pl.Boolean, "A coluna 'is_error' deve ser do tipo Boolean"


def test_conversao_data_dt_partition(arquivo_mock):
    df = process_logs(arquivo_mock)
    resultado = df.row(0, named=True)
    
    data_esperada = datetime.date(2026, 7, 27)
    assert resultado["dt_partition"] == data_esperada, f"A data da partição deveria ser {data_esperada}, mas retornou {resultado['dt_partition']}"


def test_is_error_verdadeiro():
    log_erro = '192.168.0.1 - - [27/Jul/2026:14:32:10 +0000] "GET /api/v1/users HTTP/1.1" 404 1234\n'
    
    fd, path = tempfile.mkstemp()
    with os.fdopen(fd, 'w') as f:
        f.write(log_erro)
        
    try:
        df = process_logs(path)
        resultado = df.row(0, named=True)
        
        assert resultado["status"] == 404, "O status extraído deve ser 404"
        assert resultado["is_error"] is True, "A flag 'is_error' deve ser True quando o status for >= 400"
    finally:
        os.remove(path)


def test_filtro_qualidade_dados_ip_status():
    log_valido = '192.168.0.1 - - [27/Jul/2026:14:32:10 +0000] "GET /api/v1/users HTTP/1.1" 200 3421\n'
    log_ip_invalido = '9999.1.1.1 - - [27/Jul/2026:14:32:10 +0000] "GET /api/v1/users HTTP/1.1" 200 3421\n'
    log_status_invalido = '192.168.0.1 - - [27/Jul/2026:14:32:10 +0000] "GET /api/v1/users HTTP/1.1" 900 3421\n'
    
    fd, path = tempfile.mkstemp()
    with os.fdopen(fd, 'w') as f:
        f.write(log_valido)
        f.write(log_ip_invalido)
        f.write(log_status_invalido)
        
    try:
        df = process_logs(path)
        assert df.height == 1, "O filtro de qualidade falhou ao descartar o IP ou o Status inválido"
    finally:
        os.remove(path)

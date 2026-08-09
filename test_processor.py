import os
import tempfile
import pytest
import polars as pl
from processor import process_logs

LOG_VALIDO = '192.168.0.1 - - [27/Jul/2026:14:32:10 +0000] "GET /api/v1/users HTTP/1.1" 200 3421\n'

LOG_METODO_INVALIDO = '10.0.0.5 - - [27/Jul/2026:15:00:00 +0000] "POST /login HTTP/1.1" 201 500\n'

LOG_LIXO = 'isso é um texto aleatorio que quebrou no servidor\n'

@pytest.fixture
def arquivo_mock():
    """
    Fixture do Pytest que cria um arquivo temporário com linhas de log simuladas.
    O arquivo é automaticamente deletado após a execução dos testes.
    """
    fd, path = tempfile.mkstemp()
    with os.fdopen(fd, 'w') as f:
        f.write(LOG_VALIDO)
        f.write(LOG_METODO_INVALIDO)
        f.write(LOG_LIXO)
    yield path
    os.remove(path)


def test_extracao_regex_campos_corretos(arquivo_mock):
    """
    Valida se a expressão regular extrai perfeitamente IP, endpoint, status e size.
    """
    df = process_logs(arquivo_mock)
    
    resultado = df.row(0, named=True)
    
    assert resultado["ip"] == "192.168.0.1", "Falha ao extrair o IP"
    assert resultado["endpoint"] == "/api/v1/users", "Falha ao extrair o endpoint da rota"
    assert resultado["status"] == 200, "Falha ao extrair e converter o status HTTP"
    assert resultado["size"] == 3421, "Falha ao extrair e converter o tamanho (size)"

def test_descarte_linhas_invalidas_com_drop_nulls(arquivo_mock):
    """
    Garante que linhas que não dão 'match' na regex se tornam nulas e são
    descartadas pela função drop_nulls() do pipeline.
    """
    df = process_logs(arquivo_mock)
    
    assert df.height == 1, "O DataFrame deveria conter apenas 1 linha válida após o drop_nulls()"

def test_regra_is_error(arquivo_mock):
    """
    Garante que a transformação booleana de erro está funcionando.
    """
    df = process_logs(arquivo_mock)
    resultado = df.row(0, named=True)
     
    assert resultado["is_error"] is False, "is_error deveria ser False para status 200"


from __future__ import annotations

import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def mock_log_file() -> Path:
    content = (
        '192.168.0.1 - - [27/Jul/2026:14:32:10 +0000] "GET /api/v1/users HTTP/1.1" 200 3421\n'
        '10.0.0.5 - - [27/Jul/2026:15:00:00 +0000] "POST /login HTTP/1.1" 201 500\n'
        'isso e um texto aleatorio que quebrou no servidor\n'
        '999.999.999.999 - - [27/Jul/2026:14:32:10 +0000] "GET /api/v1/users HTTP/1.1" 200 3421\n'
        '192.168.0.1 - - [27/Jul/2026:14:32:10 +0000] "GET /api/v1/users HTTP/1.1" 900 3421\n'
    )
    with tempfile.NamedTemporaryFile(mode="w", suffix=".log", delete=False) as f:
        f.write(content)
        path = Path(f.name)
    yield path
    path.unlink()


@pytest.fixture
def temp_output_dirs(tmp_path: Path) -> tuple[Path, Path]:
    output = tmp_path / 'logs_lake'
    quarantine = tmp_path / 'quarantine'
    return output, quarantine

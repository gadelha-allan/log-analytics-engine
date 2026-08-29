FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml requirements.txt ./
RUN pip install --no-cache-dir .

COPY . .

CMD ["python", "-m", "src.main"]

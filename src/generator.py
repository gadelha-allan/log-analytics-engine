from __future__ import annotations

import logging
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from faker import Faker

logger = logging.getLogger(__name__)
faker = Faker()

METHODS = ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD"]
METHOD_WEIGHTS = [55, 25, 8, 5, 5, 2]

ENDPOINTS = [
    "/",
    "/home",
    "/login",
    "/logout",
    "/api/v1/users",
    "/api/v1/users/{id}",
    "/api/v1/products",
    "/api/v1/products/{id}",
    "/api/v1/orders",
    "/api/v1/orders/{id}",
    "/checkout",
    "/cart",
    "/health",
    "/metrics",
    "/static/css/main.css",
    "/static/js/app.js",
]
ENDPOINT_WEIGHTS = [15, 10, 8, 3, 12, 8, 10, 6, 8, 5, 7, 4, 2, 1, 2, 2]

STATUS_DISTRIBUTION = {
    200: 65,
    201: 10,
    204: 5,
    301: 3,
    304: 4,
    400: 4,
    401: 2,
    403: 2,
    404: 3,
    500: 1,
    502: 0.5,
    503: 0.5,
}


def _generate_ip() -> str:
    if random.random() < 0.7:
        ranges = [
            (
                "10",
                random.randint(0, 255),
                random.randint(0, 255),
                random.randint(0, 255),
            ),
            (
                "172",
                random.randint(16, 31),
                random.randint(0, 255),
                random.randint(0, 255),
            ),
            ("192", "168", random.randint(0, 255), random.randint(0, 255)),
        ]
        return ".".join(str(o) for o in random.choice(ranges))
    return faker.ipv4_public()


def _generate_timestamp(base_date: datetime, days_spread: int = 30) -> str:
    offset = timedelta(
        days=random.randint(0, days_spread),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )
    ts = base_date - offset
    return ts.strftime("%d/%b/%Y:%H:%M:%S %z")


def _generate_endpoint() -> str:
    endpoint = random.choices(ENDPOINTS, weights=ENDPOINT_WEIGHTS)[0]
    if "{id}" in endpoint:
        endpoint = endpoint.replace("{id}", str(random.randint(1, 99999)))
    if random.random() < 0.2 and "?" not in endpoint:
        params = random.choice(
            [
                "?page=1&limit=20",
                "?sort=desc",
                "?filter=active",
                f"?search={faker.word()}",
            ]
        )
        endpoint += params
    return endpoint


def generate_mock_logs(
    filename: str | Path = "data/raw/server.log",
    lines: int = 1000,
    days_spread: int = 30,
) -> Path:
    filepath = Path(filename)
    filepath.parent.mkdir(parents=True, exist_ok=True)

    base_date = datetime.now(timezone.utc)
    statuses = list(STATUS_DISTRIBUTION.keys())
    status_weights = list(STATUS_DISTRIBUTION.values())

    logger.info("Gerando %,d linhas de log em %s", lines, filepath)

    with open(filepath, "w", encoding="utf-8") as f:
        for _ in range(lines):
            ip = _generate_ip()
            date = _generate_timestamp(base_date, days_spread)
            method = random.choices(METHODS, weights=METHOD_WEIGHTS)[0]
            endpoint = _generate_endpoint()
            status = random.choices(statuses, weights=status_weights)[0]
            size = random.randint(0, 500_000) if status != 204 else 0

            log_line = (
                f'{ip} - - [{date}] "{method} {endpoint} HTTP/1.1" {status} {size}\n'
            )
            f.write(log_line)

    logger.info("Logs gerados com sucesso: %s", filepath)
    return filepath


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    generate_mock_logs(lines=5_000_000, days_spread=90)

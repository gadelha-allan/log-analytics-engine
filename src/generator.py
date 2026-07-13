import random
import os
from datetime import datetime

def generate_mock_logs(filename="data/raw/server.log", lines=1000):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    ips = ["192.168.0.1", "10.0.0.5", "172.16.0.2", "189.10.20.30"]
    endpoints = ["/home", "/login", "/api/v1/users", "/products", "/checkout"]
    statuses = [200, 201, 404, 500, 403]
    
    with open(filename, "w") as f:
        for _ in range(lines):
            ip = random.choice(ips)
            date = datetime.now().strftime("%d/%b/%Y:%H:%M:%S +0000")
            endpoint = random.choice(endpoints)
            status = random.choices(statuses, weights=[70, 10, 10, 5, 5])[0]
            size = random.randint(200, 5000)
            
            log_line = f'{ip} - - [{date}] "GET {endpoint} HTTP/1.1" {status} {size}\n'
            f.write(log_line)
    print(f"✅ Geradas {lines} linhas de log em {filename}")

if __name__ == "__main__":
    generate_mock_logs()
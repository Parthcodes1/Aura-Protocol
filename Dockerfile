FROM python:3.11-slim

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ libssl-dev libffi-dev git curl libgmp-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps first (layer cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY . .

# Create data directory
RUN mkdir -p /app/data

# Ports: P2P | Dashboard | Metrics | DHT
EXPOSE 4001 8080 9090 5001

# Default: run with env vars
ENTRYPOINT ["python", "agent.py"]
CMD ["--agent-id", "agent_1", "--port", "4001"]

# Multi-stage Dockerfile — SentinelStream Real-Time Fraud-Scoring Data Platform

FROM python:3.10-slim AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .
COPY README.md .
COPY producer/ producer/
COPY streaming/ streaming/
COPY fraud/ fraud/
COPY ml/ ml/
COPY warehouse/ warehouse/
COPY airflow/ airflow/
COPY monitoring/ monitoring/

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir .

FROM python:3.10-slim AS runner

WORKDIR /app

COPY --from=builder /usr/local/lib/python3.10/site-packages /usr/local/lib/python3.10/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY --from=builder /app /app

EXPOSE 8000

ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "producer.generator", "--count", "50"]

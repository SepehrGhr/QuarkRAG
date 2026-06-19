# Stage 1: Build
FROM python:3.12-slim AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# Stage 2: Runtime
FROM python:3.12-slim AS runner

# NOTE on Kubernetes probes:
# This service loads sentence-transformers (on CPU) which takes 30-60s on startup.
# We must use a startupProbe (not livenessProbe) to give the model time to download/load.
# Recommended Kubernetes configuration:
# startupProbe:
#   httpGet:
#     path: /readiness
#     port: 8000
#   failureThreshold: 24
#   periodSeconds: 5  # Total 120s timeout
# readinessProbe:
#   httpGet:
#     path: /readiness
#     port: 8000
# livenessProbe:
#   httpGet:
#     path: /health
#     port: 8000

WORKDIR /app

RUN groupadd -g 10001 appgroup && \
    useradd -u 10001 -g appgroup appuser

# HuggingFace cache setup for local sentence-transformers model downloading
ENV HF_HOME=/home/appuser/.cache/huggingface
RUN mkdir -p /home/appuser/.cache/huggingface

COPY --from=builder /install /usr/local
COPY . /app

RUN chown -R appuser:appgroup /app && \
    chown -R appuser:appgroup /home/appuser/.cache

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

USER appuser

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

#!/bin/bash
# Find the root .env file relative to the script location
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/../../.env"

if [ -f "${ENV_FILE}" ]; then
  echo "Loading environment variables from ${ENV_FILE}..."
  # Export variables from .env file, ignoring comments and blank lines
  export $(grep -v '^#' "${ENV_FILE}" | xargs)
else
  echo "Warning: Central .env file not found at ${ENV_FILE}. Using existing environment variables."
fi

# Apply the namespace first
kubectl apply -f "${SCRIPT_DIR}/namespace.yaml"

# Check if envsubst is installed for templating
if command -v envsubst &> /dev/null; then
  echo "Applying configmap and secrets with envsubst substitution..."
  envsubst < "${SCRIPT_DIR}/configmap.yaml" | kubectl apply -f -
  envsubst < "${SCRIPT_DIR}/secrets.yaml" | kubectl apply -f -
else
  echo "Warning: 'envsubst' not found. Applying configmap.yaml and secrets.yaml without substitution."
  kubectl apply -f "${SCRIPT_DIR}/configmap.yaml"
  kubectl apply -f "${SCRIPT_DIR}/secrets.yaml"
fi

# Apply other manifests
manifests=(
  "postgres.yaml"
  "kafka.yaml"
  "qdrant.yaml"
  "redis.yaml"
  "traefik.yaml"
  "db-migrate-job.yaml"
  "ingestion-service.yaml"
  "embedding-service.yaml"
  "query-service.yaml"
  "llm-provider-service.yaml"
)

for manifest in "${manifests[@]}"; do
  if [ -f "${SCRIPT_DIR}/${manifest}" ]; then
    echo "Applying ${manifest}..."
    kubectl apply -f "${SCRIPT_DIR}/${manifest}"
  fi
done

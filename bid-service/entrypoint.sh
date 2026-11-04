#!/bin/sh
# Na AWS (OTEL_ENABLED=true) o processo sobe com a auto-instrumentação OpenTelemetry: traces de
# FastAPI, redis e asyncpg vão para o sidecar ADOT, que envia ao X-Ray. Localmente, sem overhead.
set -e
if [ "${OTEL_ENABLED:-false}" = "true" ]; then
  exec opentelemetry-instrument python -m auction
fi
exec python -m auction

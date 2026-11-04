#!/usr/bin/env bash
# Teste de fumaça contra um ambiente rodando (local ou ALB).
#   ./scripts/smoke-test.sh                     → http://localhost:8080
#   ./scripts/smoke-test.sh http://<alb-dns>
# Requer curl e jq.
set -euo pipefail

BASE="${1:-http://localhost:8080}"
PASS=0

check() {
  local name="$1" expected="$2" actual="$3"
  if [[ "$expected" == "$actual" ]]; then
    echo "  ok   $name ($actual)"
    PASS=$((PASS + 1))
  else
    echo "  FAIL $name: esperado $expected, veio $actual"
    exit 1
  fi
}

bid() { # usuário key valor → "status corpo"
  curl -s -o /tmp/smoke-bid.json -w '%{http_code}' -X POST "$BASE/auctions/$AUCTION/bids" \
    -H 'Content-Type: application/json' -H "X-User-Id: $1" -H "Idempotency-Key: $2" -d "{\"amount\": $3}"
}

echo "health"
check "readiness" "UP" "$(curl -s "$BASE/readyz" | jq -r .status)"
check "métricas fora do LB" 404 "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/metrics")"

echo "leilão de 8 s, anti-sniping desligado"
AUCTION=$(curl -s -X POST "$BASE/auctions" -H 'Content-Type: application/json' -d '{
  "title": "Smoke test", "startingPrice": "100.00", "minIncrement": "5.00",
  "durationSeconds": 8, "antiSnipingWindowSeconds": 0, "antiSnipingExtendSeconds": 0}' | jq -r .id)
echo "  id $AUCTION"

KEY=$(uuidgen 2>/dev/null || cat /proc/sys/kernel/random/uuid)
check "primeiro lance aceito" 201 "$(bid ana "$KEY" 100.00)"
check "seq do primeiro lance" 1 "$(jq -r .seq /tmp/smoke-bid.json)"
check "retry idempotente" 200 "$(bid ana "$KEY" 100.00)"
check "retry devolve o mesmo seq" 1 "$(jq -r .seq /tmp/smoke-bid.json)"
check "mesma key com outro valor" 422 "$(bid ana "$KEY" 500.00)"
check "empate rejeitado" 409 "$(bid bia "k-$RANDOM-1" 100.00)"
check "abaixo do incremento" 409 "$(bid bia "k-$RANDOM-2" 104.99)"
check "lance válido" 201 "$(bid bia "k-$RANDOM-3" 105.00)"
check "lista afterSeq=1" "2" "$(curl -s "$BASE/auctions/$AUCTION/bids?afterSeq=1" | jq -c '[.[].seq] | join(",")' -r)"

echo "aguardando o encerramento"
for _ in $(seq 1 30); do
  STATUS=$(curl -s "$BASE/auctions/$AUCTION" | jq -r .status)
  [[ "$STATUS" == "CLOSED" ]] && break
  sleep 1
done
check "leilão encerrado" CLOSED "$STATUS"
check "vencedor" bia "$(curl -s "$BASE/auctions/$AUCTION" | jq -r .winnerId)"
check "lance após o fim" 422 "$(bid caio "k-$RANDOM-4" 999.00)"

echo "$PASS verificações ok"

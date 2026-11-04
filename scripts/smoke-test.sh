#!/usr/bin/env bash
# Teste de fumaça contra um ambiente rodando (local ou ALB).
#   ./scripts/smoke-test.sh                     → http://localhost:8080
#   ./scripts/smoke-test.sh http://<alb-dns>
# Detecta o modo de autenticação: com Cognito (AWS), cria usuários descartáveis e usa os access tokens;
# sem ele (local), usa o header X-User-Id. Requer curl e jq.
set -euo pipefail

BASE="${1:-http://localhost:8080}"
PASS=0
SUFFIX="$RANDOM$RANDOM"
declare -A TOKEN NAME

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

MODE=header
PROBE=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE/auth/login" -H 'Content-Type: application/json' \
  -d '{"username": "smoke-probe", "password": "probe-12345"}')
case "$PROBE" in
  404) MODE=header ;;
  400|401) MODE=cognito ;;
  *) echo "ambiente não respondeu como esperado em /auth/login (HTTP $PROBE)"; exit 1 ;;
esac

# Usuário do teste: no modo cognito, cadastrado e logado de verdade (nome com sufixo aleatório).
login() {
  local who="$1"
  if [[ "$MODE" == header ]]; then
    NAME[$who]="$who"
    return
  fi
  local user="smoke-$who-$SUFFIX" pass="smoke-$SUFFIX-pw1"
  NAME[$who]="$user"
  curl -sf -o /dev/null -X POST "$BASE/auth/signup" -H 'Content-Type: application/json' \
    -d "{\"username\": \"$user\", \"password\": \"$pass\"}"
  TOKEN[$who]=$(curl -sf -X POST "$BASE/auth/login" -H 'Content-Type: application/json' \
    -d "{\"username\": \"$user\", \"password\": \"$pass\"}" | jq -r .accessToken)
}

auth() { # usuário → argumentos de curl que o identificam
  if [[ "$MODE" == header ]]; then echo "X-User-Id: $1"; else echo "Authorization: Bearer ${TOKEN[$1]}"; fi
}

bid() { # usuário key valor → status (corpo em /tmp/smoke-bid.json)
  curl -s -o /tmp/smoke-bid.json -w '%{http_code}' -X POST "$BASE/auctions/$AUCTION/bids" \
    -H 'Content-Type: application/json' -H "$(auth "$1")" -H "Idempotency-Key: $2" -d "{\"amount\": $3}"
}

echo "health (autenticação: $MODE)"
check "readiness" "UP" "$(curl -s "$BASE/readyz" | jq -r .status)"
check "métricas fora do LB" 404 "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/metrics")"

for who in ana bia caio; do login "$who"; done
if [[ "$MODE" == cognito ]]; then
  check "lance sem token recusado" 401 "$(curl -s -o /dev/null -w '%{http_code}' -X POST \
    "$BASE/auctions/00000000-0000-0000-0000-000000000000/bids" -H 'Content-Type: application/json' \
    -H 'X-User-Id: ana' -H 'Idempotency-Key: k' -d '{"amount": 1}')"
fi

echo "leilão de 8 s, anti-sniping desligado"
AUCTION=$(curl -s -X POST "$BASE/auctions" -H 'Content-Type: application/json' -H "$(auth ana)" -d '{
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
check "vencedor" "${NAME[bia]}" "$(curl -s "$BASE/auctions/$AUCTION" | jq -r .winnerId)"
check "lance após o fim" 422 "$(bid caio "k-$RANDOM-4" 999.00)"

echo "$PASS verificações ok"

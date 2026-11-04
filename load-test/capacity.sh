#!/usr/bin/env bash
# Mede a capacidade do bid-service de forma reproduzível.
#
#   ./load-test/capacity.sh <réplicas> <taxa> [<taxa> ...]
#   APP_CPUS=0.5 ./load-test/capacity.sh 1 600 800 1000 1200     → capacidade de uma task Fargate
#   APP_CPUS=0.5 ./load-test/capacity.sh 4 3200                   → linearidade
#
# Método:
#   - CPU isolada: o sistema (app, nginx, Valkey, Postgres) fica nos núcleos físicos 0-3 e o k6 nos
#     núcleos 4-5 (CPUs n e n+6 são o mesmo núcleo com SMT; ver lscpu -e). Sem isso o gerador de carga
#     disputa CPU com o sistema e o número medido é o da máquina, não o da aplicação.
#   - APP_CPUS limita cada réplica (0.5 = task de 0,5 vCPU). Ryzen x86 não é Graviton: é estimativa.
#   - Para cada taxa: 5 s de aquecimento descartados, depois DURATION (30 s) medidos com amostras de
#     docker stats.
#   - SLO: p99 < 50 ms e 0 erros. Descarte (503 do LoadShedder) também significa capacidade excedida.
#
# Saída: tabela markdown no terminal e em load-test/results/.
set -euo pipefail

REPLICAS="${1:?uso: $0 <réplicas> <taxa> [<taxa> ...]}"
shift
RATES=("$@")
[[ ${#RATES[@]} -gt 0 ]] || { echo "informe ao menos uma taxa" >&2; exit 1; }

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DURATION="${DURATION:-30s}"
SYSTEM_CPUSET="${SYSTEM_CPUSET:-0-3,6-9}"
K6_CPUSET="${K6_CPUSET:-4,5,10,11}"
APP_CPUS="${APP_CPUS:-0}"
OUT_DIR="$ROOT/load-test/results"
OUT="$OUT_DIR/capacity-$(date +%Y%m%d-%H%M%S)-r${REPLICAS}-cpu${APP_CPUS}.md"
TMP="$(mktemp -d)"
chmod 777 "$TMP"  # o k6 roda no container com outro usuário
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$OUT_DIR"

echo "== subindo $REPLICAS réplica(s), APP_CPUS=$APP_CPUS, sistema em $SYSTEM_CPUSET, k6 em $K6_CPUSET"
cd "$ROOT"
export APP_REPLICAS="$REPLICAS" APP_CPUS SYSTEM_CPUSET PER_IP_RATE_LIMIT_ENABLED=false
docker compose up -d --build --wait --remove-orphans >/dev/null 2>&1
# O nginx resolve as réplicas no startup: reinicia para enxergar todas.
docker compose restart nginx >/dev/null 2>&1
sleep 3

k6() { # taxa duração [arquivo de resumo]
  local summary=()
  [[ -n "${3:-}" ]] && summary=(--summary-export "/out/$(basename "$3")")
  docker run --rm -i --network host --cpuset-cpus "$K6_CPUSET" \
    -v "$ROOT/load-test/k6:/scripts" -v "$TMP:/out" grafana/k6:1.3.0 run \
    --summary-trend-stats "avg,p(50),p(95),p(99),max" "${summary[@]}" \
    -e RATE="$1" -e DURATION="$2" /scripts/bid-storm.js >/dev/null 2>&1 || true
}

{
  echo "# Capacidade: $REPLICAS réplica(s), APP_CPUS=$APP_CPUS, $(date -Iseconds)"
  echo
  echo "Sistema em CPUs $SYSTEM_CPUSET, k6 em $K6_CPUSET, $DURATION por taxa. SLO: p99 < 50 ms, 0 erros, 0 descartes."
  echo
  echo "| Pedida/s | Entregue/s | p50 | p95 | p99 | Erros | Descartados | CPU por réplica | CPU Valkey | SLO |"
  echo "|---:|---:|---:|---:|---:|---:|---:|---:|---:|:-:|"
} | tee "$OUT"

for rate in "${RATES[@]}"; do
  k6 "$rate" 5s
  # Amostras de CPU durante a medição (a cada ~2 s).
  ( while true; do docker stats --no-stream --format '{{.Name}} {{.CPUPerc}}' >> "$TMP/stats-$rate.txt"; done ) &
  STATS=$!
  k6 "$rate" "$DURATION" "$TMP/summary-$rate.json"
  kill "$STATS" 2>/dev/null; wait "$STATS" 2>/dev/null || true

  python3 - "$TMP/summary-$rate.json" "$TMP/stats-$rate.txt" "$rate" <<'EOF' | tee -a "$OUT"
import json, sys, re
summary, stats, rate = sys.argv[1], sys.argv[2], int(sys.argv[3])
m = json.load(open(summary))["metrics"]
lat = m.get("http_req_duration{name:bid}") or m["http_req_duration"]
reqs = m["http_reqs"]["rate"]
failed = m.get("http_req_failed", {}).get("value", 0) * m["http_reqs"]["count"]
shed = m.get("bids_shed", {}).get("count", 0)
app, valkey = [], []
for line in open(stats):
    name, _, cpu = line.strip().rpartition(" ")
    v = float(cpu.rstrip("%") or 0)
    if re.search(r"-app-\d+$", name): app.append(v)
    elif name.endswith("valkey-1"): valkey.append(v)
avg = lambda xs: sum(xs) / len(xs) if xs else 0
ok = lat["p(99)"] < 50 and failed == 0 and shed == 0
print(f"| {rate} | {reqs:.0f} | {lat['p(50)']:.2f} ms | {lat['p(95)']:.2f} ms | {lat['p(99)']:.2f} ms | "
      f"{failed:.0f} | {shed:.0f} | {avg(app):.0f}% | {avg(valkey):.0f}% | {'ok' if ok else 'violado'} |")
EOF
done

echo
echo "resultado em ${OUT#$ROOT/}"

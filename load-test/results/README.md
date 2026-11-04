# Resultados de capacidade

Gerados por [`../capacity.sh`](../capacity.sh), um arquivo por execução. Método (ver ADR-13):

- **Máquina:** Ryzen 5 5600X, 6 núcleos / 12 threads.
- **CPU isolada:** sistema (réplicas da app, nginx, Valkey, Postgres) nos núcleos físicos 0–3 e k6 nos 4–5.
- **Cota por réplica:** `APP_CPUS`, para imitar o tamanho da task do Fargate.
- **Por taxa:** 5 s de aquecimento descartados, depois 30 s medidos.
- **SLO:** p99 < 50 ms, 0 erros e 0 descartes (503 do load shedding).

## Resumo

**Capacidade de uma task** (1 réplica atrás do nginx):

| Task | Maior taxa dentro do SLO | p99 nessa taxa | Primeira taxa fora do SLO |
|---|---|---|---|
| 0,5 vCPU | ~1.000/s | 11,9 ms | 1.200/s (7 descartes); 1.400/s: p99 305 ms |
| 1 vCPU | ~2.100/s | 2,5 ms | 2.400/s (2 descartes, p99 11,9 ms) |

**Linearidade** (~80% da capacidade por réplica):

| Réplicas | Pedida | Entregue | p99 | Erros | Descartes |
|---|---|---|---|---|---|
| 2 × 0,5 vCPU | 1.600/s | 1.677/s | 1,8 ms | 0 | 0 |
| 4 × 0,5 vCPU | 3.200/s | 3.361/s | 7,0 ms | 0 | 0 |
| 6 × 0,5 vCPU | 4.800/s | 5.040/s | 33,1 ms | 0 | 0 |
| 2 × 1 vCPU | 3.400/s | 3.566/s | 5,6 ms | 0 | 0 |
| 3 × 1 vCPU | 5.100/s | 5.349/s | 32,7 ms | 0 | 469 |
| 4 × 1 vCPU | 6.800/s | 7.125/s | 86,3 ms | 0 | 6.770 (3%) |

"Entregue" inclui os retries do k6 (5% dos lances são reenviados com a mesma key).

**Limite da máquina:** com 3 ou mais réplicas de 1 vCPU, elas, o nginx, o Valkey e o Postgres não cabem em 4 núcleos físicos sem disputar os pares SMT. Por isso os descartes aparecem com cada réplica em 78–84% de CPU. Linearidade além disso só se prova com k6 distribuído contra o ALB.

**Sobrecarga** (1 task de 0,5 vCPU a 2.400/s, 2× a capacidade):
- **Com o load shedding:** 0 erros, 36 mil descartes em 503 e p99 de 103 ms.
- **Sem o limite** (máximo de 256 em andamento): p99 de 350 ms a 1.600/s.

**Outros números:**
- **Pré-filtro:** 97,3% dos lances resolvidos na task; só 2,75% chegaram ao Lua (contado nas métricas das réplicas depois das rodadas).
- **CPU por lance:** ~0,35 ms, num processo sem cota a 1.500/s.

> Ryzen x86 não é Graviton ARM: os números são estimativas para dimensionar a task, a confirmar com k6 distribuído contra o ALB.

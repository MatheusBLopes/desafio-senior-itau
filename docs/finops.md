# FinOps: o melhor pelo menor custo

> **Premissas.** Região us-east-1, preços on-demand de lista vigentes em 2025–2026, 730 h/mês, valores em USD.
> São estimativas de ordem de grandeza para comparar decisões. Antes de qualquer compromisso, confirmar no
> [AWS Pricing Calculator](https://calculator.aws/). A arquitetura básica foi implantada em 2026-11-03 (smoke test e
> WebSocket verificados no ALB) e deixada pausada; o cenário de pico continua extrapolado, não medido na AWS.

## 1. Arquitetura básica (o que o Terraform sobe)

| Serviço | Configuração | Preço unitário (aprox.) | USD/mês |
|---|---|---|---|
| ECS Fargate ARM64 | 2 tasks × 1 vCPU / 2 GB, on-demand. Medido: ~2.100 lances/s por task de 1 vCPU contra ~1.000 com 0,5 vCPU, mesmo custo por lance e metade das tasks (ADR-13). 2 GB é o mínimo do Fargate para 1 vCPU | US$ 0,03238/vCPU-h + US$ 0,00356/GB-h | ~57,70 |
| Application Load Balancer | 1 ALB, tráfego de demo (~1 LCU) | US$ 0,0225/h + US$ 0,008/LCU-h | ~22,30 |
| ElastiCache for Valkey | 1 × cache.t4g.micro | ~US$ 0,0128/h (20% abaixo do Redis OSS) | ~9,35 |
| RDS PostgreSQL | db.t4g.micro Single-AZ + gp3 20 GB | US$ 0,016/h + US$ 0,115/GB-mês | ~14,00 |
| IPv4 público | 2 tasks + 2 do ALB | US$ 0,005/h por IP | ~14,60 |
| CloudWatch | ~25 séries custom, 10 alarmes, ~2 GB de logs, 1 dashboard | US$ 0,30/métrica, US$ 0,10/alarme, US$ 0,50/GB | ~10,00 |
| X-Ray | 5% de amostragem | 100 mil traces/mês grátis | ~0 |
| ECR | ≤ 5 imagens (~1,5 GB) | US$ 0,10/GB-mês | ~0,15 |
| SSM Parameter Store | 1 SecureString standard | grátis | 0 |
| AWS Budgets | 1 budget | 2 primeiros grátis | 0 |
| S3 (state) | KB | — | ~0,01 |
| **Total** | | | **~US$ 128/mês ≈ US$ 0,18/h** |

**Pausado entre testes** (`scripts/aws-pause.sh`): tasks em 0 e RDS parado; continuam cobrando o ALB e seus IPv4 (~US$ 0,78/dia), o nó do ElastiCache (~US$ 0,31/dia) e o disco do RDS (centavos): **~US$ 1,1/dia**. `scripts/aws-resume.sh` volta em ~5-10 min.

**Para a entrevista:** subir só durante a demo. 3 horas de ambiente custam **~US$ 0,52**. O `terraform destroy` remove tudo, inclusive o ECR (`force_delete`) e o RDS sem snapshot final. O único item que sobra é o bucket de state (centavos).

## 2. Alternativas descartadas e quanto custariam

| Alternativa | Custo | Por que não |
|---|---|---|
| NAT Gateway (1 AZ) | ~US$ 32,85/mês + US$ 0,045/GB | Só serviria para as tasks alcançarem ECR, CloudWatch e X-Ray. SG fechado + IP público resolve por ~US$ 7/mês |
| VPC Interface Endpoints (ECR api + dkr, Logs, SSM, X-Ray × 2 AZs) | ~US$ 73/mês | Mais caro que o NAT. Vale em produção regulada, não no MVP |
| API Gateway WebSocket | US$ 1,00/milhão de mensagens + US$ 0,25/milhão de minutos de conexão | No cenário de pico (seção 3): ~2,4 bilhões de mensagens ≈ **US$ 2.400 por leilão**. O ALB cobra LCU: ~US$ 5 |
| Aurora PostgreSQL | db.t4g.medium ~US$ 53/mês; Serverless v2 (mín. 0,5 ACU) ~US$ 44/mês | Piso 3–4× maior que o RDS micro. Faz sentido com réplicas, Global Database e failover < 30 s |
| Amazon MSK / Kinesis | MSK 2 × kafka.t3.small ~US$ 66/mês; Kinesis on-demand ~US$ 29/mês por stream + dados | O Redis Stream já está no mesmo nó do Lua e o `XADD` é atômico com o aceite. Kafka/Kinesis exigiria outbox ou dual write |
| Secrets Manager | US$ 0,40/segredo/mês + API | SSM SecureString é grátis. A rotação automática não é necessária no MVP |
| Fargate x86 | 1 vCPU / 2 GB: US$ 0,0494/h vs US$ 0,0395/h no ARM | Graviton é ~20% mais barato e a imagem Python (multi-arquitetura) roda igual |
| Fargate só on-demand | — | O excedente em Spot sai até ~70% mais barato. A base fica on-demand, para não perder WebSockets no pico |
| ElastiCache Multi-AZ (réplica) | +1 nó: ~US$ 9,35/mês | Recomendado para produção (failover automático). Ativa com `redis_node_count = 2` |
| MemoryDB for Valkey | db.t4g.small ~US$ 25–35/mês por nó (+ réplica para HA) | Zera a perda de lance confirmado com log transacional Multi-AZ. É o upgrade de durabilidade quando o negócio exigir |

## 3. Cenário de pico: 200 mil usuários, 50 mil lances/s por 30 s

### O que foi medido (local)

Máquina: Ryzen 5 5600X (12 threads), com o sistema e o k6 em núcleos separados (`load-test/capacity.sh`, resultados em [`load-test/results/`](../load-test/results/README.md)).

- **Capacidade por task:** ~1.000 lances/s numa task de 0,5 vCPU e **~2.100/s numa de 1 vCPU**, com p99 < 50 ms e zero erros.
- **Escala linear:** 6 réplicas de 0,5 vCPU entregaram 5.040 lances/s com p99 de 33 ms e zero erros.
- **Acima da capacidade:** a task devolve o excesso como 503 (load shedding) e não registra erro.
- **Motor isolado** (`valkey-benchmark` chamando o `place_bid.lua`): **~50 mil execuções/s** numa única thread, p50 de 1,9 ms com 100 clientes.
- **Pré-filtro:** 97% dos lances rejeitados nem chegam ao Lua.

### Extrapolação (não medida na AWS)

| Componente | Dimensionamento | Raciocínio |
|---|---|---|
| Tasks (HTTP) | ~35 tasks de 1 vCPU / 2 GB | 50 mil/s ÷ (~2.100/s medidos × 70% de alvo). Para 10 mil/s, ~7 tasks |
| Tasks (WebSocket) | as mesmas tasks | 200 mil conexões ÷ 35 ≈ 6 mil por task. Cada conexão ociosa custa poucos KB no asyncio, mas o fan-out de snapshots (6,7/s por cliente) consome CPU; medir antes de assumir |
| Valkey | 1 nó cache.m7g.large por leilão quente | Com o pré-filtro, o Lua recebe uma fração dos 50 mil/s (~3%). Sem o pré-filtro, 50 mil/s bateria no teto de uma thread |
| Valkey para muitos leilões | cluster mode com N shards | O hash tag `{auctionId}` distribui os leilões entre os shards |
| Postgres | db.t4g.medium | Só lances aceitos chegam ao banco (dezenas a centenas por segundo), gravados em lote |

### Custo do pico, por leilão (janela de 30 min com escala ativa)

| Item | Cálculo | USD |
|---|---|---|
| Fargate | 35 tasks × (1 vCPU + 2 GB) × 0,5 h ≈ 35 × US$ 0,0395 × 0,5 | ~0,70 |
| ALB (LCU) | 200 mil conexões ativas ≈ 67 LCU; ~600 GB processados | ~5 |
| Transferência para a internet | 200 mil clientes × ~6,7 snapshots/s × ~250 B × 1.800 s ≈ 600 GB × US$ 0,09 | **~54** |
| Valkey m7g.large | US$ ~0,126/h × 0,5 h | ~0,06 |
| **Total** | | **~US$ 60 por leilão de pico** |

**Insight:** quem domina o custo é a **saída de dados** dos snapshots, não o compute. As alavancas, em ordem:
1. Aumentar o intervalo de conflation para espectadores (500 ms reduz ~70%).
2. Compressão WebSocket (permessage-deflate).
3. Enviar deltas em vez de snapshots completos.
4. CloudFront na frente (DTO mais barato e 1 TB/mês grátis).

## 4. Alavancas de custo aplicadas e disponíveis

**Aplicadas no código e no Terraform:**
- Graviton (−20%).
- Base on-demand + excedente em Spot (até −70% no excedente).
- Sem NAT.
- SSM em vez de Secrets Manager.
- Logs com retenção de 7 dias.
- Traces com 5% de amostragem.
- Container Insights desligado.
- Só métricas de negócio exportadas para o CloudWatch, via Embedded Metric Format (linhas de log agregadas por minuto, sem chamadas `PutMetricData`), sem `auctionId`/`userId` como dimensão (cada combinação seria uma série cobrada).
- Lifecycle do ECR.
- Budget com alerta em 80% do real e 100% do previsto.
- Tags padrão (`project`, `env`, `owner`, `cost-center`) via `default_tags`.

**Disponíveis:**
- Compute Savings Plans (até −50% em Fargate com compromisso de 1–3 anos).
- Scheduled scaling para zerar tasks fora do horário dos leilões.
- Reserved Nodes para ElastiCache/RDS em produção estável.
- `terraform destroy` depois de cada demo.

> Para o filtro do Budget por tag funcionar, a tag `project` precisa estar ativada como *cost allocation tag* no Billing (ação manual, uma vez).

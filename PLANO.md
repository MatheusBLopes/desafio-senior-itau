# Plano — Desafio Técnico Itaú: Leilão Online de Alta Concorrência

> **Nota (implementação):** este é o plano original, escrito para Java 21 + Spring Boot. A implementação foi migrada para **Python 3.13 (FastAPI + asyncio)**, com a mesma arquitetura: os mesmos scripts Lua, as mesmas tabelas, o mesmo Terraform e as mesmas garantias. As diferenças de stack (WebSocket JSON em vez de STOMP, EMF em vez de Micrometer, um processo por task) estão nos ADRs 06, 11, 12 e 13 de [`docs/decisoes-tecnicas.md`](docs/decisoes-tecnicas.md). A versão Java ficou em `bid-service-java/`, como referência.

## Contexto
O desafio (imagem `WhatsApp Image 2026-11-02 at 17.58.34.jpeg`) pede um sistema de leilão em que centenas de milhares de usuários enviam lances simultâneos nos últimos segundos. O sistema precisa:
- responder em tempo real;
- garantir uma **ordem determinística de chegada**;
- impedir que race conditions aceitem lances **inválidos ou duplicados**.

**Entregáveis:** desenho de arquitetura AWS, código funcional com instruções para rodar e documentação breve das decisões. O enunciado valoriza quem pensa além do que foi pedido.

**Escolhas feitas:** Java 21 + Spring Boot 3; **Terraform com deploy real na AWS**; Docker Compose para desenvolvimento local; MVP sólido com teste de carga; **análise FinOps** (o melhor pelo menor custo); **observabilidade**. Neste momento sobe só a **arquitetura básica funcionando**. Redundância e DR ficam **documentados** no README, com o caminho para ativar, mas não são provisionados agora. O repositório está vazio.

**Prazo e escopo:** o enunciado diz que o desafio não deve gerar horas extras e pede para entregar o que estiver pronto. Por isso as fases estão divididas em **cortes entregáveis** (ver "Fases de implementação"): cada corte termina com o repositório apresentável.

## Ideia central da solução
Cada leilão é serializado num único ponto atômico: um **script Lua no Redis/Valkey**, que executa um comando por vez e dispensa locks distribuídos. A ordem vem de `INCR seq` e o horário vem do `TIME` do servidor (não confia em relógio de cliente nem de instância).

**O que significa "ordem de chegada":** é a ordem em que os lances são serializados no Valkey. O jitter de rede entre o cliente e a AWS torna impossível uma ordem "justa" do ponto de vista do cliente, e nenhum sistema garante isso. O que garantimos é uma ordem **única, determinística e auditável** (`seq` sem buracos). Essa definição fica explícita no README.

O que o script `place_bid.lua` faz numa única execução (chaves com hash tag `{auctionId}`, compatível com cluster):
1. **Idempotência:** se `idem:{auctionId}:<userId>:<Idempotency-Key>` existe, compara o hash do payload. Se for igual, devolve o resultado original (retry ≠ duplicado). Se for diferente, devolve erro `422`. **Rejeições também são memorizadas**, para que o replay devolva sempre a mesma resposta.
2. **Status e prazo:** rejeita se o leilão não está `OPEN` ou se `now > endsAt`.
3. **Valor:** o primeiro lance exige `amount >= startingPrice`. Os seguintes exigem `amount >= currentPrice + minIncrement`, o que já rejeita o empate (vence quem chegou primeiro).
4. **Aceite:** `INCR seq`, atualiza preço e líder, grava a chave de idempotência com TTL, faz `XADD bids:{auctionId} MAXLEN ~ N` (stream limitado, para não estourar a memória) e `PUBLISH` do evento.
5. **Anti-sniping (diferencial):** lance a menos de X s do fim estende `endsAt` em Y s.

**Criação do leilão:** grava primeiro no Postgres (fonte da verdade do cadastro) e depois no Valkey (hash do leilão + ZSET `auctions:ending`). Uma falha entre os dois passos é coberta pela reidratação.

**Reidratação do estado:** o Valkey é um nó único sem persistência no MVP. Se reiniciar, perde preço, líder e `seq`, e o `seq` voltaria para 1, colidindo com `UNIQUE(auction_id, seq)`. Um `AuctionStateLoader` monta o hash do leilão a partir do Postgres (`max(seq)`, maior lance, `endsAt`) quando o `place_bid.lua` sinaliza "leilão não carregado". A janela de perda (lances aceitos e ainda não persistidos pelo consumer) fica documentada, junto com as mitigações (ver README §8).

**Persistência write-behind:** um consumer group do Redis Stream (com ACK e `XAUTOCLAIM` para pendentes) grava no Postgres. O banco é a segunda barreira, com `UNIQUE(auction_id, seq)` e `UNIQUE(auction_id, user_id, idempotency_key)` e inserção via `ON CONFLICT DO NOTHING`. O stream é limitado com `MAXLEN ~` no `XADD`; o tamanho é dimensionado para ficar bem acima do lag máximo esperado do consumer.

**Pré-filtro na aplicação (leilão quente):** cada task mantém em memória o preço atual de cada leilão, atualizado pelo Pub/Sub. Um lance claramente abaixo desse preço é rejeitado com `409` sem chegar ao Valkey. O Lua continua sendo a única fonte de verdade: o filtro só descarta o que com certeza seria rejeitado, reduzindo a carga na thread única do Valkey.

**Tempo real:** WebSocket (STOMP, **sem SockJS**, para não exigir sticky session) servido pelas tasks. Cada task assina o Pub/Sub do Redis e repassa aos seus clientes, então escala horizontalmente. Heartbeat STOMP menor que o idle timeout do ALB (60 s).
- **Conflation (diferencial):** com 50k lances/s e centenas de milhares de clientes, enviar cada lance a cada cliente é inviável. Cada task envia um **snapshot do leilão** (preço, líder, `seq`, `endsAt`) a cada ~100–250 ms por tópico, só quando houve mudança. `AUCTION_EXTENDED` e `AUCTION_CLOSED` são enviados na hora.
- Os eventos levam `seq`. Quem precisa da lista completa de lances usa `GET /bids?afterSeq=`.

**Encerramento:** um scheduler interno lê o ZSET `auctions:ending` e chama `close_auction.lua` (atômico, respeita extensão por anti-sniping). Com várias tasks rodando o scheduler, a operação continua segura porque o Lua é idempotente. O evento de encerramento entra no mesmo stream dos lances, então o consumer sempre o processa depois do último lance.

## Arquitetura AWS básica (o que o Terraform provisiona agora): foco em custo
| Camada | Serviço escolhido | Por que (FinOps) |
|---|---|---|
| Rede | VPC própria, 2 AZs, **subnets públicas para tasks (IP público + SG restrito ao ALB)**, subnets privadas para dados, **sem NAT Gateway** | O NAT custa ~US$ 32/mês + tráfego. VPC endpoints (~US$ 7/mês cada × ECR api, ECR dkr, Logs, SSM, X-Ray) também sairiam mais caros. O IPv4 público é cobrado (US$ 0,005/h ≈ US$ 3,65/mês por IP, nas tasks e no ALB), mas ainda fica mais barato. Com SG fechado, a segurança equivale |
| Entrada | **ALB** (HTTP/REST + WebSocket nativo) | API Gateway WebSocket cobra por mensagem + minuto de conexão, o que fica caro com centenas de milhares de conexões. O ALB tem custo fixo baixo |
| Compute | **ECS Fargate ARM64 (Graviton)**, 2 tasks 0.5 vCPU/1 GB, auto scaling por CPU/requests, capacity provider misto com **`base` on-demand** e o excedente em **Fargate Spot** | Graviton é ~20% mais barato. Spot sai até ~70% mais barato. A base on-demand evita que uma interrupção Spot nos segundos finais derrube as conexões WS justamente no pico |
| Motor atômico | **ElastiCache for Valkey** `cache.t4g.micro` (1 nó) | Valkey é ~20% mais barato que Redis OSS e compatível com Lua/Streams |
| Banco | **RDS PostgreSQL** `db.t4g.micro`, gp3 20 GB, Single-AZ | Aurora tem piso de custo bem maior, e para o MVP o RDS basta |
| Imagens | **ECR** com lifecycle policy (mantém as últimas 5) | Evita custo de armazenamento acumulado |
| Segredos | **SSM Parameter Store (SecureString)** em vez de Secrets Manager | Gratuito no tier standard, enquanto o Secrets Manager cobra US$ 0,40/segredo/mês |
| Observabilidade | ver seção própria | |
| Governança de custo | **AWS Budgets** com alerta por e-mail + **tags padrão** (`project`, `env`, `owner`, `cost-center`) via `default_tags` | Cost allocation e um alarme antes de surpresas |

O Terraform roda com estado remoto em S3 (lock nativo, `use_lockfile`, TF ≥ 1.10, sem DynamoDB). Há um módulo `bootstrap` para criar o bucket. O `terraform destroy` precisa deixar a conta limpa, para não deixar custo parado.

## Observabilidade
- **Logs:** JSON estruturado (logback + `logstash-encoder`) com `traceId`, `auctionId`, `userId` e `seq`, enviados ao **CloudWatch Logs** com retenção de **7 dias** (FinOps).
- **Métricas:** Micrometer → **CloudWatch** (registry `micrometer-registry-cloudwatch2`, namespace `Auction`): lances aceitos e rejeitados por motivo, rejeições do pré-filtro, latência p50/p95/p99 do Lua e da API, lag do stream (pendentes do consumer group), conexões WebSocket ativas. Também ficam disponíveis as métricas nativas de ALB, ECS, ElastiCache e RDS.
  - **Cardinalidade (FinOps):** o CloudWatch cobra por série de métrica. Nunca usar `auctionId` ou `userId` como dimensão, só `outcome`/`reason`. Esses identificadores ficam nos logs e traces.
- **Traces:** OpenTelemetry Java agent → **sidecar ADOT** → **X-Ray**, com **sampling de 5%** (FinOps). O sidecar consome CPU/memória da task e entra no dimensionamento e no custo.
- **Dashboard + alarmes** em Terraform: 5xx do ALB, p95 > X ms, CPU/memória do ECS, memória do Valkey, conexões e CPU do RDS, lag do stream. Notificação via SNS por e-mail.
- **Health checks:** Actuator `/actuator/health` (liveness e readiness, incluindo Redis e DB) usado pelo target group do ALB.
- **Local:** o Compose sobe opcionalmente Prometheus + Grafana (profile `observability`) com o endpoint `/actuator/prometheus`.

## FinOps (documento `docs/finops.md`)
- Estimativa mensal da arquitetura básica (us-east-1, preços on-demand, tabela por serviço, **incluindo IPv4 público**), mais o custo por hora para quem quiser subir só durante a demo/entrevista.
- Comparativo das alternativas descartadas e quanto custariam: NAT GW, VPC endpoints, API GW WebSocket, Aurora, MSK/Kinesis, Secrets Manager, x86 vs Graviton, Fargate vs Spot, **ElastiCache Multi-AZ vs MemoryDB for Valkey** (durabilidade dos lances).
- Custo projetado no **cenário de pico** (ex.: 200k usuários, 50k lances/s em 30 s), com a escala necessária (tasks, ElastiCache cluster mode para muitos leilões, nó maior para o leilão quente, RDS maior) e o custo por leilão. **Os números de vazão vêm da medição local do motor (k6), extrapolados**, sem prometer o que não foi medido.
- Alavancas: Savings Plans (Compute), Spot, scale-to-zero fora de horário (scheduled scaling), retenção curta de logs, sampling de traces, métricas sem alta cardinalidade, lifecycle do ECR e `terraform destroy` após a demo.

## README (conteúdo exigido)
1. **O desafio:** transcrição do que está no print (visão geral do problema, o que esperam com os 3 entregáveis, prazo, dica, importante).
2. Como a solução atende cada ponto (tempo real, ordem determinística, anti race condition, anti duplicidade), incluindo a definição de "ordem de chegada".
3. Diagrama AWS (PNG em `docs/`).
4. Como rodar: **local** (`docker compose up`) e **AWS** (bootstrap → build/push da imagem → `terraform apply` → URL do ALB → `terraform destroy`).
5. API, exemplos `curl`/`wscat` e teste de carga k6 com resultados.
6. Observabilidade: onde ver logs, métricas, traces e dashboard.
7. Resumo FinOps com link para `docs/finops.md`.
8. **Redundância, falhas e Disaster Recovery** (só documentado, não provisionado):
   - **Alta disponibilidade:** ECS com tasks em 2+ AZs; ElastiCache com réplica Multi-AZ e failover automático; RDS Multi-AZ ou Aurora com réplicas; ALB já é multi-AZ.
   - **Escala do motor:** cluster mode com shards por hash tag escala **muitos leilões**, mas **não um leilão quente**: todos os lances de um leilão caem no mesmo shard e na mesma thread. Para o leilão quente: pré-filtro na app, rate limit por usuário e nó maior (vertical).
   - **Modos de falha:**
     - **Valkey cai:** failover para a réplica, com risco de perder os últimos ms de lances não replicados. O ElastiCache **não oferece AOF** nas versões atuais. Mitigações: chamar `WAIT 1 <timeout>` **pela aplicação logo após o `EVALSHA`** (o `WAIT` não pode ser chamado dentro de um script Lua), com o custo de latência explicado; ou trocar por **MemoryDB for Valkey**, que tem log transacional Multi-AZ e não perde lance confirmado (custo maior, comparado no FinOps). Depois do restart, a reidratação a partir do Postgres restaura `seq`, preço e líder.
     - **Consumer atrasado:** o lag é visível em alarme e o stream retém os dados (`MAXLEN` dimensionado com folga).
     - **Task morre:** o cliente reconecta e se recupera por `afterSeq`.
     - **Retry do cliente:** idempotency key.
     - **Relógio dessincronizado:** só se usa o `TIME` do Valkey.
   - **DR:** RPO/RTO alvo; backups automáticos do RDS + PITR; snapshots do ElastiCache; estratégia **pilot light** em segunda região (Aurora Global Database / snapshot cross-region, ElastiCache Global Datastore, Terraform parametrizado por região, Route 53 failover); runbook de failover.
   - **Segurança e proteção de borda:** CloudFront + WAF (rate limit, bot control) + Shield, Cognito/JWT, e o custo de cada um.
   - Evolução: Kinesis/MSK como log durável de eventos, event sourcing, multi-região ativo-ativo.

## Estrutura do repositório
```
desafio-senior-itau/
├── README.md
├── docs/
│   ├── arquitetura-aws.drawio + .png (básica e alvo com HA/DR)
│   ├── decisoes-tecnicas.md   # ADRs curtos
│   └── finops.md
├── docker-compose.yml         # app x2 + nginx + valkey + postgres (+ profile observability)
├── bid-service/               # Spring Boot 3 / Java 21 / Maven, Dockerfile multi-stage ARM64
│   ├── src/main/java/.../{auction,bid,realtime,persistence,scheduler,common,observability}
│   ├── src/main/resources/{lua/place_bid.lua, lua/close_auction.lua, db/migration/, application.yml}
│   └── src/test/java/...      # unit, concorrência, integração (Testcontainers)
├── infra/terraform/
│   ├── bootstrap/             # bucket S3 do state
│   ├── modules/{network,alb,ecs,elasticache,rds,ecr,observability,budget}
│   └── envs/dev/{main.tf,variables.tf,outputs.tf,terraform.tfvars.example}
├── load-test/k6/bid-storm.js
└── scripts/{build-push.sh, smoke-test.sh}
```

## API (MVP)
- `POST /auctions`, `GET /auctions/{id}`
- `POST /auctions/{id}/bids` com `Idempotency-Key` obrigatório (escopo: leilão + usuário). Respostas: `201` aceito (com seq), `409` lance inferior ou empate, `422` encerrado ou key reutilizada com payload diferente, `200` replay idempotente (devolve a resposta original, inclusive rejeições).
- `GET /auctions/{id}/bids?afterSeq=`: lê do stream (`XRANGE`) os lances recentes, que podem ainda não estar no Postgres por causa do write-behind, e usa o banco como fallback para o histórico antigo.
- WS `/ws` → `/topic/auctions/{id}` com os eventos `AUCTION_SNAPSHOT` (conflated), `AUCTION_EXTENDED` e `AUCTION_CLOSED`
- Auth simplificada via `X-User-Id` (a doc mostra Cognito/JWT como alvo). Rate limit por usuário com token bucket em Lua (chamada separada, já que a chave do usuário fica em outro slot do cluster).

## Fases de implementação
**Corte 1 — essencial (já é uma entrega completa do desafio)**
1. **Setup:** projeto Spring Boot, Compose, Flyway (`auctions`, `bids` + constraints) e Dockerfile ARM64.
2. **Motor de lances:** `place_bid.lua` + `BidEngine` + `BidController` + `AuctionStateLoader` (reidratação) + pré-filtro.
3. **Persistência:** `BidStreamConsumer`.
4. **Encerramento + anti-sniping.**
5. **Testes:**
   - unitários do Lua: primeiro lance (`>= startingPrice`), incremento, empate, prazo, idempotência (replay, key com payload diferente, replay de rejeição);
   - **concorrência:** 10k lances paralelos com virtual threads (seqs únicas e contíguas, preços estritamente crescentes, nenhum duplicado, vencedor correto);
   - integração ponta a ponta: restart do Valkey + reidratação; `afterSeq` com o consumer parado.
6. **Docs mínimas:** README (desafio, decisões, como rodar local), diagrama AWS, `decisoes-tecnicas.md`.

**Corte 2 — tempo real e nuvem**
7. **Tempo real:** WebSocket + Pub/Sub + conflation.
8. **Terraform:** módulos, `envs/dev`, scripts de build/push e smoke test.

**Corte 3 — além do pedido**
9. **Observabilidade na app e na AWS:** logs JSON, métricas Micrometer, OTel agent, dashboard e alarmes.
10. **Carga:** k6 local (mede a vazão real do motor) e contra a AWS. Um notebook não gera 50k req/s e o `t4g.micro` não aguentaria. Os resultados medidos vão para o README e a extrapolação para o pico vai para o `finops.md`.
11. **Docs finais:** README completo (seções 5–8), `finops.md` e diagrama alvo com HA/DR.

## Verificação
- Local: `docker compose up --build` e `./mvnw verify` verdes, incluindo o teste de concorrência, a reidratação e o `afterSeq` com consumer parado.
- AWS: `terraform fmt -check && terraform validate && terraform plan` limpos; `terraform apply` → `scripts/smoke-test.sh <alb-url>` cria um leilão, envia lances, testa replay idempotente e lance após o fim; `wscat` recebe os snapshots com `seq` crescente.
- Observabilidade: logs com traceId no CloudWatch, métricas `Auction/*` (sem dimensões de alta cardinalidade) no dashboard, traces no X-Ray, alarme disparado ao forçar erro.
- Carga: `k6 run` sem 5xx; query no RDS confirma seqs únicas e preços crescentes; dashboard mostra p95 e lag; memória do Valkey estável (stream limitado).
- Custo: o Budget foi criado e as tags aparecem no Cost Explorer; `terraform destroy` remove tudo.

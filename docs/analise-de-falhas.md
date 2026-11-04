# Análise de falhas: o que acontece em cada ponto do fluxo

Para cada ponto do caminho de um lance, este documento diz o que pode falhar, como o sistema se comporta e como corrigir quando necessário. A coluna **Evidência** diz como cada comportamento foi verificado:
- **teste:** teste automatizado em `bid-service/tests`.
- **local:** falha provocada no Docker Compose, como parar um container no meio dos lances.
- **AWS:** falha provocada no ambiente real em 2026-11-03.
- **análise:** dedução a partir do código, sem experimento.

**Princípio que orienta tudo:** o único ponto que precisa estar de pé para um lance ser aceito é o **Valkey**. Os outros componentes saem do caminho crítico de propósito:
- **Postgres:** recebe os lances por write-behind.
- **Cognito:** os tokens são validados localmente.
- **Pub/Sub:** só alimenta a tela e o pré-filtro.

Quando algo falha, o sistema prefere **recusar rápido com 503 e `Retry-After`** a enfileirar ou responder 500. Um 503 diz ao cliente que o erro é temporário e que ele pode reenviar com a mesma `Idempotency-Key`, sem risco de duplicar o lance.

## O fluxo

```
cliente ─► ALB ─► task: load shedding → autenticação (JWT) → rate limit → pré-filtro → place_bid.lua (Valkey)
                                                                                     │
                       ┌──────────── PUBLISH (Pub/Sub) ◄────────────────────────────┤
                       ▼                                                             ▼ XADD (stream)
         tasks: cache + WebSocket (snapshots)                  consumer → Postgres (lote) → XACK
                                                    closer (ZSET) e reconciliador (Postgres → Valkey)
```

## 1. Cliente e ALB

| Falha | Comportamento | Evidência | Correção ou mitigação |
|---|---|---|---|
| Rede do cliente cai depois de enviar o lance, antes da resposta | O lance pode ter sido aceito. O cliente reenvia com a mesma `Idempotency-Key` e recebe a resposta original (`200`, `Idempotent-Replayed: true`), nunca um segundo lance | teste (`test_bid_rules`, `test_concurrency` com ~1.000 retries concorrentes) | Nenhuma: é o desenho. O cliente **precisa** reusar a key no retry |
| Uma AZ inteira cai | O ALB tira os alvos da AZ; a outra task segue. O auto scaling repõe a capacidade | análise (ALB multi-AZ, tasks em 2 AZs) | Valkey e RDS são single-AZ hoje: ver seção 5 e o item HA |
| ALB sobrecarregado ou alvo de DDoS | O ALB escala sozinho; a proteção L3/L4 é o Shield Standard (incluso). Não há proteção L7 | análise | WAF com regra de rate limit por IP (ver análise de segurança) |
| Conexão ociosa | O ALB fecha em 60 s; o ping de WebSocket a cada 10 s mantém a conexão viva. O keep-alive da app (75 s) é maior que o do ALB, então quem fecha a conexão ociosa é sempre o ALB | local (antes da correção, um 502 raro com 6 réplicas) | Corrigido (`timeout_keep_alive=75`) |

## 2. ALB → task

| Falha | Comportamento | Evidência | Correção ou mitigação |
|---|---|---|---|
| Task acima da capacidade | Acima de 64 requisições em andamento, responde `503 OVERLOADED` em microssegundos. Com o dobro da carga: 0 erros e p99 de 103 ms (350 ms sem o limite) | teste + local (`load-test/results`) | O auto scaling por CPU e requisições adiciona tasks; o cliente reenvia |
| Task morre de forma abrupta (crash, OOM, `SIGKILL`) | As requisições em andamento nela se perdem. Pelo nginx local, elas ficaram presas até o timeout. O ALB tira o alvo após 3 health checks falhos (~30 s) e o ECS sobe outra task. Os eventos lidos e não gravados por ela são reivindicados por outra task (`XCLAIM`, 30 s) | local (`docker kill` sob carga: 0 erros, mas o throughput caiu durante o timeout) + teste (`XCLAIM`) | O cliente reenvia com a mesma key: sem duplicata. O cliente WebSocket reconecta e recupera o que perdeu com `afterSeq` |
| Task para de forma graciosa (deploy, Spot, scale-in) | O ALB para de mandar requisições novas e drena por 30 s. O ECS manda `SIGTERM`; o uvicorn termina o que está em andamento (até 20 s) e fecha as conexões WebSocket, que reconectam em outra task | análise (`deregistration_delay=30`, `stopTimeout=30`) | Interrupção Spot: aviso de 2 min. A base de 2 tasks é on-demand para o pico não depender de Spot |
| Deploy com imagem quebrada ou sem conseguir subir | O circuit breaker do ECS desiste após falhas repetidas e **volta para a versão anterior**; as tasks antigas seguem atendendo | AWS (deploy feito com o RDS parado: 3 tasks novas falharam, rollback automático, `/readyz` 200 o tempo todo) | Nenhuma: funcionou como desenhado |
| **Postgres fora do ar** fazia todas as tasks ficarem "unhealthy" | **Antes:** o `/readyz` checava o Postgres, então uma queda do banco faria o ECS substituir **todas** as tasks em loop, derrubando também os lances | local + AWS | **Corrigido:** o `/readyz` checa só o Valkey. Com o RDS parado na AWS, as 2 tasks continuaram healthy e não foram substituídas |

## 3. Autenticação (JWT do Cognito)

| Falha | Comportamento | Evidência | Correção ou mitigação |
|---|---|---|---|
| Cognito indisponível | Cadastro e login falham com `503` (ou `429` quando o Cognito limita). **Lances com tokens já emitidos continuam funcionando**: a validação é local, com as chaves públicas em cache | teste (verificador com JWKS fora) + análise | Nenhuma: é o desenho (o Cognito fora do caminho do lance) |
| Task nova sobe com o Cognito fora (não consegue o JWKS) | Os lances respondem `503` ("não foi possível validar o token agora"), e não `401`, para o cliente não achar que o token dele é ruim. Cada requisição tenta buscar o JWKS de novo | teste | Corrigido nesta rodada (antes respondia `401`) |
| Token expira no meio do leilão (validade de 1 h) | `401 token expirado`; o cliente faz login de novo | teste | **Melhoria aberta:** expor `POST /auth/refresh` com o refresh token, para o app renovar sem pedir a senha |
| Rotação das chaves do Cognito | Um `kid` desconhecido faz buscar o JWKS de novo (no máximo 1 vez a cada 30 s) | teste | Nenhuma |
| Usuário bloqueado ou removido no Cognito | O token continua válido até expirar (validação local, sem consulta). O cache de tokens também guarda até o `exp` | análise | Aceitável com tokens de 1 h. Para corte imediato: validade menor ou uma lista de revogação no Valkey |

## 4. Rate limit e pré-filtro

| Falha | Comportamento | Evidência | Correção ou mitigação |
|---|---|---|---|
| Valkey fora | O rate limit por usuário (script no Valkey) falha e o lance responde `503` | local | Ver seção 5 |
| Cache do pré-filtro atrasado ou vazio (task nova, Pub/Sub caído) | Seguro: o preço só sobe, então o cache atrasado só **deixa passar** lances que o Lua rejeita, e nunca rejeita um válido. Cache vazio = tudo vai ao Lua (mais carga no Valkey, mesma correção) | teste (`test_bid_rules`) | Nenhuma |
| Valkey reidratado com preço menor (perda de lances) | O `AUCTION_LOADED` **substitui** o cache, para o pré-filtro não rejeitar com base num preço que deixou de existir | teste | Nenhuma |

## 5. Motor de lances (Valkey + `place_bid.lua`)

| Falha | Comportamento | Evidência | Correção ou mitigação |
|---|---|---|---|
| Valkey fora do ar | Lance responde `503 UNAVAILABLE` em ~16 ms (espera de 0,2 s por conexão no pool). `/readyz` vai a DOWN, e o ALB tira as tasks; com **todas** fora, o ALB passa a mandar para todas (fail-open), e elas respondem 503 | local | Hoje é ponto único: 1 nó. Correção: `redis_node_count = 2` (réplica + failover automático Multi-AZ) |
| Valkey reinicia **vazio** (sem persistência) | Primeiro lance recebe `NOT_LOADED` e o estado é reconstruído do Postgres: o `seq` continua de onde parou e lance abaixo do preço persistido é rejeitado. O reconciliador recarrega todos os leilões abertos em até 30 s | local (lance com `seq` 3 depois do restart; lance abaixo recusado) + teste (`FLUSHALL`) | Os lances aceitos e **ainda não persistidos** se perdem (janela = backlog do stream). Para zerar: MemoryDB ou `WAIT` (ver ADR-07) |
| Failover para uma réplica atrasada | O Valkey volta com `seq` menor que o do banco. O reconciliador avança o estado para o do Postgres; um lance que colidir antes disso vai para `bid_conflicts`, com alarme | teste | Nenhuma; o alarme de conflitos pede revisão manual daqueles lances |
| Valkey sem memória (`noeviction`) | O script é recusado no **primeiro comando de escrita**, antes de gravar qualquer coisa: sem efeito parcial. Resposta `503` | local + teste (antes: `500`) | Corrigido o status. Prevenção: alarme de memória (75%) e nó maior. Ver também a seção 7 (stream) |
| Rede cai **depois** de o script executar (resposta perdida) | O cliente recebe 503 ou timeout, mas o lance pode ter sido aceito. O retry com a mesma key devolve a resposta original | teste (idempotência) | Nenhuma: é o motivo de a key ser obrigatória |
| Valkey perde o cache de scripts (restart, failover) | `EVALSHA` responde `NOSCRIPT`; o script é recarregado e o lance segue | teste | Nenhuma |
| Um leilão quente satura a thread do Valkey | Latência do Lua sobe; a task devolve 503 pelo load shedding antes de acumular fila. Teto medido: ~49,5 mil scripts/s, e o pré-filtro tira 97% antes | local | Nó maior (vertical). Acima disso, mudança de modelo (lances em rodadas) |

## 6. Tempo real (Pub/Sub e WebSocket)

| Falha | Comportamento | Evidência | Correção ou mitigação |
|---|---|---|---|
| Conexão do Pub/Sub cai | A corrotina reconecta em 1 s. Mensagens publicadas nesse intervalo se perdem (Pub/Sub é at-most-once): a tela fica atrasada até o próximo evento, e o pré-filtro fica conservador (seguro) | local (logs de reconexão ao parar o Valkey) | Nenhuma para o preço (o próximo snapshot corrige). Histórico completo: `GET /bids?afterSeq=` |
| Cliente WebSocket lento | Fila de 32 mensagens por conexão; se enche, a conexão é fechada (1013) em vez de atrasar os outros. O cliente reconecta e recupera com `afterSeq` | análise (código em `realtime.py`) | Nenhuma |
| Conexão morta sem FIN (celular sem sinal) | O ping a cada 10 s sem resposta em 10 s fecha a conexão e libera os recursos | análise (uvicorn `ws_ping_*`) | Nenhuma |
| Task do cliente WebSocket morre | O cliente reconecta (cai em outra task) e recebe o snapshot atual na conexão | teste (snapshot inicial) | O cliente deve reconectar com backoff |

## 7. Persistência (stream → consumer → Postgres)

| Falha | Comportamento | Evidência | Correção ou mitigação |
|---|---|---|---|
| **Postgres fora do ar** | **Lances continuam sendo aceitos** (201) e acumulam no stream. Leituras e criação de leilão respondem `503`. `/readyz` segue UP. Login e cadastro funcionam (Cognito) | local + AWS (RDS parado com as tasks rodando) | Alarmes de backlog e do RDS. HA: `db_multi_az = true` |
| Postgres volta | O consumer relê **os próprios pendentes** na volta seguinte e grava: o banco alcançou em **1 s** (antes da correção, ~40 s, esperando a reivindicação de 30 s) | local (medido antes e depois) + teste | Corrigido |
| Postgres inalcançável sem resposta (instância parada, rede) | **Antes:** a conexão nova ficava pendurada até 60 s e o ALB respondia `504`. **Agora:** timeout de conexão de 3 s → `503` | AWS (504 medido com o RDS parado) | Corrigido: medido na AWS, `503` em 3,3 s (sequência no fim do documento) |
| **Task não sobe sem o Postgres** | O startup cria o pool e aplica as migrations: sem banco, a task nova não fica pronta. Com o circuit breaker, um deploy durante a queda é revertido e as tasks antigas seguem | AWS (rollback observado) | **Aberto, aceitável:** escalar durante uma queda do banco não funciona. Melhoria: subir sem o banco (migrations num job separado e o pool criado sob demanda) |
| Consumer morre com lote lido e não confirmado | Outro consumer reivindica os pendentes com `XCLAIM` após 30 s ocioso; a gravação é idempotente (`ON CONFLICT DO NOTHING` + comparação) | teste | Nenhuma |
| Mesmo evento entregue duas vezes | Ignorado (linha idêntica já existe) | teste | Nenhuma |
| Lance aceito que colide com outro já gravado | Vai para `bid_conflicts` com métrica e alarme no primeiro caso; o Valkey é alinhado ao banco | teste | Revisão manual do conflito (é sinal de failover ou key reutilizada) |
| Aviso do Pub/Sub ao consumer se perde | A varredura completa a cada 5 s grava o que faltou | teste | Nenhuma |
| **Queda longa do Postgres com muito volume** | O stream tem `MAXLEN ~100000`. Se mais de ~100 mil lances forem aceitos sem persistir, os mais antigos são **cortados do stream antes de chegar ao banco**: perda silenciosa | análise | **Aberto, prioridade alta para produção:** trocar o `MAXLEN` no Lua por `XTRIM MINID` feito pelo consumer só até o último evento confirmado. Assim o stream só é cortado depois de persistido, e o limite passa a ser a memória (alarme em 75%, `noeviction` recusa com 503 em vez de perder). O alarme de backlog (10 mil) já avisa bem antes do corte |
| Postgres lento | O backlog cresce; o `GET /bids?afterSeq=` junta banco e stream e não perde lances | teste | Alarme de backlog |

## 8. Encerramento e reconciliação

| Falha | Comportamento | Evidência | Correção ou mitigação |
|---|---|---|---|
| Closer atrasado ou parado | **Não afeta a regra:** o prazo é conferido pelo próprio `place_bid.lua` com o relógio do Valkey, então lance após o fim é rejeitado mesmo sem o closer. Só o evento `AUCTION_CLOSED` e a gravação do vencedor atrasam | teste (`test_bid_after_deadline_is_rejected_even_before_the_closer_runs`) | Nenhuma |
| Várias tasks encerram ao mesmo tempo | `close_auction.lua` é idempotente: só uma tem efeito | teste | Nenhuma |
| Entrada corrompida ou órfã no ZSET | Removida com log; os outros leilões seguem | teste | Nenhuma |
| Leilão criado no banco e não no Valkey (falha entre os dois passos) | O reconciliador o carrega em até 30 s; o primeiro lance também | teste | Nenhuma |
| Relógio de uma task errado | Irrelevante: só o `TIME` do Valkey decide prazos | análise | Nenhuma |

## 9. Observabilidade e operação

| Falha | Comportamento | Evidência | Correção ou mitigação |
|---|---|---|---|
| Sidecar ADOT cai | `essential = false`: a app segue; só os traces se perdem | análise | Nenhuma |
| CloudWatch Logs indisponível | O driver `awslogs` faz buffer; logs podem se perder, mas a app não para | análise | Nenhuma |
| Métricas param de chegar | Os alarmes ficam em `INSUFFICIENT_DATA` (não disparam) | análise | Adicionar `treat_missing_data = breaching` nos alarmes críticos (backlog, 5xx) |
| E-mail dos alarmes não confirmado | O SNS não entrega nada até a assinatura ser confirmada | AWS (`PendingConfirmation`) | Confirmar o e-mail da AWS |
| RDS parado por mais de 7 dias | A AWS religa sozinha (cobrança volta) | AWS (regra do serviço) | `aws-pause.sh` de novo ou `terraform destroy` |

## 10. Desastre

| Falha | Comportamento | Correção ou mitigação |
|---|---|---|
| Perda do RDS | Backups automáticos + PITR; o Valkey é reidratado a partir do banco restaurado. Leilões que não existem no banco restaurado são removidos do ZSET (testado) | RPO de minutos (PITR). Multi-AZ para failover |
| Perda da região | Fora do ar | Pilot light em outra região (README, seção 9): banco promovido, `terraform apply` com `region`, Route 53 |

## Itens abertos, por prioridade

| # | Risco | Impacto | Correção proposta |
|---|---|---|---|
| 1 | Valkey e RDS single-AZ | Queda do nó = sem lances (Valkey) ou sem leitura (RDS) | `redis_node_count = 2`, `db_multi_az = true` (já parametrizado) |
| 2 | Corte do stream (`MAXLEN`) numa queda longa do banco | Perda silenciosa de lances aceitos | `XTRIM MINID` pelo consumer após o ACK |
| 3 | Janela de perda num restart do Valkey | Lances aceitos e não persistidos | MemoryDB for Valkey (log Multi-AZ) ou `WAIT 1` após o `EVALSHA` |
| 4 | Task não sobe sem o Postgres | Não escala durante queda do banco | Migrations em job separado; pool sob demanda |
| 5 | Sem refresh token na API | Cliente precisa da senha a cada hora | `POST /auth/refresh` |
| 6 | Alarmes sem dados não disparam | Silêncio quando a coleta para | `treat_missing_data = breaching` |

## Sequência executada na AWS (2026-11-03/04)

| Passo | Resultado |
|---|---|
| RDS parado com 2 tasks rodando | `/readyz` 200, 2 alvos healthy, nenhuma task substituída; login via Cognito funcionando; criar leilão → `504` após 60 s (conexão nova pendurada) |
| Deploy da correção (timeout de 3 s) **com o RDS parado** | As tasks novas não sobem sem o banco: 3 falhas, circuit breaker reverteu para a versão anterior, `/readyz` 200 o tempo todo |
| RDS religado, deploy concluído, smoke test | 15/15 verificações (com Cognito) |
| RDS parado de novo, versão corrigida rodando | Leitura sem banco → **`503` em 3,3 s** (antes: `504` em 60 s); `/readyz` 200 |
| Ambiente pausado (`aws-pause.sh`) | 0 tasks, auto scaling 0/0, RDS `stopped` |

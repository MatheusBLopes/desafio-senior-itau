# Decisões técnicas (ADRs curtos)

Cada decisão segue o formato **contexto → decisão → consequências**. Os números citados foram medidos localmente (ver README, seção "Teste de carga").

---

## ADR-01 — Serializar cada leilão num script Lua no Valkey

**Contexto.** Centenas de milhares de lances simultâneos disputam o mesmo recurso (o preço de um leilão). Lock distribuído (Redlock, `SELECT ... FOR UPDATE`) serializa por contenção, e a latência cresce com a fila. Lock otimista (versão + retry) degenera justamente no pico, quando quase todo retry falha.

**Decisão.** Todo lance passa por `place_bid.lua`. O Valkey executa um script por vez, então a validação (idempotência, prazo, valor) e a escrita (`HINCRBY seq`, preço, líder, `XADD`, `PUBLISH`) formam uma única operação atômica, sem lock.

**Consequências.**
- Ordem determinística sem custo extra: o `seq` é a ordem de serialização.
- O teto de um único leilão é a thread do Valkey: medimos ~50 mil `place_bid` por segundo num Ryzen 5600X. Isso motivou o pré-filtro (ADR-05).
- Todas as chaves de um leilão usam o hash tag `{auctionId}`, então o script funciona em cluster mode sem mudanças.

## ADR-02 — O relógio é o `TIME` do Valkey

**Contexto.** Prazo e anti-sniping dependem de "agora". Relógios de clientes são manipuláveis, e os de tasks diferentes divergem.

**Decisão.** Só o `TIME` do servidor Valkey decide prazos: dentro do Lua e, fora dele, via `RedisClock` (criação de leilão, scheduler).

**Consequências.** Não existe "lance aceito por uma task e rejeitado por outra" por causa de relógio. A "ordem de chegada" passa a ter uma definição precisa: a ordem em que os lances são serializados no Valkey. Nenhum sistema garante uma ordem justa do ponto de vista do cliente, porque o jitter de rede entre o usuário e a AWS é maior que a diferença entre lances concorrentes.

## ADR-03 — Idempotência por (leilão, usuário, key), incluindo rejeições

**Contexto.** Clientes reenviam requisições (timeout, rede móvel). Um retry não pode virar um lance duplicado.

**Decisão.** O header `Idempotency-Key` é obrigatório. O resultado fica em `idem:{auctionId}:<userId>:<key>`, junto com a impressão digital do payload (o valor).
- Mesma key e mesmo valor: devolve a resposta original, inclusive se ela foi uma rejeição.
- Mesma key com outro valor: `422 IDEMPOTENCY_KEY_REUSED`.
- **Exceção:** a rejeição decidida pelo pré-filtro (lance já superado, ADR-05) não é memorizada, então não trava a key. Um retry com o mesmo valor recebe a mesma rejeição, porque o preço só sobe; a mesma key com outro valor é avaliada como lance novo. Isso nunca gera duplicata: só um lance avaliado pelo Lua trava a key.
- O `userId` não pode conter `:`, para a chave não ficar ambígua.
- **TTL:** a key de uma rejeição vive 24 h. A de um aceite vive até o fim do leilão + 24 h, porque um leilão pode durar até 30 dias e a key não pode expirar com ele aberto: o mesmo lance seria aceito de novo.

**Consequências.** Um replay sempre devolve a mesma resposta. O Postgres tem `UNIQUE (auction_id, user_id, idempotency_key)` como segunda barreira; um lance aceito que colide nela não é descartado (ADR-04).

## ADR-04 — Persistência write-behind via Redis Streams

**Contexto.** Gravar no Postgres dentro do caminho do lance limitaria a vazão ao banco e colocaria a latência dele em cada lance.

**Decisão.** O Lua faz `XADD` no stream `bids:{id}` com ID `0-<seq>`. Um consumer group (um consumer por task) lê em lotes, grava numa transação (`INSERT ... ON CONFLICT DO NOTHING` e uma projeção que só avança) e só então faz `XACK`. Pendentes de uma task morta são reivindicados com `XPENDING` + `XCLAIM`. A falha de um leilão (lote recusado, entrada corrompida no Valkey) não para a persistência dos outros.

**Consequências.**
- **Quando ler:** o consumer lê os streams que o Pub/Sub avisou que receberam eventos, na hora, e faz uma varredura completa a cada 5 s para os avisos perdidos (ADR-13).
- **Reentrega x colisão:** um insert que não grava é comparado com a linha existente. Igual, é reentrega e segue. Diferente, é um lance aceito que colidiu com outro (key reutilizada depois de expirar, Valkey que voltou atrás num failover): vai para `bid_conflicts`, incrementa `auction.persist.conflicts` (alarme no primeiro caso) e o estado do Valkey é comparado com o banco (ADR-07). Nenhum lance aceito some em silêncio.
- O ID `0-<seq>` permite `XRANGE` direto por seq. O `GET /bids?afterSeq=` usa só o banco quando ele devolve uma faixa contígua e completa; senão, une banco e stream. Com várias tasks gravando lotes em paralelo, o banco pode ter buracos temporários (4..6 gravados antes de 1..3), e o stream ainda tem tudo o que não foi persistido.
- O stream é limitado com `MAXLEN ~ 100000`; o backlog vira métrica (`auction.stream.backlog`) e alarme.
- **Janela de perda:** lances aceitos e ainda não persistidos se perdem se o Valkey cair sem réplica. Ver ADR-07.

## ADR-05 — Pré-filtro local para o leilão quente

**Contexto.** Sharding (cluster mode) escala *muitos* leilões, não *um*: todos os lances de um leilão caem no mesmo shard e na mesma thread. No pico, a maioria dos lances já nasce superada.

**Decisão.** Cada task mantém o último estado de cada leilão, alimentado pelo Pub/Sub. Lance abaixo do mínimo conhecido é rejeitado na task, sem ir ao `place_bid.lua`. Como o preço só sobe, isso nunca rejeita um lance que o Lua aceitaria. Exceção: se a idempotency key já existe (um `EXISTS` de O(1)), é retry e vai ao Lua para devolver a resposta original. A rejeição local não é memorizada (ver a exceção na ADR-03).

**Consequências.**
- Nos testes de carga locais (~950 mil lances em 5 rodadas de k6), só ~7 mil chegaram ao Lua: mais de 99% foram resolvidos na task. O cache nunca decide aceite.
- **Memorizar ou não, medido:** testamos memorizar também a rejeição local, para que a mesma key com outro valor sempre recebesse `422`. A 10 mil lances/s, o p95 subiu de ~40 ms para ~65 ms, e o Valkey ganhou ~135 MB a cada 300 mil lances (uma key de ~450 B por lance superado, por 24 h). No pico (50 mil/s × 30 s) seriam ~700 MB, acima dos 0,5 GB do `cache.t4g.micro`, onde `noeviction` faria o Valkey recusar escritas. Como a rejeição local nunca gera duplicata, a decisão foi não memorizar e documentar o contrato.

## ADR-06 — Tempo real com WebSocket nas tasks, Pub/Sub e conflation

**Contexto.** São 200 mil clientes e milhares de lances por segundo. Mandar cada lance a cada cliente daria bilhões de mensagens por minuto.

**Decisão.**
- **Transporte:** WebSocket puro com mensagens JSON em `/ws/auctions/{id}`, servido pelas próprias tasks. Sem SockJS nem sticky session: o ALB balanceia normalmente.
- **Por que não STOMP:** o cliente só escuta. Com STOMP, o broker aceitaria um `SEND` do cliente e o repassaria a todos (um `AUCTION_CLOSED` falso), e assinaturas com curinga (`/topic/**`) receberiam todos os leilões; na versão Java isso exigia um interceptor. Aqui o leilão vem no caminho da URL (validado como UUID; outro caminho recebe 403 no handshake) e qualquer mensagem do cliente fecha a conexão com o código 1008. O vetor deixa de existir por desenho.
- **Distribuição:** o Lua publica cada evento no canal `auction-events`. Cada task assina o canal e envia aos seus clientes, então o tempo real escala horizontalmente com as tasks.
- **Conflation:** cada task envia um `AUCTION_SNAPSHOT` por leilão a cada 150 ms, só se algo mudou. `AUCTION_EXTENDED` e `AUCTION_CLOSED` saem na hora. Ao conectar, o cliente já recebe o snapshot atual.
- **Recuperação:** o cliente que quer o histórico completo usa `GET /bids?afterSeq=`.
- **Uma corrotina de entrega, em ordem:** uma única corrotina lê o Pub/Sub e aplica os eventos na ordem de publicação, então um `AUCTION_LOADED` nunca é aplicado depois de um lance publicado após ele (testado).
- **Cliente lento não atrasa os outros:** cada conexão tem uma fila própria de 32 mensagens; se ela enche, a conexão é fechada (1013) e o cliente se recupera com `afterSeq`. A mensagem é serializada uma vez por evento, não uma vez por cliente.
- **Números como string no JSON do Lua:** o `cjson` serializa números com `%.14g`, e um valor de 15 dígitos em centavos (o próximo mínimo perto do teto de 999.999.999.999,99) seria arredondado.

**Consequências.**
- O ALB (custo fixo + LCU) substitui o API Gateway WebSocket, que cobraria por mensagem (ver `finops.md`).
- Ping de WebSocket a cada 10 s (uvicorn), abaixo do idle timeout de 60 s do ALB; conexões mortas caem em 20 s.

## ADR-07 — Valkey sem persistência + reidratação a partir do Postgres

**Contexto.** O ElastiCache não oferece AOF nas versões atuais, e o MVP roda com um nó só. Um restart apagaria preço, líder e `seq`. Se o `seq` voltasse a 1, colidiria com `UNIQUE (auction_id, seq)`.

**Decisão.** O `AuctionStateLoader` monta o estado a partir do Postgres (`max(seq)` da tabela de lances, maior lance, prazo vigente). O `hydrate_auction.lua` só grava se o hash não existe, então nunca sobrescreve estado vivo. A exceção é um hash com `seq` **menor** que o do banco: o Valkey perdeu lances já persistidos (failover para uma réplica atrasada), e aceitar lances assim reusaria seqs já gravados. Nesse caso o seq, o preço e o líder avançam para os do banco. A reidratação acontece em quatro momentos:
- quando o Lua responde `NOT_LOADED`;
- no reconciliador, que a cada 30 s garante que todo leilão `OPEN` está carregado;
- no encerramento;
- quando o consumer detecta uma colisão no Postgres (ADR-04).

O Lua anuncia `AUCTION_LOADED`, e as tasks substituem o cache local, já que o preço pode ter voltado.

**Consequências.** Testado com `FLUSHALL` e com um hash rebaixado (simulando a réplica atrasada): o seq continua de onde parou e lances abaixo do preço persistido continuam rejeitados. A perda fica restrita aos lances aceitos e não persistidos. Para zerar essa janela:
- **Réplica Multi-AZ:** reduz a janela, porque o failover só perde os ms não replicados.
- **`WAIT 1 <timeout>` pela aplicação após o `EVALSHA`:** o `WAIT` não pode ser chamado dentro de script; custa latência.
- **MemoryDB for Valkey:** log transacional Multi-AZ, nenhum lance confirmado perdido; é a opção para produção se o negócio exigir.

## ADR-08 — Encerramento idempotente com ZSET + anti-sniping

**Decisão.** O ZSET global `auctions:ending` guarda o prazo conhecido. Todas as tasks rodam o scheduler a cada 200 ms e chamam `close_auction.lua`:
- se o anti-sniping estendeu o prazo, o script responde `PENDING` com o prazo novo e o score do ZSET é atualizado;
- se encerrou, emite `CLOSED` no mesmo stream dos lances (com ID logo após o último seq), então o consumer sempre o persiste depois do último lance.

O ZSET fica fora do script porque é uma chave global, de outro slot no cluster.

**Consequências.** Várias tasks encerrando ao mesmo tempo não geram efeito duplicado. O lance após o prazo já é rejeitado pelo Lua, mesmo antes de o scheduler rodar. Cada leilão é tratado isoladamente: uma entrada órfã no ZSET (por exemplo, um leilão que não existe num Postgres restaurado de backup) é removida com log, em vez de abortar o ciclo e travar o encerramento de todos os outros.

## ADR-09 — Rede sem NAT Gateway

**Decisão.** As tasks ficam em subnets públicas com IP público, e o SG aceita só o ALB. Valkey e Postgres ficam em subnets privadas, sem rota para a internet.

**Consequências.** Economiza ~US$ 33/mês do NAT, ou ~US$ 73/mês de VPC endpoints. O custo é ~US$ 3,65/mês por IPv4 público, mais a exposição de um IP público na task (mitigada pelo SG). Em produção num banco, o padrão seria NAT ou endpoints + WAF; está documentado como evolução.

Duas consequências de segurança dependem desse SG:
- **IP do cliente:** só o ALB alcança a task, então o `X-Forwarded-For` traz o IP real que o ALB anexou. A app só confia no cabeçalho quando a conexão vem da rede privada, e lê da direita para a esquerda o primeiro endereço que não é proxy: os da esquerda podem ter sido escritos pelo cliente (`client_ip`, testado). É ele que alimenta o limite por IP.
- **Métricas fora do ALB:** `/metrics` (Prometheus) e o health detalhado ficam na porta 8081, que o ALB não encaminha; a porta pública expõe só a API, o WebSocket, `/readyz` e `/livez`. Um único processo escuta nas duas portas e um middleware ASGI decide o que cada uma atende.

## ADR-10 — Dinheiro em centavos (inteiros)

**Decisão.** A API recebe decimal com no máximo 2 casas (mais que isso devolve 400), como número ou string, e devolve sempre string (`"150.00"`), nunca float. O JSON é lido com `parse_float=Decimal`, então o valor não passa por float em momento nenhum. Internamente, Lua e Postgres trabalham em centavos inteiros (`bigint`).

**Consequências.** Sem erro de ponto flutuante. O Lua (double de 53 bits) é exato até ~90 trilhões de reais. O `cjson` não é: serializa com 14 dígitos significativos, então os scripts mandam os números como string nos JSONs (testado no teto da API).

## ADR-11 — Stack: Python 3.13 + asyncio, um processo por task

**Decisão.**
- **FastAPI + uvicorn (uvloop, httptools):** servidor assíncrono; uma task atende milhares de conexões HTTP e WebSocket num event loop, sem uma thread por requisição.
- **redis-py asyncio (hiredis) e asyncpg:** clientes assíncronos do Valkey e do Postgres. SQL puro, sem ORM, porque as escritas são lotes simples; o lote de lances é um único `INSERT ... SELECT FROM unnest(...)`, cujo `RETURNING` diz quais linhas entraram.
- **Migrations em SQL versionado** (`V1__`, `V2__`), aplicadas no startup sob um advisory lock do Postgres, para que várias tasks subindo juntas não as apliquem duas vezes.
- **Um processo por task:** por causa do GIL, um processo Python usa no máximo um núcleo. Em vez de vários workers na mesma task (que dividiriam porta, métricas e consumer), a escala é horizontal: mais tasks no ECS. Todo o estado de uma task (cache do pré-filtro, conexões WebSocket, consumer) já foi desenhado para N tasks.
- **Caminho quente guiado por profiler (py-spy):** o lance lê headers e corpo direto (sem o resolvedor de dependências do FastAPI, que custava ~20% da CPU) e, quando o pré-filtro vai rejeitar, envia o rate limit e o `EXISTS` da key num único pipeline com `EVALSHA` puro (passar o script ao pipeline faria o redis-py mandar um `SCRIPT EXISTS` extra a cada lance). Resultado: de ~0,55 para ~0,35 ms de CPU por lance.
- **pytest + Testcontainers:** os testes sobem Valkey e Postgres reais e a aplicação inteira num servidor uvicorn em portas aleatórias.

**Consequências.** Por requisição, Python gasta mais CPU que a JVM; isso é pago com tasks a mais, não com outra arquitetura, porque o ponto de serialização é o Valkey e não a aplicação. A versão Java original está em `bid-service-java/`, como referência.

## ADR-12 — Degradação controlada: load shedding e falha rápida

**Contexto.** No primeiro teste acima da capacidade, uma task aceitava tudo: a fila crescia, a latência de todos explodia (p99 de segundos), o k6 abria mais conexões e o throughput **caía**. Era colapso, não degradação.

**Decisão.**
- **`LoadShedder`** (middleware ASGI): no máximo 64 requisições HTTP em andamento por task; acima disso, `503` com `Retry-After: 1` em microssegundos. O limite vem da lei de Little: ~1.200 lances/s × 50 ms de fila tolerada ≈ 60. Conta requisições, não conexões (o `limit_concurrency` do uvicorn contaria as keep-alive ociosas do ALB). `/livez`, `/readyz` e WebSocket ficam de fora: uma task cheia não deve sair do target group por isso, senão a carga cascatearia para as outras.
- **Pool do Valkey com espera de 0,2 s:** pool esgotado ou Valkey fora viram `503 UNAVAILABLE`, não uma requisição presa por segundos nem um `500`.
- **Keep-alive da app (75 s) maior que o idle timeout do ALB (60 s):** com o padrão do uvicorn (5 s), a app fechava conexões que o proxy ia reutilizar e a requisição voltava como 502 (achado com 6 réplicas: um erro em ~150 mil requisições).

**Consequências.** Com o dobro da capacidade, a task entrega o que consegue, devolve o excesso como 503 e não registra nenhum erro (medido). O k6 conta o 503 à parte (`bids_shed`). O autoscaling e o ALB fazem o resto.

## ADR-13 — Dimensionamento da task pelo número medido

**Contexto.** O primeiro teste de carga rodava k6, réplicas, nginx e Valkey nos mesmos núcleos, e o número medido era o da máquina, não o da aplicação.

**Decisão.** `load-test/capacity.sh` mede com método:
- **CPU isolada:** sistema nos núcleos físicos 0–3 e k6 nos 4–5 (respeitando os pares SMT do `lscpu -e`).
- **Task do tamanho do Fargate:** cada réplica limitada por cota de CPU (`APP_CPUS`).
- **Por taxa:** 5 s de aquecimento descartados, depois 30 s medidos com amostras de `docker stats`.
- **SLO:** p99 < 50 ms, zero erros e zero descartes.

Medidos, com SLO atendido: **~1.000 lances/s numa task de 0,5 vCPU e ~2.100 numa de 1 vCPU** (detalhes no README). A de 0,5 vCPU sofre throttling da cota do cgroup (50 ms a cada 100 ms) em qualquer rajada. Com a de 1 vCPU, o custo por lance é o mesmo ou menor, o p99 é melhor e são metade das tasks. A task passou a **1 vCPU / 2 GB** (2 GB é o mínimo do Fargate para 1 vCPU; o processo usa ~80 MB), e a escala por requisições usa 90 mil req/min por task (~70% da capacidade).

**Consequências.** 10 mil lances/s cabem em ~7 tasks, dentro do máximo de 10. Ryzen x86 não é Graviton ARM: o número é uma estimativa, a confirmar com k6 distribuído contra o ALB.

**Dois achados do método:**
- O healthcheck do Compose com `python -c` custava ~190 ms de CPU por execução dentro do cgroup da task e, com 0,5 vCPU, travava o processo a cada check (p99 de 64 ms a só 400 lances/s). Trocado por `bash /dev/tcp`; o p99 caiu para 1 ms. No Fargate não há check dentro do container: o ALB checa `/readyz` pela rede.
- O consumer do write-behind varria um stream por leilão aberto a cada 20 ms, mesmo sem lances, um custo que cresce com leilões × tasks. Agora ele lê só os streams que o Pub/Sub avisou que receberam eventos, mais uma varredura de segurança a cada 5 s para avisos perdidos (o Pub/Sub é at-most-once). Testado nos dois caminhos.

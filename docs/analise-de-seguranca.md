# Análise de vulnerabilidades de segurança

Este documento cobre o código, a infraestrutura (Terraform) e a operação do ambiente implantado em 2026-11-03. Para cada item, ele diz o risco, onde está, como explorar ou o que acontece, e a correção.

A severidade considera um **leilão real com dinheiro**, e não só a demo:
- **Crítica:** exploração simples e impacto direto (fraude, roubo de conta).
- **Alta:** abuso provável com impacto no negócio.
- **Média:** exige mais condições, ou o impacto é limitado.
- **Baixa:** endurecimento (*hardening*) ou defesa em profundidade.

## Resumo

| # | Vulnerabilidade | Severidade | Status |
|---|---|---|---|
| C1 | Identidade declarada pelo cliente (`X-User-Id`) | Crítica | **Corrigido**: JWT do Cognito |
| A1 | API em HTTP, sem TLS | Crítica em produção | Aberto: correção pronta para ligar |
| A2 | Cadastro aberto, sem verificação nem CAPTCHA | Alta | Aberto |
| A3 | Sem autorização por papel: qualquer usuário cria leilões | Alta | Aberto |
| A4 | Nomes de usuário dos licitantes expostos publicamente | Média (LGPD) | Aberto |
| A5 | WebSocket sem autenticação e sem limite de conexões | Média | Aberto |
| A6 | Rate limit por IP local a cada task, sem WAF | Média | Aberto |
| A7 | Política de senha fraca, sem MFA | Média | Aberto |
| A8 | Credenciais de longa duração de um usuário IAM no deploy | Média | Aberto (operação) |
| A9 | Tasks com IP público e saída livre para a internet | Média (ambiente regulado) | Aberto, decisão de custo (ADR-09) |
| A10 | Senha do banco no state do Terraform | Média | Aberto |
| A11 | Tokens não revogáveis antes do vencimento | Baixa | Aberto |
| A12 | Valkey sem AUTH/RBAC | Baixa | Aberto |
| A13 | TLS do Postgres sem verificação de certificado | Baixa | Aberto |
| A14 | Container sem filesystem somente leitura | Baixa | Aberto |
| A15 | Sem trilha de auditoria de autenticação | Baixa | Aberto |
| F1–F9 | Itens corrigidos nesta rodada e nas anteriores | — | Ver "Já corrigido" |

## Corrigido

**C1. Identidade declarada pelo cliente (era crítica).** Até esta rodada, o usuário do lance vinha do header `X-User-Id`: qualquer pessoa podia dar lances em nome de outra.
- **Agora**, na AWS (`AUTH_MODE=cognito`), o usuário vem do access token do Cognito, validado na task (`auth.py`):
  - assinatura RS256 com as chaves do JWKS do pool;
  - `iss` do pool, `client_id` do app client e `token_use=access` (o id token é recusado);
  - `exp` com tolerância de 5 s.
- **O header `X-User-Id` é ignorado**, e o teste prova que um lance com token de A e `X-User-Id: B` sai como A.
- **Ataques testados e recusados:** `alg=none`; confusão de algoritmo (HS256 "assinado" com a chave pública); payload adulterado; chave de outro emissor com o mesmo `kid`; token de outro app client; id token no lugar do access token.

**Também corrigido nesta rodada:**

| # | O quê | Como |
|---|---|---|
| F1 | **Default inseguro:** sem `AUTH_MODE` configurado, a aplicação subia aceitando `X-User-Id` | Default `cognito` (fail-closed): sem o user pool, a aplicação não sobe. O modo `header` precisa ser explícito (só no Compose local) |
| F2 | **Formato do corpo antes da autenticação:** `POST /auctions` sem token respondia 400 listando os campos esperados | Autentica antes de validar: 401 (achado na AWS) |
| F3 | **Mensagens de 401 detalhadas** ("Token is missing the client_id claim") ajudavam a montar um token forjado | Mensagem genérica; o detalhe vai para o log |
| F4 | **Corpo sem limite:** um corpo de MBs era lido inteiro na memória | `413` acima de 16 KiB; `411` para chunked sem `Content-Length` |
| F5 | **Cadeia de suprimentos:** o build instalava as dependências sem conferir hashes | `--require-hashes` com os hashes do `uv.lock` |
| F6 | **Container como root** | Usuário sem privilégios (UID 10001) |
| F7 | **Enumeração de usuários no login** | Mesma resposta para senha errada e usuário inexistente (verificado na AWS); `prevent_user_existence_errors` no app client |
| F8 | **Cognito fora interpretado como token inválido** | `503` em vez de `401` |
| F9 | **Permissão IAM da task** para confirmar cadastro | Só `cognito-idp:AdminConfirmSignUp`, só neste user pool |

**Já corrigido nas rodadas anteriores:**
- **WebSocket:** cliente não publica eventos (o vetor do `AUCTION_CLOSED` forjado foi removido por desenho).
- **Porta interna:** métricas e health detalhado ficam numa porta que o ALB não encaminha.
- **IP do cliente:** o `X-Forwarded-For` só é aceito vindo de proxy privado, lido da direita para a esquerda.
- **ECR:** tags imutáveis e scan no push.
- **Dados:** RDS e ElastiCache criptografados em repouso e em trânsito, em subnets privadas, com SG só da task.
- **Segredo do banco:** a senha fica no SSM SecureString, injetada pelo ECS.

**Verificados e sem problema:**
- **SQL:** todo parametrizado.
- **Lua:** argumentos só via `ARGV`.
- **Chaves do Valkey:** compostas só com valores validados por regex; o `userId` não aceita `:`.
- **Idempotency key:** tem escopo por usuário, então um usuário não reaproveita a de outro.
- **Dinheiro:** não passa por float.
- **CORS:** não habilitado.
- **WebSocket:** aceita qualquer origem, mas não usa cookie, então não há sessão para sequestrar (CSWSH).

## Abertos

### A1. API em HTTP, sem TLS (crítica em produção)
- **Onde:** `modules/alb`. Hoje só existe o listener 80, porque não há certificado.
- **Risco:** senhas (`/auth/login`) e access tokens trafegam em claro. Quem estiver na mesma rede (Wi-Fi público, proxy) lê e reusa o token por até 1 h.
- **Correção:**
  - **Com domínio:** certificado no ACM e `certificate_arn` no tfvars. O Terraform já cria o listener 443 e redireciona o 80.
  - **Sem domínio:** CloudFront na frente do ALB (HTTPS com `*.cloudfront.net`, WebSocket suportado) e o SG do ALB aceitando só o prefix list do CloudFront.
  - **Em qualquer caso:** HSTS.

### A2. Cadastro aberto, sem verificação nem CAPTCHA (alta)
- **Onde:** `POST /auth/signup` confirma o usuário na hora, por desenho (pedido: sem e-mail).
- **Risco:**
  - Criação de contas em massa (*account farming*), contornando o rate limit **por usuário**: cada conta nova tem seu próprio balde. Isso amplia a capacidade de inundar um leilão.
  - *Shill bidding* com contas falsas para inflar o preço.
  - Custo: o tier Lite do Cognito é grátis até 10 mil usuários ativos por mês; acima disso, cobra por usuário.
- **Mitigação hoje:** limite por IP (200 de rajada, 50/s, por task) e Budget.
- **Correção:**
  - AWS WAF com regra *rate-based* em `/auth/signup` e CAPTCHA (WAF CAPTCHA).
  - Verificação de e-mail ou telefone em produção.
  - Cognito tier Plus (detecção de risco).
  - Vínculo de conta a um meio de pagamento antes de dar lance.

### A3. Sem autorização por papel (alta)
- **Onde:** `POST /auctions` exige apenas um usuário autenticado.
- **Risco:** qualquer conta cria leilões, inclusive em massa, poluindo a vitrine e consumindo memória do Valkey.
- **Correção:**
  - Grupo `admin` (ou `seller`) no Cognito.
  - Checar `cognito:groups` no access token antes de criar.
  - Limite de leilões abertos por usuário.

### A4. Nomes de usuário dos licitantes expostos (média, LGPD)
- **Onde:** `GET /auctions/{id}` (`leaderId`, `winnerId`), `GET /auctions/{id}/bids` (`userId` de cada lance) e o WebSocket são públicos.
- **Risco:**
  - O histórico completo de lances de cada pessoa é observável por qualquer um.
  - O username é um dado pessoal e pode ser o nome real.
- **Correção:**
  - Exibir um apelido ou pseudônimo estável por leilão (por exemplo, HMAC do usuário com um segredo).
  - Mostrar o nome só ao próprio usuário.
  - Exigir autenticação para o histórico.

### A5. WebSocket sem autenticação e sem limite de conexões (média)
- **Onde:** `/ws/auctions/{id}`.
- **Risco:** um único IP abre milhares de conexões e esgota memória e descritores da task. Cada conexão custa uma fila e uma corrotina.
- **Mitigação hoje:** cliente que publica é desconectado; a fila por conexão é limitada; o ping derruba as conexões mortas.
- **Correção:**
  - Limite de conexões por IP na task.
  - Rate limit de upgrade no WAF.
  - Se o conteúdo deixar de ser público: token JWT na abertura (query string ou primeiro frame) e validação antes do `accept`.

### A6. Rate limit por IP local e sem WAF (média)
- **Onde:** `ClientIpRateLimiter`, em memória de cada task.
- **Risco:** com N tasks, o limite efetivo por IP é N vezes o configurado. Não há proteção L7 na borda contra floods distribuídos.
- **Correção:** AWS WAF no ALB (rate-based por IP, managed rules, Bot Control). O limite local continua como segunda camada.

### A7. Política de senha fraca, sem MFA (média)
- **Onde:** `modules/cognito`: mínimo de 8 caracteres, com minúscula e número.
- **Risco:** *credential stuffing* com senhas vazadas de outros serviços.
- **Mitigação hoje:**
  - O Cognito bloqueia temporariamente após falhas repetidas.
  - Limite por IP no `/auth/login`.
  - Resposta uniforme (sem enumeração).
- **Correção:**
  - Mínimo de 12 caracteres.
  - MFA (TOTP) opcional ou obrigatório para valores altos.
  - Tier Plus (verificação de credenciais comprometidas e autenticação adaptativa).

### A8. Credenciais de longa duração no deploy (média, operação)
- **Onde:** o deploy foi feito com chaves de acesso de um usuário IAM (`mtblopes`) guardadas em `~/.aws/credentials`.
- **Risco:** chave vazada (máquina comprometida, commit acidental) = acesso à conta até alguém revogar.
- **Correção:**
  - IAM Identity Center (SSO) com MFA e credenciais temporárias.
  - Em CI, OIDC do GitHub assumindo uma role com permissões mínimas.
  - Revogar as chaves de usuário.

### A9. Tasks com IP público e saída livre (média em ambiente regulado)
- **Onde:** `modules/network`. Decisão de custo documentada no ADR-09: sem NAT.
- **Risco:**
  - Entrada: o SG aceita só o ALB, então o risco direto é baixo.
  - Saída: com egress `0.0.0.0/0`, código comprometido (dependência maliciosa) exfiltra dados sem barreira.
- **Correção:**
  - Subnets privadas com VPC endpoints (ECR, Logs, SSM, X-Ray, Cognito) ou NAT.
  - Egress restrito aos endpoints.

### A10. Senha do banco no state do Terraform (média)
- **Onde:** `random_password` em `modules/rds`. O valor vai para o state no S3, que é criptografado, versionado e com acesso público bloqueado.
- **Risco:** quem lê o bucket de state lê a senha do banco.
- **Correção:**
  - `manage_master_user_password = true` no RDS (Secrets Manager gera e rotaciona a senha, fora do state).
  - Bucket de state acessível só à role de deploy.

### A11. Tokens não revogáveis antes do vencimento (baixa)
- **Onde:** a validação é local, e o cache de tokens vale até o `exp`.
- **Risco:** uma conta bloqueada ou um token roubado continua válido por até 1 h.
- **Correção:**
  - Validade menor (15 min) com refresh.
  - Lista de revogação no Valkey (por `origin_jti` ou usuário), consultada só para quem foi revogado.

### A12. Valkey sem AUTH/RBAC (baixa)
- **Onde:** `modules/elasticache`. Já tem TLS em trânsito, criptografia em repouso e SG só da task.
- **Risco:** um processo comprometido dentro da VPC com acesso ao SG da task lê e altera o estado dos leilões.
- **Correção:** RBAC do ElastiCache (usuário e senha ou IAM auth) com permissões só para os comandos usados.

### A13. TLS do Postgres sem verificação de certificado (baixa)
- **Onde:** o asyncpg usa o modo padrão (`prefer`). A conexão é cifrada (o RDS do Postgres 17 exige TLS), mas o certificado não é verificado.
- **Risco:** *man-in-the-middle* dentro da VPC (teórico).
- **Correção:** `ssl` com `verify-full` e o bundle de CAs do RDS na imagem.

### A14. Container sem filesystem somente leitura (baixa)
- **Correção:** `readonlyRootFilesystem = true` na task definition (a app não escreve em disco; `HOME=/tmp` precisaria de um volume `tmpfs`) e `linuxParameters` com capabilities removidas.

### A15. Sem trilha de auditoria de autenticação (baixa)
- **Correção:**
  - Logs de eventos de usuário do Cognito para o CloudWatch.
  - CloudTrail com eventos de gerenciamento retidos.
  - Logar login, cadastro e falhas de token com o IP (sem a senha).

## Ordem recomendada

1. **A1 (TLS)** antes de qualquer uso real: sem ele, a autenticação vale pouco.
2. **A2 + A6 (WAF com rate limit e CAPTCHA no cadastro)** e **A3 (papéis)**: abuso e fraude.
3. **A8 (credenciais de deploy)** e **A10 (senha no state)**: segurança da conta.
4. **A4 (privacidade)**, **A5 (WebSocket)** e **A7 (MFA)**.
5. O resto como *hardening* contínuo.

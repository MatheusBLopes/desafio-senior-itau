# Principais comandos do projeto. `make` (ou `make help`) lista todos.

SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

REPLICAS ?= 2
API      ?= http://localhost:8080
TF_ENV   := infra/terraform/envs/dev
AWS_SH   := others/aws.sh

.PHONY: help
help: ## Lista os comandos
	@awk 'BEGIN {FS = ":.*## "} /^## / {printf "\n\033[1m%s\033[0m\n", substr($$0, 4)} \
	  /^[a-z0-9-]+:.*## / {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo

## Local (Docker Compose)

.PHONY: up
up: ## Sobe réplicas da app + nginx + Valkey + Postgres (REPLICAS=2)
	APP_REPLICAS=$(REPLICAS) docker compose up -d --build
	@$(MAKE) --no-print-directory ready

.PHONY: up-obs
up-obs: ## Sobe tudo + Prometheus (:9090) e Grafana (:3000)
	APP_REPLICAS=$(REPLICAS) docker compose --profile observability up -d --build
	@$(MAKE) --no-print-directory ready

.PHONY: scale
scale: ## Muda o número de réplicas da app (REPLICAS=4)
	APP_REPLICAS=$(REPLICAS) docker compose up -d --no-recreate app
	docker compose restart nginx

.PHONY: ready
ready: ## Espera a API responder em /readyz
	@for i in $$(seq 1 60); do \
	  curl -fsS $(API)/readyz >/dev/null 2>&1 && { echo "API pronta em $(API)"; exit 0; }; sleep 1; \
	done; echo "a API não respondeu em 60 s" >&2; exit 1

.PHONY: ps
ps: ## Mostra os containers
	docker compose ps

.PHONY: logs
logs: ## Acompanha os logs da app
	docker compose logs -f --tail=100 app

.PHONY: down
down: ## Para os containers (mantém os dados)
	docker compose --profile observability down

.PHONY: clean
clean: ## Para os containers e apaga os dados locais (volumes)
	docker compose --profile observability down -v --remove-orphans

## Desenvolvimento

.PHONY: install
install: ## Instala as dependências do bid-service (uv)
	cd bid-service && uv sync

.PHONY: test
test: ## Roda os testes (precisa do Docker). Um só: make test T=tests/test_auth.py
	cd bid-service && uv run pytest $(T)

.PHONY: check
check: ## Confere a sintaxe e os imports do bid-service
	cd bid-service && uv run python -m compileall -q src && uv run python -c "import auction.app"

.PHONY: run
run: ## Roda a app fora do Docker (precisa de Valkey e Postgres: docker compose up -d valkey postgres)
	cd bid-service && AUTH_MODE=header DB_USER=auction DB_PASSWORD=auction uv run python -m auction

## Terraform

.PHONY: tf-fmt
tf-fmt: ## Formata os arquivos Terraform
	terraform fmt -recursive infra/terraform

.PHONY: tf-validate
tf-validate: ## Valida a configuração do ambiente dev
	terraform fmt -check -recursive infra/terraform
	terraform -chdir=$(TF_ENV) validate

.PHONY: tf-plan
tf-plan: ## Mostra o que mudaria na AWS (não aplica nada)
	terraform -chdir=$(TF_ENV) plan

## AWS (usa others/aws.sh, que não vai para o git)

define aws
	@test -x $(AWS_SH) || { echo "$(AWS_SH) não encontrado" >&2; exit 1; }
	./$(AWS_SH) $(1)
endef

.PHONY: aws-up
aws-up: ## Cria tudo na AWS, publica a imagem e atualiza a collection
	$(call aws,up)

.PHONY: aws-pause
aws-pause: ## Desliga servidores e RDS sem apagar nada
	$(call aws,pause)

.PHONY: aws-resume
aws-resume: ## Religa o que o aws-pause desligou
	$(call aws,resume)

.PHONY: aws-reset
aws-reset: ## Apaga leilões, lances e usuários do Cognito (demo do zero)
	$(call aws,reset)

.PHONY: aws-down
aws-down: ## Remove toda a infraestrutura da AWS
	$(call aws,down)

.PHONY: aws-health
aws-health: ## Consulta o /readyz do ALB
	curl -fsS "$$(terraform -chdir=$(TF_ENV) output -raw alb_url)/readyz"; echo

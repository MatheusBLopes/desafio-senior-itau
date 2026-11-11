"""Configuração lida de variáveis de ambiente (12-factor): a mesma imagem roda local, em teste e na AWS."""

import os
from dataclasses import dataclass, field
from urllib.parse import quote


def _env(name: str, default: str) -> str:
    """Variável de ambiente, ou o padrão se ela não existir."""
    return os.environ.get(name, default)


def _bool(name: str, default: bool) -> bool:
    """Variável de ambiente booleana: 1, true ou yes (sem diferenciar maiúsculas) é verdadeiro."""
    return _env(name, str(default)).lower() in ("1", "true", "yes")


@dataclass(frozen=True)
class RateLimitConfig:
    """Limites dos token buckets por usuário (Valkey) e por IP (memória da task)."""

    # Por usuário, no Valkey (vale entre todas as tasks).
    enabled: bool = True
    capacity: int = 20
    refill_per_second: float = 10
    # Por IP de origem, na memória de cada task (IpRateLimiter).
    per_ip_enabled: bool = True
    per_ip_capacity: int = 200
    per_ip_refill_per_second: float = 50


@dataclass(frozen=True)
class Settings:
    """Toda a configuração da task. Os padrões servem para o ambiente local; from_env lê o ambiente."""

    db_dsn: str = "postgresql://auction:auction@localhost:5432/auction"
    db_pool_size: int = 10
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_tls: bool = False
    redis_pool_size: int = 64
    redis_pool_timeout_s: float = 0.2
    # Requisições HTTP em andamento por task antes de responder 503 (LoadShedder). Lei de Little:
    # capacidade × latência tolerada na fila = 1.200/s × 50 ms ≈ 60 (medido numa task de 0,5 vCPU).
    max_in_flight: int = 64

    port: int = 8080
    # Porta interna (métricas Prometheus e health detalhado): o ALB só encaminha a 8080.
    management_port: int = 8081

    events_channel: str = "auction-events"
    consumer_group: str = "persister"
    stream_max_len: int = 100_000
    idempotency_ttl_s: int = 24 * 3600
    # Leilão encerrado sai da memória do Valkey depois disso; se consultado de novo, é reidratado do Postgres.
    closed_retention_s: int = 3600
    snapshot_interval_s: float = 0.150
    closer_interval_s: float = 0.200
    reconcile_interval_s: float = 30
    consumer_batch_size: int = 500
    consumer_claim_min_idle_ms: int = 30_000
    # Varredura completa dos streams: pega avisos perdidos do Pub/Sub, reivindica pendentes e mede o backlog.
    consumer_maintenance_interval_s: float = 5

    rate_limit: RateLimitConfig = field(default_factory=RateLimitConfig)

    # cognito (AWS): JWT do Amazon Cognito. header (local, carga): X-User-Id sem prova de identidade.
    # Default seguro: sem configuração, exige Cognito (e não sobe sem o pool); o modo header é explícito.
    auth_mode: str = "cognito"
    cognito_region: str = "us-east-1"
    cognito_user_pool_id: str = ""
    cognito_client_id: str = ""

    json_logs: bool = False
    # Nível dos logs da aplicação (bibliotecas de terceiros ficam sempre em WARNING).
    log_level: str = "INFO"
    # Uma linha por requisição (método, caminho, status, duração, usuário, leilão, resultado). Desligar só
    # em teste de carga: a ~2.000 lances/s por task, são ~2.000 linhas/s no CloudWatch.
    log_requests: bool = True
    # Exporta as métricas de negócio para o CloudWatch (namespace Auction) via Embedded Metric Format.
    cloudwatch_emf: bool = False
    # Log group só de métricas (na AWS, /ecs/<ambiente>/metrics): as linhas EMF vão para ele por PutLogEvents
    # e não se misturam aos logs. Vazio: saem no stdout.
    cloudwatch_emf_log_group: str = ""
    metrics_namespace: str = "Auction"
    metrics_step_s: float = 60

    @staticmethod
    def from_env() -> "Settings":
        """Monta a configuração a partir das variáveis de ambiente (os nomes estão no Terraform e no Compose)."""
        return Settings(
            # quote: uma senha com @, /, # ou % quebraria a URL (ou mudaria o host) se entrasse crua.
            db_dsn=_env("DB_DSN", "postgresql://{user}:{password}@{host}:{port}/{name}".format(
                user=quote(_env("DB_USER", "auction"), safe=""), password=quote(_env("DB_PASSWORD", "auction"), safe=""),
                host=_env("DB_HOST", "localhost"), port=_env("DB_PORT", "5432"), name=_env("DB_NAME", "auction"))),
            db_pool_size=int(_env("DB_POOL_SIZE", "10")),
            redis_host=_env("REDIS_HOST", "localhost"),
            redis_port=int(_env("REDIS_PORT", "6379")),
            redis_tls=_bool("REDIS_TLS", False),
            port=int(_env("PORT", "8080")),
            management_port=int(_env("MANAGEMENT_PORT", "8081")),
            max_in_flight=int(_env("MAX_IN_FLIGHT", "64")),
            rate_limit=RateLimitConfig(
                enabled=_bool("AUCTION_RATE_LIMIT_ENABLED", True),
                per_ip_enabled=_bool("PER_IP_RATE_LIMIT_ENABLED", True),
            ),
            json_logs=_bool("JSON_LOGS", False),
            log_level=_env("LOG_LEVEL", "INFO"),
            log_requests=_bool("LOG_REQUESTS", True),
            auth_mode=_env("AUTH_MODE", "cognito"),
            cognito_region=_env("AWS_REGION", "us-east-1"),
            cognito_user_pool_id=_env("COGNITO_USER_POOL_ID", ""),
            cognito_client_id=_env("COGNITO_CLIENT_ID", ""),
            cloudwatch_emf=_bool("CLOUDWATCH_EMF", False),
            cloudwatch_emf_log_group=_env("CLOUDWATCH_EMF_LOG_GROUP", ""),
        )

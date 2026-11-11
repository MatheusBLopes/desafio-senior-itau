"""Clientes do Valkey (comandos e Pub/Sub) e o relógio único do sistema."""

from typing import Any

from redis.asyncio import BlockingConnectionPool, Redis
from redis.asyncio.connection import SSLConnection

from ..config import Settings


def create_redis(settings: Settings) -> Redis:
    """Cliente dos comandos e scripts, com pool limitado e espera curta por conexão."""
    kwargs: dict[str, Any] = {"connection_class": SSLConnection} if settings.redis_tls else {}
    # Pool limitado e bloqueante: sob pico, a requisição espera uma conexão em vez de abrir milhares.
    # A espera é curta: pool esgotado vira 503 rápido (api/error_handlers.py), não fila de segundos.
    pool = BlockingConnectionPool(host=settings.redis_host, port=settings.redis_port,
                                  max_connections=settings.redis_pool_size, timeout=settings.redis_pool_timeout_s,
                                  socket_timeout=2, decode_responses=True, **kwargs)
    return Redis(connection_pool=pool)


def create_pubsub_redis(settings: Settings) -> Redis:
    """Conexão dedicada ao Pub/Sub: fica bloqueada esperando mensagens, então sem timeout de leitura.

    O health check (PING a cada 15 s) detecta uma conexão morta em vez de esperar para sempre.
    """
    kwargs: dict[str, Any] = {"ssl": True} if settings.redis_tls else {}
    return Redis(host=settings.redis_host, port=settings.redis_port, decode_responses=True, socket_timeout=None,
                 health_check_interval=15, **kwargs)


class RedisClock:
    """Relógio único do sistema: o TIME do Valkey. Nenhuma decisão de prazo usa o relógio da task."""

    def __init__(self, redis: Redis):
        """Usa o cliente de comandos do Valkey."""
        self._redis = redis

    async def now_ms(self) -> int:
        """Agora, em epoch ms, segundo o servidor Valkey (o mesmo relógio que os scripts Lua usam)."""
        seconds, micros = await self._redis.time()
        return seconds * 1000 + micros // 1000

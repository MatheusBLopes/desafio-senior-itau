"""Rate limit dos lances: por usuário (no Valkey) e por IP de origem (na memória da task)."""

import ipaddress
import time
from collections.abc import Callable

from redis.asyncio.client import Pipeline

from ..config import RateLimitConfig
from ..core.errors import ApiError
from ..storage.scripts import LuaScripts


class UserRateLimiter:
    """Token bucket por usuário, no Valkey, para valer entre todas as tasks."""

    def __init__(self, scripts: LuaScripts, config: RateLimitConfig):
        """Scripts Lua (o bucket fica no Valkey) e os limites configurados."""
        self._scripts = scripts
        self._config = config

    async def check(self, user_id: str) -> None:
        """Gasta uma ficha do usuário; sem ficha, 429."""
        if self._config.enabled:
            self.verify(await self._scripts.rate_limit(user_id, self._config.capacity,
                                                       self._config.refill_per_second))

    def enqueue(self, user_id: str, pipe: Pipeline) -> None:
        """Agenda o script num pipeline com EVALSHA puro.

        Passar o script ao pipeline faria o redis-py mandar um SCRIPT EXISTS a cada execute (uma ida ao
        Valkey). Se o Valkey perdeu o cache de scripts, quem chama trata o NOSCRIPT.
        """
        self._scripts.enqueue_rate_limit(pipe, user_id, self._config.capacity, self._config.refill_per_second)

    @staticmethod
    def verify(allowed: int) -> None:
        """Converte a resposta do script (1 liberado, 0 limitado) em 429 quando limitado."""
        if allowed != 1:
            raise ApiError(429, "RATE_LIMITED", "muitos lances em pouco tempo")

    @property
    def enabled(self) -> bool:
        """False quando o limite está desligado (teste de carga)."""
        return self._config.enabled


class IpRateLimiter:
    """Token bucket por IP de origem, na memória da task.

    Complementa o limite por usuário: o X-User-Id é declarado pelo cliente, e trocá-lo a cada request
    contornaria aquele limite. Fica na memória, sem ida ao Valkey, cuja thread é o recurso escasso do
    leilão quente. O preço é a precisão: com N tasks atrás do ALB, o limite efetivo é ~N vezes o
    configurado. A defesa definitiva é autenticação e rate limit na borda (WAF).

    Sem lock: o asyncio roda uma corrotina por vez e check() não tem await no meio.
    """

    def __init__(self, config: RateLimitConfig, clock: Callable[[], float] = time.monotonic):
        """Limites configurados e relógio monotônico (injetável nos testes)."""
        self._config = config
        self._clock = clock
        self._buckets: dict[str, list[float]] = {}  # ip → [tokens, último instante]

    def check(self, ip: str) -> None:
        """Repõe as fichas do IP pelo tempo passado e gasta uma; sem ficha, 429."""
        config = self._config
        if not config.per_ip_enabled:
            return
        now = self._clock()
        bucket = self._buckets.setdefault(ip, [float(config.per_ip_capacity), now])
        bucket[0] = min(config.per_ip_capacity, bucket[0] + (now - bucket[1]) * config.per_ip_refill_per_second)
        bucket[1] = now
        if bucket[0] < 1:
            raise ApiError(429, "RATE_LIMITED", "muitas requisições deste endereço")
        bucket[0] -= 1

    def evict_idle(self) -> None:
        """Remove buckets parados há tempo suficiente para estarem cheios: equivalem a um bucket novo."""
        now = self._clock()
        refill_s = self._config.per_ip_capacity / self._config.per_ip_refill_per_second
        for ip in [ip for ip, bucket in self._buckets.items() if now - bucket[1] >= refill_s]:
            del self._buckets[ip]

    def __len__(self) -> int:
        """Quantos IPs têm bucket na memória agora."""
        return len(self._buckets)


_PRIVATE = [ipaddress.ip_network(network) for network in
            ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "127.0.0.0/8", "::1/128", "fc00::/7")]


def _is_private(ip: str) -> bool:
    """True para endereços de rede privada (onde ficam o ALB e o nginx)."""
    try:
        address = ipaddress.ip_address(ip)
    except ValueError:
        return False
    return any(address in network for network in _PRIVATE)


def client_ip(peer: str | None, forwarded_for: str | None) -> str:
    """IP do cliente atrás do ALB/nginx.

    O X-Forwarded-For só é aceito se a conexão vem de um proxy da rede privada; dele, vale o último
    endereço que não é um proxy confiável (lido da direita para a esquerda). Os da esquerda foram
    escritos pelo próprio cliente e podem ser forjados.
    """
    peer = peer or "unknown"
    if not forwarded_for or not _is_private(peer):
        return peer
    hops = [hop.strip() for hop in forwarded_for.split(",") if hop.strip()]
    for hop in reversed(hops):
        if not _is_private(hop):
            return hop
    return hops[0] if hops else peer

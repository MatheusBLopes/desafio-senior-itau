"""Scripts Lua do Valkey, com um método Python por script.

register_script calcula o SHA1 do texto e cada chamada envia só EVALSHA <sha> + parâmetros. Se o
Valkey responder NOSCRIPT (cache de scripts perdido num restart), o redis-py carrega o texto
(SCRIPT LOAD) e repete: o código Lua trafega uma vez, não a cada lance.

Cada método monta KEYS e ARGV na ordem que o script espera (documentada no cabeçalho de cada .lua),
para que nenhum outro módulo precise conhecer essas posições.
"""

import uuid
from pathlib import Path

from redis.asyncio import Redis
from redis.asyncio.client import Pipeline
from redis.commands.core import AsyncScript

from ..config import Settings
from . import keys

_DIR = Path(__file__).parent / "lua"


class LuaScripts:
    """Os quatro scripts, registrados num cliente do Valkey (o script fica preso a esse cliente)."""

    def __init__(self, redis: Redis, settings: Settings):
        """Registra os quatro scripts no cliente; Settings fornece canal, TTL, MAXLEN e consumer group."""
        self._redis = redis
        self._settings = settings
        self._place_bid = self._load("place_bid")
        self._close_auction = self._load("close_auction")
        self._hydrate_auction = self._load("hydrate_auction")
        self._rate_limit = self._load("rate_limit")

    @staticmethod
    def register(redis: Redis, settings: Settings) -> "LuaScripts":
        """Registra os scripts no cliente; nada vai ao Valkey até a primeira chamada."""
        return LuaScripts(redis, settings)

    def _load(self, name: str) -> AsyncScript:
        """Lê lua/<name>.lua e o registra no cliente."""
        return self._redis.register_script((_DIR / f"{name}.lua").read_text())

    async def place_bid(self, auction_id: uuid.UUID, user_id: str, idempotency_key: str,
                        amount_cents: int) -> tuple[str, str]:
        """Decide um lance de forma atômica. Devolve (tipo, json): NEW, REPLAY, MISMATCH ou NOT_LOADED."""
        settings = self._settings
        reply = await self._place_bid(
            keys=[keys.auction_key(auction_id), keys.idempotency_key(auction_id, user_id, idempotency_key),
                  keys.stream_key(auction_id)],
            # O fingerprint (ARGV[3]) é o valor: usuário e leilão já estão na chave.
            args=[user_id, amount_cents, amount_cents, settings.idempotency_ttl_s, settings.stream_max_len,
                  str(auction_id), settings.events_channel, idempotency_key])
        return reply[0], reply[1]

    async def close_auction(self, auction_id: uuid.UUID) -> tuple[str, int]:
        """Encerra o leilão se o prazo passou. Devolve (tipo, prazo): CLOSED, ALREADY_CLOSED, PENDING ou NOT_LOADED."""
        settings = self._settings
        outcome_kind, ends_at = await self._close_auction(
            keys=[keys.auction_key(auction_id), keys.stream_key(auction_id)],
            args=[str(auction_id), settings.events_channel, settings.stream_max_len])
        return outcome_kind, int(ends_at)

    async def hydrate_auction(self, auction_id: uuid.UUID, *, status: str, starting_price: int, min_increment: int,
                              current_price: int, leader_id: str, seq: int, ends_at_ms: int,
                              extend_window_ms: int, extend_by_ms: int) -> int:
        """Carrega o estado do leilão no Valkey. Devolve 1 se carregou, 2 se alcançou o banco, 0 se já estava em dia."""
        settings = self._settings
        return await self._hydrate_auction(
            keys=[keys.auction_key(auction_id), keys.stream_key(auction_id)],
            args=[status, starting_price, min_increment, current_price, leader_id, seq, ends_at_ms,
                  extend_window_ms, extend_by_ms, settings.consumer_group, str(auction_id), settings.events_channel])

    async def rate_limit(self, user_id: str, capacity: int, refill_per_second: float) -> int:
        """Gasta uma ficha do token bucket do usuário. Devolve 1 se liberado, 0 se limitado."""
        return await self._rate_limit(keys=[keys.rate_limit_key(user_id)], args=[capacity, refill_per_second])

    def enqueue_rate_limit(self, pipe: Pipeline, user_id: str, capacity: int, refill_per_second: float) -> None:
        """Agenda o rate limit num pipeline com EVALSHA puro.

        Passar o script ao pipeline faria o redis-py mandar um SCRIPT EXISTS a cada execute (uma ida a
        mais ao Valkey). Se o Valkey perdeu o cache de scripts, quem chama trata o NOSCRIPT com
        reload_rate_limit().
        """
        pipe.evalsha(self._rate_limit.sha, 1, keys.rate_limit_key(user_id), capacity, refill_per_second)

    async def reload_rate_limit(self) -> None:
        """Recarrega o script de rate limit no Valkey depois de um NOSCRIPT no pipeline."""
        await self._redis.script_load(self._rate_limit.script)

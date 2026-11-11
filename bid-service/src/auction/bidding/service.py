"""O caminho do lance: rate limit, pré-filtro local e o script atômico place_bid.lua no Valkey."""

import logging
import time
import uuid
from typing import Any

import orjson
from redis.asyncio import Redis
from redis.exceptions import NoScriptError

from ..config import Settings
from ..lifecycle.state_loader import AuctionStateLoader
from ..observability.logs import bind
from ..observability.metrics import Metrics
from ..realtime.cache import LiveAuctionCache, Snapshot
from ..storage import keys
from ..storage.scripts import LuaScripts
from .rate_limit import UserRateLimiter
from .result import ACCEPTED, AUCTION_CLOSED, BID_TOO_LOW, IDEMPOTENCY_KEY_REUSED, REJECTED, BidResult

log = logging.getLogger(__name__)


class BidService:
    """Decide cada lance. O cache local só rejeita; quem aceita é sempre o place_bid.lua."""

    def __init__(self, redis: Redis, scripts: LuaScripts, loader: AuctionStateLoader, cache: LiveAuctionCache,
                 rate_limiter: UserRateLimiter, metrics: Metrics, settings: Settings):
        """Recebe as dependências do caminho do lance; tudo é criado uma vez no startup."""
        self._redis = redis
        self._scripts = scripts
        self._loader = loader
        self._cache = cache
        self._rate_limiter = rate_limiter
        self._metrics = metrics
        self._settings = settings

    async def place(self, auction_id: uuid.UUID, user_id: str, key: str, amount: int) -> BidResult:
        """Processa um lance (valor em centavos): pré-filtro e, se ele não resolver, o script no Valkey."""
        start = time.perf_counter()
        bind(auctionId=auction_id)
        try:
            result = await self._pre_filter(auction_id, user_id, key, amount)
            if result is None:
                result = await self._evaluate(auction_id, user_id, key, amount)
            if not result.replayed:
                self._metrics.outcome(result.outcome, result.reason)
            if result.accepted:
                bind(seq=result.seq)
                log.debug("lance aceito amount=%s", amount)
            return result
        finally:
            self._metrics.bid_latency(time.perf_counter() - start)

    async def _pre_filter(self, auction_id: uuid.UUID, user_id: str, key: str, amount: int) -> BidResult | None:
        """Rejeita localmente o que com certeza seria rejeitado, poupando a thread única do Valkey.

        O preço só sobe, então "abaixo do mínimo conhecido" continua abaixo no Lua. Exceção: se a
        idempotency key já existe, é retry e precisa da resposta original (vai ao Lua).

        Essa rejeição não é memorizada: gravá-la custaria uma key no Valkey por lance superado (~450 B,
        centenas de MB num pico) e ~+25 ms de p95 a 10 mil lances/s. Só um lance avaliado pelo Lua trava
        a key. Um retry com o mesmo valor recebe a mesma rejeição (o preço só sobe), e a mesma key com
        outro valor é avaliada como lance novo. Nunca há duplicata (ver ADR-03).

        Também aplica o rate limit por usuário. Quando o cache prevê rejeição (a maioria dos lances num
        pico), o rate limit e o EXISTS da key vão num só pipeline: uma ida ao Valkey em vez de duas.

        Devolve None quando o lance precisa ir ao Lua.
        """
        snapshot = self._cache.get(auction_id)
        if not self._is_certainly_rejected(snapshot, amount):
            await self._rate_limiter.check(user_id)
            return None

        allowed, key_exists = await self._rate_limit_and_key_exists(
            user_id, keys.idempotency_key(auction_id, user_id, key))
        if allowed is not None:
            self._rate_limiter.verify(allowed)
        if key_exists:
            return None  # retry: o Lua devolve a resposta original
        return self._local_rejection(auction_id, amount, snapshot)

    @staticmethod
    def _is_certainly_rejected(snapshot: Snapshot | None, amount: int) -> bool:
        """True se o último estado conhecido já garante a rejeição: leilão encerrado ou valor abaixo do mínimo."""
        if snapshot is None:
            return False
        return snapshot.status == "CLOSED" or amount < snapshot.minimum_next

    @staticmethod
    def _local_rejection(auction_id: uuid.UUID, amount: int, snapshot: Snapshot) -> BidResult:
        """A rejeição montada pelo pré-filtro, com o estado conhecido (marcada como filtered)."""
        if snapshot.status == "CLOSED":
            return BidResult(auction_id, REJECTED, AUCTION_CLOSED, amount=amount, ends_at=snapshot.ends_at,
                             filtered=True)
        return BidResult(auction_id, REJECTED, BID_TOO_LOW, amount=amount,
                         current_price=snapshot.current_price if snapshot.seq else None,
                         minimum_next=snapshot.minimum_next, ends_at=snapshot.ends_at, filtered=True)

    async def _rate_limit_and_key_exists(self, user_id: str, idempotency_key: str) -> tuple[int | None, bool]:
        """Rate limit e EXISTS da key num só pipeline. Devolve (resposta do rate limit ou None se desligado, existe).

        Se o Valkey perdeu o cache de scripts (NOSCRIPT), recarrega o rate limit e repete uma vez.
        """
        for attempt in range(2):
            pipe = self._redis.pipeline(transaction=False)
            if self._rate_limiter.enabled:
                self._rate_limiter.enqueue(user_id, pipe)
            pipe.exists(idempotency_key)
            try:
                replies = await pipe.execute()
            except NoScriptError:
                if attempt:
                    raise
                # Valkey reiniciou e perdeu o cache de scripts: carrega de novo e repete.
                await self._scripts.reload_rate_limit()
                continue
            allowed = replies[0] if self._rate_limiter.enabled else None
            return allowed, bool(replies[-1])
        raise AssertionError("inalcançável")

    async def _evaluate(self, auction_id: uuid.UUID, user_id: str, key: str, amount: int) -> BidResult:
        """Chama o place_bid.lua; se o leilão não está no Valkey, reidrata do Postgres e tenta de novo."""
        outcome_kind, payload = await self._run_script(auction_id, user_id, key, amount)
        if outcome_kind == "NOT_LOADED":
            # Restart do Valkey ou leilão já expirado da memória: reidrata do Postgres e tenta de novo.
            await self._loader.load_id(auction_id)
            outcome_kind, payload = await self._run_script(auction_id, user_id, key, amount)
        if outcome_kind == "NEW":
            return self._parse(auction_id, amount, payload, replayed=False)
        if outcome_kind == "REPLAY":
            return self._parse(auction_id, amount, payload, replayed=True)
        if outcome_kind == "MISMATCH":
            return BidResult(auction_id, REJECTED, IDEMPOTENCY_KEY_REUSED, amount=amount)
        raise RuntimeError(f"resposta inesperada do place_bid.lua: {outcome_kind}")

    async def _run_script(self, auction_id: uuid.UUID, user_id: str, key: str, amount: int) -> tuple[str, str]:
        """Uma chamada ao place_bid.lua, medida na métrica de latência do Lua."""
        start = time.perf_counter()
        reply = await self._scripts.place_bid(auction_id, user_id, key, amount)
        self._metrics.lua_latency(time.perf_counter() - start)
        return reply

    @staticmethod
    def _parse(auction_id: uuid.UUID, amount: int, payload: str, replayed: bool) -> BidResult:
        """Converte o JSON devolvido pelo Lua (números como string) num BidResult."""
        response: dict[str, Any] = orjson.loads(payload)
        accepted = response["outcome"] == ACCEPTED
        reason = response.get("reason")
        current_price = int(response["currentPrice"])
        return BidResult(
            auction_id,
            ACCEPTED if accepted else REJECTED,
            reason=None if accepted else reason,
            seq=int(response["seq"]) if accepted else None,
            amount=amount,
            current_price=current_price or None,
            minimum_next=None if reason == AUCTION_CLOSED else int(response["minimumNext"]),
            ends_at=int(response["endsAt"]),
            extended=bool(response["extended"]) if accepted else None,
            server_time=int(response["serverTime"]),
            replayed=replayed,
        )

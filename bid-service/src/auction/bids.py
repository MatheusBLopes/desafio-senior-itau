"""Lances: pré-filtro, script atômico no Valkey, rate limits e histórico."""

import ipaddress
import json
import logging
import time
import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable

import asyncpg
import orjson
from redis.asyncio import Redis
from redis.asyncio.client import Pipeline
from redis.exceptions import NoScriptError

from . import common
from .auctions import AuctionStateLoader
from .common import ApiError, from_cents, from_datetime, iso, without_nulls
from .config import RateLimitConfig, Settings
from .lua import LuaScripts
from .observability import Metrics, bind
from .realtime import LiveAuctionCache

log = logging.getLogger(__name__)

ACCEPTED = "ACCEPTED"
REJECTED = "REJECTED"
BID_TOO_LOW = "BID_TOO_LOW"
AUCTION_CLOSED = "AUCTION_CLOSED"
IDEMPOTENCY_KEY_REUSED = "IDEMPOTENCY_KEY_REUSED"


MAX_AMOUNT = Decimal("999999999999.99")  # 12 dígitos inteiros e 2 decimais, como o cadastro


def parse_amount(body: bytes) -> int:
    """Lê {"amount": 150.00} (número ou string) e devolve centavos.

    O caminho quente do lance valida à mão, sem o resolvedor de dependências do FastAPI: o profiler
    mostrou ~20% da CPU da requisição nessa camada genérica. json com parse_float=Decimal lê o número
    exatamente como veio, sem passar por float.
    """
    try:
        data = json.loads(body, parse_float=Decimal)
        raw = data["amount"]
        if isinstance(raw, bool) or not isinstance(raw, (int, str, Decimal)):
            raise ValueError
        amount = Decimal(raw)
    except (ValueError, KeyError, TypeError, ArithmeticError):
        raise ApiError.bad_request('amount: informe um valor, como {"amount": 150.00}') from None
    if not amount.is_finite() or amount < Decimal("0.01") or amount > MAX_AMOUNT:
        raise ApiError.bad_request("amount: entre 0.01 e 999999999999.99")
    if amount != amount.quantize(Decimal("0.01")):
        raise ApiError.bad_request("amount: no máximo 2 casas decimais")
    return int(amount * 100)


@dataclass(frozen=True)
class BidResult:
    """Resposta de um lance. Valores em centavos; endsAt e serverTime em epoch ms.

    replayed: a idempotency key já tinha sido usada e esta é a resposta original.
    filtered: rejeitado pelo pré-filtro da task, sem ir ao Valkey.
    """

    auction_id: uuid.UUID
    outcome: str
    reason: str | None = None
    seq: int | None = None
    amount: int | None = None
    current_price: int | None = None
    minimum_next: int | None = None
    ends_at: int | None = None
    extended: bool | None = None
    server_time: int | None = None
    replayed: bool = False
    filtered: bool = False

    @property
    def accepted(self) -> bool:
        return self.outcome == ACCEPTED

    def http_status(self) -> int:
        """201 aceito; 200 replay de um aceite; 409 abaixo do mínimo; 422 encerrado ou key reutilizada."""
        if self.accepted:
            return 200 if self.replayed else 201
        return 409 if self.reason == BID_TOO_LOW else 422

    def to_json(self) -> dict[str, Any]:
        def money(c: int | None) -> str | None:
            return None if c is None else from_cents(c)

        return without_nulls({
            "auctionId": str(self.auction_id),
            "outcome": self.outcome,
            "reason": self.reason,
            "seq": self.seq,
            "amount": money(self.amount),
            "currentPrice": money(self.current_price),
            "minimumNextBid": money(self.minimum_next),
            "endsAt": None if self.ends_at is None else iso(self.ends_at),
            "extended": self.extended,
            "serverTime": None if self.server_time is None else iso(self.server_time),
            "replayed": self.replayed,
            "filtered": self.filtered,
        })


class RateLimiter:
    """Token bucket por usuário, no Valkey, para valer entre todas as tasks."""

    def __init__(self, scripts: LuaScripts, config: RateLimitConfig):
        self._scripts = scripts
        self._config = config

    async def check(self, user_id: str) -> None:
        if self._config.enabled:
            self.verify(await self._scripts.rate_limit(
                keys=[common.rate_limit_key(user_id)], args=[self._config.capacity, self._config.refill_per_second]))

    def enqueue(self, user_id: str, pipe: Pipeline) -> None:
        """Agenda o script num pipeline com EVALSHA puro.

        Passar o script ao pipeline faria o redis-py mandar um SCRIPT EXISTS a cada execute (uma ida a
        mais ao Valkey). Se o Valkey perdeu o cache de scripts, quem chama trata o NOSCRIPT.
        """
        pipe.evalsha(self._scripts.rate_limit.sha, 1, common.rate_limit_key(user_id),
                     self._config.capacity, self._config.refill_per_second)

    @staticmethod
    def verify(allowed: int) -> None:
        if allowed != 1:
            raise ApiError(429, "RATE_LIMITED", "muitos lances em pouco tempo")

    @property
    def enabled(self) -> bool:
        return self._config.enabled


class ClientIpRateLimiter:
    """Token bucket por IP de origem, na memória da task.

    Complementa o limite por usuário: o X-User-Id é declarado pelo cliente, e trocá-lo a cada request
    contornaria aquele limite. Fica na memória, sem ida ao Valkey, cuja thread é o recurso escasso do
    leilão quente. O preço é a precisão: com N tasks atrás do ALB, o limite efetivo é ~N vezes o
    configurado. A defesa definitiva é autenticação e rate limit na borda (WAF).

    Sem lock: o asyncio roda uma corrotina por vez e check() não tem await no meio.
    """

    def __init__(self, config: RateLimitConfig, clock: Callable[[], float] = time.monotonic):
        self._config = config
        self._clock = clock
        self._buckets: dict[str, list[float]] = {}  # ip → [tokens, último instante]

    def check(self, ip: str) -> None:
        c = self._config
        if not c.per_ip_enabled:
            return
        now = self._clock()
        bucket = self._buckets.setdefault(ip, [float(c.per_ip_capacity), now])
        bucket[0] = min(c.per_ip_capacity, bucket[0] + (now - bucket[1]) * c.per_ip_refill_per_second)
        bucket[1] = now
        if bucket[0] < 1:
            raise ApiError(429, "RATE_LIMITED", "muitas requisições deste endereço")
        bucket[0] -= 1

    def evict_idle(self) -> None:
        """Remove buckets parados há tempo suficiente para estarem cheios: equivalem a um bucket novo."""
        now = self._clock()
        refill = self._config.per_ip_capacity / self._config.per_ip_refill_per_second
        for ip in [ip for ip, b in self._buckets.items() if now - b[1] >= refill]:
            del self._buckets[ip]

    def __len__(self) -> int:
        return len(self._buckets)


_PRIVATE = [ipaddress.ip_network(n) for n in
            ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "127.0.0.0/8", "::1/128", "fc00::/7")]


def _is_private(ip: str) -> bool:
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        return False
    return any(addr in n for n in _PRIVATE)


def client_ip(peer: str | None, forwarded_for: str | None) -> str:
    """IP do cliente atrás do ALB/nginx.

    O X-Forwarded-For só é aceito se a conexão vem de um proxy da rede privada; dele, vale o último
    endereço que não é um proxy confiável (lido da direita para a esquerda). Os da esquerda foram
    escritos pelo próprio cliente e podem ser forjados.
    """
    peer = peer or "unknown"
    if not forwarded_for or not _is_private(peer):
        return peer
    hops = [h.strip() for h in forwarded_for.split(",") if h.strip()]
    for hop in reversed(hops):
        if not _is_private(hop):
            return hop
    return hops[0] if hops else peer


class BidRepository:
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def find_after(self, auction_id: uuid.UUID, after_seq: int, limit: int) -> list[dict[str, Any]]:
        rows = await self._pool.fetch("""
            select seq, user_id, amount, accepted_at from bids
            where auction_id = $1 and seq > $2 order by seq limit $3
            """, auction_id, after_seq, limit)
        return [bid_view(r["seq"], r["user_id"], r["amount"], from_datetime(r["accepted_at"])) for r in rows]


def bid_view(seq: int, user_id: str, amount: int, accepted_at_ms: int) -> dict[str, Any]:
    return {"seq": seq, "userId": user_id, "amount": from_cents(amount), "acceptedAt": iso(accepted_at_ms)}


class BidService:
    def __init__(self, redis: Redis, scripts: LuaScripts, loader: AuctionStateLoader, cache: LiveAuctionCache,
                 rate_limiter: RateLimiter, bids: BidRepository, metrics: Metrics, settings: Settings):
        self._redis = redis
        self._scripts = scripts
        self._loader = loader
        self._cache = cache
        self._rate_limiter = rate_limiter
        self._bids = bids
        self._metrics = metrics
        self._settings = settings

    async def place(self, auction_id: uuid.UUID, user_id: str, key: str, amount: int) -> BidResult:
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
        """
        snapshot = self._cache.get(auction_id)
        closed = snapshot is not None and snapshot.status == "CLOSED"
        if snapshot is None or (not closed and amount >= snapshot.minimum_next):
            await self._rate_limiter.check(user_id)
            return None
        replies = await self._rate_limit_and_exists(user_id, common.idempotency_key(auction_id, user_id, key))
        if self._rate_limiter.enabled:
            self._rate_limiter.verify(replies[0])
        if replies[-1]:
            return None
        if closed:
            return BidResult(auction_id, REJECTED, AUCTION_CLOSED, amount=amount, ends_at=snapshot.ends_at,
                             filtered=True)
        return BidResult(auction_id, REJECTED, BID_TOO_LOW, amount=amount,
                         current_price=snapshot.current_price if snapshot.seq else None,
                         minimum_next=snapshot.minimum_next, ends_at=snapshot.ends_at, filtered=True)

    async def _rate_limit_and_exists(self, user_id: str, idem_key: str) -> list[Any]:
        for attempt in range(2):
            pipe = self._redis.pipeline(transaction=False)
            if self._rate_limiter.enabled:
                self._rate_limiter.enqueue(user_id, pipe)
            pipe.exists(idem_key)
            try:
                return await pipe.execute()
            except NoScriptError:
                if attempt:
                    raise
                # Valkey reiniciou e perdeu o cache de scripts: carrega de novo e repete.
                await self._redis.script_load(self._scripts.rate_limit.script)
        raise AssertionError("inalcançável")

    async def _evaluate(self, auction_id: uuid.UUID, user_id: str, key: str, amount: int) -> BidResult:
        kind, payload = await self._run_script(auction_id, user_id, key, amount)
        if kind == "NOT_LOADED":
            # Restart do Valkey ou leilão já expirado da memória: reidrata do Postgres e tenta de novo.
            await self._loader.load_id(auction_id)
            kind, payload = await self._run_script(auction_id, user_id, key, amount)
        if kind == "NEW":
            return self._parse(auction_id, amount, payload, replayed=False)
        if kind == "REPLAY":
            return self._parse(auction_id, amount, payload, replayed=True)
        if kind == "MISMATCH":
            return BidResult(auction_id, REJECTED, IDEMPOTENCY_KEY_REUSED, amount=amount)
        raise RuntimeError(f"resposta inesperada do place_bid.lua: {kind}")

    async def _run_script(self, auction_id: uuid.UUID, user_id: str, key: str, amount: int) -> tuple[str, str]:
        s = self._settings
        start = time.perf_counter()
        reply = await self._scripts.place_bid(
            keys=[common.auction_key(auction_id), common.idempotency_key(auction_id, user_id, key),
                  common.stream_key(auction_id)],
            # O fingerprint (ARGV[3]) é o valor: usuário e leilão já estão na chave.
            args=[user_id, amount, amount, s.idempotency_ttl_s, s.stream_max_len, str(auction_id),
                  s.events_channel, key])
        self._metrics.lua_latency(time.perf_counter() - start)
        return reply[0], reply[1]

    @staticmethod
    def _parse(auction_id: uuid.UUID, amount: int, payload: str, replayed: bool) -> BidResult:
        n = orjson.loads(payload)
        accepted = n["outcome"] == ACCEPTED
        reason = n.get("reason")
        current_price = int(n["currentPrice"])
        return BidResult(
            auction_id,
            ACCEPTED if accepted else REJECTED,
            reason=None if accepted else reason,
            seq=int(n["seq"]) if accepted else None,
            amount=amount,
            current_price=current_price or None,
            minimum_next=None if reason == AUCTION_CLOSED else int(n["minimumNext"]),
            ends_at=int(n["endsAt"]),
            extended=bool(n["extended"]) if accepted else None,
            server_time=int(n["serverTime"]),
            replayed=replayed,
        )

    async def list_after(self, auction_id: uuid.UUID, after_seq: int, limit: int) -> list[dict[str, Any]]:
        """Lances com seq > after_seq, em ordem.

        O Postgres pode estar atrás do Valkey (write-behind) e, com várias tasks gravando lotes em
        paralelo, ter buracos temporários (4..6 gravados antes de 1..3). Se o banco devolve uma faixa
        contígua e completa, ela basta. Senão, junta com o stream, que ainda tem tudo o que não foi
        persistido, e devolve os primeiros `limit` em ordem.
        """
        from_db = await self._bids.find_after(auction_id, after_seq, limit)
        # Seqs são únicos e maiores que after_seq: se o último é after_seq + limit, não há buraco.
        if len(from_db) == limit and from_db[-1]["seq"] == after_seq + limit:
            return from_db
        merged = {b["seq"]: b for b in from_db}
        for b in await self._from_stream(auction_id, after_seq, limit):
            merged.setdefault(b["seq"], b)
        return [merged[s] for s in sorted(merged)][:limit]

    async def _from_stream(self, auction_id: uuid.UUID, after_seq: int, limit: int) -> list[dict[str, Any]]:
        records = await self._redis.xrange(common.stream_key(auction_id), min=f"0-{after_seq + 1}", count=limit)
        return [bid_view(int(v["seq"]), v["userId"], int(v["amount"]), int(v["ts"]))
                for _, v in records if v.get("type") == "BID"]

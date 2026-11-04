"""Cadastro de leilões (Postgres) e carga do estado vivo no Valkey."""

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any

import asyncpg
from pydantic import BaseModel, Field, StringConstraints
from redis.asyncio import Redis

from . import common
from .common import ApiError, RedisClock, from_cents, from_datetime, iso, to_cents, without_nulls
from .config import Settings
from .lua import LuaScripts

log = logging.getLogger(__name__)

OPEN = "OPEN"
CLOSED = "CLOSED"

Money = Annotated[Decimal, Field(ge=Decimal("0.01"), max_digits=14, decimal_places=2)]


class CreateAuctionRequest(BaseModel):
    title: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=200)]
    startingPrice: Money
    minIncrement: Money
    durationSeconds: Annotated[int, Field(ge=5, le=2_592_000)]
    antiSnipingWindowSeconds: Annotated[int, Field(ge=0, le=3600)] = 10
    antiSnipingExtendSeconds: Annotated[int, Field(ge=0, le=3600)] = 10


@dataclass(frozen=True)
class Auction:
    """Linha da tabela auctions: cadastro + projeção persistida do estado vivo. Valores em centavos."""

    id: uuid.UUID
    title: str
    starting_price: int
    min_increment: int
    anti_sniping_window_ms: int
    anti_sniping_extend_ms: int
    status: str
    created_at: datetime
    ends_at: datetime

    @staticmethod
    def from_row(r: asyncpg.Record) -> "Auction":
        return Auction(r["id"], r["title"], r["starting_price"], r["min_increment"], r["anti_sniping_window_ms"],
                       r["anti_sniping_extend_ms"], r["status"], r["created_at"], r["ends_at"])


class AuctionRepository:
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def insert(self, a: Auction) -> None:
        await self._pool.execute("""
            insert into auctions (id, title, starting_price, min_increment, anti_sniping_window_ms,
                                  anti_sniping_extend_ms, status, created_at, ends_at)
            values ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """, a.id, a.title, a.starting_price, a.min_increment, a.anti_sniping_window_ms,
            a.anti_sniping_extend_ms, a.status, a.created_at, a.ends_at)

    async def find(self, auction_id: uuid.UUID) -> Auction | None:
        row = await self._pool.fetchrow("select * from auctions where id = $1", auction_id)
        return Auction.from_row(row) if row else None

    async def get(self, auction_id: uuid.UUID) -> Auction:
        a = await self.find(auction_id)
        if a is None:
            raise ApiError.not_found(f"leilão {auction_id} não existe")
        return a

    async def open_ids(self) -> list[uuid.UUID]:
        return [r["id"] for r in await self._pool.fetch("select id from auctions where status = 'OPEN'")]

    async def is_closed(self, auction_id: uuid.UUID) -> bool:
        return bool(await self._pool.fetchval("select status = 'CLOSED' from auctions where id = $1", auction_id))


class AuctionStateLoader:
    """Monta o estado vivo de um leilão no Valkey a partir do Postgres (criação e reidratação).

    Se o Valkey reiniciar, o seq continua de max(seq) e não colide com UNIQUE(auction_id, seq). A janela
    de perda é a dos lances aceitos e ainda não persistidos pelo consumer. Se o Valkey estiver atrás do
    banco (failover para réplica atrasada), o estado avança para o do banco.
    """

    LOADED = 1
    CAUGHT_UP = 2

    def __init__(self, auctions: AuctionRepository, pool: asyncpg.Pool, redis: Redis, scripts: LuaScripts,
                 settings: Settings):
        self._auctions = auctions
        self._pool = pool
        self._redis = redis
        self._scripts = scripts
        self._settings = settings

    async def load_id(self, auction_id: uuid.UUID) -> bool:
        return await self.load(await self._auctions.get(auction_id))

    async def load(self, a: Auction) -> bool:
        """True se o estado foi carregado ou corrigido agora; False se já estava em dia no Valkey."""
        # A fonte do seq é a tabela de lances, não a projeção: é o que as constraints protegem.
        last = await self._pool.fetchrow(
            "select seq, amount, user_id from bids where auction_id = $1 order by seq desc limit 1", a.id)
        seq, amount, leader = (last["seq"], last["amount"], last["user_id"]) if last else (0, 0, "")
        s = self._settings

        result = await self._scripts.hydrate_auction(
            keys=[common.auction_key(a.id), common.stream_key(a.id)],
            args=[a.status, a.starting_price, a.min_increment, amount, leader, seq, from_datetime(a.ends_at),
                  a.anti_sniping_window_ms, a.anti_sniping_extend_ms, s.consumer_group, str(a.id), s.events_channel])

        if result == self.CAUGHT_UP:
            log.warning("Valkey estava atrás do Postgres e foi corrigido auctionId=%s seq=%s", a.id, seq)
            return True
        if result != self.LOADED:
            return False
        if a.status == OPEN:
            # Fora do script porque são chaves globais (outro slot do cluster). Ambos idempotentes.
            await self._redis.zadd(common.ENDING, {str(a.id): from_datetime(a.ends_at)})
            await self._redis.sadd(common.ACTIVE_STREAMS, str(a.id))
        else:
            await self._redis.expire(common.auction_key(a.id), s.closed_retention_s)
            await self._redis.expire(common.stream_key(a.id), s.closed_retention_s)
        log.info("estado do leilão carregado no Valkey auctionId=%s status=%s seq=%s", a.id, a.status, seq)
        return True


class AuctionService:
    def __init__(self, auctions: AuctionRepository, loader: AuctionStateLoader, redis: Redis, clock: RedisClock):
        self._auctions = auctions
        self._loader = loader
        self._redis = redis
        self._clock = clock

    async def create(self, req: CreateAuctionRequest) -> dict[str, Any]:
        """Grava primeiro no Postgres (fonte da verdade do cadastro) e depois carrega no Valkey.

        Se o segundo passo falhar, o reconciliador ou o primeiro lance reidratam o leilão.
        """
        now = await self._clock.now_ms()
        a = Auction(
            id=uuid.uuid4(),
            title=req.title,
            starting_price=to_cents(req.startingPrice),
            min_increment=to_cents(req.minIncrement),
            anti_sniping_window_ms=req.antiSnipingWindowSeconds * 1000,
            anti_sniping_extend_ms=req.antiSnipingExtendSeconds * 1000,
            status=OPEN,
            created_at=common.to_datetime(now),
            ends_at=common.to_datetime(now + req.durationSeconds * 1000),
        )
        await self._auctions.insert(a)
        await self._loader.load(a)
        return await self.get(a.id)

    async def get(self, auction_id: uuid.UUID) -> dict[str, Any]:
        """Estado vivo vem do Valkey; o cadastro (título, preços), do Postgres."""
        a = await self._auctions.get(auction_id)
        live = await self._redis.hgetall(common.auction_key(auction_id))
        if not live:
            await self._loader.load(a)
            live = await self._redis.hgetall(common.auction_key(auction_id))
        now = await self._clock.now_ms()

        status = live["status"]
        seq = int(live["seq"])
        current_price = int(live["currentPrice"])
        ends_at = int(live["endsAt"])
        leader = live.get("leaderId") or None
        minimum_next = a.starting_price if seq == 0 else current_price + a.min_increment
        closed = status == CLOSED

        return without_nulls({
            "id": str(a.id),
            "title": a.title,
            "status": status,
            "acceptingBids": not closed and now < ends_at,
            "startingPrice": from_cents(a.starting_price),
            "minIncrement": from_cents(a.min_increment),
            "currentPrice": from_cents(current_price) if seq else None,
            "minimumNextBid": None if closed else from_cents(minimum_next),
            "leaderId": leader,
            "seq": seq,
            "endsAt": iso(ends_at),
            "serverTime": iso(now),
            "winnerId": leader if closed and seq else None,
            "finalPrice": from_cents(current_price) if closed and seq else None,
        })

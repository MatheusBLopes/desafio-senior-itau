"""Criação e consulta de leilões: cadastro no Postgres, estado vivo no Valkey."""

import uuid
from typing import Any

from redis.asyncio import Redis

from ..core.money import from_cents, to_cents
from ..core.serialization import without_nulls
from ..core.time import iso, to_datetime
from ..storage import keys
from ..storage.valkey import RedisClock
from .models import CLOSED, OPEN, Auction, CreateAuctionRequest
from .repository import AuctionRepository
from .state_loader import AuctionStateLoader


class AuctionService:
    """Casos de uso de POST /auctions e GET /auctions/{id}."""

    def __init__(self, auctions: AuctionRepository, loader: AuctionStateLoader, redis: Redis, clock: RedisClock):
        """Recebe o repositório (cadastro), o loader (estado no Valkey) e o relógio do Valkey."""
        self._auctions = auctions
        self._loader = loader
        self._redis = redis
        self._clock = clock

    async def create(self, request: CreateAuctionRequest) -> dict[str, Any]:
        """Grava primeiro no Postgres (fonte da verdade do cadastro) e depois carrega no Valkey.

        Se o segundo passo falhar, o reconciliador ou o primeiro lance reidratam o leilão.
        """
        now = await self._clock.now_ms()
        auction = Auction(
            id=uuid.uuid4(),
            title=request.title,
            starting_price=to_cents(request.startingPrice),
            min_increment=to_cents(request.minIncrement),
            anti_sniping_window_ms=request.antiSnipingWindowSeconds * 1000,
            anti_sniping_extend_ms=request.antiSnipingExtendSeconds * 1000,
            status=OPEN,
            created_at=to_datetime(now),
            ends_at=to_datetime(now + request.durationSeconds * 1000),
        )
        await self._auctions.insert(auction)
        await self._loader.load(auction)
        return await self.get(auction.id)

    async def get(self, auction_id: uuid.UUID) -> dict[str, Any]:
        """Estado vivo vem do Valkey; o cadastro (título, preços), do Postgres."""
        auction = await self._auctions.get(auction_id)
        live = await self._redis.hgetall(keys.auction_key(auction_id))
        if not live:
            await self._loader.load(auction)
            live = await self._redis.hgetall(keys.auction_key(auction_id))
        now = await self._clock.now_ms()

        status = live["status"]
        seq = int(live["seq"])
        current_price = int(live["currentPrice"])
        ends_at = int(live["endsAt"])
        leader = live.get("leaderId") or None
        minimum_next = auction.starting_price if seq == 0 else current_price + auction.min_increment
        closed = status == CLOSED

        return without_nulls({
            "id": str(auction.id),
            "title": auction.title,
            "status": status,
            "acceptingBids": not closed and now < ends_at,
            "startingPrice": from_cents(auction.starting_price),
            "minIncrement": from_cents(auction.min_increment),
            "currentPrice": from_cents(current_price) if seq else None,
            "minimumNextBid": None if closed else from_cents(minimum_next),
            "leaderId": leader,
            "seq": seq,
            "endsAt": iso(ends_at),
            "serverTime": iso(now),
            "winnerId": leader if closed and seq else None,
            "finalPrice": from_cents(current_price) if closed and seq else None,
        })

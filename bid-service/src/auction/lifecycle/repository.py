"""Acesso à tabela auctions no Postgres, a fonte da verdade do cadastro."""

import uuid

import asyncpg

from ..core.errors import ApiError
from .models import Auction


class AuctionRepository:
    """Consultas e inserção de leilões. SQL puro com asyncpg, sem ORM."""

    def __init__(self, pool: asyncpg.Pool):
        """Usa o pool de conexões do Postgres da task."""
        self._pool = pool

    async def insert(self, auction: Auction) -> None:
        """Grava um leilão novo."""
        await self._pool.execute("""
            insert into auctions (id, title, starting_price, min_increment, anti_sniping_window_ms,
                                  anti_sniping_extend_ms, status, created_at, ends_at)
            values ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """, auction.id, auction.title, auction.starting_price, auction.min_increment,
            auction.anti_sniping_window_ms, auction.anti_sniping_extend_ms, auction.status, auction.created_at,
            auction.ends_at)

    async def find(self, auction_id: uuid.UUID) -> Auction | None:
        """O leilão, ou None se não existe."""
        row = await self._pool.fetchrow("select * from auctions where id = $1", auction_id)
        return Auction.from_row(row) if row else None

    async def get(self, auction_id: uuid.UUID) -> Auction:
        """O leilão, ou 404 se não existe."""
        auction = await self.find(auction_id)
        if auction is None:
            raise ApiError.not_found(f"leilão {auction_id} não existe")
        return auction

    async def open_ids(self) -> list[uuid.UUID]:
        """Ids de todos os leilões abertos (usado pelo reconciliador)."""
        return [row["id"] for row in await self._pool.fetch("select id from auctions where status = 'OPEN'")]

    async def is_closed(self, auction_id: uuid.UUID) -> bool:
        """True se o encerramento do leilão já foi persistido."""
        return bool(await self._pool.fetchval("select status = 'CLOSED' from auctions where id = $1", auction_id))

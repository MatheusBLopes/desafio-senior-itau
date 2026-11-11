"""Carga do estado vivo de um leilão no Valkey a partir do Postgres (criação e reidratação)."""

import logging
import uuid

import asyncpg
from redis.asyncio import Redis

from ..config import Settings
from ..core.time import from_datetime
from ..storage import keys
from ..storage.scripts import LuaScripts
from .models import OPEN, Auction
from .repository import AuctionRepository

log = logging.getLogger(__name__)


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
        """Lê os lances do Postgres e grava o estado no Valkey pelo hydrate_auction.lua."""
        self._auctions = auctions
        self._pool = pool
        self._redis = redis
        self._scripts = scripts
        self._settings = settings

    async def load_id(self, auction_id: uuid.UUID) -> bool:
        """Carrega o leilão pelo id (404 se ele não existe no Postgres)."""
        return await self.load(await self._auctions.get(auction_id))

    async def load(self, auction: Auction) -> bool:
        """True se o estado foi carregado ou corrigido agora; False se já estava em dia no Valkey."""
        # A fonte do seq é a tabela de lances, não a projeção: é o que as constraints protegem.
        last_bid = await self._pool.fetchrow(
            "select seq, amount, user_id from bids where auction_id = $1 order by seq desc limit 1", auction.id)
        if last_bid:
            seq, current_price, leader_id = last_bid["seq"], last_bid["amount"], last_bid["user_id"]
        else:
            seq, current_price, leader_id = 0, 0, ""

        result = await self._scripts.hydrate_auction(
            auction.id, status=auction.status, starting_price=auction.starting_price,
            min_increment=auction.min_increment, current_price=current_price, leader_id=leader_id, seq=seq,
            ends_at_ms=from_datetime(auction.ends_at), extend_window_ms=auction.anti_sniping_window_ms,
            extend_by_ms=auction.anti_sniping_extend_ms)

        if result == self.CAUGHT_UP:
            log.warning("Valkey estava atrás do Postgres e foi corrigido auctionId=%s seq=%s", auction.id, seq)
            return True
        if result != self.LOADED:
            return False
        retention_s = self._settings.closed_retention_s
        if auction.status == OPEN:
            # Fora do script porque são chaves globais (outro slot do cluster). Ambos idempotentes.
            await self._redis.zadd(keys.ENDING, {str(auction.id): from_datetime(auction.ends_at)})
            await self._redis.sadd(keys.ACTIVE_STREAMS, str(auction.id))
        else:
            await self._redis.expire(keys.auction_key(auction.id), retention_s)
            await self._redis.expire(keys.stream_key(auction.id), retention_s)
        log.info("estado do leilão carregado no Valkey auctionId=%s status=%s seq=%s", auction.id, auction.status, seq)
        return True

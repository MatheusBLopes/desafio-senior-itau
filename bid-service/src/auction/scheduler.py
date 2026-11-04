"""Tarefas periódicas de todas as tasks: encerramento de leilões e reconciliação Postgres → Valkey."""

import asyncio
import logging
import uuid

from redis.asyncio import Redis

from . import common
from .auctions import AuctionRepository, AuctionStateLoader
from .common import ApiError, RedisClock
from .config import Settings
from .lua import LuaScripts

log = logging.getLogger(__name__)


class AuctionCloser:
    """Encerra leilões vencidos.

    Roda em todas as tasks: o close_auction.lua é idempotente, então a concorrência entre elas não gera
    efeito duplicado. O ZSET guarda o prazo conhecido; se o anti-sniping estendeu, o Lua devolve
    PENDING com o prazo novo e o score é atualizado. Cada leilão é tratado isoladamente: um que falha
    não impede o encerramento dos outros.
    """

    def __init__(self, redis: Redis, scripts: LuaScripts, loader: AuctionStateLoader, clock: RedisClock,
                 settings: Settings):
        self._redis = redis
        self._scripts = scripts
        self._loader = loader
        self._clock = clock
        self._s = settings

    async def run(self) -> None:
        while True:
            await asyncio.sleep(self._s.closer_interval_s)
            await self.close_due()

    async def close_due(self) -> None:
        try:
            due = await self._redis.zrangebyscore(common.ENDING, 0, await self._clock.now_ms(), start=0, num=100)
        except Exception as e:
            log.warning("falha ao buscar leilões vencidos: %s", e)
            return
        for member in due:
            aid = common.auction_id_or_none(member)
            if aid is None:
                await self._remove_orphan(member, "não é um UUID")
                continue
            try:
                await self.close(aid)
            except ApiError as e:
                if e.status != 404:
                    log.warning("falha ao encerrar o leilão %s: %s", aid, e)
                    continue
                # Ex.: banco restaurado de um backup anterior à criação do leilão (DR).
                await self._remove_orphan(member, "não existe no Postgres")
            except Exception as e:
                log.warning("falha ao encerrar o leilão %s: %s", aid, e)

    async def _remove_orphan(self, member: str, reason: str) -> None:
        log.warning("entrada órfã removida de %s: %s (%s)", common.ENDING, member, reason)
        await self._redis.zrem(common.ENDING, member)

    async def close(self, aid: uuid.UUID) -> None:
        kind, ends_at = await self._scripts.close_auction(
            keys=[common.auction_key(aid), common.stream_key(aid)],
            args=[str(aid), self._s.events_channel, self._s.stream_max_len])
        if kind == "CLOSED":
            await self._redis.zrem(common.ENDING, str(aid))
            log.info("leilão encerrado auctionId=%s", aid)
        elif kind == "ALREADY_CLOSED":
            await self._redis.zrem(common.ENDING, str(aid))
        elif kind == "PENDING":
            await self._redis.zadd(common.ENDING, {str(aid): int(ends_at)})
        elif kind == "NOT_LOADED":
            await self._loader.load_id(aid)
        else:
            log.warning("resposta inesperada do close_auction.lua: %s", kind)


class AuctionReconciler:
    """Garante que todo leilão OPEN no Postgres está carregado no Valkey (ZSET e stream ativo incluídos).

    Cobre restart do Valkey sem lances chegando e falha entre os dois passos da criação. O
    carregamento é idempotente: se o estado já existe, nada muda.
    """

    def __init__(self, auctions: AuctionRepository, loader: AuctionStateLoader, settings: Settings):
        self._auctions = auctions
        self._loader = loader
        self._s = settings

    async def run(self) -> None:
        while True:
            await self.reconcile()
            await asyncio.sleep(self._s.reconcile_interval_s)

    async def reconcile(self) -> None:
        try:
            reloaded = 0
            for aid in await self._auctions.open_ids():
                a = await self._auctions.find(aid)
                if a is not None and await self._loader.load(a):
                    reloaded += 1
            if reloaded:
                log.warning("%s leilões abertos foram recarregados no Valkey a partir do Postgres", reloaded)
        except Exception as e:
            log.warning("falha na reconciliação: %s", e)

"""Tarefas periódicas de todas as tasks: encerramento de leilões e reconciliação Postgres → Valkey."""

import asyncio
import logging
import uuid

from redis.asyncio import Redis

from ..config import Settings
from ..core.errors import ApiError
from ..core.money import from_cents
from ..core.time import iso
from ..observability.logs import fields
from ..storage import keys
from ..storage.scripts import LuaScripts
from ..storage.valkey import RedisClock
from .repository import AuctionRepository
from .state_loader import AuctionStateLoader

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
        """Dependências do encerramento; o intervalo vem de closer_interval_s."""
        self._redis = redis
        self._scripts = scripts
        self._loader = loader
        self._clock = clock
        self._settings = settings

    async def run(self) -> None:
        """Tarefa de fundo: close_due a cada closer_interval_s (200 ms por padrão)."""
        while True:
            await asyncio.sleep(self._settings.closer_interval_s)
            await self.close_due()

    async def close_due(self) -> None:
        """Tenta encerrar até 100 leilões cujo prazo conhecido já passou."""
        try:
            due = await self._redis.zrangebyscore(keys.ENDING, 0, await self._clock.now_ms(), start=0, num=100)
        except Exception as e:
            log.warning("falha ao buscar leilões vencidos", extra=fields(error=e))
            return
        for member in due:
            auction_id = keys.auction_id_or_none(member)
            if auction_id is None:
                await self._remove_orphan(member, "não é um UUID")
                continue
            try:
                await self.close(auction_id)
            except ApiError as e:
                if e.status != 404:
                    log.warning("falha ao encerrar o leilão", extra=fields(auctionId=auction_id, error=e))
                    continue
                # Ex.: banco restaurado de um backup anterior à criação do leilão (DR).
                await self._remove_orphan(member, "não existe no Postgres")
            except Exception as e:
                log.warning("falha ao encerrar o leilão", extra=fields(auctionId=auction_id, error=e))

    async def _remove_orphan(self, member: str, reason: str) -> None:
        """Tira do ZSET uma entrada que nunca vai encerrar, para ela não travar o ciclo."""
        log.warning("entrada órfã removida da agenda de encerramento", extra=fields(member=member, reason=reason))
        await self._redis.zrem(keys.ENDING, member)

    async def close(self, auction_id: uuid.UUID) -> None:
        """Chama o close_auction.lua e atualiza o ZSET conforme a resposta."""
        outcome_kind, ends_at = await self._scripts.close_auction(auction_id)
        if outcome_kind == "CLOSED":
            await self._redis.zrem(keys.ENDING, str(auction_id))
            await self._log_closed(auction_id, ends_at)
        elif outcome_kind == "ALREADY_CLOSED":
            await self._redis.zrem(keys.ENDING, str(auction_id))
        elif outcome_kind == "PENDING":
            await self._redis.zadd(keys.ENDING, {str(auction_id): ends_at})
            log.info("encerramento adiado: prazo prorrogado por lance no fim",
                     extra=fields(auctionId=auction_id, endsAt=iso(ends_at)))
        elif outcome_kind == "NOT_LOADED":
            await self._loader.load_id(auction_id)
        else:
            log.warning("resposta inesperada do close_auction.lua",
                        extra=fields(auctionId=auction_id, kind=outcome_kind))

    async def _log_closed(self, auction_id: uuid.UUID, ends_at: int) -> None:
        """Registra o encerramento com o vencedor. Só a task que encerrou chega aqui: uma linha por leilão."""
        leader, price, seq = await self._redis.hmget(keys.auction_key(auction_id), "leaderId", "currentPrice", "seq")
        bids = int(seq or 0)
        log.info("leilão encerrado", extra=fields(
            auctionId=auction_id, endsAt=iso(ends_at), acceptedBids=bids,
            winnerId=leader if bids else None, finalPrice=from_cents(int(price)) if bids else None))


class AuctionReconciler:
    """Garante que todo leilão OPEN no Postgres está carregado no Valkey (ZSET e stream ativo incluídos).

    Cobre restart do Valkey sem lances chegando e falha entre os dois passos da criação. O
    carregamento é idempotente: se o estado já existe, nada muda.
    """

    def __init__(self, auctions: AuctionRepository, loader: AuctionStateLoader, settings: Settings):
        """Dependências da reconciliação; o intervalo vem de reconcile_interval_s."""
        self._auctions = auctions
        self._loader = loader
        self._settings = settings

    async def run(self) -> None:
        """Tarefa de fundo: reconcile no startup e depois a cada reconcile_interval_s (30 s por padrão)."""
        while True:
            await self.reconcile()
            await asyncio.sleep(self._settings.reconcile_interval_s)

    async def reconcile(self) -> None:
        """Carrega no Valkey todo leilão aberto que estiver faltando ou atrasado."""
        try:
            reloaded = 0
            for auction_id in await self._auctions.open_ids():
                auction = await self._auctions.find(auction_id)
                if auction is not None and await self._loader.load(auction):
                    reloaded += 1
            if reloaded:
                log.warning("leilões abertos recarregados no Valkey a partir do Postgres", extra=fields(count=reloaded))
        except Exception as e:
            log.warning("falha na reconciliação", extra=fields(error=e))

"""Write-behind: drena os streams bids:{id} via consumer group e grava no Postgres."""

import asyncio
import logging
import socket
import time
import uuid
from typing import Any

import asyncpg
from redis.asyncio import Redis
from redis.exceptions import ResponseError

from . import common
from .auctions import AuctionRepository, AuctionStateLoader
from .config import Settings
from .observability import STREAM_BACKLOG, Metrics

log = logging.getLogger(__name__)

Record = tuple[str, dict[str, str]]  # (id do stream, campos)


class BidPersister:
    """Grava um lote de eventos do stream numa transação.

    Tudo é idempotente: o mesmo evento pode chegar de novo (ACK perdido, XCLAIM por outra task) sem
    efeito duplicado. Um lance que colide com outro já gravado (mesmo seq ou mesma idempotency key,
    com conteúdo diferente) não é descartado: vai para bid_conflicts e é contado no retorno.
    """

    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def persist(self, auction_id: uuid.UUID, records: list[Record]) -> int:
        """Devolve quantos lances colidiram com outro já gravado e foram para bid_conflicts."""
        bids = [v for _, v in records if v.get("type") == "BID"]
        closed = next((v for _, v in records if v.get("type") == "CLOSED"), None)
        conflicts = 0

        async with self._pool.acquire() as conn, conn.transaction():
            if bids:
                rows = [(int(b["seq"]), b["userId"], int(b["amount"]), b["idemKey"],
                         common.to_datetime(int(b["ts"]))) for b in bids]
                # Segunda barreira: PK (auction_id, seq) e UNIQUE (auction_id, user_id, idempotency_key).
                # Um INSERT só para o lote inteiro (unnest); o RETURNING diz quais linhas entraram.
                inserted = {r["seq"] for r in await conn.fetch("""
                    insert into bids (auction_id, seq, user_id, amount, idempotency_key, accepted_at)
                    select $1, * from unnest($2::bigint[], $3::text[], $4::bigint[], $5::text[], $6::timestamptz[])
                    on conflict do nothing
                    returning seq
                    """, auction_id, *map(list, zip(*rows)))}
                collided = [r for r in rows if r[0] not in inserted and not await self._already_persisted(
                    conn, auction_id, r)]
                if collided:
                    await conn.executemany("""
                        insert into bid_conflicts (auction_id, seq, user_id, amount, idempotency_key, accepted_at)
                        values ($1, $2, $3, $4, $5, $6)
                        on conflict do nothing
                        """, [(auction_id, *r) for r in collided])
                    conflicts = len(collided)

                last = max(bids, key=lambda b: int(b["seq"]))
                max_ends_at = max(int(b["endsAt"]) for b in bids)
                # Projeção: só avança (lotes de consumers diferentes podem chegar fora de ordem).
                await conn.execute("""
                    update auctions
                       set current_price = case when last_seq < $1 then $2 else current_price end,
                           leader_id     = case when last_seq < $1 then $3 else leader_id end,
                           last_seq      = greatest(last_seq, $1),
                           ends_at       = greatest(ends_at, $4)
                     where id = $5
                    """, int(last["seq"]), int(last["amount"]), last["userId"], common.to_datetime(max_ends_at),
                    auction_id)

            if closed is not None:
                winner = closed.get("winnerId") or None
                await conn.execute("""
                    update auctions set status = 'CLOSED', closed_at = $1, winner_id = $2, final_price = $3
                     where id = $4 and status = 'OPEN'
                    """, common.to_datetime(int(closed["ts"])), winner,
                    int(closed["finalPrice"]) if winner else None, auction_id)
        return conflicts

    @staticmethod
    async def _already_persisted(conn: asyncpg.Connection, auction_id: uuid.UUID, row: tuple[Any, ...]) -> bool:
        """Reentrega do mesmo evento: a linha já existe com o mesmo conteúdo."""
        seq, user_id, amount, key, _ = row
        return await conn.fetchval("""
            select exists (select 1 from bids where auction_id = $1 and seq = $2 and user_id = $3
                                                and amount = $4 and idempotency_key = $5)
            """, auction_id, seq, user_id, amount, key)


class BidStreamConsumer:
    """Cada task é um consumer do grupo, então a carga se divide.

    Só faz ACK depois do commit; eventos pendentes de uma task que morreu são reivindicados (XCLAIM)
    por outra. Lê um stream por vez (sem XREADGROUP multi-chave) para funcionar também em cluster
    mode. A falha de um leilão (banco recusando o lote, entrada corrompida) não para os outros.

    Quando ler: o consumer não varre todos os streams em loop. Varrer custaria uma chamada ao Valkey
    por leilão aberto a cada volta, mesmo sem lances, e esse custo cresceria com leilões × tasks. Em vez
    disso, o Pub/Sub (que a task já assina) avisa quais leilões receberam eventos (notify) e só esses
    são lidos, na hora. Como o Pub/Sub não garante entrega, uma varredura completa a cada intervalo de
    manutenção pega o que escapou: o atraso máximo de persistência de um aviso perdido é esse intervalo.
    """

    def __init__(self, redis: Redis, persister: BidPersister, auctions: AuctionRepository,
                 loader: AuctionStateLoader, metrics: Metrics, settings: Settings):
        self._redis = redis
        self._persister = persister
        self._auctions = auctions
        self._loader = loader
        self._metrics = metrics
        self._s = settings
        self.consumer_name = f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"
        self._task: asyncio.Task[None] | None = None
        self._next_sweep = 0.0
        self._dirty: set[uuid.UUID] = set()
        self._retry: set[uuid.UUID] = set()  # leilões com lote lido e não gravado por ESTE consumer
        self._wake = asyncio.Event()

    def notify(self, auction_id: uuid.UUID) -> None:
        """Chamado pelo listener do Pub/Sub: o stream deste leilão tem eventos novos."""
        self._dirty.add(auction_id)
        self._wake.set()

    def start(self) -> None:
        self._next_sweep = 0.0  # o que chegou enquanto estava parado é pego na primeira varredura
        self._task = asyncio.create_task(self._loop(), name="bid-stream-consumer")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _loop(self) -> None:
        while True:
            try:
                sweep = time.monotonic() >= self._next_sweep
                active: set[str] = set()
                targets, self._dirty = self._dirty, set()
                self._wake.clear()
                if sweep:
                    active = await self._redis.smembers(common.ACTIVE_STREAMS)
                    targets |= await self._valid_ids(active)
                failed = False
                for aid in targets:
                    try:
                        if await self._drain(aid) >= self._s.consumer_batch_size:
                            self._dirty.add(aid)  # lote cheio: o stream ainda tem mais
                    except Exception as e:
                        # O lote fica pendente com este consumer, que tenta de novo na próxima volta (1 s),
                        # sem esperar a reivindicação de 30 s; os outros leilões seguem.
                        failed = True
                        self._retry.add(aid)
                        self._dirty.add(aid)
                        log.warning("falha ao persistir o stream do leilão %s: %s", aid, e)
                if sweep:
                    await self._maintenance(active)
                    self._next_sweep = time.monotonic() + self._s.consumer_maintenance_interval_s
                if failed:
                    await asyncio.sleep(1)
                elif not self._dirty:
                    # Dorme até um aviso do Pub/Sub ou até a próxima varredura.
                    try:
                        await asyncio.wait_for(self._wake.wait(), max(0.0, self._next_sweep - time.monotonic()))
                    except TimeoutError:
                        pass
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("falha no consumer do stream, tentando de novo: %s", e)
                await asyncio.sleep(1)

    async def _valid_ids(self, members: set[str]) -> set[uuid.UUID]:
        ids = set()
        for member in members:
            aid = common.auction_id_or_none(member)
            if aid is None:
                log.warning("membro inválido removido de %s: %s", common.ACTIVE_STREAMS, member)
                await self._redis.srem(common.ACTIVE_STREAMS, member)
            else:
                ids.add(aid)
        return ids

    async def _drain(self, auction_id: uuid.UUID) -> int:
        """Lê e grava o próximo lote. Depois de uma falha, relê primeiro os pendentes deste consumer (ID "0")."""
        key = common.stream_key(auction_id)
        position = "0" if auction_id in self._retry else ">"
        try:
            reply = await self._redis.xreadgroup(self._s.consumer_group, self.consumer_name, {key: position},
                                                 count=self._s.consumer_batch_size)
        except ResponseError as e:
            if "NOGROUP" in str(e):
                return 0  # stream sumiu (restart do Valkey); a reidratação recria o grupo
            raise
        records = [r for r in (reply[0][1] if reply else []) if r[1]]  # pendente já cortado pelo MAXLEN vem vazio
        if not records:
            if position == "0":
                self._retry.discard(auction_id)
                self._dirty.add(auction_id)  # nada pendente: volta a ler os novos
            return 0
        await self._persist_and_ack(auction_id, key, records)
        return len(records)

    async def _persist_and_ack(self, auction_id: uuid.UUID, key: str, records: list[Record]) -> None:
        collided = await self._persister.persist(auction_id, records)
        await self._redis.xack(key, self._s.consumer_group, *[r[0] for r in records])
        if collided:
            self._metrics.conflicts(collided)
            log.error("%s lances aceitos colidiram com outros já gravados e foram para bid_conflicts auctionId=%s",
                      collided, auction_id)
            # Se o Valkey ficou atrás do banco (failover), alcança o banco antes de aceitar mais lances.
            await self._loader.load_id(auction_id)

    async def _maintenance(self, active: set[str]) -> None:
        """Reivindica pendentes de consumers mortos, mede o backlog e limpa streams de leilões encerrados."""
        total = 0
        for member in active:
            aid = common.auction_id_or_none(member)
            if aid is None:
                continue
            key = common.stream_key(aid)
            try:
                await self._claim_stale(aid, key)
                pending = undelivered = 0
                for g in await self._redis.xinfo_groups(key):
                    if g["name"] == self._s.consumer_group:
                        pending = g["pending"]
                        undelivered = await self._undelivered(key, g["last-delivered-id"])
                total += pending + undelivered
                if pending == 0 and undelivered == 0 and await self._auctions.is_closed(aid):
                    # Tudo persistido: o leilão sai do conjunto ativo e da memória após a retenção.
                    await self._redis.srem(common.ACTIVE_STREAMS, member)
                    await self._redis.expire(key, self._s.closed_retention_s)
                    await self._redis.expire(common.auction_key(aid), self._s.closed_retention_s)
            except Exception as e:
                log.debug("manutenção do stream %s falhou: %s", key, e)
        STREAM_BACKLOG.set(total)

    async def _claim_stale(self, auction_id: uuid.UUID, key: str) -> None:
        stale = await self._redis.xpending_range(key, self._s.consumer_group, min="-", max="+",
                                                 count=self._s.consumer_batch_size,
                                                 idle=self._s.consumer_claim_min_idle_ms)
        if not stale:
            return
        claimed = await self._redis.xclaim(key, self._s.consumer_group, self.consumer_name,
                                           self._s.consumer_claim_min_idle_ms, [p["message_id"] for p in stale])
        claimed = [r for r in claimed if r[1]]  # entradas já cortadas pelo MAXLEN vêm sem campos
        if claimed:
            log.info("reivindicados %s eventos pendentes de outro consumer auctionId=%s", len(claimed), auction_id)
            await self._persist_and_ack(auction_id, key, claimed)

    async def _undelivered(self, key: str, last_delivered_id: str) -> int:
        """IDs são 0-<seq>, então a distância entre o último ID e o último entregue é o backlog."""
        info = await self._redis.xinfo_stream(key)

        def seq(record_id: str) -> int:
            return int(record_id.split("-", 1)[1])

        return max(0, seq(info["last-generated-id"]) - seq(last_delivered_id))

"""Write-behind: drena os streams bids:{id} via consumer group e grava no Postgres."""

import asyncio
import logging
import socket
import time
import uuid

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from ..config import Settings
from ..lifecycle.repository import AuctionRepository
from ..lifecycle.state_loader import AuctionStateLoader
from ..observability.logs import fields
from ..observability.metrics import STREAM_BACKLOG, Metrics
from ..storage import keys
from .persister import BidPersister, StreamRecord

log = logging.getLogger(__name__)


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
        """O nome do consumer é único por processo (hostname + sufixo aleatório), como o consumer group exige."""
        self._redis = redis
        self._persister = persister
        self._auctions = auctions
        self._loader = loader
        self._metrics = metrics
        self._settings = settings
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
        """Inicia o laço de consumo como tarefa de fundo."""
        self._next_sweep = 0.0  # o que chegou enquanto estava parado é pego na primeira varredura
        self._task = asyncio.create_task(self._loop(), name="bid-stream-consumer")

    async def stop(self) -> None:
        """Cancela o laço e espera ele terminar."""
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _loop(self) -> None:
        """Uma volta: escolhe os streams, drena, faz a manutenção se for hora e dorme até haver trabalho."""
        while True:
            try:
                sweep = time.monotonic() >= self._next_sweep
                targets, active = await self._collect_targets(sweep)
                failed = await self._drain_targets(targets)
                if sweep:
                    await self._maintenance(active)
                    self._next_sweep = time.monotonic() + self._settings.consumer_maintenance_interval_s
                await self._wait_for_work(failed)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("falha no consumer do stream; tentando de novo em 1 s", extra=fields(error=e))
                await asyncio.sleep(1)

    async def _collect_targets(self, sweep: bool) -> tuple[set[uuid.UUID], set[str]]:
        """Leilões a drenar nesta volta: os avisados pelo Pub/Sub e, na varredura, todos os ativos.

        Devolve também os membros brutos de ACTIVE_STREAMS (vazio fora da varredura) para a manutenção.
        """
        targets, self._dirty = self._dirty, set()
        self._wake.clear()
        active: set[str] = set()
        if sweep:
            active = await self._redis.smembers(keys.ACTIVE_STREAMS)
            targets |= await self._valid_ids(active)
        return targets, active

    async def _drain_targets(self, targets: set[uuid.UUID]) -> bool:
        """Drena cada leilão isoladamente; devolve True se algum falhou."""
        failed = False
        for auction_id in targets:
            try:
                if await self._drain(auction_id) >= self._settings.consumer_batch_size:
                    self._dirty.add(auction_id)  # lote cheio: o stream ainda tem mais
            except Exception as e:
                # O lote fica pendente com este consumer, que tenta de novo na próxima volta (1 s),
                # sem esperar a reivindicação de 30 s; os outros leilões seguem.
                failed = True
                self._retry.add(auction_id)
                self._dirty.add(auction_id)
                log.warning("falha ao gravar lances no banco; o lote será relido",
                            extra=fields(auctionId=auction_id, error=e))
        return failed

    async def _wait_for_work(self, failed: bool) -> None:
        """Depois de uma falha, espera 1 s; sem trabalho pendente, dorme até um aviso ou a próxima varredura."""
        if failed:
            await asyncio.sleep(1)
        elif not self._dirty:
            # Dorme até um aviso do Pub/Sub ou até a próxima varredura.
            try:
                await asyncio.wait_for(self._wake.wait(), max(0.0, self._next_sweep - time.monotonic()))
            except TimeoutError:
                pass

    async def _valid_ids(self, members: set[str]) -> set[uuid.UUID]:
        """Converte os membros de ACTIVE_STREAMS em ids; remove os que não são UUID."""
        auction_ids = set()
        for member in members:
            auction_id = keys.auction_id_or_none(member)
            if auction_id is None:
                log.warning("entrada inválida removida da lista de streams", extra=fields(member=member))
                await self._redis.srem(keys.ACTIVE_STREAMS, member)
            else:
                auction_ids.add(auction_id)
        return auction_ids

    async def _drain(self, auction_id: uuid.UUID) -> int:
        """Lê e grava o próximo lote. Depois de uma falha, relê primeiro os pendentes deste consumer (ID "0")."""
        stream = keys.stream_key(auction_id)
        position = "0" if auction_id in self._retry else ">"
        try:
            reply = await self._redis.xreadgroup(self._settings.consumer_group, self.consumer_name,
                                                 {stream: position}, count=self._settings.consumer_batch_size)
        except ResponseError as e:
            if "NOGROUP" in str(e):
                return 0  # stream sumiu (restart do Valkey); a reidratação recria o grupo
            raise
        # Pendente já cortado pelo MAXLEN vem sem campos.
        records = [record for record in (reply[0][1] if reply else []) if record[1]]
        if not records:
            if position == "0":
                self._retry.discard(auction_id)
                self._dirty.add(auction_id)  # nada pendente: volta a ler os novos
            return 0
        await self._persist_and_ack(auction_id, stream, records)
        return len(records)

    async def _persist_and_ack(self, auction_id: uuid.UUID, stream: str, records: list[StreamRecord]) -> None:
        """Grava o lote e só depois confirma (XACK); havendo colisão, alinha o Valkey com o banco."""
        collided = await self._persister.persist(auction_id, records)
        await self._redis.xack(stream, self._settings.consumer_group, *[record_id for record_id, _ in records])
        seqs = [int(record["seq"]) for _, record in records if record.get("type") == "BID"]
        closed = any(record.get("type") == "CLOSED" for _, record in records)
        if seqs:
            log.info("lances gravados no banco" + (" com o encerramento" if closed else ""), extra=fields(
                auctionId=auction_id, count=len(seqs), firstSeq=min(seqs), lastSeq=max(seqs), closed=closed))
        elif closed:
            log.info("encerramento do leilão gravado no banco", extra=fields(auctionId=auction_id))
        if collided:
            self._metrics.conflicts(collided)
            log.error("lances aceitos colidiram com outros já gravados e foram para bid_conflicts",
                      extra=fields(auctionId=auction_id, count=collided))
            # Se o Valkey ficou atrás do banco (failover), alcança o banco antes de aceitar mais lances.
            await self._loader.load_id(auction_id)

    async def _maintenance(self, active: set[str]) -> None:
        """Reivindica pendentes de consumers mortos, mede o backlog e limpa streams de leilões encerrados."""
        total = 0
        for member in active:
            auction_id = keys.auction_id_or_none(member)
            if auction_id is None:
                continue
            stream = keys.stream_key(auction_id)
            try:
                await self._claim_stale(auction_id, stream)
                pending = undelivered = 0
                for group in await self._redis.xinfo_groups(stream):
                    if group["name"] == self._settings.consumer_group:
                        pending = group["pending"]
                        undelivered = await self._undelivered(stream, group["last-delivered-id"])
                total += pending + undelivered
                if pending == 0 and undelivered == 0 and await self._auctions.is_closed(auction_id):
                    # Tudo persistido: o leilão sai do conjunto ativo e da memória após a retenção.
                    # O SREM vai por último: se um EXPIRE falhar, o leilão continua no conjunto e a próxima
                    # varredura repete. Na ordem inversa, as chaves ficariam para sempre (noeviction).
                    retention_s = self._settings.closed_retention_s
                    await self._redis.expire(stream, retention_s)
                    await self._redis.expire(keys.auction_key(auction_id), retention_s)
                    await self._redis.srem(keys.ACTIVE_STREAMS, member)
            except Exception as e:
                log.debug("manutenção do stream falhou", extra=fields(stream=stream, error=e))
        STREAM_BACKLOG.set(total)

    async def _claim_stale(self, auction_id: uuid.UUID, stream: str) -> None:
        """Toma (XCLAIM) os eventos pendentes há mais de consumer_claim_min_idle_ms e os grava."""
        settings = self._settings
        stale = await self._redis.xpending_range(stream, settings.consumer_group, min="-", max="+",
                                                 count=settings.consumer_batch_size,
                                                 idle=settings.consumer_claim_min_idle_ms)
        if not stale:
            return
        claimed = await self._redis.xclaim(stream, settings.consumer_group, self.consumer_name,
                                           settings.consumer_claim_min_idle_ms,
                                           [pending["message_id"] for pending in stale])
        claimed = [record for record in claimed if record[1]]  # entradas já cortadas pelo MAXLEN vêm sem campos
        if claimed:
            log.info("eventos pendentes de outra task assumidos por esta",
                     extra=fields(auctionId=auction_id, count=len(claimed)))
            await self._persist_and_ack(auction_id, stream, claimed)

    async def _undelivered(self, stream: str, last_delivered_id: str) -> int:
        """IDs são 0-<seq>, então a distância entre o último ID e o último entregue é o backlog."""
        info = await self._redis.xinfo_stream(stream)

        def seq_of(record_id: str) -> int:
            """Parte do seq de um ID de stream 0-<seq>."""
            return int(record_id.split("-", 1)[1])

        return max(0, seq_of(info["last-generated-id"]) - seq_of(last_delivered_id))

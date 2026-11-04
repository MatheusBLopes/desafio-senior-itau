"""Tempo real: cache local alimentado pelo Pub/Sub do Valkey, conflation e entrega por WebSocket.

Cada task assina o canal de eventos do Valkey e mantém o último estado de cada leilão. Os clientes
se conectam em /ws/auctions/{id} e só escutam: o servidor nunca repassa mensagens de cliente.
"""

import asyncio
import logging
import re
import uuid
from dataclasses import dataclass
from collections.abc import Callable
from collections.abc import Callable
from typing import Any

import orjson
from fastapi import WebSocket, WebSocketDisconnect
from redis.asyncio import Redis

from .common import from_cents, iso, without_nulls
from .observability import WEBSOCKET_SESSIONS

log = logging.getLogger(__name__)

SNAPSHOT = "AUCTION_SNAPSHOT"
EXTENDED = "AUCTION_EXTENDED"
CLOSED = "AUCTION_CLOSED"

_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


@dataclass(frozen=True)
class Snapshot:
    auction_id: uuid.UUID
    seq: int
    current_price: int
    minimum_next: int
    leader_id: str
    ends_at: int
    status: str


class LiveAuctionCache:
    """Último estado conhecido de cada leilão nesta task. Serve ao pré-filtro e ao snapshot do WebSocket.

    Nunca decide aceite: isso é só do Lua.
    """

    def __init__(self) -> None:
        self._snapshots: dict[uuid.UUID, Snapshot] = {}

    def apply(self, incoming: Snapshot, replace: bool) -> Snapshot:
        """Mantém o de maior seq, exceto quando replace (reidratação: o preço pode ter voltado)."""
        current = self._snapshots.get(incoming.auction_id)
        if current is None or replace or incoming.seq >= current.seq:
            self._snapshots[incoming.auction_id] = incoming
            return incoming
        return current

    def get(self, auction_id: uuid.UUID) -> Snapshot | None:
        return self._snapshots.get(auction_id)

    def evict(self, auction_id: uuid.UUID) -> None:
        self._snapshots.pop(auction_id, None)


def to_event(event_type: str, s: Snapshot) -> dict[str, Any]:
    closed = s.status == "CLOSED"
    has_bids = s.seq > 0
    price = from_cents(s.current_price) if has_bids else None
    leader = s.leader_id if has_bids else None
    return without_nulls({
        "type": event_type,
        "auctionId": str(s.auction_id),
        "seq": s.seq,
        "status": s.status,
        "currentPrice": price,
        "minimumNextBid": None if closed else from_cents(s.minimum_next),
        "leaderId": leader,
        "endsAt": iso(s.ends_at),
        "winnerId": leader if closed else None,
        "finalPrice": price if closed else None,
    })


class _Subscriber:
    """Uma conexão WebSocket com fila própria: um cliente lento não atrasa os outros."""

    QUEUE_SIZE = 32

    def __init__(self, ws: WebSocket):
        self.ws = ws
        self.queue: asyncio.Queue[str] = asyncio.Queue(self.QUEUE_SIZE)


class SnapshotBroadcaster:
    """Conflation: com milhares de lances por segundo, mandar cada lance a cada cliente é inviável.

    Os leilões que mudaram são marcados e, a cada intervalo, cada um recebe só o último estado.
    AUCTION_EXTENDED e AUCTION_CLOSED saem na hora.
    """

    def __init__(self, cache: LiveAuctionCache):
        self._cache = cache
        self._dirty: set[uuid.UUID] = set()
        self._subscribers: dict[uuid.UUID, set[_Subscriber]] = {}

    def mark_dirty(self, auction_id: uuid.UUID) -> None:
        self._dirty.add(auction_id)

    def send_now(self, event_type: str, s: Snapshot) -> None:
        self._publish(s.auction_id, to_event(event_type, s))

    def flush(self) -> None:
        dirty, self._dirty = self._dirty, set()
        for auction_id in dirty:
            s = self._cache.get(auction_id)
            if s is not None:
                self._publish(auction_id, to_event(SNAPSHOT, s))

    async def run(self, interval_s: float) -> None:
        while True:
            await asyncio.sleep(interval_s)
            self.flush()

    def _publish(self, auction_id: uuid.UUID, event: dict[str, Any]) -> None:
        subscribers = self._subscribers.get(auction_id)
        if not subscribers:
            return
        message = orjson.dumps(event).decode()  # serializa uma vez para todos
        for sub in list(subscribers):
            try:
                sub.queue.put_nowait(message)
            except asyncio.QueueFull:
                # Cliente que não consome: desconecta em vez de acumular memória. Ele reconecta e
                # recupera o que perdeu com GET /auctions/{id}/bids?afterSeq=.
                subscribers.discard(sub)
                asyncio.ensure_future(sub.ws.close(code=1013, reason="cliente lento"))

    async def serve(self, ws: WebSocket, auction_id: str) -> None:
        """Atende uma conexão de /ws/auctions/{id} até ela fechar."""
        if not _UUID.match(auction_id):
            await ws.close(code=1008, reason="destino não permitido")
            return
        await ws.accept()
        aid = uuid.UUID(auction_id)
        sub = _Subscriber(ws)
        self._subscribers.setdefault(aid, set()).add(sub)
        WEBSOCKET_SESSIONS.inc()
        current = self._cache.get(aid)
        if current is not None:
            sub.queue.put_nowait(orjson.dumps(to_event(SNAPSHOT, current)).decode())
        writer = asyncio.create_task(self._write(sub))
        try:
            # Clientes só escutam. Qualquer mensagem recebida (como um AUCTION_CLOSED forjado) fecha a conexão.
            message = await ws.receive()
            if message["type"] != "websocket.disconnect":
                await ws.close(code=1008, reason="clientes não publicam eventos")
        except (WebSocketDisconnect, RuntimeError):
            pass  # cliente saiu, ou a conexão já foi fechada pelo servidor (cliente lento)
        finally:
            writer.cancel()
            subs = self._subscribers.get(aid)
            if subs is not None:
                subs.discard(sub)
                if not subs:
                    del self._subscribers[aid]
            WEBSOCKET_SESSIONS.dec()

    @staticmethod
    async def _write(sub: _Subscriber) -> None:
        try:
            while True:
                await sub.ws.send_text(await sub.queue.get())
        except Exception:
            pass  # a conexão caiu; o loop de leitura faz a limpeza


class AuctionEventListener:
    """Assina o canal de eventos do Valkey: atualiza o cache local e entrega ao broadcaster.

    Uma única corrotina processa as mensagens na ordem de publicação: um AUCTION_LOADED nunca é
    aplicado depois de um BID_ACCEPTED publicado após ele.
    """

    def __init__(self, redis: Redis, channel: str, cache: LiveAuctionCache, broadcaster: SnapshotBroadcaster,
                 on_stream_event: Callable[[uuid.UUID], None] = lambda _: None):
        self._redis = redis
        # Avisa o consumer do write-behind que o stream deste leilão tem eventos novos.
        self._on_stream_event = on_stream_event
        self._channel = channel
        self._cache = cache
        self._broadcaster = broadcaster
        self.subscribed = asyncio.Event()

    async def run(self) -> None:
        while True:
            pubsub = self._redis.pubsub(ignore_subscribe_messages=True)
            try:
                await pubsub.subscribe(self._channel)
                self.subscribed.set()
                async for message in pubsub.listen():
                    if message["type"] == "message":
                        self.on_message(message["data"])
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("Pub/Sub desconectado, reconectando: %s", e)
                await asyncio.sleep(1)
            finally:
                await pubsub.aclose()

    def on_message(self, data: str) -> None:
        try:
            n = orjson.loads(data)
            aid = uuid.UUID(n["auctionId"])
            kind = n["type"]
            if kind == "BID_ACCEPTED":
                s = self._cache.apply(Snapshot(aid, int(n["seq"]), int(n["amount"]), int(n["minimumNext"]),
                                               n["userId"], int(n["endsAt"]), "OPEN"), replace=False)
                self._broadcaster.mark_dirty(aid)
                self._on_stream_event(aid)
                if n.get("extended"):
                    self._broadcaster.send_now(EXTENDED, s)
            elif kind == "AUCTION_LOADED":
                # Reidratação: o estado pode ter voltado (lances perdidos), então substitui sem olhar seq.
                self._cache.apply(Snapshot(aid, int(n["seq"]), int(n["amount"]), int(n["minimumNext"]),
                                           n["userId"], int(n["endsAt"]), n["status"]), replace=True)
                self._broadcaster.mark_dirty(aid)
            elif kind == "AUCTION_CLOSED":
                s = self._cache.apply(Snapshot(aid, int(n["seq"]), int(n["finalPrice"]), 2**63 - 1,
                                               n["winnerId"], int(n["endsAt"]), "CLOSED"), replace=True)
                self._broadcaster.send_now(CLOSED, s)
                self._on_stream_event(aid)
        except Exception as e:
            log.warning("evento inválido no Pub/Sub: %s", e)

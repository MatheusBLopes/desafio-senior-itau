"""Entrega por WebSocket: conflation dos snapshots e uma fila por cliente.

Os clientes se conectam em /ws/auctions/{id} e só escutam: o servidor nunca repassa mensagens de cliente.
"""

import asyncio
import re
import uuid
from typing import Any

import orjson
from fastapi import WebSocket, WebSocketDisconnect

from ..core.money import from_cents
from ..core.serialization import without_nulls
from ..core.time import iso
from ..observability.metrics import WEBSOCKET_SESSIONS
from .cache import LiveAuctionCache, Snapshot

SNAPSHOT = "AUCTION_SNAPSHOT"
EXTENDED = "AUCTION_EXTENDED"
CLOSED = "AUCTION_CLOSED"

_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


def to_event(event_type: str, snapshot: Snapshot) -> dict[str, Any]:
    """Mensagem JSON enviada ao cliente: valores em string com 2 casas, vencedor só no encerramento."""
    closed = snapshot.status == "CLOSED"
    has_bids = snapshot.seq > 0
    price = from_cents(snapshot.current_price) if has_bids else None
    leader = snapshot.leader_id if has_bids else None
    return without_nulls({
        "type": event_type,
        "auctionId": str(snapshot.auction_id),
        "seq": snapshot.seq,
        "status": snapshot.status,
        "currentPrice": price,
        "minimumNextBid": None if closed else from_cents(snapshot.minimum_next),
        "leaderId": leader,
        "endsAt": iso(snapshot.ends_at),
        "winnerId": leader if closed else None,
        "finalPrice": price if closed else None,
    })


class _Subscriber:
    """Uma conexão WebSocket com fila própria: um cliente lento não atrasa os outros."""

    QUEUE_SIZE = 32

    def __init__(self, ws: WebSocket):
        """Fila limitada a QUEUE_SIZE mensagens para esta conexão."""
        self.ws = ws
        self.queue: asyncio.Queue[str] = asyncio.Queue(self.QUEUE_SIZE)


class SnapshotBroadcaster:
    """Conflation: com milhares de lances por segundo, mandar cada lance a cada cliente é inviável.

    Os leilões que mudaram são marcados e, a cada intervalo, cada um recebe só o último estado.
    AUCTION_EXTENDED e AUCTION_CLOSED saem na hora.
    """

    def __init__(self, cache: LiveAuctionCache):
        """Começa sem clientes e sem leilões marcados."""
        self._cache = cache
        self._dirty: set[uuid.UUID] = set()
        self._subscribers: dict[uuid.UUID, set[_Subscriber]] = {}

    def mark_dirty(self, auction_id: uuid.UUID) -> None:
        """Marca o leilão para receber um snapshot no próximo intervalo."""
        self._dirty.add(auction_id)

    def send_now(self, event_type: str, snapshot: Snapshot) -> None:
        """Envia um evento sem esperar o intervalo (prorrogação e encerramento)."""
        self._publish(snapshot.auction_id, to_event(event_type, snapshot))

    def flush(self) -> None:
        """Envia o último estado de cada leilão marcado desde o intervalo anterior."""
        dirty, self._dirty = self._dirty, set()
        for auction_id in dirty:
            snapshot = self._cache.get(auction_id)
            if snapshot is not None:
                self._publish(auction_id, to_event(SNAPSHOT, snapshot))

    async def run(self, interval_s: float) -> None:
        """Tarefa de fundo: flush a cada interval_s (150 ms por padrão)."""
        while True:
            await asyncio.sleep(interval_s)
            self.flush()

    def _publish(self, auction_id: uuid.UUID, event: dict[str, Any]) -> None:
        """Põe o evento na fila de cada cliente do leilão; desconecta quem está com a fila cheia."""
        subscribers = self._subscribers.get(auction_id)
        if not subscribers:
            return
        message = orjson.dumps(event).decode()  # serializa uma vez para todos
        for subscriber in list(subscribers):
            try:
                subscriber.queue.put_nowait(message)
            except asyncio.QueueFull:
                # Cliente que não consome: desconecta em vez de acumular memória. Ele reconecta e
                # recupera o que perdeu com GET /auctions/{id}/bids?afterSeq=.
                subscribers.discard(subscriber)
                asyncio.ensure_future(subscriber.ws.close(code=1013, reason="cliente lento"))

    async def serve(self, ws: WebSocket, auction_id: str) -> None:
        """Atende uma conexão de /ws/auctions/{id} até ela fechar."""
        if not _UUID.match(auction_id):
            await ws.close(code=1008, reason="destino não permitido")
            return
        await ws.accept()
        parsed_id = uuid.UUID(auction_id)
        subscriber = _Subscriber(ws)
        self._subscribers.setdefault(parsed_id, set()).add(subscriber)
        WEBSOCKET_SESSIONS.inc()
        current = self._cache.get(parsed_id)
        if current is not None:
            subscriber.queue.put_nowait(orjson.dumps(to_event(SNAPSHOT, current)).decode())
        writer = asyncio.create_task(self._write(subscriber))
        try:
            # Clientes só escutam. Qualquer mensagem recebida (como um AUCTION_CLOSED forjado) fecha a conexão.
            message = await ws.receive()
            if message["type"] != "websocket.disconnect":
                await ws.close(code=1008, reason="clientes não publicam eventos")
        except (WebSocketDisconnect, RuntimeError):
            pass  # cliente saiu, ou a conexão já foi fechada pelo servidor (cliente lento)
        finally:
            writer.cancel()
            subscribers = self._subscribers.get(parsed_id)
            if subscribers is not None:
                subscribers.discard(subscriber)
                if not subscribers:
                    del self._subscribers[parsed_id]
            WEBSOCKET_SESSIONS.dec()

    @staticmethod
    async def _write(subscriber: _Subscriber) -> None:
        """Esvazia a fila do cliente no socket, uma mensagem por vez, até a conexão cair."""
        try:
            while True:
                await subscriber.ws.send_text(await subscriber.queue.get())
        except Exception:
            pass  # a conexão caiu; o loop de leitura faz a limpeza

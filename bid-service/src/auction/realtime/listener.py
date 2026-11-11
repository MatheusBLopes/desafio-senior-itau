"""Assinatura do canal de eventos do Valkey: cada task recebe todos os eventos de todos os leilões."""

import asyncio
import logging
import uuid
from collections.abc import Callable

import orjson
from redis.asyncio import Redis

from .broadcaster import CLOSED, EXTENDED, SnapshotBroadcaster
from .cache import LiveAuctionCache, Snapshot

log = logging.getLogger(__name__)

# Um AUCTION_CLOSED não tem próximo lance: o mínimo fica no teto, e o pré-filtro rejeita tudo.
_NO_NEXT_BID = 2**63 - 1


class AuctionEventListener:
    """Assina o canal de eventos do Valkey: atualiza o cache local e entrega ao broadcaster.

    Uma única corrotina processa as mensagens na ordem de publicação: um AUCTION_LOADED nunca é
    aplicado depois de um BID_ACCEPTED publicado após ele.
    """

    def __init__(self, redis: Redis, channel: str, cache: LiveAuctionCache, broadcaster: SnapshotBroadcaster,
                 on_stream_event: Callable[[uuid.UUID], None] = lambda _: None):
        """on_stream_event avisa o consumer do write-behind que um stream recebeu eventos."""
        self._redis = redis
        # Avisa o consumer do write-behind que o stream deste leilão tem eventos novos.
        self._on_stream_event = on_stream_event
        self._channel = channel
        self._cache = cache
        self._broadcaster = broadcaster
        self.subscribed = asyncio.Event()

    async def run(self) -> None:
        """Tarefa de fundo: assina o canal e processa as mensagens; reconecta em 1 s se a conexão cair."""
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
        """Aplica um evento ao cache e repassa ao broadcaster e ao consumer, conforme o tipo."""
        try:
            event = orjson.loads(data)
            event_type = event["type"]
            if event_type == "BID_ACCEPTED":
                snapshot = self._cache.apply(Snapshot.from_event(event, status="OPEN"), replace=False)
                self._broadcaster.mark_dirty(snapshot.auction_id)
                self._on_stream_event(snapshot.auction_id)
                if event.get("extended"):
                    self._broadcaster.send_now(EXTENDED, snapshot)
            elif event_type == "AUCTION_LOADED":
                # Reidratação: o estado pode ter voltado (lances perdidos), então substitui sem olhar seq.
                snapshot = self._cache.apply(Snapshot.from_event(event), replace=True)
                self._broadcaster.mark_dirty(snapshot.auction_id)
            elif event_type == "AUCTION_CLOSED":
                snapshot = self._cache.apply(
                    Snapshot.from_event(event, price_field="finalPrice", leader_field="winnerId",
                                        minimum_next=_NO_NEXT_BID, status="CLOSED"), replace=True)
                self._broadcaster.send_now(CLOSED, snapshot)
                self._on_stream_event(snapshot.auction_id)
        except Exception as e:
            log.warning("evento inválido no Pub/Sub: %s", e)

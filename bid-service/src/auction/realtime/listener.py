"""Assinatura do canal de eventos do Valkey: cada task recebe todos os eventos de todos os leilões."""

import asyncio
import logging
import uuid
from collections.abc import Callable

import orjson
from redis.asyncio import Redis

from ..observability.logs import fields
from .broadcaster import CLOSED, EXTENDED, SnapshotBroadcaster
from .cache import NO_NEXT_BID, LiveAuctionCache, Snapshot, read_snapshot

log = logging.getLogger(__name__)


class AuctionEventListener:
    """Assina o canal de eventos do Valkey: atualiza o cache local e entrega ao broadcaster.

    Uma única corrotina processa as mensagens na ordem de publicação: um AUCTION_LOADED nunca é
    aplicado depois de um BID_ACCEPTED publicado após ele.

    O Pub/Sub não guarda o que foi publicado com a conexão caída. Por isso, ao reassinar, o cache local é
    descartado e os leilões com clientes conectados são relidos do Valkey (ver _resync).
    """

    def __init__(self, redis: Redis, channel: str, cache: LiveAuctionCache, broadcaster: SnapshotBroadcaster,
                 on_stream_event: Callable[[uuid.UUID], None] = lambda _: None, state_redis: Redis | None = None):
        """on_stream_event avisa o consumer do write-behind; state_redis lê o estado na reassinatura."""
        self._redis = redis
        # Cliente de comandos (o do Pub/Sub fica preso à assinatura e não tem timeout de leitura).
        self._state_redis = state_redis
        # Avisa o consumer do write-behind que o stream deste leilão tem eventos novos.
        self._on_stream_event = on_stream_event
        self._channel = channel
        self._cache = cache
        self._broadcaster = broadcaster
        self.subscribed = asyncio.Event()

    async def run(self) -> None:
        """Tarefa de fundo: assina o canal e processa as mensagens; reconecta em 1 s se a conexão cair."""
        while True:
            pubsub = self._redis.pubsub()
            try:
                await pubsub.subscribe(self._channel)
                async for message in pubsub.listen():
                    if message["type"] == "message":
                        self.on_message(message["data"])
                    elif message["type"] == "subscribe":
                        # Confirmação do Valkey. Vem na primeira assinatura e a cada reassinatura, inclusive
                        # quando é o próprio redis-py que reconecta e reassina sem este laço ficar sabendo.
                        if self.subscribed.is_set():
                            # O que foi publicado enquanto a conexão esteve caída se perdeu.
                            await self._resync()
                        self.subscribed.set()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("Pub/Sub desconectado; reconectando em 1 s", extra=fields(error=e))
                await asyncio.sleep(1)
            finally:
                await pubsub.aclose()

    async def _resync(self) -> None:
        """Depois de uma reconexão, troca o cache pelo estado atual do Valkey.

        O cache inteiro é descartado: um snapshot velho com preço MAIOR que o real (failover do Valkey para
        uma réplica atrasada) faria o pré-filtro rejeitar lances válidos, e cache vazio só deixa de filtrar.
        Os leilões com clientes conectados são relidos na hora, para que eles recebam o estado atual e,
        principalmente, um encerramento que tenha acontecido durante a queda. As mensagens publicadas depois
        da reassinatura ficam na conexão e são aplicadas em seguida, na ordem.
        """
        if self._state_redis is None:
            return
        watched = self._broadcaster.watched()
        already_closed = {auction_id for auction_id in watched
                          if (known := self._cache.get(auction_id)) is not None and known.status == "CLOSED"}
        self._cache.clear()
        for auction_id in watched:
            snapshot = await read_snapshot(self._state_redis, auction_id)
            if snapshot is None:
                continue
            self._cache.apply(snapshot, replace=True)
            if snapshot.status == "CLOSED" and auction_id not in already_closed:
                self._broadcaster.send_now(CLOSED, snapshot)
            else:
                self._broadcaster.mark_dirty(auction_id)
        log.warning("Pub/Sub reassinado; cache local ressincronizado com o Valkey",
                    extra=fields(watchedAuctions=len(watched)))

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
                                        minimum_next=NO_NEXT_BID, status="CLOSED"), replace=True)
                self._broadcaster.send_now(CLOSED, snapshot)
                self._on_stream_event(snapshot.auction_id)
        except Exception as e:
            log.warning("evento inválido no Pub/Sub", extra=fields(error=e))

"""Último estado conhecido de cada leilão na task, alimentado pelo Pub/Sub do Valkey."""

import uuid
from collections.abc import Collection
from dataclasses import dataclass
from typing import Any

from redis.asyncio import Redis

from ..storage import keys

# Um leilão encerrado não tem próximo lance: o mínimo fica no teto, e o pré-filtro rejeita tudo.
NO_NEXT_BID = 2**63 - 1


@dataclass(frozen=True)
class Snapshot:
    """Estado de um leilão num instante. Valores em centavos; ends_at em epoch ms."""

    auction_id: uuid.UUID
    seq: int
    current_price: int
    minimum_next: int
    leader_id: str
    ends_at: int
    status: str

    @staticmethod
    def from_event(event: dict[str, Any], *, price_field: str = "amount", leader_field: str = "userId",
                   minimum_next: int | None = None, status: str | None = None) -> "Snapshot":
        """Monta o snapshot a partir de um evento do Pub/Sub publicado pelos scripts Lua.

        Os eventos usam nomes diferentes para o preço e o líder (o AUCTION_CLOSED traz finalPrice e
        winnerId), e o status vem do evento ou é fixado por quem chama.
        """
        return Snapshot(
            auction_id=uuid.UUID(event["auctionId"]),
            seq=int(event["seq"]),
            current_price=int(event[price_field]),
            minimum_next=int(event["minimumNext"]) if minimum_next is None else minimum_next,
            leader_id=event[leader_field],
            ends_at=int(event["endsAt"]),
            status=status or event["status"],
        )


class LiveAuctionCache:
    """Último estado conhecido de cada leilão nesta task. Serve ao pré-filtro e ao snapshot do WebSocket.

    Nunca decide aceite: isso é só do Lua.
    """

    def __init__(self) -> None:
        """Começa vazio; enche conforme os eventos do Pub/Sub chegam."""
        self._snapshots: dict[uuid.UUID, Snapshot] = {}

    def apply(self, incoming: Snapshot, replace: bool) -> Snapshot:
        """Mantém o de maior seq, exceto quando replace (reidratação: o preço pode ter voltado)."""
        current = self._snapshots.get(incoming.auction_id)
        if current is None or replace or incoming.seq >= current.seq:
            self._snapshots[incoming.auction_id] = incoming
            return incoming
        return current

    def get(self, auction_id: uuid.UUID) -> Snapshot | None:
        """Último snapshot do leilão, ou None se a task ainda não recebeu nenhum evento dele."""
        return self._snapshots.get(auction_id)

    def evict(self, auction_id: uuid.UUID) -> None:
        """Esquece o leilão (não falha se ele não estiver no cache)."""
        self._snapshots.pop(auction_id, None)

    def clear(self) -> None:
        """Esquece tudo. Seguro a qualquer momento: sem snapshot, o pré-filtro só deixa de filtrar."""
        self._snapshots.clear()

    def evict_closed(self, ended_before_ms: int, keep: Collection[uuid.UUID] = ()) -> None:
        """Esquece os leilões encerrados com prazo anterior a `ended_before_ms`, menos os de `keep`.

        Sem isso o cache cresceria com todo leilão que a task já viu. O corte usa o relógio da task: é só
        limpeza de memória, não decisão de prazo. Um lance tardio num leilão esquecido vai ao Lua, que rejeita.
        """
        for auction_id in [auction_id for auction_id, snapshot in self._snapshots.items()
                           if snapshot.status == "CLOSED" and snapshot.ends_at < ended_before_ms
                           and auction_id not in keep]:
            del self._snapshots[auction_id]

    def __len__(self) -> int:
        """Quantos leilões têm snapshot na memória agora."""
        return len(self._snapshots)


async def read_snapshot(redis: Redis, auction_id: uuid.UUID) -> Snapshot | None:
    """Estado atual do leilão lido direto do Valkey, ou None se ele não está carregado lá.

    O cache normalmente se alimenta do Pub/Sub; isto cobre o que o Pub/Sub não entrega: o estado de antes
    de a task subir e os eventos perdidos numa reconexão.
    """
    status, seq, price, starting_price, min_increment, leader, ends_at = await redis.hmget(
        keys.auction_key(auction_id), "status", "seq", "currentPrice", "startingPrice", "minIncrement",
        "leaderId", "endsAt")
    if status is None:
        return None
    seq, price = int(seq), int(price)
    if status == "CLOSED":
        minimum_next = NO_NEXT_BID
    else:
        minimum_next = int(starting_price) if seq == 0 else price + int(min_increment)
    return Snapshot(auction_id, seq, price, minimum_next, leader or "", int(ends_at), status)

"""Último estado conhecido de cada leilão na task, alimentado pelo Pub/Sub do Valkey."""

import uuid
from dataclasses import dataclass
from typing import Any


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

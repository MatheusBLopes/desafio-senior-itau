"""Modelos do cadastro de leilões: o pedido de criação (API) e a linha da tabela auctions."""

import uuid
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Annotated

import asyncpg
from pydantic import BaseModel, Field, StringConstraints

OPEN = "OPEN"
CLOSED = "CLOSED"

Money = Annotated[Decimal, Field(ge=Decimal("0.01"), max_digits=14, decimal_places=2)]


class CreateAuctionRequest(BaseModel):
    """Corpo de POST /auctions. Valores monetários com até 2 casas; prazos em segundos."""

    title: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=200)]
    startingPrice: Money
    minIncrement: Money
    durationSeconds: Annotated[int, Field(ge=5, le=2_592_000)]
    antiSnipingWindowSeconds: Annotated[int, Field(ge=0, le=3600)] = 10
    antiSnipingExtendSeconds: Annotated[int, Field(ge=0, le=3600)] = 10


@dataclass(frozen=True)
class Auction:
    """Linha da tabela auctions: cadastro + projeção persistida do estado vivo. Valores em centavos."""

    id: uuid.UUID
    title: str
    starting_price: int
    min_increment: int
    anti_sniping_window_ms: int
    anti_sniping_extend_ms: int
    status: str
    created_at: datetime
    ends_at: datetime

    @staticmethod
    def from_row(row: asyncpg.Record) -> "Auction":
        """Monta o leilão a partir de uma linha de `select * from auctions`."""
        return Auction(row["id"], row["title"], row["starting_price"], row["min_increment"],
                       row["anti_sniping_window_ms"], row["anti_sniping_extend_ms"], row["status"],
                       row["created_at"], row["ends_at"])

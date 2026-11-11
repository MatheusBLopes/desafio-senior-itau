"""O valor de um lance na entrada e o resultado dele na saída."""

import json
import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from ..core.errors import ApiError
from ..core.money import from_cents
from ..core.serialization import without_nulls
from ..core.time import iso

ACCEPTED = "ACCEPTED"
REJECTED = "REJECTED"
BID_TOO_LOW = "BID_TOO_LOW"
AUCTION_CLOSED = "AUCTION_CLOSED"
IDEMPOTENCY_KEY_REUSED = "IDEMPOTENCY_KEY_REUSED"


MAX_AMOUNT = Decimal("999999999999.99")  # 12 dígitos inteiros e 2 decimais, como o cadastro


def parse_amount(body: bytes) -> int:
    """Lê {"amount": 150.00} (número ou string) e devolve centavos.

    O caminho quente do lance valida à mão, sem o resolvedor de dependências do FastAPI: o profiler
    mostrou ~20% da CPU da requisição nessa camada genérica. json com parse_float=Decimal lê o número
    exatamente como veio, sem passar por float.
    """
    try:
        data = json.loads(body, parse_float=Decimal)
        raw = data["amount"]
        if isinstance(raw, bool) or not isinstance(raw, (int, str, Decimal)):
            raise ValueError
        amount = Decimal(raw)
    except (ValueError, KeyError, TypeError, ArithmeticError):
        raise ApiError.bad_request('amount: informe um valor, como {"amount": 150.00}') from None
    if not amount.is_finite() or amount < Decimal("0.01") or amount > MAX_AMOUNT:
        raise ApiError.bad_request("amount: entre 0.01 e 999999999999.99")
    if amount != amount.quantize(Decimal("0.01")):
        raise ApiError.bad_request("amount: no máximo 2 casas decimais")
    return int(amount * 100)


@dataclass(frozen=True)
class BidResult:
    """Resposta de um lance. Valores em centavos; endsAt e serverTime em epoch ms.

    replayed: a idempotency key já tinha sido usada e esta é a resposta original.
    filtered: rejeitado pelo pré-filtro da task, sem ir ao Valkey.
    """

    auction_id: uuid.UUID
    outcome: str
    reason: str | None = None
    seq: int | None = None
    amount: int | None = None
    current_price: int | None = None
    minimum_next: int | None = None
    ends_at: int | None = None
    extended: bool | None = None
    server_time: int | None = None
    replayed: bool = False
    filtered: bool = False

    @property
    def accepted(self) -> bool:
        """True se o lance foi aceito (novo ou replay de um aceite)."""
        return self.outcome == ACCEPTED

    def http_status(self) -> int:
        """201 aceito; 200 replay de um aceite; 409 abaixo do mínimo; 422 encerrado ou key reutilizada."""
        if self.accepted:
            return 200 if self.replayed else 201
        return 409 if self.reason == BID_TOO_LOW else 422

    def to_json(self) -> dict[str, Any]:
        """Corpo da resposta HTTP: dinheiro como string com 2 casas, instantes em ISO-8601."""
        def money(cents: int | None) -> str | None:
            """Centavos para string, preservando a ausência de valor."""
            return None if cents is None else from_cents(cents)

        return without_nulls({
            "auctionId": str(self.auction_id),
            "outcome": self.outcome,
            "reason": self.reason,
            "seq": self.seq,
            "amount": money(self.amount),
            "currentPrice": money(self.current_price),
            "minimumNextBid": money(self.minimum_next),
            "endsAt": None if self.ends_at is None else iso(self.ends_at),
            "extended": self.extended,
            "serverTime": None if self.server_time is None else iso(self.server_time),
            "replayed": self.replayed,
            "filtered": self.filtered,
        })

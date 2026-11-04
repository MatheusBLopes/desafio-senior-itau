"""Peças compartilhadas: chaves do Valkey, dinheiro, relógio, erros de API e serialização."""

import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import orjson
from fastapi.responses import Response
from redis.asyncio import Redis


# --- Chaves do Valkey -----------------------------------------------------------------------------
# Tudo que pertence a um leilão usa o hash tag {auctionId}: cai no mesmo slot do cluster e pode ser
# tocado pelo mesmo script Lua.

ENDING = "auctions:ending"          # ZSET global: auctionId → prazo (ms). O closer encerra o que venceu.
ACTIVE_STREAMS = "auctions:streams"  # SET global com os leilões cujo stream o consumer precisa drenar.


def auction_key(auction_id: uuid.UUID | str) -> str:
    return f"auction:{{{auction_id}}}"


def stream_key(auction_id: uuid.UUID | str) -> str:
    return f"bids:{{{auction_id}}}"


def idempotency_key(auction_id: uuid.UUID | str, user_id: str, key: str) -> str:
    return f"idem:{{{auction_id}}}:{user_id}:{key}"


def rate_limit_key(user_id: str) -> str:
    return f"rl:{{{user_id}}}"


def auction_id_or_none(member: str) -> uuid.UUID | None:
    """Membro de ENDING ou ACTIVE_STREAMS; None se não for um UUID (entrada corrompida)."""
    try:
        return uuid.UUID(member)
    except ValueError:
        return None


# --- Dinheiro ---------------------------------------------------------------------------------------
# Trafega como string decimal na API ("150.00") e como centavos (int) no Lua e no banco. Nunca float.

def to_cents(value: Decimal) -> int:
    cents = value * 100
    if cents != cents.to_integral_value():
        raise ValueError("no máximo 2 casas decimais")
    return int(cents)


def from_cents(cents: int) -> str:
    return f"{cents // 100}.{cents % 100:02d}"


# --- Tempo ------------------------------------------------------------------------------------------

def iso(epoch_ms: int) -> str:
    """Instante UTC em ISO-8601 com milissegundos, como 2026-11-03T12:00:00.123Z."""
    dt = datetime.fromtimestamp(epoch_ms / 1000, UTC)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{epoch_ms % 1000:03d}Z"


def from_datetime(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def to_datetime(epoch_ms: int) -> datetime:
    return datetime.fromtimestamp(epoch_ms / 1000, UTC)


class RedisClock:
    """Relógio único do sistema: o TIME do Valkey. Nenhuma decisão de prazo usa o relógio da task."""

    def __init__(self, redis: Redis):
        self._redis = redis

    async def now_ms(self) -> int:
        seconds, micros = await self._redis.time()
        return seconds * 1000 + micros // 1000


# --- Erros e respostas ------------------------------------------------------------------------------

class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message

    @staticmethod
    def not_found(message: str) -> "ApiError":
        return ApiError(404, "NOT_FOUND", message)

    @staticmethod
    def bad_request(message: str) -> "ApiError":
        return ApiError(400, "INVALID_REQUEST", message)


def json_response(body: Any, status: int = 200, headers: dict[str, str] | None = None) -> Response:
    """orjson direto, sem passar pelo jsonable_encoder do FastAPI: é o caminho quente dos lances."""
    return Response(orjson.dumps(body), status_code=status, headers=headers, media_type="application/json")


def without_nulls(d: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in d.items() if v is not None}

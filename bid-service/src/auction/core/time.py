"""Conversões de tempo. Internamente os instantes são epoch em milissegundos, como o TIME do Valkey."""

from datetime import UTC, datetime


def iso(epoch_ms: int) -> str:
    """Instante UTC em ISO-8601 com milissegundos, como 2026-11-03T12:00:00.123Z."""
    moment = datetime.fromtimestamp(epoch_ms / 1000, UTC)
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{epoch_ms % 1000:03d}Z"


def from_datetime(moment: datetime) -> int:
    """datetime (como vem do Postgres) para epoch em milissegundos."""
    return int(moment.timestamp() * 1000)


def to_datetime(epoch_ms: int) -> datetime:
    """Epoch em milissegundos para datetime UTC (como o Postgres espera)."""
    return datetime.fromtimestamp(epoch_ms / 1000, UTC)

"""Ajudas para montar os JSONs de resposta."""

from typing import Any


def without_nulls(fields: dict[str, Any]) -> dict[str, Any]:
    """Remove os campos None: a resposta só traz o que se aplica (ex.: sem vencedor num leilão aberto)."""
    return {name: value for name, value in fields.items() if value is not None}

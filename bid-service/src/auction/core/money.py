"""Dinheiro: trafega como string decimal na API ("150.00") e como centavos (int) no Lua e no banco. Nunca float."""

from decimal import Decimal


def to_cents(value: Decimal) -> int:
    """Converte um Decimal com até 2 casas em centavos; recusa frações de centavo."""
    cents = value * 100
    if cents != cents.to_integral_value():
        raise ValueError("no máximo 2 casas decimais")
    return int(cents)


def from_cents(cents: int) -> str:
    """Formata centavos como string com 2 casas (15000 → "150.00"), sem passar por float."""
    return f"{cents // 100}.{cents % 100:02d}"

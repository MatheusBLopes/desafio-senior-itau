"""Nomes das chaves no Valkey.

Tudo que pertence a um leilão usa o hash tag {auctionId}: cai no mesmo slot do cluster e pode ser
tocado pelo mesmo script Lua.
"""

import uuid

ENDING = "auctions:ending"          # ZSET global: auctionId → prazo (ms). O closer encerra o que venceu.
ACTIVE_STREAMS = "auctions:streams"  # SET global com os leilões cujo stream o consumer precisa drenar.


def auction_key(auction_id: uuid.UUID | str) -> str:
    """Hash com o estado vivo do leilão: status, prazo, preço, líder e seq."""
    return f"auction:{{{auction_id}}}"


def stream_key(auction_id: uuid.UUID | str) -> str:
    """Stream com os lances aceitos e o encerramento, drenado para o Postgres (write-behind)."""
    return f"bids:{{{auction_id}}}"


def idempotency_key(auction_id: uuid.UUID | str, user_id: str, key: str) -> str:
    """Resultado memorizado de uma Idempotency-Key, com escopo por leilão e usuário."""
    return f"idem:{{{auction_id}}}:{user_id}:{key}"


def rate_limit_key(user_id: str) -> str:
    """Token bucket do usuário. Usa o hash tag do usuário, então cai em outro slot que o do leilão."""
    return f"rl:{{{user_id}}}"


def auction_id_or_none(member: str) -> uuid.UUID | None:
    """Membro de ENDING ou ACTIVE_STREAMS; None se não for um UUID (entrada corrompida)."""
    try:
        return uuid.UUID(member)
    except ValueError:
        return None

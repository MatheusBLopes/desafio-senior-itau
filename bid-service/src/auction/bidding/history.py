"""Histórico de lances aceitos (GET /auctions/{id}/bids?afterSeq=N): Postgres e, quando ele está atrás, o stream."""

import uuid
from typing import Any

import asyncpg
from redis.asyncio import Redis

from ..core.money import from_cents
from ..core.time import from_datetime, iso
from ..storage import keys


def bid_view(seq: int, user_id: str, amount: int, accepted_at_ms: int) -> dict[str, Any]:
    """Um lance como aparece na resposta da API."""
    return {"seq": seq, "userId": user_id, "amount": from_cents(amount), "acceptedAt": iso(accepted_at_ms)}


class BidRepository:
    """Lances já persistidos na tabela bids."""

    def __init__(self, pool: asyncpg.Pool):
        """Usa o pool de conexões do Postgres da task."""
        self._pool = pool

    async def find_after(self, auction_id: uuid.UUID, after_seq: int, limit: int) -> list[dict[str, Any]]:
        """Até `limit` lances com seq > after_seq, em ordem."""
        rows = await self._pool.fetch("""
            select seq, user_id, amount, accepted_at from bids
            where auction_id = $1 and seq > $2 order by seq limit $3
            """, auction_id, after_seq, limit)
        return [bid_view(row["seq"], row["user_id"], row["amount"], from_datetime(row["accepted_at"]))
                for row in rows]


class BidHistory:
    """Junta o que já está no banco com o que ainda está só no stream."""

    def __init__(self, redis: Redis, bids: BidRepository):
        """Cliente do Valkey (para o stream) e repositório dos lances persistidos."""
        self._redis = redis
        self._bids = bids

    async def list_after(self, auction_id: uuid.UUID, after_seq: int, limit: int) -> list[dict[str, Any]]:
        """Lances com seq > after_seq, em ordem.

        O Postgres pode estar atrás do Valkey (write-behind) e, com várias tasks gravando lotes em
        paralelo, ter buracos temporários (4..6 gravados antes de 1..3). Se o banco devolve uma faixa
        contígua e completa, ela basta. Senão, junta com o stream, que ainda tem tudo o que não foi
        persistido, e devolve os primeiros `limit` em ordem.
        """
        from_db = await self._bids.find_after(auction_id, after_seq, limit)
        # Seqs são únicos e maiores que after_seq: se o último é after_seq + limit, não há buraco.
        if len(from_db) == limit and from_db[-1]["seq"] == after_seq + limit:
            return from_db
        merged = {bid["seq"]: bid for bid in from_db}
        for bid in await self._from_stream(auction_id, after_seq, limit):
            merged.setdefault(bid["seq"], bid)
        return [merged[seq] for seq in sorted(merged)][:limit]

    async def _from_stream(self, auction_id: uuid.UUID, after_seq: int, limit: int) -> list[dict[str, Any]]:
        """Lances do stream a partir de after_seq + 1 (o ID 0-<seq> permite o XRANGE direto)."""
        records = await self._redis.xrange(keys.stream_key(auction_id), min=f"0-{after_seq + 1}", count=limit)
        return [bid_view(int(fields["seq"]), fields["userId"], int(fields["amount"]), int(fields["ts"]))
                for _, fields in records if fields.get("type") == "BID"]

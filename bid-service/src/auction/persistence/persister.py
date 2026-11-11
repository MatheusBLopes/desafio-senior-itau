"""Gravação no Postgres de um lote de eventos lido do stream bids:{id}."""

import uuid
from typing import Any

import asyncpg

from ..core.time import to_datetime

StreamRecord = tuple[str, dict[str, str]]  # (id do stream, campos)


class BidPersister:
    """Grava um lote de eventos do stream numa transação.

    Tudo é idempotente: o mesmo evento pode chegar de novo (ACK perdido, XCLAIM por outra task) sem
    efeito duplicado. Um lance que colide com outro já gravado (mesmo seq ou mesma idempotency key,
    com conteúdo diferente) não é descartado: vai para bid_conflicts e é contado no retorno.
    """

    def __init__(self, pool: asyncpg.Pool):
        """Usa o pool de conexões do Postgres da task."""
        self._pool = pool

    async def persist(self, auction_id: uuid.UUID, records: list[StreamRecord]) -> int:
        """Devolve quantos lances colidiram com outro já gravado e foram para bid_conflicts."""
        bids = [fields for _, fields in records if fields.get("type") == "BID"]
        closed = next((fields for _, fields in records if fields.get("type") == "CLOSED"), None)
        conflicts = 0

        async with self._pool.acquire() as conn, conn.transaction():
            if bids:
                rows = [(int(bid["seq"]), bid["userId"], int(bid["amount"]), bid["idemKey"],
                         to_datetime(int(bid["ts"]))) for bid in bids]
                # Segunda barreira: PK (auction_id, seq) e UNIQUE (auction_id, user_id, idempotency_key).
                # Um INSERT só para o lote inteiro (unnest); o RETURNING diz quais linhas entraram.
                inserted = {row["seq"] for row in await conn.fetch("""
                    insert into bids (auction_id, seq, user_id, amount, idempotency_key, accepted_at)
                    select $1, * from unnest($2::bigint[], $3::text[], $4::bigint[], $5::text[], $6::timestamptz[])
                    on conflict do nothing
                    returning seq
                    """, auction_id, *map(list, zip(*rows)))}
                collided = [row for row in rows if row[0] not in inserted and not await self._already_persisted(
                    conn, auction_id, row)]
                if collided:
                    await conn.executemany("""
                        insert into bid_conflicts (auction_id, seq, user_id, amount, idempotency_key, accepted_at)
                        values ($1, $2, $3, $4, $5, $6)
                        on conflict do nothing
                        """, [(auction_id, *row) for row in collided])
                    conflicts = len(collided)

                last = max(bids, key=lambda bid: int(bid["seq"]))
                max_ends_at = max(int(bid["endsAt"]) for bid in bids)
                # Projeção: só avança (lotes de consumers diferentes podem chegar fora de ordem).
                await conn.execute("""
                    update auctions
                       set current_price = case when last_seq < $1 then $2 else current_price end,
                           leader_id     = case when last_seq < $1 then $3 else leader_id end,
                           last_seq      = greatest(last_seq, $1),
                           ends_at       = greatest(ends_at, $4)
                     where id = $5
                    """, int(last["seq"]), int(last["amount"]), last["userId"], to_datetime(max_ends_at),
                    auction_id)

            if closed is not None:
                winner = closed.get("winnerId") or None
                await conn.execute("""
                    update auctions set status = 'CLOSED', closed_at = $1, winner_id = $2, final_price = $3
                     where id = $4 and status = 'OPEN'
                    """, to_datetime(int(closed["ts"])), winner,
                    int(closed["finalPrice"]) if winner else None, auction_id)
        return conflicts

    @staticmethod
    async def _already_persisted(conn: asyncpg.Connection, auction_id: uuid.UUID, row: tuple[Any, ...]) -> bool:
        """Reentrega do mesmo evento: a linha já existe com o mesmo conteúdo."""
        seq, user_id, amount, key, _ = row
        return await conn.fetchval("""
            select exists (select 1 from bids where auction_id = $1 and seq = $2 and user_id = $3
                                                and amount = $4 and idempotency_key = $5)
            """, auction_id, seq, user_id, amount, key)

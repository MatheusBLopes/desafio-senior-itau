"""500 usuários disputando o mesmo leilão ao mesmo tempo, 20 lances cada: 10 mil lances.

Cada usuário cobre o mínimo que conhece (muitos empates e lances já superados) e 10% dos lances são
reenviados em paralelo com a mesma idempotency key. Verifica as garantias do enunciado no Valkey e
no Postgres.
"""

import asyncio
import random
import uuid
from collections import defaultdict
from dataclasses import dataclass

from auction.bidding.result import BidResult

from .conftest import count, eventually, new_auction

USERS = 500
BIDS_PER_USER = 20
INCREMENT = 100


@dataclass(frozen=True)
class Attempt:
    user_id: str
    key: str
    amount: int


async def test_ten_thousand_concurrent_bids_keep_all_invariants(server):
    c = server.c
    aid = await new_auction(c, 300)
    outcomes: list[tuple[Attempt, BidResult]] = []
    retries: list[asyncio.Task[None]] = []
    start = asyncio.Event()

    async def place(a: Attempt) -> BidResult:
        r = await c.bids.place(aid, a.user_id, a.key, a.amount)
        outcomes.append((a, r))
        return r

    async def user(name: str) -> None:
        await start.wait()
        known = 10_000
        for _ in range(BIDS_PER_USER):
            a = Attempt(name, str(uuid.uuid4()), known + random.randrange(3) * INCREMENT)
            if random.randrange(10) == 0:
                # Retry concorrente: o cliente reenviou antes de receber a resposta.
                retries.append(asyncio.create_task(place(a)))
            r = await place(a)
            if r.minimum_next is not None:
                known = r.minimum_next

    users = [asyncio.create_task(user(f"user{u}")) for u in range(USERS)]
    start.set()
    await asyncio.gather(*users)
    await asyncio.gather(*retries)

    # Cada lance aceito aparece uma vez como original; retries devolvem o mesmo seq.
    accepted = sorted(((a, r) for a, r in outcomes if r.accepted and not r.replayed), key=lambda o: o[1].seq)
    n = len(accepted)
    print(f"concorrência: {len(outcomes)} tentativas, {n} aceitos, "
          f"{sum(r.filtered for _, r in outcomes)} rejeitados no pré-filtro, "
          f"{sum(r.replayed for _, r in outcomes)} replays")
    assert len(outcomes) >= USERS * BIDS_PER_USER
    assert n > 20

    # 1. Ordem determinística: seqs únicos e contíguos 1..N.
    assert [r.seq for _, r in accepted] == list(range(1, n + 1))

    # 2. Nenhum lance inválido: na ordem do seq, cada um supera o anterior em pelo menos o incremento.
    for (prev, _), (cur, _) in zip(accepted, accepted[1:]):
        assert cur.amount >= prev.amount + INCREMENT

    # 3. Nenhum duplicado: no máximo um aceite original por key, e os retries concordam com ele.
    by_key: dict[str, list[BidResult]] = defaultdict(list)
    for a, r in outcomes:
        by_key[a.key].append(r)
    for same in (rs for rs in by_key.values() if len(rs) > 1):
        assert len({r.outcome for r in same}) == 1
        assert len({r.seq for r in same if r.accepted}) <= 1
        assert sum(1 for r in same if r.accepted and not r.replayed) <= 1

    # 4. Vencedor correto no estado vivo.
    last, _ = accepted[-1]
    view = await c.auctions.get(aid)
    assert view["seq"] == n
    assert view["currentPrice"] == f"{last.amount // 100}.{last.amount % 100:02d}"
    assert view["leaderId"] == last.user_id

    # 5. O Postgres (write-behind) converge para o mesmo resultado, protegido pelas constraints.
    async def all_persisted() -> bool:
        return await count(c, "bids", aid) == n

    await eventually(all_persisted, 30)
    assert await c.pool.fetchval("""
        select count(*) from (
          select amount - lag(amount) over (order by seq) as step, seq - lag(seq) over (order by seq) as gap
          from bids where auction_id = $1) t
        where step < $2 or gap <> 1
        """, aid, INCREMENT) == 0
    assert await c.pool.fetchval("select max(seq) from bids where auction_id = $1", aid) == n

    async def projection_caught_up() -> bool:
        return await c.pool.fetchval("select last_seq from auctions where id = $1", aid) == n

    await eventually(projection_caught_up, 10)
    assert await c.pool.fetchval("select current_price from auctions where id = $1", aid) == last.amount

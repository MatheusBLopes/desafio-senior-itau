"""Divergências entre Valkey e Postgres e entradas corrompidas nas estruturas globais do Valkey."""

import asyncio
import time
import uuid
from dataclasses import replace

from auction.bidding.rate_limit import UserRateLimiter
from auction.bidding.service import BidService
from auction.config import RateLimitConfig
from auction.container import _supervised
from auction.observability.metrics import PERSIST_CONFLICTS
from auction.persistence.persister import BidPersister
from auction.persistence.stream_consumer import BidStreamConsumer
from auction.storage import keys

from .conftest import count, eventually, key, new_auction


async def test_accepted_bid_that_collides_in_postgres_is_quarantined_instead_of_dropped(server):
    c = server.c
    aid = await new_auction(c, 60)
    k = key()
    await c.bids.place(aid, "ana", k, 10_000)
    before = PERSIST_CONFLICTS._value.get()

    # A key some com o leilão aberto (TTL antigo de 24 h): o Valkey aceita de novo, o UNIQUE recusa.
    await c.redis.delete(keys.idempotency_key(aid, "ana", k))
    again = await c.bids.place(aid, "ana", k, 11_000)
    assert again.seq == 2

    async def quarantined() -> bool:
        return await count(c, "bid_conflicts", aid) == 1

    await eventually(quarantined, 10)
    row = await c.pool.fetchrow("select seq, amount from bid_conflicts where auction_id = $1", aid)
    assert row["seq"] == 2 and row["amount"] == 11_000
    assert await count(c, "bids", aid) == 1
    assert PERSIST_CONFLICTS._value.get() == before + 1


async def test_valkey_behind_postgres_catches_up_instead_of_reusing_seqs(server):
    c = server.c
    aid = await new_auction(c, 60)
    await c.bids.place(aid, "ana", key(), 10_000)
    await c.bids.place(aid, "bia", key(), 12_000)
    await c.bids.place(aid, "caio", key(), 13_000)

    async def persisted(n: int):
        async def check() -> bool:
            return await count(c, "bids", aid) == n
        return check

    await eventually(await persisted(3), 10)

    # Failover para uma réplica que não recebeu os dois últimos lances.
    await c.redis.hset(keys.auction_key(aid), mapping={"seq": "1", "currentPrice": "10000", "leaderId": "ana"})
    await c.reconciler.reconcile()

    view = await c.auctions.get(aid)
    assert view["seq"] == 3 and view["leaderId"] == "caio"
    nxt = await c.bids.place(aid, "dani", key(), 14_000)
    assert nxt.seq == 4
    await eventually(await persisted(4), 10)
    assert await count(c, "bid_conflicts", aid) == 0


async def test_orphan_entries_in_the_closing_queue_do_not_block_other_auctions(server):
    # Ex.: Postgres restaurado de um backup anterior a esses leilões.
    c = server.c
    orphan = str(uuid.uuid4())
    await c.redis.zadd(keys.ENDING, {orphan: 0, "lixo": 0})
    try:
        aid = await new_auction(c, 5)
        await c.bids.place(aid, "ana", key(), 10_000)

        async def closed() -> bool:
            return await c.pool.fetchval("select status from auctions where id = $1", aid) == "CLOSED"

        await eventually(closed, 15)
        assert await c.redis.zscore(keys.ENDING, orphan) is None
        assert await c.redis.zscore(keys.ENDING, "lixo") is None
    finally:
        await c.redis.zrem(keys.ENDING, orphan, "lixo")


async def test_invalid_member_in_active_streams_does_not_stop_persistence(server):
    c = server.c
    await c.redis.sadd(keys.ACTIVE_STREAMS, "lixo")
    try:
        aid = await new_auction(c, 60)
        await c.bids.place(aid, "ana", key(), 10_000)

        async def persisted() -> bool:
            return await count(c, "bids", aid) == 1

        await eventually(persisted, 10)

        # A entrada inválida é removida na próxima varredura completa.
        async def removed() -> bool:
            return not await c.redis.sismember(keys.ACTIVE_STREAMS, "lixo")

        await eventually(removed, 5)
    finally:
        await c.redis.srem(keys.ACTIVE_STREAMS, "lixo")


async def test_bids_keep_working_after_valkey_loses_its_script_cache(server):
    # Restart do Valkey sem FLUSHALL do estado (ex.: failover): o EVALSHA responde NOSCRIPT.
    c = server.c
    bids = BidService(c.redis, c.scripts, c.loader, c.cache, UserRateLimiter(c.scripts, RateLimitConfig()),
                      c.metrics, c.settings)
    aid = await new_auction(c, 60)
    await bids.place(aid, "ana", key(), 50_000)
    await eventually(lambda: (s := c.cache.get(aid)) is not None and s.seq == 1, 5)

    await c.redis.script_flush()
    # Caminho do pré-filtro (pipeline com EVALSHA do rate limit) e caminho do place_bid.lua.
    assert (await bids.place(aid, "bia", key(), 20_000)).filtered
    await c.redis.script_flush()
    assert (await bids.place(aid, "bia", key(), 60_000)).seq == 2


async def test_consumer_persists_on_notification_and_sweeps_up_lost_notifications(server):
    # Um consumer com varredura a cada 3 s no lugar do da task, recebendo os avisos do Pub/Sub.
    c = server.c
    consumer = BidStreamConsumer(c.redis, BidPersister(c.pool), c.auction_repo, c.loader, c.metrics,
                                 replace(c.settings, consumer_maintenance_interval_s=3))
    listener = c.listener
    await c.consumer.stop()
    consumer.start()
    await asyncio.sleep(0.2)  # primeira varredura (a do start) já passou
    try:
        aid = await new_auction(c, 60)
        listener._on_stream_event = consumer.notify
        await c.bids.place(aid, "ana", key(), 10_000)
        started = time.monotonic()

        async def persisted(n: int):
            async def check() -> bool:
                return await count(c, "bids", aid) == n
            return check

        await eventually(await persisted(1), 1)  # pelo aviso, sem esperar a varredura
        assert time.monotonic() - started < 1

        # Aviso perdido (Pub/Sub é at-most-once): só a varredura persiste, em até um intervalo.
        listener._on_stream_event = lambda _: None
        await c.bids.place(aid, "bia", key(), 11_000)
        await asyncio.sleep(0.5)
        await eventually(await persisted(2), 4)
    finally:
        listener._on_stream_event = c.consumer.notify
        await consumer.stop()
        c.consumer.start()


async def test_reconciler_puts_back_an_open_auction_missing_from_the_closing_queue(server):
    # Falha entre o hydrate (Lua) e o ZADD/SADD, que não são atômicos com ele: o hash existe, mas o closer
    # e a varredura do consumer não sabem do leilão. Sem o reparo ele ficaria aberto para sempre.
    c = server.c
    aid = await new_auction(c, 5)
    await c.bids.place(aid, "ana", key(), 10_000)
    await c.redis.zrem(keys.ENDING, str(aid))
    await c.redis.srem(keys.ACTIVE_STREAMS, str(aid))

    await c.reconciler.reconcile()
    assert await c.redis.zscore(keys.ENDING, str(aid)) is not None
    assert await c.redis.sismember(keys.ACTIVE_STREAMS, str(aid))

    async def closed() -> bool:
        return await c.pool.fetchval("select status from auctions where id = $1", aid) == "CLOSED"

    await eventually(closed, 15)


async def test_reconciler_does_not_pull_back_a_deadline_extended_by_anti_sniping(server):
    c = server.c
    aid = await new_auction(c, 60)
    extended = await c.redis.zscore(keys.ENDING, str(aid)) + 30_000
    await c.redis.zadd(keys.ENDING, {str(aid): extended})

    await c.reconciler.reconcile()
    assert await c.redis.zscore(keys.ENDING, str(aid)) == extended


async def test_a_background_task_that_crashes_is_logged_and_restarted():
    runs = 0
    recovered = asyncio.Event()

    async def run() -> None:
        nonlocal runs
        runs += 1
        if runs == 1:
            raise RuntimeError("falha não prevista")
        recovered.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(_supervised("teste", run))
    try:
        await asyncio.wait_for(recovered.wait(), 5)
    finally:
        task.cancel()
    assert runs == 2

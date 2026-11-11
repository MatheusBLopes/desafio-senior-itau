"""Comportamento com dependências fora do ar: o que continua funcionando e como cada falha aparece."""

import asyncio
import time
from dataclasses import replace

import pytest

from auction.lifecycle.service import AuctionService
from auction.persistence.persister import BidPersister
from auction.persistence.stream_consumer import BidStreamConsumer

from .conftest import count, eventually, key, new_auction


class DeadPool:
    """Faz o papel de um Postgres fora do ar."""

    async def fetchval(self, *a, **k):
        raise OSError("connection refused")

    async def fetchrow(self, *a, **k):
        raise OSError("connection refused")

    async def fetch(self, *a, **k):
        raise OSError("connection refused")


async def test_postgres_down_keeps_the_task_ready_and_reads_answer_503(server, http):
    c = server.c
    aid = await new_auction(c, 60)
    original_pool, original_auctions = c.pool, c.auctions
    dead = DeadPool()
    c.pool = dead
    c.auctions = AuctionService(type(c.auction_repo)(dead), c.loader, c.redis, c.clock)
    try:
        # O ALB não tira a task: lances não dependem do banco.
        assert (await http.get("/readyz")).status_code == 200
        response = await http.get(f"/auctions/{aid}")
        assert response.status_code == 503 and response.headers["retry-after"] == "1"
        bid = await http.post(f"/auctions/{aid}/bids", json={"amount": "100.00"},
                              headers={"X-User-Id": "ana", "Idempotency-Key": key()})
        assert bid.status_code == 201
        import httpx
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{server.management_port}") as internal:
            health = await internal.get("/health")
        assert health.status_code == 503 and health.json()["components"]["postgres"] == "DOWN"
    finally:
        c.pool, c.auctions = original_pool, original_auctions


async def test_valkey_out_of_memory_is_a_503_and_nothing_is_half_written(server, http):
    c = server.c
    aid = await new_auction(c, 60)
    k = key()
    await c.redis.config_set("maxmemory", "1")
    try:
        bid = await http.post(f"/auctions/{aid}/bids", json={"amount": "100.00"},
                              headers={"X-User-Id": "ana", "Idempotency-Key": k})
        assert bid.status_code == 503
    finally:
        await c.redis.config_set("maxmemory", "0")
    # O script foi recusado inteiro: o retry com a mesma key é avaliado como lance novo e aceito.
    retry = await http.post(f"/auctions/{aid}/bids", json={"amount": "100.00"},
                            headers={"X-User-Id": "ana", "Idempotency-Key": k})
    assert retry.status_code == 201 and retry.json()["seq"] == 1


async def test_oversized_and_unbounded_bodies_are_refused(server, http):
    aid = await new_auction(server.c, 60)
    big = await http.post(f"/auctions/{aid}/bids", content=b"{" + b" " * 20_000 + b"}",
                          headers={"Content-Type": "application/json", "X-User-Id": "ana", "Idempotency-Key": key()})
    assert big.status_code == 413

    async def chunks():
        yield b'{"amount": 100}'

    chunked = await http.post(f"/auctions/{aid}/bids", content=chunks(),
                              headers={"Content-Type": "application/json", "X-User-Id": "ana", "Idempotency-Key": key()})
    assert chunked.status_code == 411

    # O valor do header não diferencia maiúsculas: "Chunked" também não pode passar (socket cru, porque o
    # httpx sempre escreve em minúsculas).
    reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
    try:
        writer.write((f"POST /auctions/{aid}/bids HTTP/1.1\r\nHost: t\r\nX-User-Id: ana\r\n"
                      f"Idempotency-Key: {key()}\r\nTransfer-Encoding: Chunked\r\n\r\n"
                      'f\r\n{"amount": 100}\r\n0\r\n\r\n').encode())
        await writer.drain()
        assert b" 411 " in await asyncio.wait_for(reader.readline(), 5)
    finally:
        writer.close()


class FlakyPersister(BidPersister):
    """Falha na primeira gravação, como um Postgres que caiu e voltou."""

    def __init__(self, pool):
        super().__init__(pool)
        self.failures = 1

    async def persist(self, auction_id, records):
        if self.failures:
            self.failures -= 1
            raise OSError("postgres reiniciando")
        return await super().persist(auction_id, records)


async def test_consumer_retries_its_own_failed_batch_without_waiting_for_the_claim(server):
    c = server.c
    consumer = BidStreamConsumer(c.redis, FlakyPersister(c.pool), c.auction_repo, c.loader, c.metrics,
                                 replace(c.settings, consumer_claim_min_idle_ms=600_000,
                                         consumer_maintenance_interval_s=600))
    await c.consumer.stop()
    listener = c.listener
    consumer.start()
    await asyncio.sleep(0.2)
    try:
        aid = await new_auction(c, 60)
        listener._on_stream_event = consumer.notify
        await c.bids.place(aid, "ana", key(), 10_000)
        started = time.monotonic()

        async def persisted() -> bool:
            return await count(c, "bids", aid) == 1

        # Sem o retry próprio, só a reivindicação (aqui, 10 min) recuperaria o lote.
        await eventually(persisted, 5)
        assert time.monotonic() - started < 5
    finally:
        listener._on_stream_event = c.consumer.notify
        await consumer.stop()
        c.consumer.start()

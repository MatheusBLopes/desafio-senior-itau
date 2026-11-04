"""Degradação controlada: excesso de carga e Valkey indisponível viram 503 rápido, não fila nem 500."""

import asyncio
from dataclasses import replace

import httpx

from auction.app import LoadShedder, create_redis
from auction.bids import BidRepository, BidService, RateLimiter
from auction.common import json_response
from auction.lua import LuaScripts

from .conftest import key, new_auction


async def test_requests_above_the_in_flight_limit_are_shed_but_probes_are_not():
    release = asyncio.Event()

    async def slow_app(scope, receive, send):
        await release.wait()
        await json_response({"ok": True})(scope, receive, send)

    shedder = LoadShedder(slow_app, max_in_flight=2)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=shedder), base_url="http://t") as client:
        held = [asyncio.create_task(client.post("/auctions/x/bids")) for _ in range(2)]
        await asyncio.sleep(0.05)

        shed = await client.post("/auctions/x/bids")
        assert shed.status_code == 503
        assert shed.headers["retry-after"] == "1"
        assert shed.json()["code"] == "OVERLOADED"

        probe = asyncio.create_task(client.get("/readyz"))  # isento: entra mesmo com a task cheia
        await asyncio.sleep(0.05)
        assert shedder.in_flight == 2
        release.set()
        assert (await probe).status_code == 200
        assert [r.status_code for r in await asyncio.gather(*held)] == [200, 200]
        assert shedder.in_flight == 0


async def test_valkey_unavailable_returns_503_with_retry_after(server, http):
    c = server.c
    aid = await new_auction(c, 60)
    # Um BidService cujo Valkey não responde (porta fechada), no lugar do da task.
    dead = create_redis(replace(c.settings, redis_port=1))
    original = c.bids
    scripts = LuaScripts.register(dead)  # o script fica preso ao cliente em que foi registrado
    c.bids = BidService(dead, scripts, c.loader, c.cache, RateLimiter(scripts, c.settings.rate_limit),
                        BidRepository(c.pool), c.metrics, c.settings)
    try:
        r = await http.post(f"/auctions/{aid}/bids", json={"amount": "100.00"},
                            headers={"X-User-Id": "ana", "Idempotency-Key": key()})
        assert r.status_code == 503
        assert r.headers["retry-after"] == "1"
        assert r.json()["code"] == "UNAVAILABLE"
    finally:
        c.bids = original
        await dead.aclose()

"""Ponta a ponta pela API HTTP e pelo WebSocket, com Valkey e Postgres reais."""

import asyncio
import uuid

import httpx
import orjson
import websockets
from websockets.exceptions import ConnectionClosed, InvalidStatus

from auction import common

from .conftest import count, eventually, key


async def create_auction(http: httpx.AsyncClient, duration_s: int) -> str:
    created = await http.post("/auctions", json={
        "title": "Ingresso final", "startingPrice": "100.00", "minIncrement": "1.00",
        "durationSeconds": duration_s, "antiSnipingWindowSeconds": 0, "antiSnipingExtendSeconds": 0})
    assert created.status_code == 201
    assert created.headers["location"] == f"/auctions/{created.json()['id']}"
    return created.json()["id"]


async def bid(http: httpx.AsyncClient, aid: str, user: str, k: str, amount: str) -> httpx.Response:
    return await http.post(f"/auctions/{aid}/bids", content=f'{{"amount":{amount}}}',
                           headers={"Content-Type": "application/json", "X-User-Id": user, "Idempotency-Key": k})


async def seqs(http: httpx.AsyncClient, aid: str, after_seq: int) -> list[int]:
    return [b["seq"] for b in (await http.get(f"/auctions/{aid}/bids?afterSeq={after_seq}")).json()]


async def test_http_contract(http):
    aid = await create_auction(http, 60)
    k = key()

    first = await bid(http, aid, "ana", k, "100.00")
    assert first.status_code == 201
    assert first.json()["seq"] == 1 and first.json()["outcome"] == "ACCEPTED"
    assert first.json()["amount"] == "100.00" and first.json()["minimumNextBid"] == "101.00"

    replay = await bid(http, aid, "ana", k, "100.00")
    assert replay.status_code == 200
    assert replay.headers["idempotent-replayed"] == "true"
    assert replay.json()["seq"] == 1

    assert (await bid(http, aid, "bia", key(), "100.00")).status_code == 409
    assert (await bid(http, aid, "ana", k, "999.00")).status_code == 422
    assert (await bid(http, aid, "user:with:colon", key(), "200.00")).status_code == 400
    assert (await bid(http, aid, "ana", key(), "200.001")).status_code == 400
    assert (await bid(http, aid, "ana", key(), '"200.00"')).status_code == 201  # valor como string também vale
    assert (await bid(http, str(uuid.uuid4()), "ana", key(), "300.00")).status_code == 404
    assert (await bid(http, "nao-e-uuid", "ana", key(), "300.00")).status_code == 400

    no_key = await http.post(f"/auctions/{aid}/bids", json={"amount": 300}, headers={"X-User-Id": "ana"})
    assert no_key.status_code == 400
    assert no_key.json()["code"] == "INVALID_REQUEST"


async def test_management_endpoints_are_only_on_the_internal_port_but_probes_stay_public(server, http):
    assert (await http.get("/metrics")).status_code == 404
    assert (await http.get("/health")).status_code == 404
    assert (await http.get("/readyz")).json() == {"status": "UP"}
    assert (await http.get("/livez")).status_code == 200

    async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{server.management_port}") as internal:
        assert "auction_bid_latency_seconds" in (await internal.get("/metrics")).text
        health = await internal.get("/health")
        assert health.status_code == 200
        assert health.json()["components"] == {"valkey": "UP", "postgres": "UP"}
        assert (await internal.get("/auctions/" + str(uuid.uuid4()))).status_code == 404


async def test_auction_closes_and_winner_is_persisted(server, http):
    c = server.c
    aid = await create_auction(http, 5)
    await bid(http, aid, "ana", key(), "100.00")
    await bid(http, aid, "bia", key(), "150.00")

    async def closed() -> bool:
        return await c.pool.fetchval("select status from auctions where id = $1", uuid.UUID(aid)) == "CLOSED"

    await eventually(closed, 15)

    row = await c.pool.fetchrow("select winner_id, final_price from auctions where id = $1", uuid.UUID(aid))
    assert row["winner_id"] == "bia" and row["final_price"] == 15_000
    view = (await http.get(f"/auctions/{aid}")).json()
    assert view["status"] == "CLOSED" and view["winnerId"] == "bia" and view["finalPrice"] == "150.00"
    assert (await bid(http, aid, "caio", key(), "500.00")).status_code == 422


async def test_after_seq_reads_from_stream_while_database_is_behind(server, http):
    c = server.c
    aid = await create_auction(http, 60)
    await c.consumer.stop()
    try:
        for i in range(5):
            await bid(http, aid, f"user{i}", key(), f"{100 + i * 10}.00")
        assert await count(c, "bids", uuid.UUID(aid)) == 0
        assert await seqs(http, aid, 2) == [3, 4, 5]
    finally:
        c.consumer.start()

    async def persisted() -> bool:
        return await count(c, "bids", uuid.UUID(aid)) == 5

    await eventually(persisted, 10)
    assert await seqs(http, aid, 2) == [3, 4, 5]
    assert await seqs(http, aid, 0) == [1, 2, 3, 4, 5]


async def test_after_seq_does_not_skip_bids_when_batches_are_persisted_out_of_order(server, http):
    c = server.c
    aid = await create_auction(http, 60)
    await c.consumer.stop()
    try:
        for i in range(3):
            await bid(http, aid, f"user{i}", key(), f"{100 + i * 10}.00")
        # Outra task lê os seqs 1..3 e ainda não fez commit: ficam pendentes no grupo.
        await c.redis.xreadgroup(c.settings.consumer_group, "task-lenta", {common.stream_key(aid): ">"}, count=10)
    finally:
        c.consumer.start()
    for i in range(3, 6):
        await bid(http, aid, f"user{i}", key(), f"{100 + i * 10}.00")

    async def three_persisted() -> bool:
        return await count(c, "bids", uuid.UUID(aid)) == 3

    await eventually(three_persisted, 10)
    rows = await c.pool.fetch("select seq from bids where auction_id = $1 order by seq", uuid.UUID(aid))
    assert [r["seq"] for r in rows] == [4, 5, 6]

    assert await seqs(http, aid, 0) == [1, 2, 3, 4, 5, 6]
    assert await seqs(http, aid, 3) == [4, 5, 6]

    # Os pendentes da task lenta são reivindicados e o banco converge.
    async def all_persisted() -> bool:
        return await count(c, "bids", uuid.UUID(aid)) == 6

    await eventually(all_persisted, 15)


async def test_state_is_rebuilt_from_postgres_after_valkey_loses_everything(server, http):
    c = server.c
    aid = await create_auction(http, 60)
    await bid(http, aid, "ana", key(), "100.00")
    await bid(http, aid, "bia", key(), "120.00")
    await bid(http, aid, "caio", key(), "130.00")

    async def persisted(n: int):
        async def check() -> bool:
            return await count(c, "bids", uuid.UUID(aid)) == n
        return check

    await eventually(await persisted(3), 10)

    # Simula um restart do Valkey sem persistência.
    await c.redis.flushall()

    # O preço não "volta": lance abaixo do persistido continua rejeitado.
    assert (await bid(http, aid, "dani", key(), "110.00")).status_code == 409
    # E o seq continua de onde parou, sem colidir com UNIQUE(auction_id, seq).
    nxt = await bid(http, aid, "dani", key(), "140.00")
    assert nxt.status_code == 201
    assert nxt.json()["seq"] == 4
    await eventually(await persisted(4), 10)


async def ws_events(server, aid: str):
    return websockets.connect(f"ws://127.0.0.1:{server.port}/ws/auctions/{aid}")


async def next_event(ws, event_type: str, seq: int, timeout: float = 15) -> dict:
    async def wait() -> dict:
        while True:
            e = orjson.loads(await ws.recv())
            if e["type"] == event_type and e["seq"] == seq:
                return e

    return await asyncio.wait_for(wait(), timeout)


async def closed_with(ws) -> ConnectionClosed:
    """Lê (e descarta, como o snapshot inicial) até o servidor fechar a conexão."""
    try:
        async with asyncio.timeout(5):
            while True:
                await ws.recv()
    except ConnectionClosed as e:
        return e
    raise AssertionError("o servidor deveria ter fechado a conexão")


async def test_websocket_receives_snapshots_and_close(server, http):
    aid = await create_auction(http, 6)
    async with await ws_events(server, aid) as ws:
        await asyncio.sleep(0.3)
        for i in range(3):
            await bid(http, aid, f"user{i}", key(), f"{100 + i * 10}.00")

        snapshot = await next_event(ws, "AUCTION_SNAPSHOT", 3)
        assert snapshot["currentPrice"] == "120.00" and snapshot["leaderId"] == "user2"

        closed = await next_event(ws, "AUCTION_CLOSED", 3)
        assert closed["winnerId"] == "user2" and closed["status"] == "CLOSED"


async def test_websocket_clients_cannot_publish_nor_subscribe_outside_an_auction(server, http):
    aid = await create_auction(http, 60)
    async with await ws_events(server, aid) as victim:
        # Um cliente que tenta publicar um AUCTION_CLOSED falso é desconectado; ninguém recebe nada.
        async with await ws_events(server, aid) as attacker:
            await attacker.send(orjson.dumps({"type": "AUCTION_CLOSED", "auctionId": aid, "seq": 1,
                                              "status": "CLOSED", "winnerId": "atacante"}).decode())
            e = await closed_with(attacker)
            assert e.rcvd.code == 1008 and "clientes não publicam" in e.rcvd.reason

        # Destino que não é um leilão (curinga, por exemplo): o handshake é recusado com 403.
        try:
            async with websockets.connect(f"ws://127.0.0.1:{server.port}/ws/auctions/%2A"):
                raise AssertionError("o servidor deveria ter recusado o destino")
        except InvalidStatus as e:
            assert e.response.status_code == 403

        await asyncio.sleep(0.3)
        await bid(http, aid, "ana", key(), "100.00")
        event = await next_event(victim, "AUCTION_SNAPSHOT", 1)
        assert event.get("winnerId") != "atacante"

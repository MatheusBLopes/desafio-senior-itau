"""Log de requisições: cada requisição tem um requestId, devolvido ao cliente e presente em todos os seus logs."""

import logging

import httpx

from auction.api.middleware import RequestLogger
from auction.api.responses import json_response
from auction.observability.logs import bind, log_context


async def handler_app(scope, receive, send):
    """Uma rota qualquer: acrescenta campos ao contexto, como as rotas reais fazem."""
    bind(userId="ana", outcome="ACCEPTED")
    status = 503 if scope["path"] == "/falha" else 201
    await json_response({"ok": True}, status)(scope, receive, send)


def access_records(caplog) -> list[logging.LogRecord]:
    return [r for r in caplog.records if r.name == "auction.api.access"]


async def test_request_gets_an_id_that_comes_back_in_the_response_and_in_the_log(caplog):
    caplog.set_level(logging.INFO, logger="auction")
    transport = httpx.ASGITransport(app=RequestLogger(handler_app), client=("10.0.0.5", 1234))
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        generated = await client.post("/auctions/x/bids", headers={"X-Forwarded-For": "200.1.2.3"})
        given = await client.post("/auctions/x/bids", headers={"X-Request-Id": "demo-ana-0001"})
        unsafe = await client.post("/auctions/x/bids", headers={"X-Request-Id": "tem espaco e <tag>"})
        failure = await client.get("/falha")
        await client.get("/readyz")

    assert len(generated.headers["x-request-id"]) == 16
    assert given.headers["x-request-id"] == "demo-ana-0001"  # o id do cliente é reaproveitado
    assert unsafe.headers["x-request-id"] != "tem espaco e <tag>"  # id inseguro é trocado

    records = access_records(caplog)
    assert len(records) == 4  # health check não gera linha
    first = records[0]
    assert first.fields == {"method": "POST", "route": "/auctions/x/bids", "path": "/auctions/x/bids",
                            "status": 201, "durationMs": first.fields["durationMs"]}
    assert first.levelno == logging.INFO
    assert records[-1].levelno == logging.WARNING  # 5xx sobe para WARNING


async def test_access_line_carries_the_request_context(caplog):
    caplog.set_level(logging.INFO, logger="auction")
    seen = {}

    async def app(scope, receive, send):
        await handler_app(scope, receive, send)
        seen.update(log_context.get())

    transport = httpx.ASGITransport(app=RequestLogger(app), client=("10.0.0.5", 1234))
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        response = await client.post("/auctions/x/bids", headers={"X-Forwarded-For": "200.1.2.3"})

    # O contexto que a rota montou (usuário, resultado) é o mesmo que a linha final usa.
    assert seen["requestId"] == response.headers["x-request-id"]
    assert seen["clientIp"] == "200.1.2.3"  # IP real atrás do proxy privado
    assert seen["userId"] == "ana" and seen["outcome"] == "ACCEPTED"


async def test_request_log_can_be_disabled_but_keeps_the_request_id(caplog):
    caplog.set_level(logging.INFO, logger="auction")
    transport = httpx.ASGITransport(app=RequestLogger(handler_app, enabled=False))
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        response = await client.post("/auctions/x/bids")
    assert "x-request-id" in response.headers
    assert access_records(caplog) == []

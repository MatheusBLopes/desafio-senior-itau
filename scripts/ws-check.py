"""Verifica o tempo real ponta a ponta: cria um leilão de 8 s, dá 3 lances e espera, pelo WebSocket,
o snapshot com o líder e o AUCTION_CLOSED com o vencedor. Com Cognito (AWS), cadastra usuários
descartáveis e usa os access tokens; sem ele (local), usa o header X-User-Id.

    uv run --project bid-service python scripts/ws-check.py http://<alb-dns>
"""

import asyncio
import json
import sys
import uuid

import httpx
import websockets


async def identities(http: httpx.AsyncClient, names: list[str]) -> dict[str, tuple[str, dict]]:
    """nome → (usuário que aparece no leilão, headers que o identificam)."""
    probe = await http.post("/auth/login", json={"username": "ws-probe", "password": "probe-12345"})
    if probe.status_code == 404:
        return {n: (n, {"X-User-Id": n}) for n in names}
    suffix, result = uuid.uuid4().hex[:8], {}
    for n in names:
        user, password = f"ws-{n}-{suffix}", f"ws-{suffix}-pw1"
        (await http.post("/auth/signup", json={"username": user, "password": password})).raise_for_status()
        login = await http.post("/auth/login", json={"username": user, "password": password})
        login.raise_for_status()
        result[n] = (user, {"Authorization": f"Bearer {login.json()['accessToken']}"})
    return result


async def main(base: str) -> None:
    ws_base = base.replace("http://", "ws://").replace("https://", "wss://")
    async with httpx.AsyncClient(base_url=base, timeout=10) as http:
        who = await identities(http, ["ana", "bia", "caio"])
        auction = (await http.post("/auctions", headers=who["ana"][1], json={
            "title": "ws-check", "startingPrice": "100.00", "minIncrement": "1.00", "durationSeconds": 8,
            "antiSnipingWindowSeconds": 0, "antiSnipingExtendSeconds": 0})).json()["id"]
        async with websockets.connect(f"{ws_base}/ws/auctions/{auction}") as ws:
            for i, name in enumerate(["ana", "bia", "caio"]):
                r = await http.post(f"/auctions/{auction}/bids", json={"amount": f"{100 + i * 10}.00"},
                                    headers={**who[name][1], "Idempotency-Key": str(uuid.uuid4())})
                assert r.status_code == 201, r.text
            seen = set()
            async with asyncio.timeout(30):
                while True:
                    e = json.loads(await ws.recv())
                    if e["type"] == "AUCTION_SNAPSHOT" and e["seq"] == 3 and "snapshot" not in seen:
                        assert e["leaderId"] == who["caio"][0] and e["currentPrice"] == "120.00", e
                        seen.add("snapshot")
                        print(f"  ok   snapshot seq=3 líder={e['leaderId']} preço={e['currentPrice']}")
                    if e["type"] == "AUCTION_CLOSED":
                        assert e["winnerId"] == who["caio"][0], e
                        print(f"  ok   encerramento vencedor={e['winnerId']} preço final={e['finalPrice']}")
                        break
    assert "snapshot" in seen, "snapshot não chegou"
    print("WebSocket ok")


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8080"))

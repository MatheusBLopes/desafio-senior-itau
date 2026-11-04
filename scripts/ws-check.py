"""Verifica o tempo real ponta a ponta: cria um leilão de 8 s, dá 3 lances e espera, pelo WebSocket,
o snapshot com o líder e o AUCTION_CLOSED com o vencedor.

    uv run --project bid-service python scripts/ws-check.py http://<alb-dns>
"""

import asyncio
import json
import sys
import uuid

import httpx
import websockets


async def main(base: str) -> None:
    ws_base = base.replace("http://", "ws://").replace("https://", "wss://")
    async with httpx.AsyncClient(base_url=base, timeout=10) as http:
        auction = (await http.post("/auctions", json={
            "title": "ws-check", "startingPrice": "100.00", "minIncrement": "1.00", "durationSeconds": 8,
            "antiSnipingWindowSeconds": 0, "antiSnipingExtendSeconds": 0})).json()["id"]
        async with websockets.connect(f"{ws_base}/ws/auctions/{auction}") as ws:
            for i, user in enumerate(["ana", "bia", "caio"]):
                r = await http.post(f"/auctions/{auction}/bids", json={"amount": f"{100 + i * 10}.00"},
                                    headers={"X-User-Id": user, "Idempotency-Key": str(uuid.uuid4())})
                assert r.status_code == 201, r.text
            seen = set()
            async with asyncio.timeout(30):
                while True:
                    e = json.loads(await ws.recv())
                    if e["type"] == "AUCTION_SNAPSHOT" and e["seq"] == 3 and "snapshot" not in seen:
                        assert e["leaderId"] == "caio" and e["currentPrice"] == "120.00", e
                        seen.add("snapshot")
                        print(f"  ok   snapshot seq=3 líder={e['leaderId']} preço={e['currentPrice']}")
                    if e["type"] == "AUCTION_CLOSED":
                        assert e["winnerId"] == "caio", e
                        print(f"  ok   encerramento vencedor={e['winnerId']} preço final={e['finalPrice']}")
                        break
    assert "snapshot" in seen, "snapshot não chegou"
    print("WebSocket ok")


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8080"))

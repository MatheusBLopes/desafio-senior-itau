"""Uma aplicação real para todos os testes: Valkey e Postgres em containers (Testcontainers) e o
servidor uvicorn escutando em portas aleatórias, no mesmo event loop dos testes."""

import asyncio
import socket
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, replace
from decimal import Decimal

import httpx
import pytest
import uvicorn
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.postgres import PostgresContainer

from auction.app import Container, create_app
from auction.auctions import CreateAuctionRequest
from auction.config import RateLimitConfig, Settings


@dataclass
class Server:
    c: Container
    port: int
    management_port: int

    @property
    def base(self) -> str:
        return f"http://127.0.0.1:{self.port}"


def _free_socket() -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    return sock


@pytest.fixture(scope="session")
def containers():
    with PostgresContainer("postgres:17-alpine", driver=None) as pg, \
            DockerContainer("valkey/valkey:8.1-alpine") \
            .with_command("valkey-server --maxmemory-policy noeviction").with_exposed_ports(6379) as valkey:
        wait_for_logs(valkey, "Ready to accept connections")
        yield pg, valkey


@pytest.fixture(scope="session")
async def server(containers) -> AsyncIterator[Server]:
    pg, valkey = containers
    api, management = _free_socket(), _free_socket()
    settings = replace(
        Settings(),
        db_dsn=pg.get_connection_url(),
        redis_host=valkey.get_container_host_ip(),
        redis_port=int(valkey.get_exposed_port(6379)),
        port=api.getsockname()[1],
        management_port=management.getsockname()[1],
        closer_interval_s=0.1,
        snapshot_interval_s=0.1,
        consumer_claim_min_idle_ms=2000,
        consumer_maintenance_interval_s=1,
        rate_limit=RateLimitConfig(enabled=False, per_ip_enabled=False),
        # O teste de concorrência chama o serviço direto, sem o LoadShedder na frente: mais de 10 mil
        # corrotinas disputam o pool de 64 conexões. Ele mede correção, não capacidade.
        redis_pool_timeout_s=30,
    )
    app = create_app(settings)
    uv = uvicorn.Server(uvicorn.Config(app, ws="websockets-sansio", log_config=None, access_log=False))
    task = asyncio.create_task(uv.serve(sockets=[api, management]))
    while not uv.started:
        await asyncio.sleep(0.05)
    yield Server(app.state.c, settings.port, settings.management_port)
    uv.should_exit = True
    await task


@pytest.fixture(scope="session")
async def http(server: Server) -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(base_url=server.base, timeout=10) as client:
        yield client


def key() -> str:
    return str(uuid.uuid4())


async def new_auction(c: Container, duration_s: int, window_s: int = 0, extend_s: int = 0) -> uuid.UUID:
    """Leilão com preço inicial 100,00, incremento 1,00 e anti-sniping desligado por padrão."""
    view = await c.auctions.create(CreateAuctionRequest(
        title="Item raro", startingPrice=Decimal("100.00"), minIncrement=Decimal("1.00"),
        durationSeconds=duration_s, antiSnipingWindowSeconds=window_s, antiSnipingExtendSeconds=extend_s))
    return uuid.UUID(view["id"])


async def eventually(condition: Callable[[], Awaitable[bool] | bool], timeout: float = 10) -> None:
    """Equivalente ao Awaitility: repete até a condição valer ou estourar o prazo."""
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        result = condition()
        if asyncio.iscoroutine(result):
            result = await result
        if result:
            return
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condição não foi atingida no prazo")
        await asyncio.sleep(0.05)


async def count(c: Container, table: str, auction_id: uuid.UUID) -> int:
    return await c.pool.fetchval(f"select count(*) from {table} where auction_id = $1", auction_id)

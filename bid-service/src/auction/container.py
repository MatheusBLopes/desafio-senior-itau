"""Montagem das dependências da task e ciclo de vida das tarefas de fundo."""

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import asyncpg
from redis.asyncio import Redis

from .auth.authenticator import Authenticator, create_authenticator
from .bidding.history import BidHistory, BidRepository
from .bidding.rate_limit import IpRateLimiter, UserRateLimiter
from .bidding.service import BidService
from .config import Settings
from .lifecycle.repository import AuctionRepository
from .lifecycle.scheduler import AuctionCloser, AuctionReconciler
from .lifecycle.service import AuctionService
from .lifecycle.state_loader import AuctionStateLoader
from .observability.metrics import Metrics, export_emf
from .observability.tracing import untraced
from .persistence.persister import BidPersister
from .persistence.stream_consumer import BidStreamConsumer
from .realtime.broadcaster import SnapshotBroadcaster
from .realtime.cache import LiveAuctionCache
from .realtime.listener import AuctionEventListener
from .storage import postgres
from .storage.scripts import LuaScripts
from .storage.valkey import RedisClock, create_pubsub_redis, create_redis


@dataclass
class Container:
    """Todas as dependências da task, criadas uma vez no startup"""

    settings: Settings
    redis: Redis
    pubsub_redis: Redis
    pool: asyncpg.Pool
    clock: RedisClock
    scripts: LuaScripts
    metrics: Metrics
    cache: LiveAuctionCache
    broadcaster: SnapshotBroadcaster
    listener: AuctionEventListener
    auctions: AuctionService
    auction_repo: AuctionRepository
    loader: AuctionStateLoader
    bids: BidService
    bid_history: BidHistory
    ip_limiter: IpRateLimiter
    consumer: BidStreamConsumer
    closer: AuctionCloser
    reconciler: AuctionReconciler
    auth: Authenticator
    tasks: list[asyncio.Task[Any]] = field(default_factory=list)


async def build(settings: Settings) -> Container:
    """Conecta ao Valkey e ao Postgres, aplica as migrations e liga todas as peças."""
    redis = create_redis(settings)
    pool = await postgres.create_pool(settings.db_dsn, settings.db_pool_size)
    await postgres.migrate(pool)
    clock = RedisClock(redis)
    scripts = LuaScripts.register(redis, settings)
    metrics = Metrics()
    cache = LiveAuctionCache()
    broadcaster = SnapshotBroadcaster(cache)
    auction_repo = AuctionRepository(pool)
    loader = AuctionStateLoader(auction_repo, pool, redis, scripts, settings)
    consumer = BidStreamConsumer(redis, BidPersister(pool), auction_repo, loader, metrics, settings)
    pubsub_redis = create_pubsub_redis(settings)
    listener = AuctionEventListener(pubsub_redis, settings.events_channel, cache, broadcaster, consumer.notify)
    return Container(
        settings=settings, redis=redis, pubsub_redis=pubsub_redis, pool=pool, clock=clock, scripts=scripts,
        metrics=metrics, cache=cache, broadcaster=broadcaster, listener=listener,
        auctions=AuctionService(auction_repo, loader, redis, clock), auction_repo=auction_repo, loader=loader,
        bids=BidService(redis, scripts, loader, cache, UserRateLimiter(scripts, settings.rate_limit), metrics,
                        settings),
        bid_history=BidHistory(redis, BidRepository(pool)),
        ip_limiter=IpRateLimiter(settings.rate_limit),
        consumer=consumer,
        closer=AuctionCloser(redis, scripts, loader, clock, settings),
        reconciler=AuctionReconciler(auction_repo, loader, settings),
        auth=create_authenticator(settings),
    )


async def _every(seconds: float, action: Callable[[], None]) -> None:
    """Tarefa de fundo que executa `action` a cada `seconds`."""
    while True:
        await asyncio.sleep(seconds)
        action()


def start_background(container: Container) -> None:
    """Inicia as tarefas de fundo fora do tracing (ver untraced)."""
    with untraced():
        _start_background(container)


def _start_background(container: Container) -> None:
    """Listener do Pub/Sub, snapshots, encerramento, reconciliação, limpeza do limite por IP, EMF e consumer."""
    settings = container.settings
    container.tasks += [
        asyncio.create_task(container.listener.run(), name="auction-events"),
        asyncio.create_task(container.broadcaster.run(settings.snapshot_interval_s), name="snapshots"),
        asyncio.create_task(container.closer.run(), name="closer"),
        asyncio.create_task(container.reconciler.run(), name="reconciler"),
        asyncio.create_task(_every(60, container.ip_limiter.evict_idle), name="ip-limiter-evict"),
    ]
    if settings.cloudwatch_emf:
        container.tasks.append(asyncio.create_task(
            export_emf(container.metrics, settings.metrics_namespace, settings.metrics_step_s)))
    container.consumer.start()


async def shutdown(container: Container) -> None:
    """Para o consumer e as tarefas de fundo e fecha as conexões."""
    await container.consumer.stop()
    for task in container.tasks:
        task.cancel()
    await asyncio.gather(*container.tasks, return_exceptions=True)
    await container.pool.close()
    await container.redis.aclose()
    await container.pubsub_redis.aclose()

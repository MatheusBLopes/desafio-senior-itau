"""Montagem da aplicação: dependências, rotas HTTP/WebSocket, tratamento de erros e tarefas de fundo."""

import asyncio
import contextlib
import logging
import re
import uuid
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass, field
from typing import Annotated, Any

import asyncpg
import orjson
from fastapi import FastAPI, Query, Request, WebSocket
from fastapi.exceptions import RequestValidationError
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from redis.asyncio import BlockingConnectionPool, Redis
from redis.asyncio.connection import SSLConnection
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from . import db
from .auth import (Authenticator, CognitoConfig, CognitoIdentityProvider, HttpJwks, JwtVerifier,
                   validate_credentials)
from .auctions import AuctionRepository, AuctionService, AuctionStateLoader, CreateAuctionRequest
from .bids import BidRepository, BidService, ClientIpRateLimiter, RateLimiter, client_ip, parse_amount
from .common import ApiError, RedisClock, json_response
from .config import Settings
from .lua import LuaScripts
from .observability import REQUESTS_SHED, Metrics, bind, configure_logging, export_emf, log_context
from .persistence import BidPersister, BidStreamConsumer
from .realtime import AuctionEventListener, LiveAuctionCache, SnapshotBroadcaster
from .scheduler import AuctionCloser, AuctionReconciler

log = logging.getLogger(__name__)

IDEMPOTENCY_KEY = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")

MANAGEMENT_PATHS = {"/metrics", "/health"}


@dataclass
class Container:
    """Todas as dependências da task, criadas uma vez no startup (o papel do contexto do Spring)."""

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
    ip_limiter: ClientIpRateLimiter
    consumer: BidStreamConsumer
    closer: AuctionCloser
    reconciler: AuctionReconciler
    auth: Authenticator
    tasks: list[asyncio.Task[Any]] = field(default_factory=list)


def create_redis(s: Settings) -> Redis:
    kwargs: dict[str, Any] = {"connection_class": SSLConnection} if s.redis_tls else {}
    # Pool limitado e bloqueante: sob pico, a requisição espera uma conexão em vez de abrir milhares.
    # A espera é curta: pool esgotado vira 503 rápido (handler overloaded), não fila de segundos.
    pool = BlockingConnectionPool(host=s.redis_host, port=s.redis_port, max_connections=s.redis_pool_size,
                                  timeout=s.redis_pool_timeout_s, socket_timeout=2, decode_responses=True,
                                  **kwargs)
    return Redis(connection_pool=pool)


def create_pubsub_redis(s: Settings) -> Redis:
    """Conexão dedicada ao Pub/Sub: fica bloqueada esperando mensagens, então sem timeout de leitura.

    O health check (PING a cada 15 s) detecta uma conexão morta em vez de esperar para sempre.
    """
    kwargs: dict[str, Any] = {"ssl": True} if s.redis_tls else {}
    return Redis(host=s.redis_host, port=s.redis_port, decode_responses=True, socket_timeout=None,
                 health_check_interval=15, **kwargs)


def create_authenticator(s: Settings) -> Authenticator:
    if s.auth_mode != "cognito":
        log.warning("AUTH_MODE=%s: o usuário vem do X-User-Id, sem autenticação. Só para desenvolvimento.",
                    s.auth_mode)
        return Authenticator(s.auth_mode)
    if not s.cognito_user_pool_id or not s.cognito_client_id:
        raise RuntimeError("AUTH_MODE=cognito exige COGNITO_USER_POOL_ID e COGNITO_CLIENT_ID")
    config = CognitoConfig(s.cognito_region, s.cognito_user_pool_id, s.cognito_client_id)
    verifier = JwtVerifier(config.issuer, config.client_id, HttpJwks(config.jwks_url))
    return Authenticator("cognito", verifier, CognitoIdentityProvider(config))


async def build(s: Settings) -> Container:
    redis = create_redis(s)
    pool = await db.create_pool(s.db_dsn, s.db_pool_size)
    await db.migrate(pool)
    clock = RedisClock(redis)
    scripts = LuaScripts.register(redis)
    metrics = Metrics()
    cache = LiveAuctionCache()
    broadcaster = SnapshotBroadcaster(cache)
    auction_repo = AuctionRepository(pool)
    loader = AuctionStateLoader(auction_repo, pool, redis, scripts, s)
    consumer = BidStreamConsumer(redis, BidPersister(pool), auction_repo, loader, metrics, s)
    pubsub_redis = create_pubsub_redis(s)
    listener = AuctionEventListener(pubsub_redis, s.events_channel, cache, broadcaster, consumer.notify)
    return Container(
        settings=s, redis=redis, pubsub_redis=pubsub_redis, pool=pool, clock=clock, scripts=scripts,
        metrics=metrics, cache=cache, broadcaster=broadcaster, listener=listener,
        auctions=AuctionService(auction_repo, loader, redis, clock), auction_repo=auction_repo, loader=loader,
        bids=BidService(redis, scripts, loader, cache, RateLimiter(scripts, s.rate_limit), BidRepository(pool),
                        metrics, s),
        ip_limiter=ClientIpRateLimiter(s.rate_limit),
        consumer=consumer,
        closer=AuctionCloser(redis, scripts, loader, clock, s),
        reconciler=AuctionReconciler(auction_repo, loader, s),
        auth=create_authenticator(s),
    )


async def _every(seconds: float, fn: Any) -> None:
    while True:
        await asyncio.sleep(seconds)
        fn()


@contextlib.contextmanager
def _untraced() -> Iterator[None]:
    """Contexto sem tracing para as tarefas de fundo.

    O closer (a cada 200 ms), o reconciliador e o consumer gerariam traces raiz o tempo todo, sem
    requisição nenhuma: ruído no X-Ray e, mesmo a 5%, ~1,3 milhão de traces/mês com 2 tasks, acima do
    free tier. As tarefas nascem sob um span pai marcado como "não amostrado": o sampler
    parentbased_traceidratio segue o pai, então todo span filho (redis, asyncpg) é descartado. O
    suppress_instrumentation do OpenTelemetry não serviria: os instrumentadores de redis e asyncpg o
    ignoram. As tasks do asyncio copiam o contexto em que são criadas. Sem OpenTelemetry, não faz nada.
    """
    try:
        from opentelemetry import context, trace
        from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags
    except ImportError:
        yield
        return
    unsampled = NonRecordingSpan(SpanContext(trace_id=1, span_id=1, is_remote=False,
                                             trace_flags=TraceFlags(TraceFlags.DEFAULT)))
    token = context.attach(trace.set_span_in_context(unsampled))
    try:
        yield
    finally:
        context.detach(token)


def start_background(c: Container) -> None:
    with _untraced():
        _start_background(c)


def _start_background(c: Container) -> None:
    s = c.settings
    c.tasks += [
        asyncio.create_task(c.listener.run(), name="auction-events"),
        asyncio.create_task(c.broadcaster.run(s.snapshot_interval_s), name="snapshots"),
        asyncio.create_task(c.closer.run(), name="closer"),
        asyncio.create_task(c.reconciler.run(), name="reconciler"),
        asyncio.create_task(_every(60, c.ip_limiter.evict_idle), name="ip-limiter-evict"),
    ]
    if s.cloudwatch_emf:
        c.tasks.append(asyncio.create_task(export_emf(c.metrics, s.metrics_namespace, s.metrics_step_s)))
    c.consumer.start()


async def shutdown(c: Container) -> None:
    await c.consumer.stop()
    for t in c.tasks:
        t.cancel()
    await asyncio.gather(*c.tasks, return_exceptions=True)
    await c.pool.close()
    await c.redis.aclose()
    await c.pubsub_redis.aclose()


class PortSeparation:
    """Métricas e health detalhado só na porta interna; a API só na pública (o ALB vê só a 8080).

    Middleware ASGI puro (sem BaseHTTPMiddleware): vale para HTTP e WebSocket e custa quase nada.
    Também zera o contexto de log a cada requisição; o userId entra nele só depois de autenticado.
    """

    def __init__(self, app: Any):
        self.app = app

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] not in ("http", "websocket"):
            return await self.app(scope, receive, send)
        on_management = scope["server"][1] in scope["app"].state.management_ports
        if on_management != (scope["path"] in MANAGEMENT_PATHS):
            if scope["type"] == "websocket":
                return await send({"type": "websocket.close", "code": 1008})
            return await json_response({"code": "NOT_FOUND", "message": "não encontrado"}, 404)(scope, receive, send)
        log_context.set({})
        return await self.app(scope, receive, send)


class LoadShedder:
    """Limita as requisições HTTP em andamento nesta task; acima do limite responde 503 na hora.

    Sem limite, uma task sobrecarregada aceita tudo, a fila cresce, a latência de todos explode e o
    throughput cai (colapso). Com o limite, ela atende o que consegue com latência estável e devolve
    o excesso em microssegundos, com Retry-After; o ALB e o autoscaling cuidam do resto.

    Conta requisições, não conexões: o ALB mantém conexões keep-alive ociosas que não custam nada.
    Probes (/livez, /readyz) e WebSocket ficam de fora: uma task cheia não deve sair do ALB por isso.
    """

    EXEMPT = {"/livez", "/readyz"}

    def __init__(self, app: Any, max_in_flight: int):
        self.app = app
        self.max_in_flight = max_in_flight
        self.in_flight = 0

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] != "http" or scope["path"] in self.EXEMPT:
            return await self.app(scope, receive, send)
        if self.in_flight >= self.max_in_flight:
            REQUESTS_SHED.inc()
            return await json_response({"code": "OVERLOADED", "message": "tente de novo em instantes"}, 503,
                                       {"Retry-After": "1"})(scope, receive, send)
        self.in_flight += 1
        try:
            await self.app(scope, receive, send)
        finally:
            self.in_flight -= 1


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        c = await build(settings)
        app.state.c = c
        start_background(c)
        # Pronto só depois de assinar o Pub/Sub: senão o cache do pré-filtro nasceria cego.
        await asyncio.wait_for(c.listener.subscribed.wait(), timeout=10)
        log.info("bid-service pronto na porta %s (gestão em %s)", settings.port, settings.management_port)
        yield
        await shutdown(c)

    app = FastAPI(title="bid-service", lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    app.state.management_ports = {settings.management_port}
    app.add_middleware(PortSeparation)
    # Adicionado por último = executa primeiro: rejeitar o excesso deve ser o caminho mais barato.
    app.add_middleware(LoadShedder, max_in_flight=settings.max_in_flight)

    @app.exception_handler(ApiError)
    async def api_error(_: Request, e: ApiError) -> Response:
        return json_response({"code": e.code, "message": e.message}, e.status)

    @app.exception_handler(RequestValidationError)
    async def validation_error(_: Request, e: RequestValidationError) -> Response:
        message = "; ".join(f"{'.'.join(str(p) for p in err['loc'][1:]) or err['loc'][0]}: {err['msg']}"
                            for err in e.errors())
        return json_response({"code": "INVALID_REQUEST", "message": message}, 400)

    @app.exception_handler(RedisConnectionError)
    @app.exception_handler(RedisTimeoutError)
    async def overloaded(_: Request, e: Exception) -> Response:
        # Pool do Valkey esgotado ou Valkey fora: 503 diz ao cliente (e ao ALB) que é temporário.
        log.warning("Valkey indisponível: %s", e)
        return json_response({"code": "UNAVAILABLE", "message": "tente de novo em instantes"}, 503,
                             {"Retry-After": "1"})

    @app.exception_handler(Exception)
    async def unexpected(_: Request, e: Exception) -> Response:
        log.exception("erro inesperado")
        return json_response({"code": "INTERNAL_ERROR", "message": "erro interno"}, 500)

    def container(request: Request) -> Container:
        return request.app.state.c

    # --- Leilões ---------------------------------------------------------------------------------

    # --- Autenticação ------------------------------------------------------------------------------

    @app.post("/auth/signup")
    async def signup(request: Request) -> Response:
        """Cria o usuário no Cognito já confirmado (sem e-mail). Limitado por IP contra cadastro em massa."""
        c = container(request)
        c.ip_limiter.check(client_ip(request.client.host if request.client else None,
                                     request.headers.get("x-forwarded-for")))
        username, password = validate_credentials(await _json(request))
        await c.auth.identity_provider().sign_up(username, password)
        return json_response({"username": username}, 201)

    @app.post("/auth/login")
    async def login(request: Request) -> Response:
        """Troca usuário e senha por um access token do Cognito (JWT, 1 h)."""
        c = container(request)
        c.ip_limiter.check(client_ip(request.client.host if request.client else None,
                                     request.headers.get("x-forwarded-for")))
        username, password = validate_credentials(await _json(request))
        return json_response(await c.auth.identity_provider().login(username, password))

    # --- Leilões ---------------------------------------------------------------------------------

    @app.post("/auctions")
    async def create_auction(body: CreateAuctionRequest, request: Request) -> Response:
        c = container(request)
        if c.auth.mode == "cognito":
            bind(userId=await c.auth.user_of(request.headers))
        view = await c.auctions.create(body)
        return json_response(view, 201, {"Location": f"/auctions/{view['id']}"})

    @app.get("/auctions/{auction_id}")
    async def get_auction(auction_id: uuid.UUID, request: Request) -> Response:
        return json_response(await container(request).auctions.get(auction_id))

    # --- Lances ----------------------------------------------------------------------------------

    @app.post("/auctions/{auction_id}/bids")
    async def place_bid(auction_id: uuid.UUID, request: Request) -> Response:
        """201 aceito; 200 replay de um aceite; 409 abaixo do mínimo; 422 encerrado ou key reutilizada.

        O replay de uma rejeição devolve o mesmo status da resposta original, com Idempotent-Replayed.
        Caminho quente: headers e corpo são lidos e validados aqui, sem a injeção de dependências.
        """
        headers = request.headers
        c = container(request)
        user_id = await c.auth.user_of(headers)
        bind(userId=user_id)
        key = headers.get("idempotency-key") or ""
        if not IDEMPOTENCY_KEY.match(key):
            raise ApiError.bad_request("Idempotency-Key: use 1 a 128 caracteres [A-Za-z0-9_.:-]")
        amount = parse_amount(await request.body())
        c.ip_limiter.check(client_ip(request.client.host if request.client else None,
                                     headers.get("x-forwarded-for")))
        result = await c.bids.place(auction_id, user_id, key, amount)
        return json_response(result.to_json(), result.http_status(),
                             {"Idempotent-Replayed": "true" if result.replayed else "false"})

    @app.get("/auctions/{auction_id}/bids")
    async def list_bids(
            auction_id: uuid.UUID,
            request: Request,
            afterSeq: Annotated[int, Query(ge=0)] = 0,
            limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    ) -> Response:
        return json_response(await container(request).bids.list_after(auction_id, afterSeq, limit))

    # --- Tempo real ------------------------------------------------------------------------------

    @app.websocket("/ws/auctions/{auction_id}")
    async def auction_events(ws: WebSocket, auction_id: str) -> None:
        await ws.app.state.c.broadcaster.serve(ws, auction_id)

    # --- Saúde e métricas ------------------------------------------------------------------------

    @app.get("/livez")
    async def livez() -> Response:
        return json_response({"status": "UP"})

    async def readiness(c: Container) -> dict[str, str]:
        checks = {}
        try:
            await asyncio.wait_for(c.redis.ping(), 2)
            checks["valkey"] = "UP"
        except Exception:
            checks["valkey"] = "DOWN"
        try:
            await asyncio.wait_for(c.pool.fetchval("select 1"), 2)
            checks["postgres"] = "UP"
        except Exception:
            checks["postgres"] = "DOWN"
        return checks

    @app.get("/readyz")
    async def readyz(request: Request) -> Response:
        """Health check do ALB: só recebe tráfego a task que alcança o Valkey e o Postgres."""
        up = all(v == "UP" for v in (await readiness(container(request))).values())
        return json_response({"status": "UP" if up else "DOWN"}, 200 if up else 503)

    @app.get("/health")
    async def health(request: Request) -> Response:
        checks = await readiness(container(request))
        up = all(v == "UP" for v in checks.values())
        return json_response({"status": "UP" if up else "DOWN", "components": checks}, 200 if up else 503)

    @app.get("/metrics")
    async def metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return app


async def _json(request: Request) -> object:
    try:
        return orjson.loads(await request.body())
    except orjson.JSONDecodeError:
        raise ApiError.bad_request("corpo JSON inválido") from None


def configure(settings: Settings) -> None:
    configure_logging(settings.json_logs)

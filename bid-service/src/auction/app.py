"""Montagem da aplicação FastAPI: ciclo de vida, middlewares, handlers de erro e rotas."""

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator

from fastapi import FastAPI

from .api import auction_routes, auth_routes, bid_routes, health_routes, realtime_routes
from .api.error_handlers import register_error_handlers
from .api.middleware import LoadShedder, PortSeparation, RequestLogger
from .config import Settings
from .container import build, shutdown, start_background
from .observability.logs import configure_logging, fields

log = logging.getLogger(__name__)


def create_app(settings: Settings | None = None) -> FastAPI:
    """Cria a aplicação. As dependências só são montadas no startup (lifespan), já com o event loop rodando."""
    settings = settings or Settings.from_env()

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        """Startup: monta o container e inicia as tarefas de fundo. Shutdown: para tudo."""
        container = await build(settings)
        app.state.container = container
        start_background(container)
        # Pronto só depois de assinar o Pub/Sub: senão o cache do pré-filtro nasceria cego.
        await asyncio.wait_for(container.listener.subscribed.wait(), timeout=10)
        log.info("bid-service pronto", extra=fields(
            port=settings.port, managementPort=settings.management_port, authMode=settings.auth_mode,
            maxInFlight=settings.max_in_flight, rateLimit=settings.rate_limit.enabled,
            logRequests=settings.log_requests))
        yield
        await shutdown(container)
        log.info("bid-service encerrado")

    app = FastAPI(title="bid-service", lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    app.state.management_ports = {settings.management_port}
    # Ordem de execução: LoadShedder → RequestLogger → PortSeparation → rotas (o último adicionado roda primeiro).
    app.add_middleware(PortSeparation)
    # Abre o contexto de log antes de tudo que pode responder; o excesso recusado pelo LoadShedder não gera
    # linha (seria uma por requisição justamente na sobrecarga) e é contado na métrica auction.requests.shed.
    app.add_middleware(RequestLogger, enabled=settings.log_requests)
    # Adicionado por último = executa primeiro: rejeitar o excesso deve ser o caminho mais barato.
    app.add_middleware(LoadShedder, max_in_flight=settings.max_in_flight)

    register_error_handlers(app)
    for routes in (auth_routes, auction_routes, bid_routes, realtime_routes, health_routes):
        app.include_router(routes.router)
    return app


def configure(settings: Settings) -> None:
    """Configuração do processo que vem antes da aplicação (hoje, só os logs)."""
    configure_logging(settings.json_logs, settings.log_level)

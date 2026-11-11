"""Saúde e métricas: /livez e /readyz na porta pública; /health e /metrics só na porta interna."""

import asyncio

from fastapi import APIRouter, Request
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from ..container import Container
from .request_helpers import container_of
from .responses import json_response

router = APIRouter()


@router.get("/livez")
async def livez() -> Response:
    """O processo está de pé e o event loop responde."""
    return json_response({"status": "UP"})


async def _component_status(container: Container) -> dict[str, str]:
    """UP/DOWN de cada dependência, com 2 s de prazo para cada uma."""
    checks = {}
    try:
        await asyncio.wait_for(container.redis.ping(), 2)
        checks["valkey"] = "UP"
    except Exception:
        checks["valkey"] = "DOWN"
    try:
        await asyncio.wait_for(container.pool.fetchval("select 1"), 2)
        checks["postgres"] = "UP"
    except Exception:
        checks["postgres"] = "DOWN"
    return checks


@router.get("/readyz")
async def readyz(request: Request) -> Response:
    """Health check do ALB: só o Valkey, que é o caminho crítico do lance.

    O Postgres fica de fora de propósito. Com ele aqui, uma queda do banco deixaria TODAS as tasks
    "unhealthy" e o ECS as substituiria em loop, derrubando os lances, que continuam funcionando sem o
    banco (write-behind: acumulam no stream). O estado do Postgres aparece em /health e nos alarmes.
    """
    container = container_of(request)
    try:
        await asyncio.wait_for(container.redis.ping(), 2)
        return json_response({"status": "UP"})
    except Exception:
        return json_response({"status": "DOWN"}, 503)


@router.get("/health")
async def health(request: Request) -> Response:
    """Detalhe por componente (porta interna): 200 se tudo está UP, senão 503."""
    checks = await _component_status(container_of(request))
    up = all(status == "UP" for status in checks.values())
    return json_response({"status": "UP" if up else "DOWN", "components": checks}, 200 if up else 503)


@router.get("/metrics")
async def metrics() -> Response:
    """Métricas no formato do Prometheus (porta interna)."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

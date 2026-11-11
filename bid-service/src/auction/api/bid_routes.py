"""Rotas de lance: POST /auctions/{id}/bids (o caminho quente) e GET /auctions/{id}/bids?afterSeq=N."""

import re
import uuid
from typing import Annotated

from fastapi import APIRouter, Query, Request
from fastapi.responses import Response

from ..bidding.result import parse_amount
from ..core.errors import ApiError
from ..observability.logs import bind
from .request_helpers import container_of, request_client_ip
from .responses import json_response

IDEMPOTENCY_KEY = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")

router = APIRouter()


@router.post("/auctions/{auction_id}/bids")
async def place_bid(auction_id: uuid.UUID, request: Request) -> Response:
    """201 aceito; 200 replay de um aceite; 409 abaixo do mínimo; 422 encerrado ou key reutilizada.

    O replay de uma rejeição devolve o mesmo status da resposta original, com Idempotent-Replayed.
    Caminho quente: headers e corpo são lidos e validados aqui, sem a injeção de dependências.
    """
    headers = request.headers
    container = container_of(request)
    user_id = await container.auth.user_of(headers)
    bind(userId=user_id)
    key = headers.get("idempotency-key") or ""
    if not IDEMPOTENCY_KEY.match(key):
        raise ApiError.bad_request("Idempotency-Key: use 1 a 128 caracteres [A-Za-z0-9_.:-]")
    amount = parse_amount(await request.body())
    container.ip_limiter.check(request_client_ip(request))
    result = await container.bids.place(auction_id, user_id, key, amount)
    return json_response(result.to_json(), result.http_status(),
                         {"Idempotent-Replayed": "true" if result.replayed else "false"})


@router.get("/auctions/{auction_id}/bids")
async def list_bids(
        auction_id: uuid.UUID,
        request: Request,
        afterSeq: Annotated[int, Query(ge=0)] = 0,
        limit: Annotated[int, Query(ge=1, le=1000)] = 100,
) -> Response:
    """Lances aceitos com seq > afterSeq, em ordem: é como o cliente recupera o que a conflation pulou."""
    return json_response(await container_of(request).bid_history.list_after(auction_id, afterSeq, limit))

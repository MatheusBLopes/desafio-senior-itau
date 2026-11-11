"""Rotas de leilão: POST /auctions e GET /auctions/{id}."""

import uuid

from fastapi import APIRouter, Request
from fastapi.responses import Response
from pydantic import ValidationError

from ..core.errors import ApiError
from ..lifecycle.models import CreateAuctionRequest
from ..observability.logs import bind
from .request_helpers import container_of, read_json
from .responses import format_validation_errors, json_response

router = APIRouter()


@router.post("/auctions")
async def create_auction(request: Request) -> Response:
    """Cria um leilão. Exige token no modo cognito; devolve 201 com o estado inicial e o Location."""
    # Autentica ANTES de validar o corpo: quem não tem token recebe 401, não o formato esperado do corpo.
    container = container_of(request)
    if container.auth.mode == "cognito":
        bind(userId=await container.auth.user_of(request.headers))
    try:
        body = CreateAuctionRequest.model_validate(await read_json(request))
    except ValidationError as e:
        raise ApiError.bad_request(format_validation_errors(e.errors(), from_request=False)) from None
    view = await container.auctions.create(body)
    return json_response(view, 201, {"Location": f"/auctions/{view['id']}"})


@router.get("/auctions/{auction_id}")
async def get_auction(auction_id: uuid.UUID, request: Request) -> Response:
    """Estado vivo do leilão: preço, líder, seq, próximo mínimo, prazo e, se encerrado, vencedor."""
    return json_response(await container_of(request).auctions.get(auction_id))

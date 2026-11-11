"""O que as rotas precisam extrair de uma requisição: o container, o corpo JSON e o IP do cliente."""

import orjson
from fastapi import Request

from ..bidding.rate_limit import client_ip
from ..container import Container
from ..core.errors import ApiError


def container_of(request: Request) -> Container:
    """As dependências da task, criadas no startup.

    Lido a cada requisição (e não capturado na criação da rota) para que os testes possam trocar uma
    peça do container com a aplicação no ar.
    """
    return request.app.state.container


async def read_json(request: Request) -> object:
    """Corpo da requisição como JSON; corpo inválido vira 400."""
    try:
        return orjson.loads(await request.body())
    except orjson.JSONDecodeError:
        raise ApiError.bad_request("corpo JSON inválido") from None


def request_client_ip(request: Request) -> str:
    """IP real do cliente, considerando o X-Forwarded-For do ALB (ver client_ip)."""
    return client_ip(request.client.host if request.client else None, request.headers.get("x-forwarded-for"))

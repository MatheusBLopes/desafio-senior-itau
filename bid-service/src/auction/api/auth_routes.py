"""Rotas de cadastro e login: POST /auth/signup e POST /auth/login."""

from fastapi import APIRouter, Request
from fastapi.responses import Response

from ..auth.authenticator import validate_credentials
from ..observability.logs import bind
from .request_helpers import container_of, read_json, request_client_ip
from .responses import json_response

router = APIRouter()


@router.post("/auth/signup")
async def signup(request: Request) -> Response:
    """Cria o usuário no Cognito já confirmado (sem e-mail). Limitado por IP contra cadastro em massa."""
    container = container_of(request)
    container.ip_limiter.check(request_client_ip(request))
    username, password = validate_credentials(await read_json(request))
    bind(userId=username)
    await container.auth.identity_provider().sign_up(username, password)
    return json_response({"username": username}, 201)


@router.post("/auth/login")
async def login(request: Request) -> Response:
    """Troca usuário e senha por um access token do Cognito (JWT, 1 h)."""
    container = container_of(request)
    container.ip_limiter.check(request_client_ip(request))
    username, password = validate_credentials(await read_json(request))
    bind(userId=username)
    return json_response(await container.auth.identity_provider().login(username, password))

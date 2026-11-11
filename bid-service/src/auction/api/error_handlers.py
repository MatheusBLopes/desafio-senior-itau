"""Tradução de exceções em respostas HTTP: erros de API, validação, dependências fora do ar e erros inesperados."""

import logging
from collections.abc import Awaitable, Callable

import asyncpg
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import Response
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import OutOfMemoryError
from redis.exceptions import TimeoutError as RedisTimeoutError

from ..core.errors import ApiError
from ..observability.logs import bind
from .responses import RETRY_SOON, error_response, format_validation_errors

log = logging.getLogger(__name__)

# Exceções que significam "dependência indisponível agora": todas viram 503 UNAVAILABLE com Retry-After.
_UNAVAILABLE: list[tuple[type[Exception], int, str]] = [
    # Pool do Valkey esgotado ou Valkey fora: 503 diz ao cliente (e ao ALB) que é temporário.
    (RedisConnectionError, logging.WARNING, "Valkey indisponível: %s"),
    (RedisTimeoutError, logging.WARNING, "Valkey indisponível: %s"),
    # Valkey cheio com noeviction: o script é recusado no primeiro comando de escrita, antes de gravar
    # qualquer coisa, então não há efeito parcial. Temporário para o cliente; o alarme de memória avisa.
    (OutOfMemoryError, logging.ERROR, "Valkey sem memória (noeviction): %s"),
    # Postgres fora, pool esgotado ou rede: só cadastro e leitura dependem dele; lances seguem pelo Valkey.
    (asyncpg.PostgresError, logging.WARNING, "Postgres indisponível: %r"),
    (asyncpg.InterfaceError, logging.WARNING, "Postgres indisponível: %r"),
    (OSError, logging.WARNING, "Postgres indisponível: %r"),
    (TimeoutError, logging.WARNING, "Postgres indisponível: %r"),
]


def _unavailable(level: int, log_message: str) -> Callable[[Request, Exception], Awaitable[Response]]:
    """Handler que registra a falha no nível dado e responde 503 UNAVAILABLE com Retry-After: 1."""
    async def handler(_: Request, error: Exception) -> Response:
        """Registra a exceção e responde 503 com Retry-After."""
        bind(errorCode="UNAVAILABLE")
        log.log(level, log_message, error)
        return error_response(503, "UNAVAILABLE", "tente de novo em instantes", RETRY_SOON)

    return handler


def register_error_handlers(app: FastAPI) -> None:
    """Registra na app todos os handlers de exceção."""

    @app.exception_handler(ApiError)
    async def api_error(_: Request, error: ApiError) -> Response:
        """Erro esperado: devolve o status e o código que o próprio erro carrega (o código vai para o log)."""
        bind(errorCode=error.code)
        return error_response(error.status, error.code, error.message)

    @app.exception_handler(RequestValidationError)
    async def validation_error(_: Request, error: RequestValidationError) -> Response:
        """Parâmetro de path ou query inválido (ex.: id que não é UUID, afterSeq negativo): 400."""
        bind(errorCode="INVALID_REQUEST")
        return error_response(400, "INVALID_REQUEST", format_validation_errors(error.errors(), from_request=True))

    for exception_class, level, log_message in _UNAVAILABLE:
        app.add_exception_handler(exception_class, _unavailable(level, log_message))

    @app.exception_handler(Exception)
    async def unexpected(_: Request, error: Exception) -> Response:
        """Qualquer outra falha: registra o stack trace e responde 500 sem detalhes."""
        bind(errorCode="INTERNAL_ERROR")
        log.exception("erro inesperado")
        return error_response(500, "INTERNAL_ERROR", "erro interno")

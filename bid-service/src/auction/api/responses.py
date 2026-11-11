"""Montagem das respostas JSON e das mensagens de erro de validação."""

from collections.abc import Sequence
from typing import Any

import orjson
from fastapi.responses import Response

RETRY_SOON = {"Retry-After": "1"}


def json_response(body: Any, status: int = 200, headers: dict[str, str] | None = None) -> Response:
    """orjson direto, sem passar pelo jsonable_encoder do FastAPI: é o caminho quente dos lances."""
    return Response(orjson.dumps(body), status_code=status, headers=headers, media_type="application/json")


def error_response(status: int, code: str, message: str, headers: dict[str, str] | None = None) -> Response:
    """Corpo de erro padrão da API: {"code", "message"}."""
    return json_response({"code": code, "message": message}, status, headers)


def format_validation_errors(errors: Sequence[dict[str, Any]], *, from_request: bool) -> str:
    """Junta os erros do pydantic numa mensagem só, como "minIncrement: Input should be greater than 0".

    Nos erros de requisição do FastAPI, o primeiro elemento da localização é a origem (path, query, body)
    e é omitido; nos erros de um modelo validado à mão, a localização já é o campo.
    """
    parts = []
    for error in errors:
        location = error["loc"][1:] if from_request else error["loc"]
        fallback = error["loc"][0] if from_request else "corpo"
        parts.append(f"{'.'.join(str(part) for part in location) or fallback}: {error['msg']}")
    return "; ".join(parts)

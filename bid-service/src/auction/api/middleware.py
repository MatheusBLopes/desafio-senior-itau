"""Middlewares ASGI que rodam antes de qualquer rota: load shedding, log de requisições e separação de portas."""

import logging
import re
import time
from typing import Any

from ..bidding.rate_limit import client_ip
from ..observability.logs import fields, log_context, new_request_id
from ..observability.metrics import REQUESTS_SHED
from .responses import RETRY_SOON, error_response

MANAGEMENT_PATHS = {"/metrics", "/health"}
# Nenhum corpo legítimo passa de algumas centenas de bytes; o limite barra requisições gigantes (memória).
MAX_BODY_BYTES = 16 * 1024

access_log = logging.getLogger("auction.api.access")

# Health checks do ALB (a cada poucos segundos, por task) e métricas: sem linha de log, para não poluir.
UNLOGGED_PATHS = {"/livez", "/readyz", "/health", "/metrics"}
# Um X-Request-Id vindo do cliente é reaproveitado só se tiver formato seguro para ir ao log.
_CLIENT_REQUEST_ID = re.compile(r"^[A-Za-z0-9._-]{8,64}$")


def _header(scope: dict[str, Any], name: bytes) -> str | None:
    """Valor de um header da requisição ASGI, ou None."""
    for key, value in scope.get("headers", []):
        if key == name:
            return value.decode("latin-1")
    return None


class RequestLogger:
    """Dá um identificador a cada requisição e, ao final, registra uma linha com o que aconteceu.

    - O requestId vem do header X-Request-Id do cliente (se tiver formato válido) ou é gerado, e volta na
      resposta no mesmo header. Ele entra em todos os logs da requisição: basta copiá-lo da resposta para
      encontrar no CloudWatch tudo o que aquela requisição fez.
    - A linha final traz método, caminho, status, duração e IP; as rotas acrescentam usuário, leilão,
      resultado do lance e código de erro pelo contexto (bind).
    - Health checks e métricas não geram linha. WebSockets recebem o mesmo contexto; o broadcaster registra
      a abertura e o fechamento da conexão.
    """

    def __init__(self, app: Any, enabled: bool = True):
        """Envolve a aplicação ASGI; enabled=False mantém o requestId mas não registra a linha final."""
        self.app = app
        self.enabled = enabled

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        """Abre o contexto de log da requisição, repassa o requestId na resposta e registra a linha final."""
        if scope["type"] not in ("http", "websocket"):
            return await self.app(scope, receive, send)
        given = _header(scope, b"x-request-id")
        request_id = given if given and _CLIENT_REQUEST_ID.match(given) else new_request_id()
        peer = scope["client"][0] if scope.get("client") else None
        log_context.set({"requestId": request_id, "clientIp": client_ip(peer, _header(scope, b"x-forwarded-for"))})
        if scope["type"] == "websocket":
            return await self.app(scope, receive, send)

        status = 500
        header = (b"x-request-id", request_id.encode())

        async def send_with_request_id(message: dict[str, Any]) -> None:
            """Guarda o status e acrescenta o X-Request-Id à resposta."""
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
                message = {**message, "headers": [*message.get("headers", []), header]}
            await send(message)

        start = time.perf_counter()
        try:
            await self.app(scope, receive, send_with_request_id)
        finally:
            if self.enabled and scope["path"] not in UNLOGGED_PATHS:
                duration_ms = round((time.perf_counter() - start) * 1000, 1)
                method, path = scope["method"], scope["path"]
                # O formato da rota (/auctions/{auction_id}/bids) deixa a mensagem curta e agrupável; o id real
                # já está no campo auctionId e o caminho completo, em path.
                route = getattr(scope.get("route"), "path", path)
                access_log.log(logging.WARNING if status >= 500 else logging.INFO,
                               "%s %s → %s em %s ms", method, route, status, duration_ms,
                               extra=fields(method=method, route=route, path=path, status=status,
                                            durationMs=duration_ms))


class PortSeparation:
    """Métricas e health detalhado só na porta interna; a API só na pública (o ALB vê só a 8080).

    Middleware ASGI puro (sem BaseHTTPMiddleware): vale para HTTP e WebSocket e custa quase nada.
    O contexto de log de cada requisição é aberto antes, pelo RequestLogger; o userId entra nele só depois
    de autenticado.
    """

    def __init__(self, app: Any):
        """Envolve a aplicação ASGI."""
        self.app = app

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        """Recusa rotas na porta errada (404, ou close 1008 no WebSocket) e corpos grandes ou sem tamanho."""
        if scope["type"] not in ("http", "websocket"):
            return await self.app(scope, receive, send)
        on_management = scope["server"][1] in scope["app"].state.management_ports
        if on_management != (scope["path"] in MANAGEMENT_PATHS):
            if scope["type"] == "websocket":
                return await send({"type": "websocket.close", "code": 1008})
            return await error_response(404, "NOT_FOUND", "não encontrado")(scope, receive, send)
        if scope["type"] == "http" and scope["method"] in ("POST", "PUT", "PATCH"):
            headers = dict(scope["headers"])
            length = headers.get(b"content-length")
            # lower(): o valor do header não diferencia maiúsculas ("Chunked" também é chunked para o parser).
            if length is None and b"chunked" in headers.get(b"transfer-encoding", b"").lower():
                return await error_response(411, "LENGTH_REQUIRED", "envie Content-Length")(scope, receive, send)
            if length is not None and (not length.isdigit() or int(length) > MAX_BODY_BYTES):
                return await error_response(413, "PAYLOAD_TOO_LARGE", "corpo grande demais")(scope, receive, send)
        return await self.app(scope, receive, send)


class LoadShedder:
    """Limita as requisições HTTP em andamento nesta task; acima do limite responde 503 na hora.

    Sem limite, uma task sobrecarregada aceita tudo, a fila cresce, a latência de todos explode e o
    throughput cai (colapso). Com o limite, ela atende o que consegue com latência estável e devolve
    o excesso em microssegundos, com Retry-After; o ALB e o autoscaling cuidam do resto.

    Conta requisições, não conexões: o ALB mantém conexões keep-alive ociosas que não custam nada.
    Probes (/livez, /readyz) e WebSocket ficam de fora: uma task cheia não deve sair do ALB por isso.
    Métricas e health detalhado também: é na sobrecarga que mais se precisa deles.
    """

    EXEMPT = {"/livez", "/readyz"} | MANAGEMENT_PATHS

    def __init__(self, app: Any, max_in_flight: int):
        """Envolve a aplicação ASGI; max_in_flight vem de MAX_IN_FLIGHT (padrão 64)."""
        self.app = app
        self.max_in_flight = max_in_flight
        self.in_flight = 0

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        """Deixa passar até max_in_flight requisições ao mesmo tempo; as excedentes recebem 503 OVERLOADED."""
        if scope["type"] != "http" or scope["path"] in self.EXEMPT:
            return await self.app(scope, receive, send)
        if self.in_flight >= self.max_in_flight:
            REQUESTS_SHED.inc()
            return await error_response(503, "OVERLOADED", "tente de novo em instantes",
                                        RETRY_SOON)(scope, receive, send)
        self.in_flight += 1
        try:
            await self.app(scope, receive, send)
        finally:
            self.in_flight -= 1

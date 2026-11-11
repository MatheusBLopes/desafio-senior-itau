"""Middlewares ASGI que rodam antes de qualquer rota: separação de portas e load shedding."""

from typing import Any

from ..observability.logs import log_context
from ..observability.metrics import REQUESTS_SHED
from .responses import RETRY_SOON, error_response

MANAGEMENT_PATHS = {"/metrics", "/health"}
# Nenhum corpo legítimo passa de algumas centenas de bytes; o limite barra requisições gigantes (memória).
MAX_BODY_BYTES = 16 * 1024


class PortSeparation:
    """Métricas e health detalhado só na porta interna; a API só na pública (o ALB vê só a 8080).

    Middleware ASGI puro (sem BaseHTTPMiddleware): vale para HTTP e WebSocket e custa quase nada.
    Também zera o contexto de log a cada requisição; o userId entra nele só depois de autenticado.
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
            if length is None and b"chunked" in headers.get(b"transfer-encoding", b""):
                return await error_response(411, "LENGTH_REQUIRED", "envie Content-Length")(scope, receive, send)
            if length is not None and (not length.isdigit() or int(length) > MAX_BODY_BYTES):
                return await error_response(413, "PAYLOAD_TOO_LARGE", "corpo grande demais")(scope, receive, send)
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

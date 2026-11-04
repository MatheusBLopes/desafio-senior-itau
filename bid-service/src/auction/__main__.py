"""Ponto de entrada: python -m auction.

Um processo por task (escala horizontal no ECS, não com workers). Um único servidor uvicorn escuta
em duas portas: 8080 (API, WebSocket, /livez, /readyz) e 8081 (métricas e health detalhado, só na
rede interna). O middleware separate_ports decide o que cada porta atende.
"""

import socket

import uvicorn

from .app import configure, create_app
from .config import Settings


def _listen(port: int) -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("0.0.0.0", port))
    return sock


def main() -> None:
    settings = Settings.from_env()
    configure(settings)
    config = uvicorn.Config(
        create_app(settings),
        loop="uvloop",
        http="httptools",
        ws="websockets-sansio",
        # Ping a cada 10 s, bem abaixo do idle timeout de 60 s do ALB; derruba conexões mortas.
        ws_ping_interval=10,
        ws_ping_timeout=10,
        # O ALB já filtra; X-Forwarded-For é tratado pela aplicação (client_ip).
        proxy_headers=False,
        # Keep-alive maior que o idle timeout do ALB (60 s), como a AWS recomenda. Com o padrão do
        # uvicorn (5 s), a app fecha uma conexão ociosa no instante em que o proxy a reutiliza e a
        # requisição volta como 502 (achado no teste de carga: "connection reset by peer" no nginx).
        timeout_keep_alive=75,
        timeout_graceful_shutdown=20,
        backlog=4096,
        access_log=False,
        log_config=None,
    )
    server = uvicorn.Server(config)
    server.run(sockets=[_listen(settings.port), _listen(settings.management_port)])


if __name__ == "__main__":
    main()

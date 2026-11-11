"""Rota de tempo real: WebSocket /ws/auctions/{id}, só de leitura."""

from fastapi import APIRouter, WebSocket

router = APIRouter()


@router.websocket("/ws/auctions/{auction_id}")
async def auction_events(ws: WebSocket, auction_id: str) -> None:
    """Entrega AUCTION_SNAPSHOT, AUCTION_EXTENDED e AUCTION_CLOSED do leilão até o cliente sair."""
    await ws.app.state.container.broadcaster.serve(ws, auction_id)

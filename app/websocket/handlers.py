"""WebSocket route + broadcast manager."""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

log = logging.getLogger("email-monitor.ws")


class ConnectionManager:
    def __init__(self) -> None:
        self._clients: Set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        async with self._lock:
            self._clients.add(ws)

    async def disconnect(self, ws: WebSocket) -> None:
        async with self._lock:
            self._clients.discard(ws)

    async def broadcast(self, event: dict) -> None:
        payload = json.dumps(event, default=str)
        dead: list[WebSocket] = []

        async with self._lock:
            targets = list(self._clients)

        for ws in targets:
            try:
                await ws.send_text(payload)
            except Exception:
                dead.append(ws)

        if dead:
            async with self._lock:
                for ws in dead:
                    self._clients.discard(ws)


def register_ws_routes(app: FastAPI) -> None:
    @app.websocket("/ws")
    async def stream(ws: WebSocket) -> None:
        manager: ConnectionManager = ws.app.state.manager
        await manager.connect(ws)
        log.info("client connected (total=%d)", len(manager._clients))
        try:
            while True:
                await ws.receive_text()
        except WebSocketDisconnect:
            pass
        except Exception as e:
            log.warning("ws error: %s", e)
        finally:
            await manager.disconnect(ws)
            log.info("client disconnected (total=%d)", len(manager._clients))

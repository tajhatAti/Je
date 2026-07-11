"""FastAPI application entry point."""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .api.routes import router as api_router
from .config import allowed_origins, load_accounts
from .services.imap_pool import ImapPool
from .websocket.handlers import ConnectionManager, register_ws_routes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
)
log = logging.getLogger("email-monitor")


@asynccontextmanager
async def lifespan(app: FastAPI):
    accounts = load_accounts()
    manager = ConnectionManager()
    pool = ImapPool(accounts=accounts, on_event=manager.broadcast)
    app.state.manager = manager
    app.state.pool = pool

    log.info("Starting IMAP pool for %d accounts", len(accounts))
    await pool.start()
    try:
        yield
    finally:
        log.info("Stopping IMAP pool")
        await pool.stop()


app = FastAPI(title="Email Monitor Service", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins(),
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)
register_ws_routes(app)

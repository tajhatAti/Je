from __future__ import annotations

from fastapi import APIRouter, Request

router = APIRouter()


@router.get("/health")
async def health(request: Request) -> dict:
    pool = getattr(request.app.state, "pool", None)
    return {
        "status": "ok",
        "accounts": len(pool.accounts) if pool else 0,
        "workers_alive": pool.alive_count() if pool else 0,
    }


@router.get("/accounts")
async def list_accounts(request: Request) -> dict:
    """Return the account labels only — never credentials."""
    pool = getattr(request.app.state, "pool", None)
    return {"accounts": [a.email for a in (pool.accounts if pool else [])]}

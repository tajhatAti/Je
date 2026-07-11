"""Pool manager that supervises one IMAP worker task per account."""
from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable, Dict, List

from ..config import ImapAccount
from .imap_worker import run_worker

log = logging.getLogger("email-monitor.pool")


class ImapPool:
    def __init__(
        self,
        accounts: List[ImapAccount],
        on_event: Callable[[dict], Awaitable[None]],
    ) -> None:
        self.accounts = accounts
        self._on_event = on_event
        self._stop = asyncio.Event()
        self._tasks: Dict[str, asyncio.Task] = {}
        self._supervisor: asyncio.Task | None = None

    async def start(self) -> None:
        for acc in self.accounts:
            self._tasks[acc.email] = asyncio.create_task(
                run_worker(acc, self._on_event, self._stop),
                name=f"imap:{acc.email}",
            )
        self._supervisor = asyncio.create_task(self._supervise(), name="imap-supervisor")

    async def _supervise(self) -> None:
        """Restart any worker that exits unexpectedly (crash-loop-safe)."""
        while not self._stop.is_set():
            await asyncio.sleep(10)
            for acc in self.accounts:
                task = self._tasks.get(acc.email)
                if task is None or task.done():
                    if task is not None and task.exception():
                        log.error("[%s] worker crashed: %s", acc.email, task.exception())
                    if self._stop.is_set():
                        return
                    log.info("[%s] respawning worker", acc.email)
                    self._tasks[acc.email] = asyncio.create_task(
                        run_worker(acc, self._on_event, self._stop),
                        name=f"imap:{acc.email}",
                    )

    def alive_count(self) -> int:
        return sum(1 for t in self._tasks.values() if not t.done())

    async def stop(self) -> None:
        self._stop.set()
        if self._supervisor:
            self._supervisor.cancel()
        for t in self._tasks.values():
            t.cancel()
        await asyncio.gather(
            *self._tasks.values(),
            *([self._supervisor] if self._supervisor else []),
            return_exceptions=True,
        )

"""Per-account IMAP IDLE worker.

Runs an infinite loop that:
1. Opens an aioimaplib connection and logs in.
2. Selects INBOX and issues IDLE.
3. Emits an event whenever the server pushes EXISTS.
4. Recovers from any failure with exponential backoff.
"""
from __future__ import annotations

import asyncio
import email
import logging
from email.utils import parseaddr, parsedate_to_datetime
from typing import Awaitable, Callable

from aioimaplib import aioimaplib

from ..config import ImapAccount

log = logging.getLogger("email-monitor.imap")

IDLE_TIMEOUT = 25 * 60


async def _fetch_latest(client: aioimaplib.IMAP4_SSL, account_email: str) -> dict | None:
    """Fetch metadata for the newest message in the mailbox."""
    typ, data = await client.uid_search("ALL")
    if typ != "OK" or not data or not data[0]:
        return None

    uids = data[0].split()
    if not uids:
        return None

    latest_uid = uids[-1].decode()
    typ, msg_data = await client.uid("fetch", latest_uid, "(BODY.PEEK[HEADER])")
    if typ != "OK":
        return None

    raw = next((p for p in msg_data if isinstance(p, (bytes, bytearray)) and len(p) > 40), None)
    if not raw:
        return None

    msg = email.message_from_bytes(bytes(raw))
    from_name, from_addr = parseaddr(msg.get("From", ""))
    ts = msg.get("Date", "")
    try:
        ts_iso = parsedate_to_datetime(ts).isoformat() if ts else None
    except Exception:
        ts_iso = None

    return {
        "id": f"{account_email}:{latest_uid}",
        "account": account_email,
        "sender": from_name or from_addr or "Unknown",
        "senderEmail": from_addr or None,
        "subject": msg.get("Subject", "(no subject)"),
        "timestamp": ts_iso,
    }


async def run_worker(
    account: ImapAccount,
    on_event: Callable[[dict], Awaitable[None]],
    stop_event: asyncio.Event,
) -> None:
    backoff = 2.0

    while not stop_event.is_set():
        client: aioimaplib.IMAP4_SSL | None = None
        try:
            client = aioimaplib.IMAP4_SSL(host=account.host, port=account.port, timeout=30)
            await client.wait_hello_from_server()
            await client.login(account.email, account.password)
            await client.select("INBOX")
            log.info("[%s] connected", account.email)
            backoff = 2.0

            latest = await _fetch_latest(client, account.email)
            if latest:
                await on_event(latest)

            while not stop_event.is_set():
                idle_task = await client.idle_start(timeout=IDLE_TIMEOUT)
                log.info("[%s] idling", account.email)

                got_push = False
                while client.has_pending_idle():
                    push_task = asyncio.ensure_future(client.wait_server_push())
                    stop_wait = asyncio.ensure_future(stop_event.wait())
                    done, pending = await asyncio.wait(
                        {push_task, stop_wait},
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    for p in pending:
                        p.cancel()

                    if stop_event.is_set():
                        client.idle_done()
                        break

                    msg = push_task.result()
                    log.info("[%s] server push: %s", account.email, msg)
                    if msg != aioimaplib.STOP_WAIT_SERVER_PUSH:
                        got_push = True
                    client.idle_done()
                    break

                try:
                    await asyncio.wait_for(idle_task, timeout=10)
                except asyncio.TimeoutError:
                    pass

                if stop_event.is_set():
                    break

                if got_push:
                    latest = await _fetch_latest(client, account.email)
                    if latest:
                        await on_event(latest)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("[%s] worker error: %s (retry in %.0fs)", account.email, e, backoff)
            try:
                if client is not None:
                    await client.logout()
            except Exception:
                pass
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=backoff)
            except asyncio.TimeoutError:
                pass
            backoff = min(backoff * 2, 60.0)
        else:
            try:
                if client is not None:
                    await client.logout()
            except Exception:
                pass

"""Per-account IMAP polling worker.

Runs an infinite loop that:
1. Opens an aioimaplib connection and logs in.
2. Selects INBOX.
3. Every POLL_INTERVAL seconds, checks for the newest message.
4. Emits an event when the newest message UID changes.
5. Recovers from any failure with exponential backoff.
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

POLL_INTERVAL = 15  # seconds between checks


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
        "_uid": latest_uid,
    }


async def run_worker(
    account: ImapAccount,
    on_event: Callable[[dict], Awaitable[None]],
    stop_event: asyncio.Event,
) -> None:
    backoff = 2.0

    while not stop_event.is_set():
        client: aioimaplib.IMAP4_SSL | None = None
        last_uid: str | None = None
        try:
            client = aioimaplib.IMAP4_SSL(host=account.host, port=account.port, timeout=30)
            await client.wait_hello_from_server()
            await client.login(account.email, account.password)
            await client.select("INBOX")
            log.info("[%s] connected", account.email)
            backoff = 2.0

            # Emit the latest existing message once so the UI has context.
            latest = await _fetch_latest(client, account.email)
            if latest:
                last_uid = latest["_uid"]
                await on_event(latest)

            while not stop_event.is_set():
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=POLL_INTERVAL)
                except asyncio.TimeoutError:
                    pass

                if stop_event.is_set():
                    break

                latest = await _fetch_latest(client, account.email)
                if latest and latest["_uid"] != last_uid:
                    log.info("[%s] new mail detected: %s", account.email, latest["subject"])
                    last_uid = latest["_uid"]
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

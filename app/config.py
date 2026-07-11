"""Configuration & credential loading.

Credentials are read from the ACCOUNTS_JSON environment variable (a JSON array).
Never log the raw values. On HF Spaces, set this as a *secret* variable.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class ImapAccount:
    email: str
    password: str
    host: str
    port: int = 993


def load_accounts() -> List[ImapAccount]:
    raw = os.environ.get("ACCOUNTS_JSON", "").strip()
    if not raw:
        return []

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"ACCOUNTS_JSON is not valid JSON: {e}") from e

    if not isinstance(data, list):
        raise RuntimeError("ACCOUNTS_JSON must be a JSON array of objects")

    out: List[ImapAccount] = []
    for i, entry in enumerate(data):
        try:
            out.append(
                ImapAccount(
                    email=entry["email"],
                    password=entry["password"],
                    host=entry["host"],
                    port=int(entry.get("port", 993)),
                )
            )
        except KeyError as e:
            raise RuntimeError(f"Account #{i} missing required field: {e}") from e

    return out


def allowed_origins() -> List[str]:
    raw = os.environ.get("ALLOWED_ORIGINS", "*")
    return [o.strip() for o in raw.split(",") if o.strip()]

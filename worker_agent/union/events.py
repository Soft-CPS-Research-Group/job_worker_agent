from __future__ import annotations

import json
from typing import Any, Mapping


EVENT_PREFIX = "OPEVA_EVENT_V1="


def encode_event(kind: str, sequence: int, payload: Mapping[str, Any] | None = None) -> str:
    event = {
        "version": 1,
        "kind": str(kind),
        "sequence": int(sequence),
        **dict(payload or {}),
    }
    return EVENT_PREFIX + json.dumps(event, separators=(",", ":"), sort_keys=True)


def parse_event(line: str) -> dict[str, Any] | None:
    marker = line.find(EVENT_PREFIX)
    if marker < 0:
        return None
    raw = line[marker + len(EVENT_PREFIX) :].strip()
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(value, dict) or value.get("version") != 1:
        return None
    if not isinstance(value.get("kind"), str):
        return None
    try:
        value["sequence"] = int(value.get("sequence", 0))
    except (TypeError, ValueError):
        return None
    return value

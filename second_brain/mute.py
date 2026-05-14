from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any


def save_mute_state(mute_until: datetime | None, mute_state_file: Path, logger: Any) -> None:
    try:
        payload = {"mute_until": mute_until.isoformat() if mute_until else None}
        mute_state_file.write_text(json.dumps(payload))
    except Exception as e:
        logger.error("Failed saving mute state: %s", e)


def load_mute_state(mute_state_file: Path, tz, logger: Any) -> datetime | None:
    mute_until = None
    try:
        if not mute_state_file.exists():
            return None
        payload = json.loads(mute_state_file.read_text() or "{}")
        raw = payload.get("mute_until")
        if raw:
            mute_until = datetime.fromisoformat(raw)
        if mute_until and datetime.now(tz) >= mute_until:
            save_mute_state(None, mute_state_file, logger)
            return None
        return mute_until
    except Exception as e:
        logger.error("Failed loading mute state: %s", e)
        return None


def is_muted(mute_until: datetime | None, tz) -> bool:
    if not mute_until:
        return False
    return datetime.now(tz) < mute_until

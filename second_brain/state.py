from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from second_brain.utils import ExpiringDict


@dataclass
class BotState:
    digest_map: dict[int, list[dict]] = field(default_factory=dict)
    pending_map: ExpiringDict = field(default_factory=lambda: ExpiringDict(ttl_seconds=3600))
    capture_map: dict[int, dict] = field(default_factory=dict)
    done_picker_map: ExpiringDict = field(default_factory=lambda: ExpiringDict(ttl_seconds=3600))
    notes_pending: set[int] = field(default_factory=set)
    habit_cache: dict[str, dict] = field(default_factory=dict)
    started_at: datetime = field(default_factory=datetime.utcnow)


STATE = BotState()

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

from second_brain.utils import ExpiringDict


@dataclass
class BotState:
    digest_map: dict[int, list[dict]] = field(default_factory=dict)
    pending_map: ExpiringDict = field(default_factory=lambda: ExpiringDict(ttl_seconds=3600))
    capture_map: dict[int, dict] = field(default_factory=dict)
    done_picker_map: ExpiringDict = field(default_factory=lambda: ExpiringDict(ttl_seconds=3600))
    notes_pending: set[int] = field(default_factory=set)
    habit_cache: dict[str, dict] = field(default_factory=dict)
    counter_done_picker: int = 0
    counter_todo_picker: int = 0
    counter_v10: int = 0
    habits_data_cache: ExpiringDict = field(default_factory=lambda: ExpiringDict(ttl_seconds=300))
    mute_until: datetime | None = None
    signoff_notes: dict[str, str] = field(default_factory=lambda: {"second_brain": "", "brian_ii": ""})
    claude_activity: list[str] = field(default_factory=list)
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


STATE = BotState()

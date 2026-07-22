from __future__ import annotations

import logging
from pathlib import Path
import traceback
from importlib import import_module

log = logging.getLogger(__name__)


async def send_system_log(bot, text: str) -> None:
    """Send an internal error report to the configured error alert channel."""
    if bot is None:
        log.error("System log bot unavailable: %s", text)
        return
    config = import_module("second_brain.config")
    my_chat_id = getattr(config, "MY_CHAT_ID", None)
    error_chat_id = getattr(config, "ERROR_CHANNEL_ID", None) or getattr(config, "SYSTEM_LOGS_CHAT_ID", None)
    if not error_chat_id or str(error_chat_id) == str(my_chat_id):
        log.warning("system_log (error_channel_ID not set separately): %s", text)
        return
    try:
        await bot.send_message(chat_id=error_chat_id, text=text)
    except Exception as exc:
        log.error("Failed to send system log: %s", exc)


def friendly_error_area(module: str, function: str) -> str:
    if module.startswith("second_brain.crossfit."):
        if function in {"create_wod_log", "create_strength_log", "save_programme", "save_programme_from_notion_row"}:
            return "CrossFit Notion save"
        return "CrossFit flow"
    if module.startswith("second_brain.notion."):
        return "Notion write"
    if module.startswith("second_brain.cinema."):
        return "cinema sync"
    if module.startswith("second_brain.asana."):
        return "Asana sync"
    if module.startswith("second_brain.healthtrack."):
        return "health tracking"
    if module.startswith("second_brain."):
        return module.removeprefix("second_brain.").replace(".", " ")
    return function or "Telegram handling"


# Frames from the generic Notion retry wrapper are the deepest second_brain
# frames of every Notion API failure; naming them hides the real caller.
_WRAPPER_FRAME_NAMES = {"notion_call", "notion_call_async", "<lambda>"}


def _is_wrapper_frame(frame: traceback.FrameSummary) -> bool:
    return (
        frame.filename.endswith("/second_brain/notion/__init__.py")
        and frame.name in _WRAPPER_FRAME_NAMES
    )


def telegram_error_location(exc: BaseException | None) -> str:
    if exc is None or exc.__traceback__ is None:
        return "Telegram handling"
    frames = traceback.extract_tb(exc.__traceback__)
    chosen = None
    wrapper_fallback = None
    for frame in reversed(frames):
        if "/second_brain/" in frame.filename and not (
            frame.filename.endswith("/main.py") and frame.name == "error_handler"
        ):
            if _is_wrapper_frame(frame):
                wrapper_fallback = wrapper_fallback or frame
                continue
            chosen = frame
            break
    if chosen is None:
        chosen = wrapper_fallback
    if chosen is None and frames:
        chosen = frames[-1]
    if chosen is None:
        return "Telegram handling"
    path = Path(chosen.filename)
    try:
        module = ".".join(path.with_suffix("").parts[path.parts.index("second_brain"):])
    except ValueError:
        module = path.stem
    area = friendly_error_area(module, chosen.name)
    return f"{area} ({module}.{chosen.name})"

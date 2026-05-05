"""
Health tracker configuration.

Environment variables:
  HEALTH_STEPS_THRESHOLD   — daily step goal (default: 10000)
  HEALTH_HABIT_NAME        — exact name of the Steps habit in Notion (default: "Steps")
  HEALTH_STEPS_FINAL_HOUR  — hour for nightly final-stamp job in 24h (default: 23)
  HEALTH_STEPS_FINAL_MIN   — minute for nightly final-stamp job (default: 59)
  HEALTH_WEBHOOK_SECRET    — optional shared secret for Health Auto Export auth
"""

from __future__ import annotations

import os

STEPS_THRESHOLD: int = int(os.environ.get("HEALTH_STEPS_THRESHOLD", "10000"))
STEPS_HABIT_NAME: str = os.environ.get("HEALTH_HABIT_NAME", "Steps")
STEPS_SOURCE_LABEL: str = "📱 Apple Watch"

_final_h, _final_m = os.environ.get("HEALTH_STEPS_FINAL_TIME", "23:59").split(":")
STEPS_FINAL_HOUR: int = int(_final_h)
STEPS_FINAL_MIN: int = int(_final_m)

WEBHOOK_SECRET: str = os.environ.get("HEALTH_WEBHOOK_SECRET", "")

STEPS_WRITE_INTRADAY_BELOW_THRESHOLD: bool = os.environ.get("HEALTH_STEPS_WRITE_INTRADAY", "1").strip().lower() in {"1", "true", "yes", "on"}

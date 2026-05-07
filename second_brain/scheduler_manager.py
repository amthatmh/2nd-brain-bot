"""Notion-driven Utility Scheduler manager.

Reads the Utility Scheduler database, registers APScheduler jobs dynamically, and
keeps load/run status columns in Notion up to date.
"""

from __future__ import annotations

import inspect
import logging
from datetime import datetime, timezone as dt_timezone
from typing import Any, Callable

log = logging.getLogger(__name__)

_JOB_PREFIX = "utility_"
_RELOAD_JOB_ID = "utility_scheduler_reload"


class UtilitySchedulerManager:
    """Manage APScheduler jobs whose configuration lives in Notion."""

    def __init__(
        self,
        *,
        notion: Any,
        db_id: str,
        scheduler: Any,
        bot: Any,
        chat_id: str | int,
        tz: Any,
        reload_minutes: int = 15,
        env_fallbacks: dict[str, int] | None = None,
    ) -> None:
        self._notion = notion
        self._db_id = db_id
        self._scheduler = scheduler
        self._bot = bot
        self._chat_id = chat_id
        self._tz = tz
        self._reload_minutes = max(int(reload_minutes or 15), 1)
        self._env_fallbacks = env_fallbacks or {}
        self._handlers: dict[str, Callable[..., Any]] = {}
        self._known_jobs: set[str] = set()
        self._applied_configs: dict[str, tuple[Any, ...]] = {}

    def register_handler(self, job_key: str, handler_fn: Callable[..., Any]) -> None:
        """Register a Python handler for a Notion ``Job Key`` value."""
        if job_key in self._handlers:
            log.warning("scheduler_manager: overwriting handler for job_key=%s", job_key)
        self._handlers[job_key] = handler_fn
        log.debug("scheduler_manager: registered handler for job_key=%s", job_key)

    async def initialize(self) -> None:
        """Load jobs from Notion and schedule periodic reloads."""
        log.info("scheduler_manager: initializing (reload every %d min)", self._reload_minutes)
        await self.reload()
        self._scheduler.add_job(
            self.reload,
            "interval",
            minutes=self._reload_minutes,
            id=_RELOAD_JOB_ID,
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        log.info("scheduler_manager: initialized ✓ — %d handlers registered", len(self._handlers))

    async def reload(self) -> None:
        """Read Notion rows and synchronize managed APScheduler jobs."""
        if not self._db_id:
            log.warning("scheduler_manager: db_id not set — reload skipped")
            return

        log.info("scheduler_manager: reloading from Notion...")
        try:
            rows = self._query_all_rows()
        except Exception as exc:
            log.error("scheduler_manager: failed to query Notion: %s", exc)
            return

        active_keys: set[str] = set()
        loaded_at = self._utc_iso()

        for row in rows:
            page_id = str(row.get("id") or "")
            try:
                props = row.get("properties", {}) or {}
                job_key = self._extract_text(props.get("Job Key", {}))
                if not job_key:
                    continue

                enabled = self._extract_checkbox(props.get("Enabled", {}), default=False)
                if not enabled:
                    self._remove_job_if_exists(job_key)
                    self._update_notion_loaded_at(page_id, loaded_at)
                    continue

                active_keys.add(job_key)
                if job_key not in self._handlers:
                    log.warning("scheduler_manager: no handler for job_key=%s", job_key)
                    self._remove_job_if_exists(job_key)
                    self._update_notion_status(
                        page_id,
                        status="unknown_job",
                        error=f"Unknown Job Key: {job_key}",
                        loaded_at=loaded_at,
                    )
                    continue

                config = self._extract_job_config(props, env_fallbacks=self._env_fallbacks)
                signature = self._config_signature(config)
                apscheduler_id = self._apscheduler_id(job_key)
                existing_job = self._scheduler.get_job(apscheduler_id)
                if existing_job and self._applied_configs.get(job_key) != signature:
                    log.info("scheduler_manager: config changed for %s, re-registering", job_key)
                    self._remove_job_if_exists(job_key)
                    existing_job = None

                if not existing_job:
                    self._add_job(job_key, config, page_id)

                self._update_notion_status(page_id, status="ok", error="", loaded_at=loaded_at)
            except Exception as exc:
                log.error("scheduler_manager: error processing row %s: %s", page_id, exc)
                if page_id:
                    self._update_notion_status(page_id, status="error", error=str(exc), loaded_at=loaded_at)

        for job_key in list(self._known_jobs):
            if job_key not in active_keys:
                log.info("scheduler_manager: removing job no longer active in Notion: %s", job_key)
                self._remove_job_if_exists(job_key)

        log.info(
            "scheduler_manager: reload complete — %d active jobs, %d handlers available",
            len(active_keys),
            len(self._handlers),
        )

    async def _execute_job(self, job_key: str, page_id: str) -> None:
        """Run a registered handler and record success/failure in Notion."""
        log.info("scheduler_manager: executing job_key=%s", job_key)
        handler = self._handlers.get(job_key)
        ran_at = self._utc_iso()
        if not handler:
            error = f"Unknown Job Key at execution time: {job_key}"
            self._update_notion_run_result(page_id=page_id, ran_at=ran_at, status="unknown_job", error=error)
            log.error("scheduler_manager: %s", error)
            return

        try:
            result = handler(self._bot)
            if inspect.isawaitable(result):
                result = await result
            log.info("scheduler_manager: job_key=%s completed: %s", job_key, result)
            self._update_notion_run_result(page_id=page_id, ran_at=ran_at, status="ok", error=None)
        except Exception as exc:
            log.exception("scheduler_manager: job_key=%s FAILED", job_key)
            self._update_notion_run_result(page_id=page_id, ran_at=ran_at, status="error", error=str(exc))
            await self._send_failure_alert(job_key, exc)

    async def _send_failure_alert(self, job_key: str, error: Exception) -> None:
        """Send a Telegram alert when a managed Utility Scheduler job fails."""
        if not self._bot or not self._chat_id:
            return
        try:
            job = self._scheduler.get_job(self._apscheduler_id(job_key))
            next_run = job.next_run_time.strftime("%Y-%m-%d %H:%M %Z") if job and job.next_run_time else "unknown"
            await self._bot.send_message(
                chat_id=self._chat_id,
                text=(
                    f"🚨 <b>Scheduler: {job_key} failed</b>\n\n"
                    f"Error: <code>{str(error)[:300]}</code>\n\n"
                    f"Next run: {next_run}"
                ),
                parse_mode="HTML",
            )
        except Exception as alert_error:
            log.error("scheduler_manager: failed to send Telegram alert: %s", alert_error)

    def _add_job(self, job_key: str, config: dict[str, Any], page_id: str) -> None:
        apscheduler_id = self._apscheduler_id(job_key)
        trigger_type = config["trigger_type"]
        add_kwargs: dict[str, Any] = {
            "id": apscheduler_id,
            "replace_existing": True,
            "max_instances": config["max_instances"],
            "misfire_grace_time": config["misfire_grace_seconds"],
            "coalesce": config["coalesce"],
            "kwargs": {"job_key": job_key, "page_id": page_id},
        }

        run_on_start = config.get("run_on_start", False)
        if run_on_start:
            add_kwargs["next_run_time"] = datetime.now(self._scheduler.timezone)

        if trigger_type == "interval":
            trigger_kwargs = self._build_interval_kwargs(config)
            if not trigger_kwargs:
                raise ValueError(f"Interval job {job_key} must set an interval value")
            add_kwargs.update(trigger_kwargs)
        elif trigger_type == "cron":
            add_kwargs.update(self._build_cron_kwargs(config))
        else:
            raise ValueError(f"Trigger Type must be interval or cron, got: {trigger_type}")

        self._scheduler.add_job(self._execute_job, trigger_type, **add_kwargs)
        self._known_jobs.add(job_key)
        self._applied_configs[job_key] = self._config_signature(config)
        log.info("scheduler_manager: registered job_key=%s trigger=%s config=%s", job_key, trigger_type, config)

    def _remove_job_if_exists(self, job_key: str) -> None:
        apscheduler_id = self._apscheduler_id(job_key)
        try:
            if self._scheduler.get_job(apscheduler_id):
                self._scheduler.remove_job(apscheduler_id)
                log.info("scheduler_manager: removed job_key=%s", job_key)
        except Exception as exc:
            log.debug("scheduler_manager: remove skipped for %s: %s", job_key, exc)
        self._known_jobs.discard(job_key)
        self._applied_configs.pop(job_key, None)

    def _extract_job_config(
        self,
        props: dict[str, Any],
        env_fallbacks: dict[str, int] | None = None,
    ) -> dict[str, Any]:
        """Extract job configuration from Notion row properties."""
        trigger_type_raw = self._extract_select(props.get("Trigger Type", {}))
        trigger_type = (trigger_type_raw or "interval").lower()
        job_key = self._extract_text(props.get("Job Key", {})) or ""

        interval_seconds = self._extract_int_from_text_or_number(props.get("Interval Seconds", {}))
        interval_minutes = self._extract_int_from_text_or_number(props.get("Interval Minutes", {}))
        interval_hours = self._extract_int_from_text_or_number(props.get("Interval Hours", {}))

        if (
            trigger_type == "interval"
            and interval_seconds is None
            and interval_minutes is None
            and interval_hours is None
            and env_fallbacks
            and job_key in env_fallbacks
        ):
            interval_seconds = env_fallbacks[job_key]
            log.info(
                "scheduler_manager: using env fallback interval for %s: %ds",
                job_key,
                interval_seconds,
            )

        return {
            "trigger_type": trigger_type,
            "interval_seconds": interval_seconds,
            "interval_minutes": interval_minutes,
            "interval_hours": interval_hours,
            "cron_day_of_week": self._extract_text(props.get("Cron Day Of Week", {})),
            "cron_hour": self._extract_int_from_text_or_number(props.get("Cron Hour", {})),
            "cron_minute": self._extract_int_from_text_or_number(props.get("Cron Minute", {})),
            "run_on_start": self._extract_checkbox(props.get("Run On Startup", {}), default=False),
            "max_instances": int(self._extract_int_from_text_or_number(props.get("Max Instances", {})) or 1),
            "misfire_grace_seconds": int(self._extract_int_from_text_or_number(props.get("Misfire Grace Seconds", {})) or 300),
            "coalesce": self._extract_checkbox(props.get("Coalesce", {}), default=True),
        }

    @staticmethod
    def _build_interval_kwargs(config: dict[str, Any]) -> dict[str, int]:
        values = {
            "seconds": config.get("interval_seconds"),
            "minutes": config.get("interval_minutes"),
            "hours": config.get("interval_hours"),
        }
        populated = {key: int(value) for key, value in values.items() if value is not None and value > 0}
        if len(populated) > 1:
            raise ValueError("Interval jobs must set only one interval field")
        return populated

    @staticmethod
    def _build_cron_kwargs(config: dict[str, Any]) -> dict[str, Any]:
        kwargs: dict[str, Any] = {}
        if config.get("cron_day_of_week"):
            kwargs["day_of_week"] = config["cron_day_of_week"]

        cron_hour = config.get("cron_hour")
        if cron_hour is not None:
            kwargs["hour"] = int(cron_hour)

        cron_minute = config.get("cron_minute")
        if cron_minute is not None:
            kwargs["minute"] = int(cron_minute)
        elif "hour" in kwargs:
            kwargs["minute"] = 0

        if "hour" not in kwargs:
            raise ValueError("Cron jobs must set Cron Hour")
        return kwargs

    @staticmethod
    def _extract_int_from_text_or_number(prop: dict[str, Any]) -> int | None:
        """Extract an integer from a Notion number, rich_text, or title property."""
        val = prop.get("number")
        if val is not None:
            try:
                return int(val)
            except (ValueError, TypeError):
                pass

        items = prop.get("rich_text", [])
        if items:
            text = items[0].get("plain_text", "").strip()
            if text:
                try:
                    return int(text)
                except (ValueError, TypeError):
                    pass

        items = prop.get("title", [])
        if items:
            text = items[0].get("plain_text", "").strip()
            if text:
                try:
                    return int(text)
                except (ValueError, TypeError):
                    pass

        return None

    @staticmethod
    def _config_signature(config: dict[str, Any]) -> tuple[Any, ...]:
        return (
            config.get("trigger_type"),
            config.get("interval_seconds"),
            config.get("interval_minutes"),
            config.get("interval_hours"),
            config.get("cron_day_of_week"),
            config.get("cron_hour"),
            config.get("cron_minute"),
            config.get("run_on_start"),
            config.get("max_instances"),
            config.get("misfire_grace_seconds"),
            config.get("coalesce"),
        )

    def _query_all_rows(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            kwargs: dict[str, Any] = {"database_id": self._db_id}
            if cursor:
                kwargs["start_cursor"] = cursor
            resp = self._notion.databases.query(**kwargs)
            rows.extend(resp.get("results", []))
            if not resp.get("has_more"):
                break
            cursor = resp.get("next_cursor")
        return rows

    def _update_notion_loaded_at(self, page_id: str, iso_timestamp: str) -> None:
        self._update_notion_status(page_id, status=None, error=None, loaded_at=iso_timestamp)

    def _update_notion_run_result(
        self,
        *,
        page_id: str,
        ran_at: str,
        status: str,
        error: str | None,
    ) -> None:
        props: dict[str, Any] = {
            "Last Run At": {"date": {"start": ran_at}},
            "Last Status": {"select": {"name": status}},
            "Last Error": self._notion_rich_text(error or ""),
        }
        self._update_page(page_id, props)

    def _update_notion_status(
        self,
        page_id: str,
        *,
        status: str | None,
        error: str | None,
        loaded_at: str | None = None,
    ) -> None:
        props: dict[str, Any] = {}
        if loaded_at:
            props["Last Loaded At"] = {"date": {"start": loaded_at}}
        if status:
            props["Last Status"] = {"select": {"name": status}}
        if error is not None:
            props["Last Error"] = self._notion_rich_text(error)
        self._update_page(page_id, props)

    def _update_page(self, page_id: str, properties: dict[str, Any]) -> None:
        if not page_id or not properties:
            return
        try:
            self._notion.pages.update(page_id=page_id, properties=properties)
        except Exception as exc:
            log.warning("scheduler_manager: failed to update Notion page %s: %s", page_id, exc)

    @staticmethod
    def _notion_rich_text(value: str) -> dict[str, Any]:
        if not value:
            return {"rich_text": []}
        return {"rich_text": [{"text": {"content": value[:500]}}]}

    @staticmethod
    def _extract_text(prop: dict[str, Any]) -> str | None:
        for field in ("title", "rich_text"):
            items = prop.get(field, []) or []
            if items:
                return "".join(item.get("plain_text", "") for item in items).strip() or None
        if prop.get("select"):
            return (prop["select"].get("name") or "").strip() or None
        return None

    @staticmethod
    def _extract_number(prop: dict[str, Any]) -> float | None:
        value = prop.get("number")
        return float(value) if value is not None else None

    @staticmethod
    def _extract_select(prop: dict[str, Any]) -> str | None:
        select = prop.get("select")
        return select.get("name") if select else None

    @staticmethod
    def _extract_checkbox(prop: dict[str, Any], *, default: bool) -> bool:
        if "checkbox" not in prop:
            return default
        return bool(prop.get("checkbox"))

    @staticmethod
    def _utc_iso() -> str:
        return datetime.now(dt_timezone.utc).isoformat()

    @staticmethod
    def _apscheduler_id(job_key: str) -> str:
        return f"{_JOB_PREFIX}{job_key}"

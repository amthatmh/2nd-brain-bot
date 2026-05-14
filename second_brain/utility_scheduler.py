"""Notion-backed utility scheduler helpers."""

from __future__ import annotations

import inspect
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable
from second_brain.notion.properties import rich_text_prop


STATUS_NOT_LOADED = "not_loaded"
STATUS_OK = "ok"
STATUS_DISABLED = "disabled"
STATUS_SKIPPED_CONFIG = "skipped_config"
STATUS_FAILED_VALIDATION = "failed_validation"
STATUS_FAILED = "failed"
STATUS_UNKNOWN_JOB = "unknown_job"


@dataclass(frozen=True)
class UtilityJobDefinition:
    func: Callable[..., Any]
    args: tuple[Any, ...] = ()
    available: bool = True
    unavailable_reason: str = ""


@dataclass(frozen=True)
class UtilityJobSpec:
    page_id: str
    job_key: str
    enabled: bool
    trigger_type: str
    interval_seconds: int | None = None
    interval_minutes: int | None = None
    interval_hours: int | None = None
    cron_day_of_week: str | None = None
    cron_hour: str | int | None = None
    cron_minute: int | None = None
    run_on_startup: bool = False
    max_instances: int = 1
    coalesce: bool = True
    misfire_grace_seconds: int | None = None
    validation_error: str = ""
    alert_config: dict[str, Any] = field(default_factory=dict)

    @property
    def valid(self) -> bool:
        return not self.validation_error


_APPLIED_SIGNATURES: dict[str, tuple[Any, ...]] = {}


class UtilitySchedulerStatusRecorder:
    """Writes utility scheduler load/run state back to Notion."""

    def __init__(self, *, notion: Any, notion_call: Callable[..., Any], logger: logging.Logger | None = None):
        self.notion = notion
        self.notion_call = notion_call
        self.log = logger or logging.getLogger(__name__)

    def mark_loaded(self, spec: UtilityJobSpec, status: str = STATUS_OK, error: str = "") -> None:
        self._update_page(
            spec.page_id,
            {
                "Last Loaded At": _notion_date_now(),
                "Last Status": _notion_select(status),
                "Last Error": _notion_text(error),
            },
        )

    def mark_run_ok(self, spec: UtilityJobSpec) -> None:
        self._update_page(
            spec.page_id,
            {
                "Last Run At": _notion_date_now(),
                "Last Status": _notion_select(STATUS_OK),
                "Last Error": _notion_text(""),
            },
        )

    def mark_run_failed(self, spec: UtilityJobSpec, error: str) -> None:
        self._update_page(
            spec.page_id,
            {
                "Last Run At": _notion_date_now(),
                "Last Status": _notion_select(STATUS_FAILED),
                "Last Error": _notion_text(error[:1900]),
            },
        )

    def _update_page(self, page_id: str, properties: dict[str, Any]) -> None:
        try:
            self.notion_call(self.notion.pages.update, page_id=page_id, properties=properties)
        except Exception as exc:
            self.log.warning("Utility scheduler status update failed for %s: %s", page_id, exc)


class NullUtilitySchedulerStatusRecorder:
    def mark_loaded(self, spec: UtilityJobSpec, status: str = STATUS_OK, error: str = "") -> None:
        return None

    def mark_run_ok(self, spec: UtilityJobSpec) -> None:
        return None

    def mark_run_failed(self, spec: UtilityJobSpec, error: str) -> None:
        return None


def parse_utility_job_rows(rows: list[dict[str, Any]], *, logger: logging.Logger | None = None) -> list[UtilityJobSpec]:
    log = logger or logging.getLogger(__name__)
    specs: list[UtilityJobSpec] = []
    seen: set[str] = set()
    for row in rows:
        props = row.get("properties", {}) or {}
        job_key = _read_text(props.get("Job Key")).strip()
        page_id = str(row.get("id") or job_key or "unknown")
        if not job_key:
            log.warning("Skipping Utility Scheduler row without Job Key: %s", page_id)
            continue
        validation_error = ""
        if job_key in seen:
            validation_error = f"duplicate Job Key: {job_key}"
        seen.add(job_key)

        trigger_type = _normalize_token(_read_select_or_text(props.get("Trigger Type")))
        spec = UtilityJobSpec(
            page_id=page_id,
            job_key=job_key,
            enabled=_read_checkbox(props.get("Enabled")),
            trigger_type=trigger_type,
            interval_seconds=_read_positive_int(props.get("Interval Seconds")),
            interval_minutes=_read_positive_int(props.get("Interval Minutes")),
            interval_hours=_read_positive_int(props.get("Interval Hours")),
            cron_day_of_week=_read_optional_text(props.get("Cron Day Of Week")),
            cron_hour=_read_cron_hour(props.get("Cron Hour")),
            cron_minute=_read_non_negative_int(props.get("Cron Minute")),
            run_on_startup=_read_checkbox(props.get("Run On Startup")),
            max_instances=_read_positive_int(props.get("Max Instances")) or 1,
            coalesce=_read_checkbox(props.get("Coalesce")),
            misfire_grace_seconds=_read_positive_int(props.get("Misfire Grace Seconds")),
            validation_error=validation_error,
            alert_config=_extract_alert_config(props),
        )
        if not validation_error:
            spec = _validate_spec(spec)
        specs.append(spec)
    return specs


def apply_utility_job_specs(
    *,
    scheduler: Any,
    specs: list[UtilityJobSpec],
    registry: dict[str, UtilityJobDefinition],
    status_recorder: Any | None = None,
    initial_load: bool = False,
    tz: Any = None,
    now_fn: Callable[[Any], datetime] | None = None,
    logger: logging.Logger | None = None,
) -> dict[str, int]:
    log = logger or logging.getLogger(__name__)
    recorder = status_recorder or NullUtilitySchedulerStatusRecorder()
    stats = {"registered": 0, "unchanged": 0, "disabled": 0, "invalid": 0, "unknown": 0, "skipped_config": 0, "removed_missing": 0}
    seen_job_keys = {spec.job_key for spec in specs}
    for missing_job_key in set(registry) - seen_job_keys:
        if _job_exists(scheduler, missing_job_key):
            _remove_job_if_present(scheduler, missing_job_key)
            stats["removed_missing"] += 1
            log.info("Utility Scheduler removed missing job %s", missing_job_key)

    for spec in specs:
        if not spec.enabled:
            _remove_job_if_present(scheduler, spec.job_key)
            recorder.mark_loaded(spec, STATUS_DISABLED)
            stats["disabled"] += 1
            continue
        if not spec.valid:
            _remove_job_if_present(scheduler, spec.job_key)
            recorder.mark_loaded(spec, STATUS_FAILED_VALIDATION, spec.validation_error)
            stats["invalid"] += 1
            log.warning("Invalid Utility Scheduler row %s: %s", spec.job_key, spec.validation_error)
            continue
        job_definition = registry.get(spec.job_key)
        if job_definition is None:
            _remove_job_if_present(scheduler, spec.job_key)
            recorder.mark_loaded(spec, STATUS_UNKNOWN_JOB, f"Unknown Job Key: {spec.job_key}")
            stats["unknown"] += 1
            continue
        if not job_definition.available:
            _remove_job_if_present(scheduler, spec.job_key)
            recorder.mark_loaded(spec, STATUS_SKIPPED_CONFIG, job_definition.unavailable_reason)
            stats["skipped_config"] += 1
            continue

        from second_brain.monitoring import load_alert_config

        load_alert_config(spec.job_key, spec.alert_config)
        log.info("[SCHEDULER] Loaded alert config for %s: %s", spec.job_key, spec.alert_config)

        signature = _spec_signature(spec, job_definition)
        if (
            not initial_load
            and _APPLIED_SIGNATURES.get(spec.job_key) == signature
            and _job_exists(scheduler, spec.job_key)
        ):
            recorder.mark_loaded(spec, STATUS_OK)
            stats["unchanged"] += 1
            continue

        kwargs = _scheduler_kwargs_for_spec(spec)
        kwargs.update(
            {
                "id": spec.job_key,
                "replace_existing": True,
                "max_instances": spec.max_instances,
                "coalesce": spec.coalesce,
            }
        )
        if spec.misfire_grace_seconds is not None:
            kwargs["misfire_grace_time"] = spec.misfire_grace_seconds
        if initial_load and spec.run_on_startup:
            now = now_fn(tz) if now_fn is not None else datetime.now(tz)
            kwargs["next_run_time"] = now

        scheduler.add_job(
            _tracked_job(spec, job_definition, recorder),
            spec.trigger_type,
            **kwargs,
        )
        _APPLIED_SIGNATURES[spec.job_key] = signature
        recorder.mark_loaded(spec, STATUS_OK)
        stats["registered"] += 1
        log.info("Utility Scheduler registered %s (%s)", spec.job_key, spec.trigger_type)
    return stats


async def load_and_apply_utility_scheduler(
    *,
    database_id: str,
    notion_query_all: Callable[..., list[dict[str, Any]]],
    scheduler: Any,
    registry: dict[str, UtilityJobDefinition],
    status_recorder: Any | None = None,
    initial_load: bool = False,
    tz: Any = None,
    now_fn: Callable[[Any], datetime] | None = None,
    logger: logging.Logger | None = None,
) -> dict[str, int]:
    log = logger or logging.getLogger(__name__)
    rows = notion_query_all(database_id)
    specs = parse_utility_job_rows(rows, logger=log)
    return apply_utility_job_specs(
        scheduler=scheduler,
        specs=specs,
        registry=registry,
        status_recorder=status_recorder,
        initial_load=initial_load,
        tz=tz,
        now_fn=now_fn,
        logger=log,
    )


def _extract_alert_config(props: dict[str, Any]) -> dict[str, Any]:
    return {
        "alert_on_success": _normalize_token(_read_select_or_text(props.get("Alert On Success"))) or "full",
        "alert_on_failure": _normalize_token(_read_select_or_text(props.get("Alert On Failure"))) or "always",
        "alert_on_overlap": _read_checkbox_default(props.get("Alert On Overlap"), default=True),
        "success_cooldown_hours": _read_non_negative_int(props.get("Success Cooldown Hours")) or 0,
        "failure_cooldown_hours": _read_non_negative_int(props.get("Failure Cooldown Hours")) or 6,
        "overlap_cooldown_hours": _read_non_negative_int(props.get("Overlap Cooldown Hours")) or 6,
        "overlap_threshold_seconds": _read_non_negative_int(props.get("Overlap Threshold Seconds")) or 180,
    }


def _validate_spec(spec: UtilityJobSpec) -> UtilityJobSpec:
    error = ""
    if spec.trigger_type not in {"cron", "interval"}:
        error = "Trigger Type must be cron or interval"
    elif spec.trigger_type == "interval":
        interval_fields = [spec.interval_seconds, spec.interval_minutes, spec.interval_hours]
        populated = [value for value in interval_fields if value is not None]
        if len(populated) != 1:
            error = "Interval jobs must set exactly one interval field"
    elif spec.trigger_type == "cron":
        if spec.cron_hour is None or spec.cron_minute is None:
            error = "Cron jobs must set Cron Hour and Cron Minute"
    if error:
        return UtilityJobSpec(**{**spec.__dict__, "validation_error": error})
    return spec


def _scheduler_kwargs_for_spec(spec: UtilityJobSpec) -> dict[str, Any]:
    if spec.trigger_type == "interval":
        if spec.interval_seconds is not None:
            return {"seconds": spec.interval_seconds}
        if spec.interval_minutes is not None:
            return {"minutes": spec.interval_minutes}
        if spec.interval_hours is not None:
            return {"hours": spec.interval_hours}
    kwargs: dict[str, Any] = {"hour": spec.cron_hour, "minute": spec.cron_minute}
    if spec.cron_day_of_week:
        kwargs["day_of_week"] = spec.cron_day_of_week
    return kwargs


def _tracked_job(spec: UtilityJobSpec, job_definition: UtilityJobDefinition, recorder: Any) -> Callable[[], Any]:
    async def _run() -> Any:
        try:
            result = job_definition.func(*job_definition.args)
            if inspect.isawaitable(result):
                result = await result
            recorder.mark_run_ok(spec)
            return result
        except Exception as exc:
            recorder.mark_run_failed(spec, str(exc))
            raise

    return _run


def _spec_signature(spec: UtilityJobSpec, job_definition: UtilityJobDefinition) -> tuple[Any, ...]:
    return (
        spec.trigger_type,
        spec.interval_seconds,
        spec.interval_minutes,
        spec.interval_hours,
        spec.cron_day_of_week,
        spec.cron_hour,
        spec.cron_minute,
        spec.run_on_startup,
        spec.max_instances,
        spec.coalesce,
        spec.misfire_grace_seconds,
        id(job_definition.func),
        tuple(id(arg) for arg in job_definition.args),
    )


def _job_exists(scheduler: Any, job_id: str) -> bool:
    get_job = getattr(scheduler, "get_job", None)
    if callable(get_job):
        return get_job(job_id) is not None
    return False


def _remove_job_if_present(scheduler: Any, job_id: str) -> None:
    _APPLIED_SIGNATURES.pop(job_id, None)
    get_job = getattr(scheduler, "get_job", None)
    if callable(get_job):
        job = get_job(job_id)
        if job is not None:
            job.remove()
        return
    remove_job = getattr(scheduler, "remove_job", None)
    if callable(remove_job):
        try:
            remove_job(job_id)
        except Exception:
            return


def _read_text(prop: dict[str, Any] | None) -> str:
    if not prop:
        return ""
    if "title" in prop:
        return "".join(part.get("plain_text") or part.get("text", {}).get("content", "") for part in (prop.get("title") or []))
    if "rich_text" in prop:
        return "".join(part.get("plain_text") or part.get("text", {}).get("content", "") for part in (prop.get("rich_text") or []))
    if "number" in prop and prop.get("number") is not None:
        return str(prop.get("number"))
    if "select" in prop and prop.get("select"):
        return str(prop["select"].get("name") or "")
    return ""


def _read_select_or_text(prop: dict[str, Any] | None) -> str:
    return _read_text(prop)


def _read_optional_text(prop: dict[str, Any] | None) -> str | None:
    value = _read_text(prop).strip()
    return value or None


def _read_checkbox(prop: dict[str, Any] | None) -> bool:
    return bool((prop or {}).get("checkbox"))


def _read_checkbox_default(prop: dict[str, Any] | None, *, default: bool) -> bool:
    if not prop or "checkbox" not in prop:
        return default
    return bool(prop.get("checkbox"))


def _read_positive_int(prop: dict[str, Any] | None) -> int | None:
    value = _read_number(prop)
    if value is None or value <= 0:
        return None
    return int(value)


def _read_non_negative_int(prop: dict[str, Any] | None) -> int | None:
    value = _read_number(prop)
    if value is None or value < 0:
        return None
    return int(value)


def _read_number(prop: dict[str, Any] | None) -> float | None:
    if not prop:
        return None
    raw = prop.get("number")
    if raw is None:
        text = _read_text(prop).strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None
    return float(raw)


def _read_cron_hour(prop: dict[str, Any] | None) -> str | int | None:
    value = _read_text(prop).strip()
    if not value:
        return None
    try:
        numeric = float(value)
        if numeric.is_integer():
            return int(numeric)
    except ValueError:
        pass
    return value


def _normalize_token(value: str) -> str:
    return value.strip().lower().replace(" ", "_")


def _notion_select(name: str) -> dict[str, Any]:
    return {"select": {"name": name}}


def _notion_date_now() -> dict[str, Any]:
    return {"date": {"start": datetime.now(timezone.utc).isoformat()}}


def _notion_text(value: str) -> dict[str, Any]:
    if not value:
        return {"rich_text": []}
    return rich_text_prop(value[:1900])

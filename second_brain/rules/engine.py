"""Lightweight rule execution engine for cross-database automations."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from second_brain.rules import entertainment_rules

log = logging.getLogger(__name__)


class RuleEngine:
    """Evaluate post-save rules and execute matching Notion actions."""

    def __init__(self, notion_client):
        self.notion = notion_client
        self.rules: list[dict[str, Any]] = []
        self.loaded = False

    def startup(self) -> bool:
        """Load and validate rules at bot startup."""
        is_valid, errors = entertainment_rules.validate_rules()
        if not is_valid:
            log.error("Rule validation failed: %s", errors)
            return False

        self.rules = entertainment_rules.get_active_rules()
        self.loaded = True
        log.info("Rule engine loaded: %d active rules", len(self.rules))
        return True

    async def execute_on_save(
        self,
        source_db: str,
        entry_data: dict[str, Any],
        db_ids: dict[str, str],
    ) -> list[dict[str, Any]]:
        """Execute rules triggered by a database save."""
        if not self.loaded:
            log.warning("Rule engine not initialized")
            return []

        matching_rules = [rule for rule in self.rules if rule.get("trigger", {}).get("source_db") == source_db]
        if not matching_rules:
            return []

        results: list[dict[str, Any]] = []
        for rule in matching_rules:
            try:
                trigger = rule["trigger"]
                trigger_field = trigger.get("field")
                trigger_value = trigger.get("equals")

                if trigger_field not in entry_data:
                    msg = f"Field '{trigger_field}' not in entry"
                    log.debug("Rule %s: %s", rule["id"], msg)
                    results.append({"rule_id": rule["id"], "success": False, "message": msg})
                    continue

                if entry_data[trigger_field] != trigger_value:
                    log.debug(
                        "Rule %s: condition not met (%s=%r)",
                        rule["id"],
                        trigger_field,
                        entry_data[trigger_field],
                    )
                    results.append({"rule_id": rule["id"], "success": False, "message": "Condition not met"})
                    continue

                for action in rule.get("actions", []):
                    try:
                        await self._execute_action(action, entry_data, db_ids, rule["id"])
                        results.append({
                            "rule_id": rule["id"],
                            "success": True,
                            "message": f"Action '{action.get('type')}' executed",
                        })
                    except Exception as exc:
                        log.error("Rule %s action %s failed: %s", rule["id"], action.get("type"), exc)
                        results.append({
                            "rule_id": rule["id"],
                            "success": False,
                            "message": f"Action failed: {exc}",
                        })
            except Exception as exc:
                log.error("Rule %s execution failed: %s", rule.get("id", "unknown"), exc)
                results.append({"rule_id": rule.get("id", "unknown"), "success": False, "message": str(exc)})

        return results

    async def _execute_action(
        self,
        action: dict[str, Any],
        source_entry: dict[str, Any],
        db_ids: dict[str, str],
        rule_id: str,
    ) -> None:
        action_type = action.get("type")
        if action_type == "create_entry":
            await self._action_create_entry(action, source_entry, db_ids, rule_id)
        else:
            log.warning("Unknown action type: %s", action_type)

    async def _action_create_entry(
        self,
        action: dict[str, Any],
        source_entry: dict[str, Any],
        db_ids: dict[str, str],
        rule_id: str,
    ) -> None:
        target_db = action.get("target_db")
        field_mapping = action.get("field_mapping", {})
        dedup_field = action.get("dedup_field")

        target_db_id = db_ids.get(target_db)
        if not target_db_id:
            raise ValueError(f"Unknown target database: {target_db}")

        mapped_entry = {
            target_field: source_entry[source_field]
            for source_field, target_field in field_mapping.items()
            if source_field in source_entry and source_entry[source_field] is not None
        }
        if not mapped_entry:
            raise ValueError(f"No fields mapped for {rule_id}")

        if dedup_field and dedup_field in mapped_entry:
            existing = await asyncio.to_thread(
                self.notion.databases.query,
                database_id=target_db_id,
                filter={"property": dedup_field, "title": {"equals": str(mapped_entry[dedup_field])}},
            )
            if existing.get("results", []):
                log.info("Rule %s: duplicate found in %s, skipping", rule_id, target_db)
                return

        properties: dict[str, Any] = {}
        for field_name, field_value in mapped_entry.items():
            if field_name == "Title":
                properties[field_name] = {"title": [{"text": {"content": str(field_value)}}]}
            elif field_name == "DateWatched":
                properties[field_name] = {"date": {"start": str(field_value)}}
            else:
                properties[field_name] = {"rich_text": [{"text": {"content": str(field_value)}}]}

        page = await asyncio.to_thread(
            self.notion.pages.create,
            parent={"database_id": target_db_id},
            properties=properties,
        )
        log.info("Rule %s: created entry in %s, page_id=%s", rule_id, target_db, page.get("id"))

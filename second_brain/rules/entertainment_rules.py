"""
Cross-database rule definitions for entertainment logs.

Rules are evaluated after primary database writes to trigger optional follow-up
actions such as adding favourite cinema entries to the Favourite Films database.
"""

from __future__ import annotations

RULES_VERSION = "1.0"

CROSS_DB_RULES = [
    {
        "id": "cinema_to_favourite",
        "version": "1.0",
        "description": "When Cinema Log is marked favourite, auto-create a Favourite Films entry",
        "enabled": True,
        "trigger": {
            "source_db": "cinema_log",
            "field": "Favourite",
            "equals": True,
        },
        "actions": [
            {
                "type": "create_entry",
                "target_db": "favourite_films",
                "dedup_field": "Title",
                "field_mapping": {
                    "Title": "Title",
                    "DateWatched": "DateWatched",
                },
            }
        ],
    },
]


def get_active_rules(source_db: str | None = None) -> list[dict]:
    """Return enabled rules, optionally filtered by source database key."""
    rules = [rule for rule in CROSS_DB_RULES if rule.get("enabled", True)]
    if source_db:
        rules = [rule for rule in rules if rule.get("trigger", {}).get("source_db") == source_db]
    return rules


def validate_rules() -> tuple[bool, list[str]]:
    """Validate rule structure at startup."""
    errors: list[str] = []

    for i, rule in enumerate(CROSS_DB_RULES):
        rule_id = rule.get("id", i)
        if "id" not in rule:
            errors.append(f"Rule {i}: missing 'id'")
        if "trigger" not in rule:
            errors.append(f"Rule {rule_id}: missing 'trigger'")
        if "actions" not in rule or not rule["actions"]:
            errors.append(f"Rule {rule_id}: missing 'actions'")

        trigger = rule.get("trigger", {})
        if "source_db" not in trigger:
            errors.append(f"Rule {rule_id}: trigger missing 'source_db'")
        if "field" not in trigger or "equals" not in trigger:
            errors.append(f"Rule {rule_id}: trigger must have 'field' and 'equals'")

        for j, action in enumerate(rule.get("actions", [])):
            if action.get("type") == "create_entry":
                if "target_db" not in action:
                    errors.append(f"Rule {rule_id}, action {j}: missing 'target_db'")
                if "field_mapping" not in action:
                    errors.append(f"Rule {rule_id}, action {j}: missing 'field_mapping'")

    return len(errors) == 0, errors

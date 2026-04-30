from telegram import InlineKeyboardButton, InlineKeyboardMarkup


def _clean_pid(pid: str) -> str:
    return (pid or "").replace("-", "")


def crossfit_submenu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Log Strength (B)", callback_data="cf:log_strength"), InlineKeyboardButton("🏋️ Log WOD (C)", callback_data="cf:log_wod")],
        [InlineKeyboardButton("📤 Upload Programme", callback_data="cf:upload_programme"), InlineKeyboardButton("🔍 Sub / Add-on", callback_data="cf:subs")],
        [InlineKeyboardButton("🏆 My PRs", callback_data="cf:prs")],
    ])


def wod_format_keyboard(key: str) -> InlineKeyboardMarkup:
    formats = [("For Time", "for_time"), ("AMRAP", "amrap"), ("EMOM", "emom"), ("Chipper", "chipper"), ("Max Reps", "max_reps"), ("Tabata", "tabata")]
    rows = []
    for i in range(0, len(formats), 2):
        rows.append([InlineKeyboardButton(f[0], callback_data=f"cf:fmt:{key}:{f[1]}") for f in formats[i:i + 2]])
    return InlineKeyboardMarkup(rows)


def result_type_keyboard(key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("⏱ Time", callback_data=f"cf:rt:{key}:time"), InlineKeyboardButton("🔄 Rounds+Reps", callback_data=f"cf:rt:{key}:rounds_reps")], [InlineKeyboardButton("💪 Reps", callback_data=f"cf:rt:{key}:reps"), InlineKeyboardButton("🏋️ Load", callback_data=f"cf:rt:{key}:load")]])


def rx_scaled_keyboard(key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("✅ Rx", callback_data=f"cf:rx:{key}:rx"), InlineKeyboardButton("📉 Scaled", callback_data=f"cf:rx:{key}:scaled"), InlineKeyboardButton("🔧 Modified", callback_data=f"cf:rx:{key}:modified")]])


def partner_keyboard(key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("👥 Partner WOD", callback_data=f"cf:partner:{key}:yes"), InlineKeyboardButton("🙋 Solo", callback_data=f"cf:partner:{key}:no")]])


def readiness_keyboard(key: str, field: str) -> InlineKeyboardMarkup:
    labels = {"1": "😴", "2": "😕", "3": "😐", "4": "🙂", "5": "💪"}
    return InlineKeyboardMarkup([[InlineKeyboardButton(f"{v} {k}", callback_data=f"cf:ready:{key}:{field}:{k}") for k, v in labels.items()]])


def sub_type_keyboard(key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Substitution", callback_data=f"cf:subtype:{key}:Sub"), InlineKeyboardButton("➕ Add-on", callback_data=f"cf:subtype:{key}:Add-on")], [InlineKeyboardButton("⚠️ Weakness work", callback_data=f"cf:subtype:{key}:Weakness")]])


def my_level_keyboard(key: str, steps) -> InlineKeyboardMarkup:
    rows = []
    current_idx = next((i for i, s in enumerate(steps) if s.get("is_current_level")), None)
    goal_idx = (current_idx + 1) if current_idx is not None and current_idx + 1 < len(steps) else None
    for idx, step in enumerate(steps):
        prefix = ""
        if current_idx is not None and idx == current_idx:
            prefix = "✅ "
        elif goal_idx is not None and idx == goal_idx:
            prefix = "🎯 "
        label = f"{prefix}{step.get('name') or 'Unnamed'}"
        rows.append([InlineKeyboardButton(label, callback_data=f"cf:setlevel:{key}:{_clean_pid(step.get('page_id', ''))}")])
    return InlineKeyboardMarkup(rows)


def level_confirm_keyboard(key: str, level_name: str, goal_name: str | None) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(f"✅ Yes, logging at {level_name}", callback_data=f"cf:levelok:{key}")],
        [InlineKeyboardButton("🔄 Change my level", callback_data=f"cf:changelevel:{key}")],
    ]
    if goal_name:
        rows.append([InlineKeyboardButton(f"🎯 I hit {goal_name} today!", callback_data=f"cf:levelup:{key}")])
    return InlineKeyboardMarkup(rows)

from telegram import InlineKeyboardButton, InlineKeyboardMarkup


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

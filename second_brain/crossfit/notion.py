from __future__ import annotations
import json
from datetime import datetime
from second_brain.notion import notion_call


def _title(props, key="Name"):
    try:return props[key]["title"][0]["plain_text"]
    except Exception:return ""

# existing functions unchanged ...
def find_movement_by_name(notion, movements_db_id: str, name: str):
    res = notion_call(notion.databases.query, database_id=movements_db_id).get("results", [])
    needle = (name or "").strip().lower()
    exact = []
    fuzzy = []
    for r in res:
        nm = _title(r.get("properties", {}))
        if nm.lower() == needle: exact.append({"page_id": r["id"], "name": nm})
        elif needle and needle in nm.lower(): fuzzy.append({"page_id": r["id"], "name": nm})
    return (exact or fuzzy or [None])[0]

def get_or_create_movement(notion, movements_db_id: str, name: str) -> str:
    found = find_movement_by_name(notion, movements_db_id, name)
    if found: return found["page_id"]
    page = notion_call(notion.pages.create, parent={"database_id": movements_db_id}, properties={"Name": {"title": [{"text": {"content": name}}]}, "Category": {"select": {"name": "Compound"}}})
    return page["id"]

def get_current_week_programme(notion, program_db_id: str):
    res = notion_call(notion.databases.query, database_id=program_db_id, sorts=[{"timestamp": "created_time", "direction": "descending"}], page_size=1).get("results", [])
    if not res: return None
    p=res[0]; props=p.get("properties",{})
    full = props.get("Full Program", {}).get("rich_text", [])
    full_text = "".join(x.get("plain_text", "") for x in full)
    parsed = None
    try: parsed = json.loads(full_text)
    except Exception: parsed = None
    return {"page_id": p["id"], "name": _title(props), "full_program": full_text, "week_label": _title(props), "days_parsed": parsed}

def save_programme(notion, program_db_id, movements_db_id, parsed, full_text):
    names=set()
    for d in parsed.get("days",[]):
        for s in ("section_b","section_c"):
            sec=d.get(s) or {}
            for m in sec.get("movements",[]): names.add(m)
    rel=[{"id": get_or_create_movement(notion,movements_db_id,m)} for m in sorted(names)]
    page=notion_call(notion.pages.create,parent={"database_id":program_db_id},properties={"Name":{"title":[{"text":{"content":parsed.get("week_label","Week")}}]},"Full Program":{"rich_text":[{"text":{"content":full_text[:1900]}}]},"Rep Scheme":{"rich_text":[{"text":{"content":parsed.get("rep_scheme_this_week") or ""}}]},"Movements":{"relation":rel}})
    return page["id"]

def get_previous_best(notion, prs_db_id, movement_page_id, reps):
    res=notion_call(notion.databases.query,database_id=prs_db_id,page_size=50).get("results",[])
    best=None
    for r in res:
        p=r.get("properties",{})
        rs=p.get("Reps",{}).get("number")
        rel=[x.get("id") for x in p.get("Movement",{}).get("relation",[])]
        wt=p.get("Weight (lbs)",{}).get("number")
        if rs==reps and movement_page_id in rel and wt is not None and (best is None or wt>best["weight_lbs"]):
            best={"page_id":r["id"],"weight_lbs":wt,"date":(p.get("Date",{}).get("date") or {}).get("start")}
    return best

def create_pr_entry(notion, prs_db_id, cycles_db_id, movement_page_id, movement_name, weight_lbs, reps, previous_best_lbs, notes):
    props={"Name":{"title":[{"text":{"content":f"{movement_name} {reps}RM — {datetime.utcnow().date().isoformat()}"}}]},"Date":{"date":{"start":datetime.utcnow().date().isoformat()}},"Movement":{"relation":[{"id":movement_page_id}]},"Weight (lbs)":{"number":weight_lbs},"Previous Best (lbs)":{"number":previous_best_lbs or 0},"Reps":{"number":reps},"Rep Format":{"rich_text":[{"text":{"content":f"{reps}RM"}}]},"Notes":{"rich_text":[{"text":{"content":notes or ""}}]}}
    page=notion_call(notion.pages.create,parent={"database_id":prs_db_id},properties=props); return page["id"]

def create_strength_log(notion, workout_log_db_id, movement_page_id, movement_name, load_lbs, effort_sets, effort_reps, is_max_attempt, weekly_program_page_id, cycle_page_id, readiness):
    props={"Name":{"title":[{"text":{"content":f"{movement_name} — {datetime.utcnow().date().isoformat()}"}}]},"Date":{"date":{"start":datetime.utcnow().date().isoformat()}},"Movement":{"relation":[{"id":movement_page_id}]},"load_lbs":{"number":load_lbs},"effort_sets":{"number":effort_sets},"effort_reps":{"number":effort_reps},"is_max_attempt":{"checkbox":bool(is_max_attempt)}}
    page=notion_call(notion.pages.create,parent={"database_id":workout_log_db_id},properties=props); return page["id"]

def create_wod_log(notion, wod_log_db_id, wod_format, duration_mins, time_cap_mins, result_type, result_seconds, result_rounds, result_reps, rx_scaled, scaling_notes, is_partner, wod_name, movement_page_ids, weekly_program_page_id, readiness):
    props={"Name":{"title":[{"text":{"content":f"{(wod_name or wod_format)} — {datetime.utcnow().date().isoformat()}"}}]},"Date":{"date":{"start":datetime.utcnow().date().isoformat()}},"Format":{"select":{"name":wod_format}},"Result Type":{"select":{"name":result_type}},"Rx / Scaled":{"select":{"name":rx_scaled}},"Partner?":{"checkbox":bool(is_partner)}}
    page=notion_call(notion.pages.create,parent={"database_id":wod_log_db_id},properties=props); return page["id"]

def query_subs(notion, subs_db_id, movements_db_id, movement_name, sub_type):
    m=find_movement_by_name(notion,movements_db_id,movement_name)
    if not m: return []
    res=notion_call(notion.databases.query,database_id=subs_db_id).get("results",[])
    out=[]
    for r in res:
        p=r.get("properties",{})
        rel=[x.get("id") for x in p.get("Movement",{}).get("relation",[])]
        typ=(p.get("Type",{}).get("select") or {}).get("name")
        if m["page_id"] in rel and typ==sub_type:
            out.append({"name":_title(p),"alt_movement":"","difficulty":((p.get("Difficulty",{}).get("select") or {}).get("name") or ""),"equipment_needed":"","rationale":""})
    return out


def get_progressions_for_movement(notion, progressions_db_id, movement_page_id) -> list[dict]:
    res = notion_call(notion.databases.query, database_id=progressions_db_id, page_size=100).get("results", [])
    out = []
    for row in res:
        props = row.get("properties", {})
        rel = [x.get("id") for x in props.get("Target Movement", {}).get("relation", [])]
        if movement_page_id not in rel:
            continue
        out.append({
            "page_id": row.get("id"),
            "name": _title(props),
            "order": props.get("Order", {}).get("number") or 0,
            "is_current_level": bool(props.get("Is My Current Level", {}).get("checkbox")),
            "notes": "".join(x.get("plain_text", "") for x in props.get("Notes", {}).get("rich_text", [])),
        })
    return sorted(out, key=lambda x: x.get("order", 0))


def get_movement_category(notion, movements_db_id, movement_page_id) -> str:
    del movements_db_id
    page = notion_call(notion.pages.retrieve, page_id=movement_page_id)
    props = page.get("properties", {})
    return ((props.get("Category", {}).get("select") or {}).get("name") or "")


def set_current_level(notion, progressions_db_id, movement_page_id, new_current_page_id):
    steps = get_progressions_for_movement(notion, progressions_db_id, movement_page_id)
    for step in steps:
        notion_call(
            notion.pages.update,
            page_id=step["page_id"],
            properties={"Is My Current Level": {"checkbox": step["page_id"] == new_current_page_id}},
        )

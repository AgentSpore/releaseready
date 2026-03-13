from __future__ import annotations
import json
from datetime import datetime, timezone
from collections import Counter

import aiosqlite

SQL = """
CREATE TABLE IF NOT EXISTS checklists (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    service TEXT NOT NULL,
    version TEXT NOT NULL,
    environment TEXT NOT NULL DEFAULT 'production',
    owner_email TEXT,
    description TEXT,
    status TEXT NOT NULL DEFAULT 'in_progress',
    created_at TEXT NOT NULL,
    completed_at TEXT
);

CREATE TABLE IF NOT EXISTS check_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    checklist_id INTEGER NOT NULL,
    category TEXT NOT NULL DEFAULT 'general',
    title TEXT NOT NULL,
    description TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    is_blocking INTEGER NOT NULL DEFAULT 1,
    owner_email TEXT,
    notes TEXT,
    checked_at TEXT,
    checked_by TEXT,
    depends_on INTEGER,
    FOREIGN KEY (checklist_id) REFERENCES checklists(id)
);

CREATE TABLE IF NOT EXISTS rollback_plans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    checklist_id INTEGER NOT NULL UNIQUE,
    steps TEXT NOT NULL,
    estimated_minutes INTEGER NOT NULL DEFAULT 15,
    contacts TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL,
    FOREIGN KEY (checklist_id) REFERENCES checklists(id)
);

CREATE TABLE IF NOT EXISTS sign_offs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    checklist_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    role TEXT NOT NULL,
    comment TEXT,
    signed_at TEXT NOT NULL,
    FOREIGN KEY (checklist_id) REFERENCES checklists(id)
);

CREATE TABLE IF NOT EXISTS templates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    description TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS template_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    template_id INTEGER NOT NULL,
    category TEXT NOT NULL DEFAULT 'general',
    title TEXT NOT NULL,
    description TEXT,
    is_blocking INTEGER NOT NULL DEFAULT 1,
    FOREIGN KEY (template_id) REFERENCES templates(id) ON DELETE CASCADE
);
"""

DEFAULT_CHECKS = [
    ("infra",     "All infrastructure changes reviewed",       True),
    ("infra",     "Load balancer health checks passing",        True),
    ("infra",     "Database migrations tested on staging",      True),
    ("code",      "Feature flags configured correctly",         True),
    ("code",      "No debug/TODO code in production path",      False),
    ("code",      "Unit tests passing on CI",                   True),
    ("code",      "Integration tests passing on CI",            True),
    ("data",      "Database backup taken before deploy",        True),
    ("data",      "Migration rollback script prepared",         True),
    ("security",  "Secrets rotated / no hardcoded credentials", True),
    ("security",  "Dependency vulnerability scan clean",        False),
    ("comms",     "Stakeholders notified of deploy window",     False),
    ("comms",     "Status page updated if user-visible",        False),
    ("rollback",  "Rollback procedure documented",              True),
    ("rollback",  "Previous version artifact available",        True),
]


async def init_db(path: str) -> aiosqlite.Connection:
    db = await aiosqlite.connect(path)
    db.row_factory = aiosqlite.Row
    await db.executescript(SQL)
    # Migration: add depends_on column if missing
    try:
        await db.execute("SELECT depends_on FROM check_items LIMIT 1")
    except Exception:
        await db.execute("ALTER TABLE check_items ADD COLUMN depends_on INTEGER")
    await db.commit()
    return db


def _checklist_row(r: aiosqlite.Row, stats: dict) -> dict:
    return {
        "id": r["id"], "name": r["name"], "service": r["service"],
        "version": r["version"], "environment": r["environment"],
        "owner_email": r["owner_email"], "description": r["description"],
        "status": r["status"],
        "readiness_score": stats.get("score", 0),
        "total_checks": stats.get("total", 0),
        "passed_checks": stats.get("passed", 0),
        "failed_checks": stats.get("failed", 0),
        "blocked_checks": stats.get("blocking_failures", 0),
        "created_at": r["created_at"],
        "completed_at": r["completed_at"] if "completed_at" in r.keys() else None,
    }


def _item_row(r: aiosqlite.Row) -> dict:
    return {
        "id": r["id"], "checklist_id": r["checklist_id"],
        "category": r["category"], "title": r["title"],
        "description": r["description"], "status": r["status"],
        "is_blocking": bool(r["is_blocking"]),
        "owner_email": r["owner_email"], "notes": r["notes"],
        "checked_at": r["checked_at"], "checked_by": r["checked_by"],
        "depends_on": r["depends_on"],
    }


def _signoff_row(r: aiosqlite.Row) -> dict:
    return {
        "id": r["id"], "checklist_id": r["checklist_id"],
        "name": r["name"], "role": r["role"],
        "comment": r["comment"], "signed_at": r["signed_at"],
    }


async def _compute_stats(db: aiosqlite.Connection, checklist_id: int) -> dict:
    rows = await db.execute_fetchall(
        "SELECT status, is_blocking FROM check_items WHERE checklist_id = ?", (checklist_id,))
    total = len(rows)
    passed = sum(1 for r in rows if r["status"] == "pass")
    failed = sum(1 for r in rows if r["status"] == "fail")
    blocking_failures = sum(1 for r in rows if r["status"] == "fail" and r["is_blocking"])
    pending = sum(1 for r in rows if r["status"] == "pending")
    score = round(passed / total * 100) if total > 0 else 0
    return {"total": total, "passed": passed, "failed": failed,
            "blocking_failures": blocking_failures, "pending": pending, "score": score}


# ── Templates ─────────────────────────────────────────────────────────────

async def create_template(db: aiosqlite.Connection, data: dict) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    cur = await db.execute(
        "INSERT INTO templates (name, description, created_at) VALUES (?, ?, ?)",
        (data["name"], data.get("description"), now),
    )
    template_id = cur.lastrowid
    from_id = data.get("from_checklist_id")
    if from_id:
        items = await db.execute_fetchall(
            "SELECT category, title, description, is_blocking FROM check_items WHERE checklist_id = ? ORDER BY id",
            (from_id,),
        )
        for item in items:
            await db.execute(
                "INSERT INTO template_items (template_id, category, title, description, is_blocking) VALUES (?,?,?,?,?)",
                (template_id, item["category"], item["title"], item["description"], item["is_blocking"]),
            )
    else:
        for category, title, is_blocking in DEFAULT_CHECKS:
            await db.execute(
                "INSERT INTO template_items (template_id, category, title, is_blocking) VALUES (?,?,?,?)",
                (template_id, category, title, int(is_blocking)),
            )
    await db.commit()
    return await get_template(db, template_id)


async def list_templates(db: aiosqlite.Connection) -> list[dict]:
    rows = await db.execute_fetchall("SELECT * FROM templates ORDER BY created_at DESC")
    result = []
    for r in rows:
        cnt = await db.execute_fetchall(
            "SELECT COUNT(*) as c FROM template_items WHERE template_id = ?", (r["id"],))
        result.append({
            "id": r["id"], "name": r["name"], "description": r["description"],
            "check_count": cnt[0]["c"], "created_at": r["created_at"],
        })
    return result


async def get_template(db: aiosqlite.Connection, template_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM templates WHERE id = ?", (template_id,))
    if not rows:
        return None
    r = rows[0]
    cnt = await db.execute_fetchall(
        "SELECT COUNT(*) as c FROM template_items WHERE template_id = ?", (template_id,))
    return {
        "id": r["id"], "name": r["name"], "description": r["description"],
        "check_count": cnt[0]["c"], "created_at": r["created_at"],
    }


async def get_template_items(db: aiosqlite.Connection, template_id: int) -> list[dict]:
    rows = await db.execute_fetchall(
        "SELECT * FROM template_items WHERE template_id = ? ORDER BY id", (template_id,))
    return [{"id": r["id"], "category": r["category"], "title": r["title"],
             "description": r["description"], "is_blocking": bool(r["is_blocking"])} for r in rows]


async def add_template_item(db: aiosqlite.Connection, template_id: int, data: dict) -> dict | None:
    tmpl = await get_template(db, template_id)
    if not tmpl:
        return None
    cur = await db.execute(
        "INSERT INTO template_items (template_id, category, title, description, is_blocking) VALUES (?,?,?,?,?)",
        (template_id, data["category"], data["title"], data.get("description"), int(data.get("is_blocking", True))),
    )
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM template_items WHERE id = ?", (cur.lastrowid,))
    r = rows[0]
    return {"id": r["id"], "category": r["category"], "title": r["title"],
            "description": r["description"], "is_blocking": bool(r["is_blocking"])}


async def delete_template(db: aiosqlite.Connection, template_id: int) -> bool:
    await db.execute("DELETE FROM template_items WHERE template_id = ?", (template_id,))
    cur = await db.execute("DELETE FROM templates WHERE id = ?", (template_id,))
    await db.commit()
    return cur.rowcount > 0


# ── Checklists ────────────────────────────────────────────────────────────

async def create_checklist(db: aiosqlite.Connection, data: dict) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    cur = await db.execute(
        """INSERT INTO checklists (name, service, version, environment, owner_email, description, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (data["name"], data["service"], data["version"], data.get("environment", "production"),
         data.get("owner_email"), data.get("description"), now)
    )
    checklist_id = cur.lastrowid
    template_id = data.get("template_id")
    if template_id:
        items = await db.execute_fetchall(
            "SELECT category, title, description, is_blocking FROM template_items WHERE template_id = ? ORDER BY id",
            (template_id,),
        )
        if not items:
            # Fallback to defaults if template empty or not found
            for category, title, is_blocking in DEFAULT_CHECKS:
                await db.execute(
                    "INSERT INTO check_items (checklist_id, category, title, is_blocking) VALUES (?, ?, ?, ?)",
                    (checklist_id, category, title, int(is_blocking))
                )
        else:
            for item in items:
                await db.execute(
                    "INSERT INTO check_items (checklist_id, category, title, description, is_blocking) VALUES (?,?,?,?,?)",
                    (checklist_id, item["category"], item["title"], item["description"], item["is_blocking"]),
                )
    else:
        for category, title, is_blocking in DEFAULT_CHECKS:
            await db.execute(
                "INSERT INTO check_items (checklist_id, category, title, is_blocking) VALUES (?, ?, ?, ?)",
                (checklist_id, category, title, int(is_blocking))
            )
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM checklists WHERE id = ?", (checklist_id,))
    stats = await _compute_stats(db, checklist_id)
    return _checklist_row(rows[0], stats)


async def list_checklists(db: aiosqlite.Connection, environment: str | None = None,
                           status: str | None = None) -> list[dict]:
    q, params = "SELECT * FROM checklists", []
    conds = []
    if environment:
        conds.append("environment = ?"); params.append(environment)
    if status:
        conds.append("status = ?"); params.append(status)
    if conds:
        q += " WHERE " + " AND ".join(conds)
    q += " ORDER BY created_at DESC"
    rows = await db.execute_fetchall(q, params)
    result = []
    for r in rows:
        stats = await _compute_stats(db, r["id"])
        result.append(_checklist_row(r, stats))
    return result


async def get_checklist(db: aiosqlite.Connection, checklist_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM checklists WHERE id = ?", (checklist_id,))
    if not rows:
        return None
    stats = await _compute_stats(db, checklist_id)
    return _checklist_row(rows[0], stats)


async def delete_checklist(db: aiosqlite.Connection, checklist_id: int) -> bool:
    cur = await db.execute("DELETE FROM sign_offs WHERE checklist_id = ?", (checklist_id,))
    await db.execute("DELETE FROM check_items WHERE checklist_id = ?", (checklist_id,))
    await db.execute("DELETE FROM rollback_plans WHERE checklist_id = ?", (checklist_id,))
    await db.execute("DELETE FROM checklists WHERE id = ?", (checklist_id,))
    await db.commit()
    return cur.rowcount >= 0


async def list_check_items(db: aiosqlite.Connection, checklist_id: int) -> list[dict]:
    rows = await db.execute_fetchall(
        "SELECT * FROM check_items WHERE checklist_id = ? ORDER BY category, id", (checklist_id,))
    return [_item_row(r) for r in rows]


async def _check_dependency(db: aiosqlite.Connection, item_id: int) -> str | None:
    """Check if item has an unmet dependency. Returns error message or None."""
    rows = await db.execute_fetchall("SELECT depends_on FROM check_items WHERE id = ?", (item_id,))
    if not rows or not rows[0]["depends_on"]:
        return None
    dep_id = rows[0]["depends_on"]
    dep_rows = await db.execute_fetchall("SELECT status, title FROM check_items WHERE id = ?", (dep_id,))
    if not dep_rows:
        return None
    if dep_rows[0]["status"] != "pass":
        return f"Dependency not met: '{dep_rows[0]['title']}' (item #{dep_id}) must pass first"
    return None


async def update_check_item(db: aiosqlite.Connection, item_id: int,
                             status: str, notes: str | None, checked_by: str | None) -> dict | str | None:
    rows = await db.execute_fetchall("SELECT * FROM check_items WHERE id = ?", (item_id,))
    if not rows:
        return None
    if status == "pass":
        dep_err = await _check_dependency(db, item_id)
        if dep_err:
            return dep_err
    now = datetime.now(timezone.utc).isoformat()
    await db.execute(
        "UPDATE check_items SET status=?, notes=?, checked_by=?, checked_at=? WHERE id=?",
        (status, notes, checked_by, now, item_id)
    )
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM check_items WHERE id = ?", (item_id,))
    return _item_row(rows[0]) if rows else None


async def add_check_item(db: aiosqlite.Connection, data: dict) -> dict:
    cur = await db.execute(
        """INSERT INTO check_items (checklist_id, category, title, description, is_blocking, owner_email, depends_on)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (data["checklist_id"], data["category"], data["title"], data.get("description"),
         int(data.get("is_blocking", True)), data.get("owner_email"), data.get("depends_on"))
    )
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM check_items WHERE id = ?", (cur.lastrowid,))
    return _item_row(rows[0])


async def create_rollback_plan(db: aiosqlite.Connection, data: dict) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    await db.execute(
        "INSERT OR REPLACE INTO rollback_plans (checklist_id, steps, estimated_minutes, contacts, created_at) VALUES (?, ?, ?, ?, ?)",
        (data["checklist_id"], json.dumps(data["steps"]),
         data.get("estimated_minutes", 15), json.dumps(data.get("contacts", [])), now)
    )
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM rollback_plans WHERE checklist_id = ?", (data["checklist_id"],))
    r = rows[0]
    return {"id": r["id"], "checklist_id": r["checklist_id"], "steps": json.loads(r["steps"]),
            "estimated_minutes": r["estimated_minutes"], "contacts": json.loads(r["contacts"]),
            "created_at": r["created_at"]}


async def add_sign_off(db: aiosqlite.Connection, checklist_id: int, data: dict) -> dict | str | None:
    cl = await db.execute_fetchall("SELECT * FROM checklists WHERE id = ?", (checklist_id,))
    if not cl:
        return None
    if cl[0]["status"] == "completed":
        return "already_completed"
    stats = await _compute_stats(db, checklist_id)
    if stats["blocking_failures"] > 0:
        return "blocking_failures"
    now = datetime.now(timezone.utc).isoformat()
    cur = await db.execute(
        "INSERT INTO sign_offs (checklist_id, name, role, comment, signed_at) VALUES (?,?,?,?,?)",
        (checklist_id, data["name"], data["role"], data.get("comment"), now),
    )
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM sign_offs WHERE id = ?", (cur.lastrowid,))
    return _signoff_row(rows[0]) if rows else None


async def list_sign_offs(db: aiosqlite.Connection, checklist_id: int) -> list[dict]:
    rows = await db.execute_fetchall(
        "SELECT * FROM sign_offs WHERE checklist_id = ? ORDER BY signed_at ASC",
        (checklist_id,),
    )
    return [_signoff_row(r) for r in rows]


async def complete_checklist(db: aiosqlite.Connection, checklist_id: int) -> dict | str | None:
    cl = await db.execute_fetchall("SELECT * FROM checklists WHERE id = ?", (checklist_id,))
    if not cl:
        return None
    if cl[0]["status"] == "completed":
        return "already_completed"
    sign_offs = await list_sign_offs(db, checklist_id)
    if not sign_offs:
        return "no_sign_offs"
    stats = await _compute_stats(db, checklist_id)
    if stats["blocking_failures"] > 0:
        return "blocking_failures"
    now = datetime.now(timezone.utc).isoformat()
    await db.execute(
        "UPDATE checklists SET status='completed', completed_at=? WHERE id=?",
        (now, checklist_id),
    )
    await db.commit()
    return await get_checklist(db, checklist_id)


async def get_readiness_report(db: aiosqlite.Connection, checklist_id: int) -> dict | None:
    checklist = await get_checklist(db, checklist_id)
    if not checklist:
        return None
    items = await list_check_items(db, checklist_id)
    blocking_failures = [{"id": i["id"], "title": i["title"], "category": i["category"]}
                         for i in items if i["status"] == "fail" and i["is_blocking"]]
    rollback_rows = await db.execute_fetchall(
        "SELECT * FROM rollback_plans WHERE checklist_id = ?", (checklist_id,))
    has_rollback = len(rollback_rows) > 0
    sign_offs = await list_sign_offs(db, checklist_id)
    score = checklist["readiness_score"]
    failed = checklist["failed_checks"]
    blocking = len(blocking_failures)

    if blocking > 0:
        recommendation = f"BLOCKED — {blocking} blocking check(s) failed. Resolve before deploying."
        status = "blocked"
    elif failed > 0:
        recommendation = f"CAUTION — {failed} non-blocking check(s) failed. Proceed with care."
        status = "caution"
    elif score >= 90 and has_rollback and sign_offs:
        recommendation = "READY — all checks passed, rollback plan in place, signed off. Safe to deploy."
        status = "ready"
    elif score >= 90 and has_rollback:
        recommendation = "MOSTLY_READY — checks passed, rollback plan ready. Awaiting sign-off."
        status = "mostly_ready"
    elif score >= 80:
        recommendation = "MOSTLY_READY — good score but add rollback plan before deploying."
        status = "mostly_ready"
    else:
        recommendation = f"NOT_READY — only {score}% checks passing. Complete remaining items."
        status = "not_ready"

    return {
        "checklist_id": checklist_id,
        "service": checklist["service"],
        "version": checklist["version"],
        "environment": checklist["environment"],
        "status": status,
        "readiness_score": score,
        "passed": checklist["passed_checks"],
        "failed": checklist["failed_checks"],
        "blocked": blocking,
        "skipped": sum(1 for i in items if i["status"] in ("skip", "na")),
        "blocking_failures": blocking_failures,
        "has_rollback_plan": has_rollback,
        "sign_off_count": len(sign_offs),
        "recommendation": recommendation,
    }


async def get_aggregate_stats(db: aiosqlite.Connection) -> dict:
    cl_rows = await db.execute_fetchall("SELECT * FROM checklists ORDER BY created_at DESC")
    total = len(cl_rows)
    if total == 0:
        return {"total_releases": 0, "by_environment": {}, "by_status": {},
                "avg_readiness_score": 0, "most_failed_checks": [], "services": []}

    by_env: Counter = Counter(r["environment"] for r in cl_rows)
    by_status: Counter = Counter(r["status"] for r in cl_rows)

    scores = []
    for r in cl_rows:
        s = await _compute_stats(db, r["id"])
        scores.append(s["score"])
    avg_score = round(sum(scores) / len(scores), 1)

    failed_rows = await db.execute_fetchall(
        "SELECT title, COUNT(*) as cnt FROM check_items WHERE status='fail' AND is_blocking=1 GROUP BY title ORDER BY cnt DESC LIMIT 5"
    )
    most_failed = [{"title": r["title"], "fail_count": r["cnt"]} for r in failed_rows]

    svc_rows = await db.execute_fetchall(
        "SELECT service, COUNT(*) as releases, MAX(created_at) as last_release FROM checklists GROUP BY service ORDER BY last_release DESC LIMIT 10"
    )
    services = [{"service": r["service"], "releases": r["releases"], "last_release": r["last_release"]} for r in svc_rows]

    return {
        "total_releases": total,
        "by_environment": dict(by_env),
        "by_status": dict(by_status),
        "avg_readiness_score": avg_score,
        "most_failed_checks": most_failed,
        "services": services,
    }


async def clone_checklist(db: aiosqlite.Connection, checklist_id: int,
                           new_version: str, new_name: str | None = None) -> dict | None:
    src = await db.execute_fetchall("SELECT * FROM checklists WHERE id=?", (checklist_id,))
    if not src:
        return None
    s = src[0]
    now = datetime.now(timezone.utc).isoformat()
    name = new_name or f"{s['name']} (clone)"
    cur = await db.execute(
        """INSERT INTO checklists (name, service, version, environment, owner_email, description, created_at)
           VALUES (?,?,?,?,?,?,?)""",
        (name, s["service"], new_version, s["environment"], s["owner_email"], s["description"], now)
    )
    new_id = cur.lastrowid
    src_items = await db.execute_fetchall(
        "SELECT * FROM check_items WHERE checklist_id=? ORDER BY id", (checklist_id,)
    )
    for item in src_items:
        await db.execute(
            """INSERT INTO check_items (checklist_id, category, title, description, is_blocking, owner_email, depends_on)
               VALUES (?,?,?,?,?,?,?)""",
            (new_id, item["category"], item["title"], item["description"],
             item["is_blocking"], item["owner_email"], None)
        )
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM checklists WHERE id=?", (new_id,))
    stats = await _compute_stats(db, new_id)
    return _checklist_row(rows[0], stats)


async def bulk_update_items(db: aiosqlite.Connection, checklist_id: int,
                             updates: list[dict]) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    updated = []
    not_found = []
    dep_blocked = []
    for u in updates:
        item_id = u["item_id"]
        rows = await db.execute_fetchall(
            "SELECT id FROM check_items WHERE id=? AND checklist_id=?", (item_id, checklist_id)
        )
        if not rows:
            not_found.append(item_id)
            continue
        if u["status"] == "pass":
            dep_err = await _check_dependency(db, item_id)
            if dep_err:
                dep_blocked.append({"item_id": item_id, "reason": dep_err})
                continue
        await db.execute(
            "UPDATE check_items SET status=?, notes=?, checked_by=?, checked_at=? WHERE id=?",
            (u["status"], u.get("notes"), u.get("checked_by"), now, item_id)
        )
        updated.append(item_id)
    await db.commit()
    return {"updated": len(updated), "not_found": not_found, "updated_ids": updated,
            "dependency_blocked": dep_blocked}
# Additional functions to append to releaseready engine.py for v0.6.0

async def get_release_timeline(db: aiosqlite.Connection, checklist_id: int) -> dict | None:
    """Get chronological timeline of all actions on a checklist."""
    cl = await get_checklist(db, checklist_id)
    if not cl:
        return None
    events = []
    # Checklist creation
    events.append({
        "type": "checklist_created",
        "timestamp": cl["created_at"],
        "actor": cl.get("owner_email") or "system",
        "detail": f"Created release checklist '{cl['name']}' for {cl['service']} {cl['version']}",
    })
    # Check item updates
    items = await db.execute_fetchall(
        "SELECT * FROM check_items WHERE checklist_id = ? AND checked_at IS NOT NULL ORDER BY checked_at ASC",
        (checklist_id,),
    )
    for item in items:
        events.append({
            "type": f"check_{item['status']}",
            "timestamp": item["checked_at"],
            "actor": item["checked_by"] or "unknown",
            "detail": f"[{item['category']}] {item['title']}: {item['status']}" + (f" — {item['notes']}" if item["notes"] else ""),
        })
    # Rollback plan
    rp_rows = await db.execute_fetchall(
        "SELECT * FROM rollback_plans WHERE checklist_id = ?", (checklist_id,))
    for rp in rp_rows:
        events.append({
            "type": "rollback_plan_added",
            "timestamp": rp["created_at"],
            "actor": "system",
            "detail": f"Rollback plan added ({rp['estimated_minutes']} min estimated)",
        })
    # Sign-offs
    sign_offs = await db.execute_fetchall(
        "SELECT * FROM sign_offs WHERE checklist_id = ? ORDER BY signed_at ASC",
        (checklist_id,),
    )
    for so in sign_offs:
        events.append({
            "type": "sign_off",
            "timestamp": so["signed_at"],
            "actor": so["name"],
            "detail": f"Signed off as {so['role']}" + (f": {so['comment']}" if so["comment"] else ""),
        })
    # Completion
    if cl.get("completed_at"):
        events.append({
            "type": "checklist_completed",
            "timestamp": cl["completed_at"],
            "actor": "system",
            "detail": f"Release marked as completed (score: {cl['readiness_score']}%)",
        })
    events.sort(key=lambda e: e["timestamp"])
    return {
        "checklist_id": checklist_id,
        "service": cl["service"],
        "version": cl["version"],
        "total_events": len(events),
        "events": events,
    }


async def get_service_releases(db: aiosqlite.Connection, service: str, limit: int = 20) -> dict:
    """Get release history for a specific service."""
    rows = await db.execute_fetchall(
        "SELECT * FROM checklists WHERE service = ? ORDER BY created_at DESC LIMIT ?",
        (service, limit),
    )
    releases = []
    for r in rows:
        stats = await _compute_stats(db, r["id"])
        releases.append({
            "id": r["id"],
            "version": r["version"],
            "environment": r["environment"],
            "status": r["status"],
            "readiness_score": stats["score"],
            "total_checks": stats["total"],
            "passed_checks": stats["passed"],
            "blocking_failures": stats["blocking_failures"],
            "created_at": r["created_at"],
            "completed_at": r["completed_at"],
        })
    completed = [r for r in releases if r["status"] == "completed"]
    avg_score = round(sum(r["readiness_score"] for r in completed) / len(completed), 1) if completed else 0
    return {
        "service": service,
        "total_releases": len(releases),
        "completed": len(completed),
        "in_progress": sum(1 for r in releases if r["status"] == "in_progress"),
        "avg_readiness_score": avg_score,
        "releases": releases,
    }


async def get_risk_assessment(db: aiosqlite.Connection, checklist_id: int) -> dict | None:
    """Automated risk scoring based on check failures, environment, history."""
    cl = await get_checklist(db, checklist_id)
    if not cl:
        return None
    stats = await _compute_stats(db, checklist_id)
    items = await list_check_items(db, checklist_id)

    risk_factors = []
    risk_score = 0

    # Factor 1: Blocking failures
    if stats["blocking_failures"] > 0:
        risk_score += 30
        risk_factors.append({
            "factor": "blocking_failures",
            "severity": "critical",
            "detail": f"{stats['blocking_failures']} blocking check(s) failed",
            "impact": 30,
        })

    # Factor 2: Low readiness score
    if stats["score"] < 80:
        impact = min(25, (80 - stats["score"]))
        risk_score += impact
        risk_factors.append({
            "factor": "low_readiness",
            "severity": "high",
            "detail": f"Readiness score {stats['score']}% (below 80% threshold)",
            "impact": impact,
        })

    # Factor 3: Production environment
    if cl["environment"] == "production":
        risk_score += 10
        risk_factors.append({
            "factor": "production_environment",
            "severity": "medium",
            "detail": "Deploying to production increases risk",
            "impact": 10,
        })

    # Factor 4: No rollback plan
    rp = await db.execute_fetchall(
        "SELECT id FROM rollback_plans WHERE checklist_id = ?", (checklist_id,))
    if not rp:
        risk_score += 15
        risk_factors.append({
            "factor": "no_rollback_plan",
            "severity": "high",
            "detail": "No rollback plan documented",
            "impact": 15,
        })

    # Factor 5: No sign-offs
    so = await list_sign_offs(db, checklist_id)
    if not so:
        risk_score += 10
        risk_factors.append({
            "factor": "no_sign_offs",
            "severity": "medium",
            "detail": "No approvals/sign-offs recorded",
            "impact": 10,
        })

    # Factor 6: Pending checks
    if stats["pending"] > 0:
        impact = min(15, stats["pending"] * 3)
        risk_score += impact
        risk_factors.append({
            "factor": "pending_checks",
            "severity": "medium",
            "detail": f"{stats['pending']} check(s) still pending",
            "impact": impact,
        })

    # Factor 7: Security failures
    security_fails = [i for i in items if i["category"] == "security" and i["status"] == "fail"]
    if security_fails:
        risk_score += 20
        risk_factors.append({
            "factor": "security_failures",
            "severity": "critical",
            "detail": f"{len(security_fails)} security check(s) failed",
            "impact": 20,
        })

    risk_score = min(risk_score, 100)
    if risk_score >= 60:
        level = "critical"
    elif risk_score >= 40:
        level = "high"
    elif risk_score >= 20:
        level = "medium"
    else:
        level = "low"

    return {
        "checklist_id": checklist_id,
        "service": cl["service"],
        "version": cl["version"],
        "environment": cl["environment"],
        "risk_score": risk_score,
        "risk_level": level,
        "readiness_score": stats["score"],
        "total_factors": len(risk_factors),
        "factors": risk_factors,
    }

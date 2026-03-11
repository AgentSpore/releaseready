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


async def create_checklist(db: aiosqlite.Connection, data: dict) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    cur = await db.execute(
        """INSERT INTO checklists (name, service, version, environment, owner_email, description, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (data["name"], data["service"], data["version"], data.get("environment", "production"),
         data.get("owner_email"), data.get("description"), now)
    )
    checklist_id = cur.lastrowid
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


async def update_check_item(db: aiosqlite.Connection, item_id: int,
                             status: str, notes: str | None, checked_by: str | None) -> dict | None:
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
        """INSERT INTO check_items (checklist_id, category, title, description, is_blocking, owner_email)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (data["checklist_id"], data["category"], data["title"], data.get("description"),
         int(data.get("is_blocking", True)), data.get("owner_email"))
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
            """INSERT INTO check_items (checklist_id, category, title, description, is_blocking, owner_email)
               VALUES (?,?,?,?,?,?)""",
            (new_id, item["category"], item["title"], item["description"],
             item["is_blocking"], item["owner_email"])
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
    for u in updates:
        item_id = u["item_id"]
        rows = await db.execute_fetchall(
            "SELECT id FROM check_items WHERE id=? AND checklist_id=?", (item_id, checklist_id)
        )
        if not rows:
            not_found.append(item_id)
            continue
        await db.execute(
            "UPDATE check_items SET status=?, notes=?, checked_by=?, checked_at=? WHERE id=?",
            (u["status"], u.get("notes"), u.get("checked_by"), now, item_id)
        )
        updated.append(item_id)
    await db.commit()
    return {"updated": len(updated), "not_found": not_found, "updated_ids": updated}

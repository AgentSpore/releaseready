from __future__ import annotations
import csv
import io
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

CREATE TABLE IF NOT EXISTS checklist_comments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    checklist_id INTEGER NOT NULL,
    author TEXT NOT NULL,
    body TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (checklist_id) REFERENCES checklists(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS release_windows (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    environment TEXT NOT NULL,
    day_of_week TEXT NOT NULL DEFAULT '[]',
    start_hour INTEGER NOT NULL,
    end_hour INTEGER NOT NULL,
    description TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS check_assignments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    item_id INTEGER NOT NULL,
    checklist_id INTEGER NOT NULL,
    assignee_email TEXT NOT NULL,
    due_at TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (item_id) REFERENCES check_items(id) ON DELETE CASCADE,
    FOREIGN KEY (checklist_id) REFERENCES checklists(id) ON DELETE CASCADE,
    UNIQUE(item_id)
);

CREATE INDEX IF NOT EXISTS idx_comments_checklist ON checklist_comments(checklist_id);
CREATE INDEX IF NOT EXISTS idx_windows_env ON release_windows(environment);
CREATE INDEX IF NOT EXISTS idx_assignments_checklist ON check_assignments(checklist_id);
CREATE INDEX IF NOT EXISTS idx_assignments_assignee ON check_assignments(assignee_email);

CREATE TABLE IF NOT EXISTS checklist_labels (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    checklist_id INTEGER NOT NULL,
    label TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (checklist_id) REFERENCES checklists(id) ON DELETE CASCADE,
    UNIQUE(checklist_id, label)
);

CREATE INDEX IF NOT EXISTS idx_labels_checklist ON checklist_labels(checklist_id);
CREATE INDEX IF NOT EXISTS idx_labels_label ON checklist_labels(label);

CREATE TABLE IF NOT EXISTS checklist_watchers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    checklist_id INTEGER NOT NULL,
    email TEXT NOT NULL,
    name TEXT,
    added_at TEXT NOT NULL,
    FOREIGN KEY (checklist_id) REFERENCES checklists(id) ON DELETE CASCADE,
    UNIQUE(checklist_id, email)
);

CREATE INDEX IF NOT EXISTS idx_watchers_checklist ON checklist_watchers(checklist_id);

-- v1.0.0: Approval Gates
CREATE TABLE IF NOT EXISTS approval_gates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    checklist_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    required_roles TEXT NOT NULL DEFAULT '[]',
    min_approvals INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT NOT NULL,
    FOREIGN KEY (checklist_id) REFERENCES checklists(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS gate_approvals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    gate_id INTEGER NOT NULL,
    approver_email TEXT NOT NULL,
    approver_role TEXT NOT NULL,
    decision TEXT NOT NULL DEFAULT 'approved',
    comment TEXT,
    decided_at TEXT NOT NULL,
    FOREIGN KEY (gate_id) REFERENCES approval_gates(id) ON DELETE CASCADE,
    UNIQUE(gate_id, approver_email)
);

CREATE INDEX IF NOT EXISTS idx_gates_checklist ON approval_gates(checklist_id);
CREATE INDEX IF NOT EXISTS idx_gate_approvals_gate ON gate_approvals(gate_id);

-- v1.0.0: Automation Rules
CREATE TABLE IF NOT EXISTS automation_rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    checklist_id INTEGER NOT NULL,
    item_id INTEGER NOT NULL,
    rule_type TEXT NOT NULL,
    condition TEXT NOT NULL DEFAULT '{}',
    is_enabled INTEGER NOT NULL DEFAULT 1,
    times_fired INTEGER NOT NULL DEFAULT 0,
    last_fired_at TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (checklist_id) REFERENCES checklists(id) ON DELETE CASCADE,
    FOREIGN KEY (item_id) REFERENCES check_items(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_automation_rules_checklist ON automation_rules(checklist_id);
CREATE INDEX IF NOT EXISTS idx_automation_rules_item ON automation_rules(item_id);

-- v1.0.0: Release Calendar
CREATE TABLE IF NOT EXISTS release_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    checklist_id INTEGER,
    title TEXT NOT NULL,
    scheduled_start TEXT NOT NULL,
    scheduled_end TEXT NOT NULL,
    environment TEXT NOT NULL,
    owner_email TEXT,
    status TEXT NOT NULL DEFAULT 'planned',
    notes TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (checklist_id) REFERENCES checklists(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_release_events_env ON release_events(environment);
CREATE INDEX IF NOT EXISTS idx_release_events_start ON release_events(scheduled_start);
CREATE INDEX IF NOT EXISTS idx_release_events_status ON release_events(status);
"""

VALID_LABELS = frozenset({
    "critical", "hotfix", "minor", "major",
    "security", "emergency", "regression", "planned",
})

VALID_RULE_TYPES = frozenset({
    "auto_pass_after_date",
    "auto_pass_when_dependency_met",
    "auto_fail_after_deadline",
    "auto_pass_on_label",
})

VALID_EVENT_STATUSES = frozenset({
    "planned", "confirmed", "in_progress", "completed", "cancelled",
})

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


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


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


async def _checklist_labels(db: aiosqlite.Connection, checklist_id: int) -> list[str]:
    rows = await db.execute_fetchall(
        "SELECT label FROM checklist_labels WHERE checklist_id = ? ORDER BY created_at ASC",
        (checklist_id,),
    )
    return [r["label"] for r in rows]


def _checklist_row(r: aiosqlite.Row, stats: dict, comment_count: int = 0,
                   labels: list[str] | None = None) -> dict:
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
        "comment_count": comment_count,
        "labels": labels if labels is not None else [],
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


async def _comment_count(db: aiosqlite.Connection, checklist_id: int) -> int:
    rows = await db.execute_fetchall(
        "SELECT COUNT(*) as cnt FROM checklist_comments WHERE checklist_id = ?", (checklist_id,))
    return rows[0]["cnt"] if rows else 0


# ── Templates ─────────────────────────────────────────────────────────────

async def create_template(db: aiosqlite.Connection, data: dict) -> dict:
    now = _now()
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
    now = _now()
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
    cc = await _comment_count(db, checklist_id)
    lbs = await _checklist_labels(db, checklist_id)
    return _checklist_row(rows[0], stats, cc, lbs)


async def list_checklists(db: aiosqlite.Connection, environment: str | None = None,
                           status: str | None = None,
                           label: str | None = None) -> list[dict]:
    q, params = "SELECT * FROM checklists", []
    conds = []
    if environment:
        conds.append("environment = ?"); params.append(environment)
    if status:
        conds.append("status = ?"); params.append(status)
    if label:
        conds.append(
            "id IN (SELECT checklist_id FROM checklist_labels WHERE label = ?)"
        )
        params.append(label)
    if conds:
        q += " WHERE " + " AND ".join(conds)
    q += " ORDER BY created_at DESC"
    rows = await db.execute_fetchall(q, params)
    result = []
    for r in rows:
        stats = await _compute_stats(db, r["id"])
        cc = await _comment_count(db, r["id"])
        lbs = await _checklist_labels(db, r["id"])
        result.append(_checklist_row(r, stats, cc, lbs))
    return result


async def get_checklist(db: aiosqlite.Connection, checklist_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM checklists WHERE id = ?", (checklist_id,))
    if not rows:
        return None
    stats = await _compute_stats(db, checklist_id)
    cc = await _comment_count(db, checklist_id)
    lbs = await _checklist_labels(db, checklist_id)
    return _checklist_row(rows[0], stats, cc, lbs)


async def delete_checklist(db: aiosqlite.Connection, checklist_id: int) -> bool:
    await db.execute("DELETE FROM check_assignments WHERE checklist_id = ?", (checklist_id,))
    await db.execute("DELETE FROM checklist_comments WHERE checklist_id = ?", (checklist_id,))
    await db.execute("DELETE FROM sign_offs WHERE checklist_id = ?", (checklist_id,))
    await db.execute("DELETE FROM check_items WHERE checklist_id = ?", (checklist_id,))
    await db.execute("DELETE FROM rollback_plans WHERE checklist_id = ?", (checklist_id,))
    await db.execute("DELETE FROM checklist_labels WHERE checklist_id = ?", (checklist_id,))
    await db.execute("DELETE FROM checklist_watchers WHERE checklist_id = ?", (checklist_id,))
    await db.execute("DELETE FROM automation_rules WHERE checklist_id = ?", (checklist_id,))
    await db.execute("DELETE FROM gate_approvals WHERE gate_id IN (SELECT id FROM approval_gates WHERE checklist_id = ?)", (checklist_id,))
    await db.execute("DELETE FROM approval_gates WHERE checklist_id = ?", (checklist_id,))
    await db.execute("UPDATE release_events SET checklist_id = NULL WHERE checklist_id = ?", (checklist_id,))
    await db.execute("DELETE FROM checklists WHERE id = ?", (checklist_id,))
    await db.commit()
    return True


async def list_check_items(db: aiosqlite.Connection, checklist_id: int) -> list[dict]:
    rows = await db.execute_fetchall(
        "SELECT * FROM check_items WHERE checklist_id = ? ORDER BY category, id", (checklist_id,))
    return [_item_row(r) for r in rows]


async def _check_dependency(db: aiosqlite.Connection, item_id: int) -> str | None:
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
    now = _now()
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
    now = _now()
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
    now = _now()
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
    # v1.0.0: all approval gates must be approved before completion
    all_gates_ok = await check_all_gates_approved(db, checklist_id)
    if not all_gates_ok:
        return "gates_not_approved"
    now = _now()
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
                "avg_readiness_score": 0, "most_failed_checks": [], "services": [],
                "total_comments": 0, "total_assignments": 0, "release_windows": 0}

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

    total_comments = (await db.execute_fetchall("SELECT COUNT(*) as c FROM checklist_comments"))[0]["c"]
    total_assignments = (await db.execute_fetchall("SELECT COUNT(*) as c FROM check_assignments"))[0]["c"]
    windows_count = (await db.execute_fetchall("SELECT COUNT(*) as c FROM release_windows"))[0]["c"]

    return {
        "total_releases": total,
        "by_environment": dict(by_env),
        "by_status": dict(by_status),
        "avg_readiness_score": avg_score,
        "most_failed_checks": most_failed,
        "services": services,
        "total_comments": total_comments,
        "total_assignments": total_assignments,
        "release_windows": windows_count,
    }


async def clone_checklist(db: aiosqlite.Connection, checklist_id: int,
                           new_version: str, new_name: str | None = None) -> dict | None:
    src = await db.execute_fetchall("SELECT * FROM checklists WHERE id=?", (checklist_id,))
    if not src:
        return None
    s = src[0]
    now = _now()
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
    return await get_checklist(db, new_id)


async def bulk_update_items(db: aiosqlite.Connection, checklist_id: int,
                             updates: list[dict]) -> dict:
    now = _now()
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


# ── Release Windows ──────────────────────────────────────────────────────

async def create_release_window(db: aiosqlite.Connection, data: dict) -> dict:
    now = _now()
    days = json.dumps(sorted(set(data["day_of_week"])))
    cur = await db.execute(
        "INSERT INTO release_windows (environment, day_of_week, start_hour, end_hour, description, created_at) VALUES (?,?,?,?,?,?)",
        (data["environment"], days, data["start_hour"], data["end_hour"], data.get("description"), now),
    )
    await db.commit()
    return _window_row(await db.execute_fetchall("SELECT * FROM release_windows WHERE id=?", (cur.lastrowid,)))


async def list_release_windows(db: aiosqlite.Connection, environment: str | None = None) -> list[dict]:
    if environment:
        rows = await db.execute_fetchall(
            "SELECT * FROM release_windows WHERE environment=? ORDER BY created_at DESC", (environment,))
    else:
        rows = await db.execute_fetchall("SELECT * FROM release_windows ORDER BY created_at DESC")
    return [_window_row_single(r) for r in rows]


async def delete_release_window(db: aiosqlite.Connection, window_id: int) -> bool:
    cur = await db.execute("DELETE FROM release_windows WHERE id=?", (window_id,))
    await db.commit()
    return cur.rowcount > 0


async def check_in_release_window(db: aiosqlite.Connection, environment: str) -> dict:
    now = datetime.now(timezone.utc)
    current_day = now.weekday()
    current_hour = now.hour
    windows = await db.execute_fetchall(
        "SELECT * FROM release_windows WHERE environment=?", (environment,))
    for w in windows:
        days = json.loads(w["day_of_week"])
        start_h = w["start_hour"]
        end_h = w["end_hour"]
        if current_day in days:
            if start_h <= end_h:
                in_window = start_h <= current_hour < end_h
            else:
                in_window = current_hour >= start_h or current_hour < end_h
            if in_window:
                return {
                    "environment": environment,
                    "in_window": True,
                    "current_day": current_day,
                    "current_hour": current_hour,
                    "matching_window": _window_row_single(w),
                    "message": "Current time is within a release window",
                }
    return {
        "environment": environment,
        "in_window": len(windows) == 0,
        "current_day": current_day,
        "current_hour": current_hour,
        "matching_window": None,
        "message": "No release window configured" if not windows else "Current time is outside all release windows",
    }


def _window_row(rows) -> dict:
    return _window_row_single(rows[0]) if rows else {}


def _window_row_single(r) -> dict:
    return {
        "id": r["id"],
        "environment": r["environment"],
        "day_of_week": json.loads(r["day_of_week"]),
        "start_hour": r["start_hour"],
        "end_hour": r["end_hour"],
        "description": r["description"],
        "created_at": r["created_at"],
    }


# ── Check Item Assignments ───────────────────────────────────────────────

async def assign_check_item(db: aiosqlite.Connection, item_id: int, data: dict) -> dict | str | None:
    item_rows = await db.execute_fetchall("SELECT * FROM check_items WHERE id=?", (item_id,))
    if not item_rows:
        return None
    item = item_rows[0]
    # Remove existing assignment if any
    await db.execute("DELETE FROM check_assignments WHERE item_id=?", (item_id,))
    now = _now()
    cur = await db.execute(
        "INSERT INTO check_assignments (item_id, checklist_id, assignee_email, due_at, created_at) VALUES (?,?,?,?,?)",
        (item_id, item["checklist_id"], data["assignee_email"], data.get("due_at"), now),
    )
    await db.commit()
    return _assignment_row(
        await db.execute_fetchall(
            "SELECT a.*, ci.title, ci.category, ci.status as item_status FROM check_assignments a "
            "JOIN check_items ci ON a.item_id = ci.id WHERE a.id=?", (cur.lastrowid,)),
    )


async def list_assignments(db: aiosqlite.Connection, checklist_id: int,
                            assignee: str | None = None) -> list[dict]:
    q = ("SELECT a.*, ci.title, ci.category, ci.status as item_status FROM check_assignments a "
         "JOIN check_items ci ON a.item_id = ci.id WHERE a.checklist_id = ?")
    params: list = [checklist_id]
    if assignee:
        q += " AND a.assignee_email = ?"
        params.append(assignee)
    q += " ORDER BY a.due_at ASC NULLS LAST, a.created_at ASC"
    rows = await db.execute_fetchall(q, params)
    return [_assignment_row_single(r) for r in rows]


async def get_overdue_assignments(db: aiosqlite.Connection, checklist_id: int) -> list[dict]:
    now = _now()
    rows = await db.execute_fetchall(
        "SELECT a.*, ci.title, ci.category, ci.status as item_status FROM check_assignments a "
        "JOIN check_items ci ON a.item_id = ci.id "
        "WHERE a.checklist_id = ? AND a.due_at IS NOT NULL AND a.due_at < ? AND ci.status = 'pending'",
        (checklist_id, now),
    )
    return [_assignment_row_single(r) for r in rows]


def _assignment_row(rows) -> dict | None:
    return _assignment_row_single(rows[0]) if rows else None


def _assignment_row_single(r) -> dict:
    now_str = _now()
    is_overdue = (r["due_at"] is not None and r["due_at"] < now_str and r["item_status"] == "pending")
    return {
        "id": r["id"],
        "item_id": r["item_id"],
        "checklist_id": r["checklist_id"],
        "title": r["title"],
        "category": r["category"],
        "assignee_email": r["assignee_email"],
        "due_at": r["due_at"],
        "status": r["item_status"],
        "is_overdue": is_overdue,
        "created_at": r["created_at"],
    }


# ── Release Comparison ───────────────────────────────────────────────────

async def compare_releases(db: aiosqlite.Connection, id_a: int, id_b: int) -> dict | None:
    cl_a = await get_checklist(db, id_a)
    cl_b = await get_checklist(db, id_b)
    if not cl_a or not cl_b:
        return None
    items_a = await list_check_items(db, id_a)
    items_b = await list_check_items(db, id_b)

    # Category breakdown
    cats = sorted(set(i["category"] for i in items_a + items_b))
    category_breakdown = []
    for cat in cats:
        a_items = [i for i in items_a if i["category"] == cat]
        b_items = [i for i in items_b if i["category"] == cat]
        category_breakdown.append({
            "category": cat,
            "checklist_a_passed": sum(1 for i in a_items if i["status"] == "pass"),
            "checklist_a_total": len(a_items),
            "checklist_b_passed": sum(1 for i in b_items if i["status"] == "pass"),
            "checklist_b_total": len(b_items),
        })

    # Common failures
    failed_a = {i["title"] for i in items_a if i["status"] == "fail"}
    failed_b = {i["title"] for i in items_b if i["status"] == "fail"}
    common_failures = sorted(failed_a & failed_b)
    unique_to_a = sorted(failed_a - failed_b)
    unique_to_b = sorted(failed_b - failed_a)

    # Status diff
    status_diff = {}
    for key in ("status", "environment", "readiness_score", "total_checks",
                "passed_checks", "failed_checks"):
        if cl_a.get(key) != cl_b.get(key):
            status_diff[key] = {"a": cl_a.get(key), "b": cl_b.get(key)}

    return {
        "checklist_a": cl_a,
        "checklist_b": cl_b,
        "score_diff": cl_a["readiness_score"] - cl_b["readiness_score"],
        "status_diff": status_diff,
        "category_breakdown": category_breakdown,
        "common_failures": common_failures,
        "unique_to_a": unique_to_a,
        "unique_to_b": unique_to_b,
    }


# ── Timeline ─────────────────────────────────────────────────────────────

async def get_release_timeline(db: aiosqlite.Connection, checklist_id: int) -> dict | None:
    cl = await get_checklist(db, checklist_id)
    if not cl:
        return None
    events = []
    events.append({
        "type": "checklist_created",
        "timestamp": cl["created_at"],
        "actor": cl.get("owner_email") or "system",
        "detail": f"Created release checklist '{cl['name']}' for {cl['service']} {cl['version']}",
    })
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
    rp_rows = await db.execute_fetchall(
        "SELECT * FROM rollback_plans WHERE checklist_id = ?", (checklist_id,))
    for rp in rp_rows:
        events.append({
            "type": "rollback_plan_added",
            "timestamp": rp["created_at"],
            "actor": "system",
            "detail": f"Rollback plan added ({rp['estimated_minutes']} min estimated)",
        })
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
    # Include comments in timeline
    comments = await db.execute_fetchall(
        "SELECT * FROM checklist_comments WHERE checklist_id = ? ORDER BY created_at ASC",
        (checklist_id,),
    )
    for c in comments:
        events.append({
            "type": "comment",
            "timestamp": c["created_at"],
            "actor": c["author"],
            "detail": c["body"][:200],
        })
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
    cl = await get_checklist(db, checklist_id)
    if not cl:
        return None
    stats = await _compute_stats(db, checklist_id)
    items = await list_check_items(db, checklist_id)

    risk_factors = []
    risk_score = 0

    if stats["blocking_failures"] > 0:
        risk_score += 30
        risk_factors.append({
            "factor": "blocking_failures", "severity": "critical",
            "detail": f"{stats['blocking_failures']} blocking check(s) failed", "impact": 30,
        })
    if stats["score"] < 80:
        impact = min(25, (80 - stats["score"]))
        risk_score += impact
        risk_factors.append({
            "factor": "low_readiness", "severity": "high",
            "detail": f"Readiness score {stats['score']}% (below 80% threshold)", "impact": impact,
        })
    if cl["environment"] == "production":
        risk_score += 10
        risk_factors.append({
            "factor": "production_environment", "severity": "medium",
            "detail": "Deploying to production increases risk", "impact": 10,
        })
    rp = await db.execute_fetchall(
        "SELECT id FROM rollback_plans WHERE checklist_id = ?", (checklist_id,))
    if not rp:
        risk_score += 15
        risk_factors.append({
            "factor": "no_rollback_plan", "severity": "high",
            "detail": "No rollback plan documented", "impact": 15,
        })
    so = await list_sign_offs(db, checklist_id)
    if not so:
        risk_score += 10
        risk_factors.append({
            "factor": "no_sign_offs", "severity": "medium",
            "detail": "No approvals/sign-offs recorded", "impact": 10,
        })
    if stats["pending"] > 0:
        impact = min(15, stats["pending"] * 3)
        risk_score += impact
        risk_factors.append({
            "factor": "pending_checks", "severity": "medium",
            "detail": f"{stats['pending']} check(s) still pending", "impact": impact,
        })
    security_fails = [i for i in items if i["category"] == "security" and i["status"] == "fail"]
    if security_fails:
        risk_score += 20
        risk_factors.append({
            "factor": "security_failures", "severity": "critical",
            "detail": f"{len(security_fails)} security check(s) failed", "impact": 20,
        })
    # Check release window
    window_check = await check_in_release_window(db, cl["environment"])
    if not window_check["in_window"] and window_check["matching_window"] is None and window_check["message"] != "No release window configured":
        risk_score += 10
        risk_factors.append({
            "factor": "outside_release_window", "severity": "medium",
            "detail": "Current time is outside the configured release window", "impact": 10,
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
        "service": cl["service"], "version": cl["version"],
        "environment": cl["environment"],
        "risk_score": risk_score, "risk_level": level,
        "readiness_score": stats["score"],
        "total_factors": len(risk_factors), "factors": risk_factors,
    }


# ── Checklist Comments ───────────────────────────────────────────────────

async def add_comment(db: aiosqlite.Connection, checklist_id: int, data: dict) -> dict | None:
    cl = await db.execute_fetchall("SELECT id FROM checklists WHERE id = ?", (checklist_id,))
    if not cl:
        return None
    now = _now()
    cur = await db.execute(
        "INSERT INTO checklist_comments (checklist_id, author, body, created_at) VALUES (?,?,?,?)",
        (checklist_id, data["author"], data["body"], now),
    )
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM checklist_comments WHERE id = ?", (cur.lastrowid,))
    r = rows[0]
    return {"id": r["id"], "checklist_id": r["checklist_id"],
            "author": r["author"], "body": r["body"], "created_at": r["created_at"]}


async def list_comments(db: aiosqlite.Connection, checklist_id: int) -> list[dict]:
    rows = await db.execute_fetchall(
        "SELECT * FROM checklist_comments WHERE checklist_id = ? ORDER BY created_at ASC",
        (checklist_id,),
    )
    return [{"id": r["id"], "checklist_id": r["checklist_id"],
             "author": r["author"], "body": r["body"], "created_at": r["created_at"]}
            for r in rows]


async def delete_comment(db: aiosqlite.Connection, comment_id: int) -> bool:
    cur = await db.execute("DELETE FROM checklist_comments WHERE id = ?", (comment_id,))
    await db.commit()
    return cur.rowcount > 0


# ── Environment Promotion ────────────────────────────────────────────────

async def promote_checklist(db: aiosqlite.Connection, checklist_id: int,
                             target_env: str, new_version: str | None = None,
                             owner_email: str | None = None) -> dict | str | None:
    src = await db.execute_fetchall("SELECT * FROM checklists WHERE id = ?", (checklist_id,))
    if not src:
        return None
    s = src[0]
    if s["environment"] == target_env:
        return "same_environment"
    now = _now()
    version = new_version or s["version"]
    owner = owner_email or s["owner_email"]
    name = f"{s['service']} {version} ({target_env})"
    cur = await db.execute(
        """INSERT INTO checklists (name, service, version, environment, owner_email, description, created_at)
           VALUES (?,?,?,?,?,?,?)""",
        (name, s["service"], version, target_env, owner,
         f"Promoted from {s['environment']} checklist #{checklist_id}", now)
    )
    new_id = cur.lastrowid
    # Copy check items with their current status (pass items stay pass, fail reset to pending)
    src_items = await db.execute_fetchall(
        "SELECT * FROM check_items WHERE checklist_id = ? ORDER BY id", (checklist_id,))
    for item in src_items:
        carry_status = item["status"] if item["status"] == "pass" else "pending"
        carry_at = item["checked_at"] if item["status"] == "pass" else None
        carry_by = item["checked_by"] if item["status"] == "pass" else None
        await db.execute(
            """INSERT INTO check_items (checklist_id, category, title, description, is_blocking,
               owner_email, notes, status, checked_at, checked_by)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (new_id, item["category"], item["title"], item["description"],
             item["is_blocking"], item["owner_email"], item["notes"],
             carry_status, carry_at, carry_by)
        )
    await db.commit()
    return await get_checklist(db, new_id)


# ── Checklist CSV Export ─────────────────────────────────────────────────

async def export_checklist_csv(db: aiosqlite.Connection, checklist_id: int) -> str | None:
    cl = await get_checklist(db, checklist_id)
    if not cl:
        return None
    items = await list_check_items(db, checklist_id)
    sign_offs = await list_sign_offs(db, checklist_id)
    assignments = await list_assignments(db, checklist_id)

    buf = io.StringIO()
    # Header section
    buf.write(f"# Release Checklist: {cl['name']}\n")
    buf.write(f"# Service: {cl['service']} | Version: {cl['version']} | Env: {cl['environment']}\n")
    buf.write(f"# Status: {cl['status']} | Score: {cl['readiness_score']}%\n")
    buf.write(f"# Created: {cl['created_at']}\n\n")

    fieldnames = ["id", "category", "title", "status", "is_blocking",
                  "owner_email", "notes", "checked_by", "checked_at"]
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    for item in items:
        writer.writerow({k: item.get(k, "") for k in fieldnames})

    if assignments:
        buf.write("\n# Assignments\n")
        a_fields = ["item_id", "title", "assignee_email", "due_at", "status", "is_overdue"]
        a_writer = csv.DictWriter(buf, fieldnames=a_fields)
        a_writer.writeheader()
        for a in assignments:
            a_writer.writerow({k: a.get(k, "") for k in a_fields})

    if sign_offs:
        buf.write("\n# Sign-Offs\n")
        so_fields = ["name", "role", "comment", "signed_at"]
        so_writer = csv.DictWriter(buf, fieldnames=so_fields)
        so_writer.writeheader()
        for so in sign_offs:
            so_writer.writerow({k: so.get(k, "") for k in so_fields})

    return buf.getvalue()


# ── Release Labels (v0.9.0) ───────────────────────────────────────────────

async def add_checklist_label(db: aiosqlite.Connection, checklist_id: int,
                               label: str) -> dict | str | None:
    """Add a label to a checklist. Returns dict on success, None if checklist not found,
    'invalid_label' if label not in VALID_LABELS, 'duplicate' if already present."""
    if label not in VALID_LABELS:
        return "invalid_label"
    cl = await db.execute_fetchall("SELECT id FROM checklists WHERE id = ?", (checklist_id,))
    if not cl:
        return None
    now = _now()
    try:
        cur = await db.execute(
            "INSERT INTO checklist_labels (checklist_id, label, created_at) VALUES (?,?,?)",
            (checklist_id, label, now),
        )
        await db.commit()
    except Exception:
        # UNIQUE constraint violation — label already present
        return "duplicate"
    rows = await db.execute_fetchall(
        "SELECT * FROM checklist_labels WHERE id = ?", (cur.lastrowid,))
    r = rows[0]
    return {"id": r["id"], "checklist_id": r["checklist_id"],
            "label": r["label"], "created_at": r["created_at"]}


async def remove_checklist_label(db: aiosqlite.Connection, checklist_id: int,
                                  label: str) -> bool:
    cur = await db.execute(
        "DELETE FROM checklist_labels WHERE checklist_id = ? AND label = ?",
        (checklist_id, label),
    )
    await db.commit()
    return cur.rowcount > 0


async def list_checklist_labels(db: aiosqlite.Connection, checklist_id: int) -> list[dict]:
    rows = await db.execute_fetchall(
        "SELECT * FROM checklist_labels WHERE checklist_id = ? ORDER BY created_at ASC",
        (checklist_id,),
    )
    return [{"id": r["id"], "checklist_id": r["checklist_id"],
             "label": r["label"], "created_at": r["created_at"]} for r in rows]


async def list_checklists_by_label(db: aiosqlite.Connection, label: str,
                                    limit: int = 50) -> list[dict]:
    rows = await db.execute_fetchall(
        """SELECT c.* FROM checklists c
           JOIN checklist_labels lbl ON c.id = lbl.checklist_id
           WHERE lbl.label = ?
           ORDER BY c.created_at DESC
           LIMIT ?""",
        (label, limit),
    )
    result = []
    for r in rows:
        stats = await _compute_stats(db, r["id"])
        cc = await _comment_count(db, r["id"])
        lbs = await _checklist_labels(db, r["id"])
        result.append(_checklist_row(r, stats, cc, lbs))
    return result


# ── Checklist Watchers (v0.9.0) ───────────────────────────────────────────

async def add_watcher(db: aiosqlite.Connection, checklist_id: int,
                      email: str, name: str | None = None) -> dict | str | None:
    """Subscribe an email to a checklist. Returns dict, None if checklist not found,
    or 'duplicate' if already watching."""
    cl = await db.execute_fetchall("SELECT id FROM checklists WHERE id = ?", (checklist_id,))
    if not cl:
        return None
    now = _now()
    try:
        cur = await db.execute(
            "INSERT INTO checklist_watchers (checklist_id, email, name, added_at) VALUES (?,?,?,?)",
            (checklist_id, email, name, now),
        )
        await db.commit()
    except Exception:
        return "duplicate"
    rows = await db.execute_fetchall(
        "SELECT * FROM checklist_watchers WHERE id = ?", (cur.lastrowid,))
    return _watcher_row(rows[0])


async def list_watchers(db: aiosqlite.Connection, checklist_id: int) -> list[dict]:
    rows = await db.execute_fetchall(
        "SELECT * FROM checklist_watchers WHERE checklist_id = ? ORDER BY added_at ASC",
        (checklist_id,),
    )
    return [_watcher_row(r) for r in rows]


async def remove_watcher(db: aiosqlite.Connection, watcher_id: int) -> bool:
    cur = await db.execute("DELETE FROM checklist_watchers WHERE id = ?", (watcher_id,))
    await db.commit()
    return cur.rowcount > 0


async def get_watcher_checklists(db: aiosqlite.Connection, email: str) -> list[dict]:
    """Return all checklists watched by the given email."""
    rows = await db.execute_fetchall(
        """SELECT c.* FROM checklists c
           JOIN checklist_watchers w ON c.id = w.checklist_id
           WHERE w.email = ?
           ORDER BY c.created_at DESC""",
        (email,),
    )
    result = []
    for r in rows:
        stats = await _compute_stats(db, r["id"])
        cc = await _comment_count(db, r["id"])
        lbs = await _checklist_labels(db, r["id"])
        result.append(_checklist_row(r, stats, cc, lbs))
    return result


def _watcher_row(r) -> dict:
    return {
        "id": r["id"],
        "checklist_id": r["checklist_id"],
        "email": r["email"],
        "name": r["name"],
        "added_at": r["added_at"],
    }


# ── Release Velocity Dashboard (v0.9.0) ───────────────────────────────────

async def get_release_velocity(db: aiosqlite.Connection, days: int = 30) -> dict:
    """Compute release velocity and bottleneck analytics for the given rolling period."""
    cutoff = datetime.now(timezone.utc)
    # Build ISO cutoff string for the period start
    from datetime import timedelta
    period_start = (cutoff - timedelta(days=days)).isoformat()

    completed_rows = await db.execute_fetchall(
        "SELECT * FROM checklists WHERE status = 'completed' AND completed_at >= ? ORDER BY completed_at ASC",
        (period_start,),
    )
    total_releases = len(completed_rows)

    # Compute per-release hours (created_at -> completed_at)
    def _hours(row) -> float | None:
        try:
            c = datetime.fromisoformat(row["created_at"].replace("Z", "+00:00"))
            d = datetime.fromisoformat(row["completed_at"].replace("Z", "+00:00"))
            return (d - c).total_seconds() / 3600.0
        except Exception:
            return None

    all_hours = [h for row in completed_rows if (h := _hours(row)) is not None]
    avg_completion_hours: float | None = (
        round(sum(all_hours) / len(all_hours), 2) if all_hours else None
    )
    releases_per_week = round(total_releases / (days / 7), 2) if days > 0 else 0.0

    # By service
    svc_map: dict[str, list] = {}
    for row in completed_rows:
        svc_map.setdefault(row["service"], []).append(row)

    by_service = []
    for svc, rows in sorted(svc_map.items()):
        h_list = [h for r in rows if (h := _hours(r)) is not None]
        scores = []
        for r in rows:
            st = await _compute_stats(db, r["id"])
            scores.append(st["score"])
        by_service.append({
            "service": svc,
            "completed": len(rows),
            "avg_hours": round(sum(h_list) / len(h_list), 2) if h_list else None,
            "avg_score": round(sum(scores) / len(scores), 1) if scores else None,
        })

    # By environment
    env_map: dict[str, list] = {}
    for row in completed_rows:
        env_map.setdefault(row["environment"], []).append(row)

    by_environment = []
    for env, rows in sorted(env_map.items()):
        h_list = [h for r in rows if (h := _hours(r)) is not None]
        by_environment.append({
            "environment": env,
            "completed": len(rows),
            "avg_hours": round(sum(h_list) / len(h_list), 2) if h_list else None,
        })

    # Bottleneck categories — check items that failed within the period checklists
    checklist_ids = [r["id"] for r in completed_rows]
    bottleneck_categories: list[dict] = []
    if checklist_ids:
        placeholders = ",".join("?" * len(checklist_ids))
        cat_rows = await db.execute_fetchall(
            f"""SELECT category,
                       COUNT(*) as total_checks,
                       SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END) as failed
                FROM check_items
                WHERE checklist_id IN ({placeholders})
                GROUP BY category
                ORDER BY failed DESC""",
            checklist_ids,
        )
        for r in cat_rows:
            total_c = r["total_checks"] or 0
            failed_c = r["failed"] or 0
            fail_rate = round(failed_c / total_c * 100, 1) if total_c > 0 else 0.0
            bottleneck_categories.append({
                "category": r["category"],
                "total_checks": total_c,
                "failed": failed_c,
                "fail_rate_pct": fail_rate,
            })
        bottleneck_categories.sort(key=lambda x: x["fail_rate_pct"], reverse=True)

    # Fastest and slowest releases
    fastest_release: dict | None = None
    slowest_release: dict | None = None
    if all_hours:
        paired = [
            (h, row) for row, h in
            [(r, _hours(r)) for r in completed_rows] if h is not None
        ]
        if paired:
            fastest_row = min(paired, key=lambda x: x[0])
            slowest_row = max(paired, key=lambda x: x[0])
            fastest_release = {
                "service": fastest_row[1]["service"],
                "version": fastest_row[1]["version"],
                "hours": round(fastest_row[0], 2),
            }
            slowest_release = {
                "service": slowest_row[1]["service"],
                "version": slowest_row[1]["version"],
                "hours": round(slowest_row[0], 2),
            }

    return {
        "period_days": days,
        "total_releases": total_releases,
        "avg_completion_hours": avg_completion_hours,
        "releases_per_week": releases_per_week,
        "by_service": by_service,
        "by_environment": by_environment,
        "bottleneck_categories": bottleneck_categories,
        "fastest_release": fastest_release,
        "slowest_release": slowest_release,
    }


# ══════════════════════════════════════════════════════════════════════════
# Feature 1: Release Approvals (v1.0.0)
# ══════════════════════════════════════════════════════════════════════════

def _gate_decision_row(r) -> dict:
    return {
        "id": r["id"],
        "gate_id": r["gate_id"],
        "approver_email": r["approver_email"],
        "approver_role": r["approver_role"],
        "decision": r["decision"],
        "comment": r["comment"],
        "decided_at": r["decided_at"],
    }


async def _gate_row(db: aiosqlite.Connection, r) -> dict:
    """Build a full ApprovalGateResponse dict from a gate row."""
    decision_rows = await db.execute_fetchall(
        "SELECT * FROM gate_approvals WHERE gate_id = ? ORDER BY decided_at ASC",
        (r["id"],),
    )
    approvals = [_gate_decision_row(d) for d in decision_rows]
    approvals_count = sum(1 for d in decision_rows if d["decision"] == "approved")
    rejections_count = sum(1 for d in decision_rows if d["decision"] == "rejected")
    return {
        "id": r["id"],
        "checklist_id": r["checklist_id"],
        "name": r["name"],
        "required_roles": json.loads(r["required_roles"]),
        "min_approvals": r["min_approvals"],
        "status": r["status"],
        "approvals_count": approvals_count,
        "rejections_count": rejections_count,
        "approvals": approvals,
        "created_at": r["created_at"],
    }


async def create_approval_gate(db: aiosqlite.Connection, checklist_id: int,
                                data: dict) -> dict | None:
    """Create an approval gate for a checklist. Returns None if checklist not found."""
    cl = await db.execute_fetchall("SELECT id FROM checklists WHERE id = ?", (checklist_id,))
    if not cl:
        return None
    now = _now()
    roles_json = json.dumps(data["required_roles"])
    min_approvals = data.get("min_approvals", 1)
    cur = await db.execute(
        "INSERT INTO approval_gates (checklist_id, name, required_roles, min_approvals, status, created_at) "
        "VALUES (?,?,?,?,?,?)",
        (checklist_id, data["name"], roles_json, min_approvals, "pending", now),
    )
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM approval_gates WHERE id = ?", (cur.lastrowid,))
    return await _gate_row(db, rows[0])


async def list_approval_gates(db: aiosqlite.Connection, checklist_id: int) -> list[dict]:
    rows = await db.execute_fetchall(
        "SELECT * FROM approval_gates WHERE checklist_id = ? ORDER BY created_at ASC",
        (checklist_id,),
    )
    return [await _gate_row(db, r) for r in rows]


async def get_approval_gate(db: aiosqlite.Connection, gate_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM approval_gates WHERE id = ?", (gate_id,))
    if not rows:
        return None
    return await _gate_row(db, rows[0])


async def approve_or_reject_gate(db: aiosqlite.Connection, gate_id: int,
                                  data: dict) -> dict | str | None:
    """Submit an approval/rejection decision for a gate.
    Returns:
      - None if gate not found
      - 'invalid_role' if approver_role not in required_roles
      - 'duplicate' if approver already decided
      - 'gate_already_resolved' if gate is already approved/rejected
      - GateDecisionResponse dict on success
    """
    gate_rows = await db.execute_fetchall("SELECT * FROM approval_gates WHERE id = ?", (gate_id,))
    if not gate_rows:
        return None
    gate = gate_rows[0]
    if gate["status"] in ("approved", "rejected"):
        return "gate_already_resolved"
    required_roles = json.loads(gate["required_roles"])
    if data["approver_role"] not in required_roles:
        return "invalid_role"
    # Check for duplicate
    existing = await db.execute_fetchall(
        "SELECT id FROM gate_approvals WHERE gate_id = ? AND approver_email = ?",
        (gate_id, data["approver_email"]),
    )
    if existing:
        return "duplicate"
    now = _now()
    cur = await db.execute(
        "INSERT INTO gate_approvals (gate_id, approver_email, approver_role, decision, comment, decided_at) "
        "VALUES (?,?,?,?,?,?)",
        (gate_id, data["approver_email"], data["approver_role"],
         data["decision"], data.get("comment"), now),
    )
    await db.commit()

    # Auto-update gate status
    if data["decision"] == "rejected":
        await db.execute(
            "UPDATE approval_gates SET status = 'rejected' WHERE id = ?", (gate_id,))
        await db.commit()
    else:
        # Count approvals
        approval_count_rows = await db.execute_fetchall(
            "SELECT COUNT(*) as cnt FROM gate_approvals WHERE gate_id = ? AND decision = 'approved'",
            (gate_id,),
        )
        approval_count = approval_count_rows[0]["cnt"]
        if approval_count >= gate["min_approvals"]:
            await db.execute(
                "UPDATE approval_gates SET status = 'approved' WHERE id = ?", (gate_id,))
            await db.commit()

    rows = await db.execute_fetchall("SELECT * FROM gate_approvals WHERE id = ?", (cur.lastrowid,))
    return _gate_decision_row(rows[0])


async def check_all_gates_approved(db: aiosqlite.Connection, checklist_id: int) -> bool:
    """Return True if all gates for the checklist are approved (or if there are no gates)."""
    gates = await db.execute_fetchall(
        "SELECT status FROM approval_gates WHERE checklist_id = ?", (checklist_id,))
    if not gates:
        return True
    return all(g["status"] == "approved" for g in gates)


async def get_gates_status(db: aiosqlite.Connection, checklist_id: int) -> dict:
    """Return summary of all gates' statuses for a checklist."""
    gates = await db.execute_fetchall(
        "SELECT status FROM approval_gates WHERE checklist_id = ?", (checklist_id,))
    total = len(gates)
    approved = sum(1 for g in gates if g["status"] == "approved")
    pending = sum(1 for g in gates if g["status"] == "pending")
    rejected = sum(1 for g in gates if g["status"] == "rejected")
    return {
        "checklist_id": checklist_id,
        "total_gates": total,
        "approved_gates": approved,
        "pending_gates": pending,
        "rejected_gates": rejected,
        "all_approved": total > 0 and approved == total,
    }


# ══════════════════════════════════════════════════════════════════════════
# Feature 2: Checklist Automation Rules (v1.0.0)
# ══════════════════════════════════════════════════════════════════════════

def _automation_rule_row(r) -> dict:
    return {
        "id": r["id"],
        "checklist_id": r["checklist_id"],
        "item_id": r["item_id"],
        "rule_type": r["rule_type"],
        "condition": json.loads(r["condition"]),
        "is_enabled": bool(r["is_enabled"]),
        "times_fired": r["times_fired"],
        "last_fired_at": r["last_fired_at"],
        "created_at": r["created_at"],
    }


async def create_automation_rule(db: aiosqlite.Connection, checklist_id: int,
                                  data: dict) -> dict | str | None:
    """Create an automation rule. Returns None if checklist not found,
    'item_not_in_checklist' if item doesn't belong to checklist,
    'invalid_rule_type' if rule_type is not valid."""
    cl = await db.execute_fetchall("SELECT id FROM checklists WHERE id = ?", (checklist_id,))
    if not cl:
        return None
    if data["rule_type"] not in VALID_RULE_TYPES:
        return "invalid_rule_type"
    item_rows = await db.execute_fetchall(
        "SELECT id FROM check_items WHERE id = ? AND checklist_id = ?",
        (data["item_id"], checklist_id),
    )
    if not item_rows:
        return "item_not_in_checklist"
    now = _now()
    condition_json = json.dumps(data["condition"])
    is_enabled = int(data.get("is_enabled", True))
    cur = await db.execute(
        "INSERT INTO automation_rules (checklist_id, item_id, rule_type, condition, is_enabled, created_at) "
        "VALUES (?,?,?,?,?,?)",
        (checklist_id, data["item_id"], data["rule_type"], condition_json, is_enabled, now),
    )
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM automation_rules WHERE id = ?", (cur.lastrowid,))
    return _automation_rule_row(rows[0])


async def list_automation_rules(db: aiosqlite.Connection, checklist_id: int) -> list[dict]:
    rows = await db.execute_fetchall(
        "SELECT * FROM automation_rules WHERE checklist_id = ? ORDER BY created_at ASC",
        (checklist_id,),
    )
    return [_automation_rule_row(r) for r in rows]


async def update_automation_rule(db: aiosqlite.Connection, rule_id: int,
                                  data: dict) -> dict | None:
    """Update condition and/or is_enabled on a rule. Returns None if not found."""
    rows = await db.execute_fetchall("SELECT * FROM automation_rules WHERE id = ?", (rule_id,))
    if not rows:
        return None
    sets = []
    params = []
    if data.get("condition") is not None:
        sets.append("condition = ?")
        params.append(json.dumps(data["condition"]))
    if data.get("is_enabled") is not None:
        sets.append("is_enabled = ?")
        params.append(int(data["is_enabled"]))
    if not sets:
        return _automation_rule_row(rows[0])
    params.append(rule_id)
    await db.execute(f"UPDATE automation_rules SET {', '.join(sets)} WHERE id = ?", params)
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM automation_rules WHERE id = ?", (rule_id,))
    return _automation_rule_row(rows[0])


async def delete_automation_rule(db: aiosqlite.Connection, rule_id: int) -> bool:
    cur = await db.execute("DELETE FROM automation_rules WHERE id = ?", (rule_id,))
    await db.commit()
    return cur.rowcount > 0


async def evaluate_automation_rules(db: aiosqlite.Connection, checklist_id: int) -> dict:
    """Evaluate all enabled rules for a checklist, apply actions, return fired count."""
    rules = await db.execute_fetchall(
        "SELECT * FROM automation_rules WHERE checklist_id = ? AND is_enabled = 1",
        (checklist_id,),
    )
    now = _now()
    fired_count = 0
    fired_rules: list[dict] = []

    # Pre-fetch labels for label-based rules
    labels = await _checklist_labels(db, checklist_id)

    for rule in rules:
        condition = json.loads(rule["condition"])
        rule_type = rule["rule_type"]
        item_id = rule["item_id"]
        should_fire = False
        new_status = None

        # Fetch current item status
        item_rows = await db.execute_fetchall(
            "SELECT status FROM check_items WHERE id = ?", (item_id,))
        if not item_rows:
            continue
        current_status = item_rows[0]["status"]
        # Skip if already resolved (pass or fail)
        if current_status in ("pass", "fail"):
            continue

        if rule_type == "auto_pass_after_date":
            target_date = condition.get("date", "")
            if target_date and now >= target_date:
                should_fire = True
                new_status = "pass"

        elif rule_type == "auto_fail_after_deadline":
            deadline = condition.get("deadline", "")
            if deadline and now >= deadline:
                should_fire = True
                new_status = "fail"

        elif rule_type == "auto_pass_when_dependency_met":
            dep_item_ids = condition.get("item_ids", [])
            if dep_item_ids:
                placeholders = ",".join("?" * len(dep_item_ids))
                dep_rows = await db.execute_fetchall(
                    f"SELECT id, status FROM check_items WHERE id IN ({placeholders})",
                    dep_item_ids,
                )
                if len(dep_rows) == len(dep_item_ids) and all(
                    d["status"] == "pass" for d in dep_rows
                ):
                    should_fire = True
                    new_status = "pass"

        elif rule_type == "auto_pass_on_label":
            target_label = condition.get("label", "")
            if target_label and target_label in labels:
                should_fire = True
                new_status = "pass"

        if should_fire and new_status:
            await db.execute(
                "UPDATE check_items SET status = ?, checked_by = ?, checked_at = ?, notes = ? WHERE id = ?",
                (new_status, "automation", now,
                 f"Auto-{new_status} by rule '{rule_type}'", item_id),
            )
            await db.execute(
                "UPDATE automation_rules SET times_fired = times_fired + 1, last_fired_at = ? WHERE id = ?",
                (now, rule["id"]),
            )
            fired_count += 1
            fired_rules.append({
                "rule_id": rule["id"],
                "item_id": item_id,
                "rule_type": rule_type,
                "new_status": new_status,
            })

    if fired_count > 0:
        await db.commit()

    return {
        "checklist_id": checklist_id,
        "rules_evaluated": len(rules),
        "rules_fired": fired_count,
        "fired_details": fired_rules,
    }


# ══════════════════════════════════════════════════════════════════════════
# Feature 3: Release Calendar (v1.0.0)
# ══════════════════════════════════════════════════════════════════════════

async def _release_event_row(db: aiosqlite.Connection, r, check_conflicts: bool = True) -> dict:
    """Build a ReleaseEventResponse dict from a release_events row."""
    checklist_name = None
    if r["checklist_id"]:
        cl_rows = await db.execute_fetchall(
            "SELECT name FROM checklists WHERE id = ?", (r["checklist_id"],))
        if cl_rows:
            checklist_name = cl_rows[0]["name"]

    has_conflicts = False
    if check_conflicts:
        # Check if this event overlaps with any other event in the same environment
        conflicts = await db.execute_fetchall(
            """SELECT id FROM release_events
               WHERE environment = ? AND id != ?
               AND scheduled_start < ? AND scheduled_end > ?""",
            (r["environment"], r["id"], r["scheduled_end"], r["scheduled_start"]),
        )
        has_conflicts = len(conflicts) > 0

    return {
        "id": r["id"],
        "checklist_id": r["checklist_id"],
        "checklist_name": checklist_name,
        "title": r["title"],
        "scheduled_start": r["scheduled_start"],
        "scheduled_end": r["scheduled_end"],
        "environment": r["environment"],
        "owner_email": r["owner_email"],
        "status": r["status"],
        "notes": r["notes"],
        "has_conflicts": has_conflicts,
        "created_at": r["created_at"],
        "updated_at": r["updated_at"],
    }


async def create_release_event(db: aiosqlite.Connection, data: dict) -> dict | str:
    """Create a release calendar event. Returns 'invalid_checklist' if checklist_id specified but not found."""
    if data.get("checklist_id"):
        cl = await db.execute_fetchall(
            "SELECT id FROM checklists WHERE id = ?", (data["checklist_id"],))
        if not cl:
            return "invalid_checklist"
    now = _now()
    cur = await db.execute(
        """INSERT INTO release_events
           (checklist_id, title, scheduled_start, scheduled_end, environment, owner_email, status, notes, created_at, updated_at)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (data.get("checklist_id"), data["title"], data["scheduled_start"],
         data["scheduled_end"], data["environment"],
         data.get("owner_email"), "planned", data.get("notes"), now, now),
    )
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM release_events WHERE id = ?", (cur.lastrowid,))
    return await _release_event_row(db, rows[0])


async def list_release_events(db: aiosqlite.Connection,
                               environment: str | None = None,
                               status: str | None = None,
                               from_date: str | None = None,
                               to_date: str | None = None) -> list[dict]:
    q = "SELECT * FROM release_events"
    conds = []
    params: list = []
    if environment:
        conds.append("environment = ?")
        params.append(environment)
    if status:
        conds.append("status = ?")
        params.append(status)
    if from_date:
        conds.append("scheduled_end >= ?")
        params.append(from_date)
    if to_date:
        conds.append("scheduled_start <= ?")
        params.append(to_date)
    if conds:
        q += " WHERE " + " AND ".join(conds)
    q += " ORDER BY scheduled_start ASC"
    rows = await db.execute_fetchall(q, params)
    return [await _release_event_row(db, r) for r in rows]


async def get_release_event(db: aiosqlite.Connection, event_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM release_events WHERE id = ?", (event_id,))
    if not rows:
        return None
    return await _release_event_row(db, rows[0])


async def update_release_event(db: aiosqlite.Connection, event_id: int,
                                data: dict) -> dict | str | None:
    """Update a release event. Returns None if not found, 'invalid_status' if bad status."""
    rows = await db.execute_fetchall("SELECT * FROM release_events WHERE id = ?", (event_id,))
    if not rows:
        return None
    sets = []
    params: list = []
    if data.get("title") is not None:
        sets.append("title = ?")
        params.append(data["title"])
    if data.get("scheduled_start") is not None:
        sets.append("scheduled_start = ?")
        params.append(data["scheduled_start"])
    if data.get("scheduled_end") is not None:
        sets.append("scheduled_end = ?")
        params.append(data["scheduled_end"])
    if data.get("status") is not None:
        if data["status"] not in VALID_EVENT_STATUSES:
            return "invalid_status"
        sets.append("status = ?")
        params.append(data["status"])
    if data.get("notes") is not None:
        sets.append("notes = ?")
        params.append(data["notes"])
    if not sets:
        return await _release_event_row(db, rows[0])
    now = _now()
    sets.append("updated_at = ?")
    params.append(now)
    params.append(event_id)
    await db.execute(f"UPDATE release_events SET {', '.join(sets)} WHERE id = ?", params)
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM release_events WHERE id = ?", (event_id,))
    return await _release_event_row(db, rows[0])


async def delete_release_event(db: aiosqlite.Connection, event_id: int) -> bool:
    cur = await db.execute("DELETE FROM release_events WHERE id = ?", (event_id,))
    await db.commit()
    return cur.rowcount > 0


async def detect_conflicts(db: aiosqlite.Connection,
                            environment: str | None = None,
                            from_date: str | None = None,
                            to_date: str | None = None) -> list[dict]:
    """Find overlapping release events in the same environment."""
    q = "SELECT * FROM release_events"
    conds = []
    params: list = []
    if environment:
        conds.append("environment = ?")
        params.append(environment)
    if from_date:
        conds.append("scheduled_end >= ?")
        params.append(from_date)
    if to_date:
        conds.append("scheduled_start <= ?")
        params.append(to_date)
    # Exclude cancelled events from conflict detection
    conds.append("status != 'cancelled'")
    if conds:
        q += " WHERE " + " AND ".join(conds)
    q += " ORDER BY scheduled_start ASC"
    rows = await db.execute_fetchall(q, params)

    # Group by environment
    env_events: dict[str, list] = {}
    for r in rows:
        env_events.setdefault(r["environment"], []).append(r)

    conflicts: list[dict] = []
    for env, events in env_events.items():
        for i in range(len(events)):
            for j in range(i + 1, len(events)):
                a = events[i]
                b = events[j]
                # Check overlap: a.start < b.end AND a.end > b.start
                if a["scheduled_start"] < b["scheduled_end"] and a["scheduled_end"] > b["scheduled_start"]:
                    overlap_start = max(a["scheduled_start"], b["scheduled_start"])
                    overlap_end = min(a["scheduled_end"], b["scheduled_end"])
                    conflicts.append({
                        "event_a_id": a["id"],
                        "event_b_id": b["id"],
                        "event_a_title": a["title"],
                        "event_b_title": b["title"],
                        "overlap_start": overlap_start,
                        "overlap_end": overlap_end,
                        "environment": env,
                    })
    return conflicts


async def get_calendar_view(db: aiosqlite.Connection,
                             from_date: str, to_date: str,
                             environment: str | None = None) -> dict:
    """Full calendar view with events, conflicts, and stats."""
    events = await list_release_events(db, environment=environment,
                                        from_date=from_date, to_date=to_date)
    conflicts = await detect_conflicts(db, environment=environment,
                                        from_date=from_date, to_date=to_date)
    now = _now()
    upcoming_count = sum(1 for e in events if e["scheduled_start"] > now and e["status"] != "cancelled")
    return {
        "events": events,
        "conflicts": conflicts,
        "total_events": len(events),
        "upcoming_count": upcoming_count,
    }

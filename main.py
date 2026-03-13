from __future__ import annotations
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from models import (
    ChecklistCreate, ChecklistResponse,
    CheckItemCreate, CheckItemResponse, CheckItemUpdate,
    RollbackPlanCreate, RollbackPlanResponse,
    ReadinessReport, BulkItemUpdate,
    SignOffCreate, SignOffResponse,
    TemplateCreate, TemplateResponse, TemplateItemCreate,
    ReleaseTimeline, ServiceReleases, RiskAssessment,
    CommentCreate, CommentResponse, PromoteRequest,
    ReleaseWindowCreate, ReleaseWindowResponse, WindowCheckResponse,
    AssignmentCreate, AssignmentResponse,
    ReleaseComparison,
    LabelAddRequest, LabelResponse,
    WatcherAddRequest, WatcherResponse,
    ReleaseVelocity,
    # v1.0.0: Release Approvals
    ApprovalGateCreate, ApprovalGateResponse,
    GateDecision, GateDecisionResponse, AllGatesStatus,
    # v1.0.0: Automation Rules
    AutomationRuleCreate, AutomationRuleUpdate, AutomationRuleResponse,
    # v1.0.0: Release Calendar
    ReleaseEventCreate, ReleaseEventUpdate, ReleaseEventResponse,
    CalendarConflict, CalendarView,
)
from engine import (
    init_db, create_checklist, list_checklists, get_checklist, delete_checklist,
    list_check_items, update_check_item, add_check_item,
    create_rollback_plan, get_readiness_report, get_aggregate_stats,
    clone_checklist, bulk_update_items,
    add_sign_off, list_sign_offs, complete_checklist,
    create_template, list_templates, get_template, get_template_items,
    add_template_item, delete_template,
    get_release_timeline, get_service_releases, get_risk_assessment,
    add_comment, list_comments, delete_comment,
    promote_checklist, export_checklist_csv,
    create_release_window, list_release_windows, delete_release_window,
    check_in_release_window,
    assign_check_item, list_assignments, get_overdue_assignments,
    compare_releases,
    add_checklist_label, remove_checklist_label, list_checklist_labels,
    list_checklists_by_label,
    add_watcher, list_watchers, remove_watcher, get_watcher_checklists,
    get_release_velocity,
    VALID_LABELS,
    # v1.0.0: Release Approvals
    create_approval_gate, list_approval_gates, get_approval_gate,
    approve_or_reject_gate, check_all_gates_approved, get_gates_status,
    # v1.0.0: Automation Rules
    create_automation_rule, list_automation_rules, update_automation_rule,
    delete_automation_rule, evaluate_automation_rules,
    VALID_RULE_TYPES,
    # v1.0.0: Release Calendar
    create_release_event, list_release_events, get_release_event,
    update_release_event, delete_release_event,
    detect_conflicts, get_calendar_view,
    VALID_EVENT_STATUSES,
)

DB_PATH = os.getenv("DB_PATH", "releaseready.db")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = await init_db(DB_PATH)
    yield
    await app.state.db.close()


app = FastAPI(
    title="ReleaseReady",
    description=(
        "Production release readiness checklist with templates, check dependencies, "
        "sign-off workflow, rollback playbooks, deployment gates, "
        "release timeline, service release cadence, automated risk assessment, "
        "checklist comments, environment promotion, CSV export, "
        "release windows (allowed deploy times), check item assignments with due dates, "
        "side-by-side release comparison, "
        "release labels for categorization, checklist watchers, "
        "release velocity dashboard with bottleneck analytics, "
        "multi-level approval gates with role-based quorum, "
        "checklist automation rules for automatic status updates, "
        "and release calendar with conflict detection."
    ),
    version="1.0.0",
    lifespan=lifespan,
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/health")
async def health():
    return {"status": "ok", "version": "1.0.0"}


# ── Release Windows ──────────────────────────────────────────────────────

@app.post("/release-windows", response_model=ReleaseWindowResponse, status_code=201)
async def create_window(body: ReleaseWindowCreate):
    """Define an allowed release window for an environment."""
    return await create_release_window(app.state.db, body.model_dump())


@app.get("/release-windows", response_model=list[ReleaseWindowResponse])
async def get_windows(
    environment: str | None = Query(None, description="Filter by environment"),
):
    return await list_release_windows(app.state.db, environment)


@app.delete("/release-windows/{window_id}", status_code=204)
async def remove_window(window_id: int):
    if not await delete_release_window(app.state.db, window_id):
        raise HTTPException(404, "Release window not found")


@app.get("/release-windows/check", response_model=WindowCheckResponse)
async def check_window(
    environment: str = Query(..., description="Environment to check"),
):
    """Check if current UTC time falls within a release window for the given environment."""
    return await check_in_release_window(app.state.db, environment)


# ── Templates ─────────────────────────────────────────────────────────────

@app.post("/templates", response_model=TemplateResponse, status_code=201)
async def create_tmpl(body: TemplateCreate):
    if body.from_checklist_id:
        c = await get_checklist(app.state.db, body.from_checklist_id)
        if not c:
            raise HTTPException(404, "Source checklist not found")
    return await create_template(app.state.db, body.model_dump())


@app.get("/templates", response_model=list[TemplateResponse])
async def list_tmpls():
    return await list_templates(app.state.db)


@app.get("/templates/{template_id}", response_model=TemplateResponse)
async def get_tmpl(template_id: int):
    t = await get_template(app.state.db, template_id)
    if not t:
        raise HTTPException(404, "Template not found")
    return t


@app.get("/templates/{template_id}/items")
async def get_tmpl_items(template_id: int):
    t = await get_template(app.state.db, template_id)
    if not t:
        raise HTTPException(404, "Template not found")
    return await get_template_items(app.state.db, template_id)


@app.post("/templates/{template_id}/items", status_code=201)
async def add_tmpl_item(template_id: int, body: TemplateItemCreate):
    result = await add_template_item(app.state.db, template_id, body.model_dump())
    if not result:
        raise HTTPException(404, "Template not found")
    return result


@app.delete("/templates/{template_id}", status_code=204)
async def remove_tmpl(template_id: int):
    if not await delete_template(app.state.db, template_id):
        raise HTTPException(404, "Template not found")


# ── Checklists ────────────────────────────────────────────────────────────

@app.post("/checklists", response_model=ChecklistResponse, status_code=201)
async def create(body: ChecklistCreate):
    if body.template_id:
        t = await get_template(app.state.db, body.template_id)
        if not t:
            raise HTTPException(404, "Template not found")
    return await create_checklist(app.state.db, body.model_dump())


@app.get("/checklists/stats")
async def aggregate_stats():
    return await get_aggregate_stats(app.state.db)


@app.get("/checklists/compare", response_model=ReleaseComparison)
async def compare_checklists(
    a: int = Query(..., description="First checklist ID"),
    b: int = Query(..., description="Second checklist ID"),
):
    """Compare two release checklists side by side."""
    result = await compare_releases(app.state.db, a, b)
    if not result:
        raise HTTPException(404, "One or both checklists not found")
    return result


@app.get("/checklists", response_model=list[ChecklistResponse])
async def list_all(
    environment: str | None = Query(None, description="staging | production | canary"),
    status: str | None = Query(None, description="in_progress | ready | blocked | completed"),
    label: str | None = Query(None, description="Filter by label: critical | hotfix | minor | major | security | emergency | regression | planned"),
):
    return await list_checklists(app.state.db, environment, status, label)


@app.get("/checklists/{checklist_id}", response_model=ChecklistResponse)
async def get_one(checklist_id: int):
    c = await get_checklist(app.state.db, checklist_id)
    if not c:
        raise HTTPException(404, "Checklist not found")
    return c


@app.delete("/checklists/{checklist_id}", status_code=204)
async def remove_checklist(checklist_id: int):
    c = await get_checklist(app.state.db, checklist_id)
    if not c:
        raise HTTPException(404, "Checklist not found")
    await delete_checklist(app.state.db, checklist_id)


@app.post("/checklists/{checklist_id}/clone", response_model=ChecklistResponse, status_code=201)
async def clone(
    checklist_id: int,
    new_version: str = Query(..., description="Version string for the cloned checklist"),
    new_name: str | None = Query(None, description="Optional name override for the clone"),
):
    result = await clone_checklist(app.state.db, checklist_id, new_version, new_name)
    if not result:
        raise HTTPException(404, "Checklist not found")
    return result


# ── Environment Promotion ────────────────────────────────────────────────

@app.post("/checklists/{checklist_id}/promote", response_model=ChecklistResponse, status_code=201)
async def promote(checklist_id: int, body: PromoteRequest):
    """Promote a checklist to a different environment (e.g. staging -> production).
    Passed checks carry over; failed/pending items reset to pending."""
    result = await promote_checklist(
        app.state.db, checklist_id,
        body.target_environment, body.new_version, body.owner_email,
    )
    if result is None:
        raise HTTPException(404, "Checklist not found")
    if result == "same_environment":
        raise HTTPException(422, "Target environment is the same as source")
    return result


# ── Checklist Export ─────────────────────────────────────────────────────

@app.get("/checklists/{checklist_id}/export/csv")
async def export_csv(checklist_id: int):
    """Export checklist items, assignments, and sign-offs as CSV for audit/compliance."""
    data = await export_checklist_csv(app.state.db, checklist_id)
    if data is None:
        raise HTTPException(404, "Checklist not found")
    return StreamingResponse(
        iter([data]), media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=checklist_{checklist_id}.csv"},
    )


# ── Timeline ─────────────────────────────────────────────────────────────

@app.get("/checklists/{checklist_id}/timeline", response_model=ReleaseTimeline)
async def timeline(checklist_id: int):
    """Chronological timeline of all actions on this release checklist."""
    result = await get_release_timeline(app.state.db, checklist_id)
    if not result:
        raise HTTPException(404, "Checklist not found")
    return result


# ── Risk Assessment ──────────────────────────────────────────────────────

@app.get("/checklists/{checklist_id}/risk", response_model=RiskAssessment)
async def risk_assessment(checklist_id: int):
    """Automated risk scoring based on check failures, environment, release window, and configuration."""
    result = await get_risk_assessment(app.state.db, checklist_id)
    if not result:
        raise HTTPException(404, "Checklist not found")
    return result


# ── Checklist Comments ───────────────────────────────────────────────────

@app.post("/checklists/{checklist_id}/comments", response_model=CommentResponse, status_code=201)
async def create_comment(checklist_id: int, body: CommentCreate):
    """Add a discussion comment to a checklist."""
    result = await add_comment(app.state.db, checklist_id, body.model_dump())
    if result is None:
        raise HTTPException(404, "Checklist not found")
    return result


@app.get("/checklists/{checklist_id}/comments", response_model=list[CommentResponse])
async def get_comments(checklist_id: int):
    c = await get_checklist(app.state.db, checklist_id)
    if not c:
        raise HTTPException(404, "Checklist not found")
    return await list_comments(app.state.db, checklist_id)


@app.delete("/comments/{comment_id}", status_code=204)
async def remove_comment(comment_id: int):
    if not await delete_comment(app.state.db, comment_id):
        raise HTTPException(404, "Comment not found")


# ── Sign-Off ──────────────────────────────────────────────────────────────

@app.post("/checklists/{checklist_id}/sign-off", response_model=SignOffResponse, status_code=201)
async def sign_off(checklist_id: int, body: SignOffCreate):
    result = await add_sign_off(app.state.db, checklist_id, body.model_dump())
    if result is None:
        raise HTTPException(404, "Checklist not found")
    if result == "already_completed":
        raise HTTPException(409, "Checklist already completed")
    if result == "blocking_failures":
        raise HTTPException(422, "Cannot sign off: blocking check failures exist")
    return result


@app.get("/checklists/{checklist_id}/sign-offs", response_model=list[SignOffResponse])
async def get_sign_offs(checklist_id: int):
    c = await get_checklist(app.state.db, checklist_id)
    if not c:
        raise HTTPException(404, "Checklist not found")
    return await list_sign_offs(app.state.db, checklist_id)


@app.post("/checklists/{checklist_id}/complete", response_model=ChecklistResponse)
async def complete(checklist_id: int):
    result = await complete_checklist(app.state.db, checklist_id)
    if result is None:
        raise HTTPException(404, "Checklist not found")
    if result == "already_completed":
        raise HTTPException(409, "Checklist already completed")
    if result == "no_sign_offs":
        raise HTTPException(422, "Cannot complete: at least one sign-off is required")
    if result == "blocking_failures":
        raise HTTPException(422, "Cannot complete: blocking check failures exist")
    if result == "gates_not_approved":
        raise HTTPException(422, "Cannot complete: not all approval gates are approved")
    return result


# ── Check Items ───────────────────────────────────────────────────────────

@app.get("/checklists/{checklist_id}/items", response_model=list[CheckItemResponse])
async def get_items(checklist_id: int):
    c = await get_checklist(app.state.db, checklist_id)
    if not c:
        raise HTTPException(404, "Checklist not found")
    return await list_check_items(app.state.db, checklist_id)


@app.post("/checklists/{checklist_id}/items", response_model=CheckItemResponse, status_code=201)
async def add_item(checklist_id: int, body: CheckItemCreate):
    c = await get_checklist(app.state.db, checklist_id)
    if not c:
        raise HTTPException(404, "Checklist not found")
    body.checklist_id = checklist_id
    return await add_check_item(app.state.db, body.model_dump())


@app.patch("/checklists/{checklist_id}/items/bulk")
async def bulk_update(checklist_id: int, body: BulkItemUpdate):
    c = await get_checklist(app.state.db, checklist_id)
    if not c:
        raise HTTPException(404, "Checklist not found")
    return await bulk_update_items(
        app.state.db, checklist_id,
        [u.model_dump() for u in body.updates]
    )


@app.patch("/items/{item_id}", response_model=CheckItemResponse)
async def update_item(item_id: int, body: CheckItemUpdate):
    result = await update_check_item(
        app.state.db, item_id, body.status, body.notes, body.checked_by)
    if result is None:
        raise HTTPException(404, "Check item not found")
    if isinstance(result, str):
        raise HTTPException(422, result)
    return result


# ── Check Item Assignments ───────────────────────────────────────────────

@app.post("/items/{item_id}/assign", response_model=AssignmentResponse, status_code=201)
async def assign_item(item_id: int, body: AssignmentCreate):
    """Assign a check item to a team member with optional due date."""
    result = await assign_check_item(app.state.db, item_id, body.model_dump())
    if result is None:
        raise HTTPException(404, "Check item not found")
    return result


@app.get("/checklists/{checklist_id}/assignments", response_model=list[AssignmentResponse])
async def checklist_assignments(
    checklist_id: int,
    assignee: str | None = Query(None, description="Filter by assignee email"),
):
    """List all assignments for a checklist, optionally filtered by assignee."""
    c = await get_checklist(app.state.db, checklist_id)
    if not c:
        raise HTTPException(404, "Checklist not found")
    return await list_assignments(app.state.db, checklist_id, assignee)


@app.get("/checklists/{checklist_id}/overdue", response_model=list[AssignmentResponse])
async def overdue_assignments(checklist_id: int):
    """List overdue assignments (past due_at and still pending)."""
    c = await get_checklist(app.state.db, checklist_id)
    if not c:
        raise HTTPException(404, "Checklist not found")
    return await get_overdue_assignments(app.state.db, checklist_id)


# ── Rollback Plans ────────────────────────────────────────────────────────

@app.post("/checklists/{checklist_id}/rollback", response_model=RollbackPlanResponse, status_code=201)
async def add_rollback(checklist_id: int, body: RollbackPlanCreate):
    c = await get_checklist(app.state.db, checklist_id)
    if not c:
        raise HTTPException(404, "Checklist not found")
    body.checklist_id = checklist_id
    return await create_rollback_plan(app.state.db, body.model_dump())


# ── Readiness Report ──────────────────────────────────────────────────────

@app.get("/checklists/{checklist_id}/report", response_model=ReadinessReport)
async def readiness_report(checklist_id: int):
    report = await get_readiness_report(app.state.db, checklist_id)
    if not report:
        raise HTTPException(404, "Checklist not found")
    return report


# ── Service Releases ─────────────────────────────────────────────────────

@app.get("/services/{service_name}/releases", response_model=ServiceReleases)
async def service_releases(
    service_name: str,
    limit: int = Query(20, ge=1, le=100),
):
    """Release history for a specific service with cadence metrics."""
    return await get_service_releases(app.state.db, service_name, limit)


# ── Release Labels (v0.9.0) ───────────────────────────────────────────────

@app.post("/checklists/{checklist_id}/labels", response_model=LabelResponse, status_code=201)
async def add_label(checklist_id: int, body: LabelAddRequest):
    """Attach a label to a checklist for categorization."""
    result = await add_checklist_label(app.state.db, checklist_id, body.label)
    if result is None:
        raise HTTPException(404, "Checklist not found")
    if result == "invalid_label":
        raise HTTPException(
            422,
            f"Invalid label '{body.label}'. Valid labels: {sorted(VALID_LABELS)}",
        )
    if result == "duplicate":
        raise HTTPException(409, f"Label '{body.label}' already applied to this checklist")
    return result


@app.delete("/checklists/{checklist_id}/labels/{label}", status_code=204)
async def delete_label(checklist_id: int, label: str):
    """Remove a label from a checklist."""
    if not await remove_checklist_label(app.state.db, checklist_id, label):
        raise HTTPException(404, "Label not found on this checklist")


@app.get("/checklists/{checklist_id}/labels", response_model=list[LabelResponse])
async def get_labels(checklist_id: int):
    """List all labels on a checklist."""
    c = await get_checklist(app.state.db, checklist_id)
    if not c:
        raise HTTPException(404, "Checklist not found")
    return await list_checklist_labels(app.state.db, checklist_id)


@app.get("/labels/{label}/checklists", response_model=list[ChecklistResponse])
async def checklists_by_label(
    label: str,
    limit: int = Query(50, ge=1, le=200, description="Maximum number of results"),
):
    """List all checklists tagged with the given label."""
    if label not in VALID_LABELS:
        raise HTTPException(
            422,
            f"Invalid label '{label}'. Valid labels: {sorted(VALID_LABELS)}",
        )
    return await list_checklists_by_label(app.state.db, label, limit)


# ── Checklist Watchers (v0.9.0) ───────────────────────────────────────────

@app.post("/checklists/{checklist_id}/watchers", response_model=WatcherResponse, status_code=201)
async def add_checklist_watcher(checklist_id: int, body: WatcherAddRequest):
    """Subscribe a team member to a checklist."""
    result = await add_watcher(app.state.db, checklist_id, body.email, body.name)
    if result is None:
        raise HTTPException(404, "Checklist not found")
    if result == "duplicate":
        raise HTTPException(409, f"'{body.email}' is already watching this checklist")
    return result


@app.get("/checklists/{checklist_id}/watchers", response_model=list[WatcherResponse])
async def get_checklist_watchers(checklist_id: int):
    """List all watchers of a checklist."""
    c = await get_checklist(app.state.db, checklist_id)
    if not c:
        raise HTTPException(404, "Checklist not found")
    return await list_watchers(app.state.db, checklist_id)


@app.delete("/watchers/{watcher_id}", status_code=204)
async def delete_watcher(watcher_id: int):
    """Unsubscribe a watcher by their watcher record ID."""
    if not await remove_watcher(app.state.db, watcher_id):
        raise HTTPException(404, "Watcher not found")


@app.get("/watchers/by-email", response_model=list[ChecklistResponse])
async def checklists_by_watcher(
    email: str = Query(..., description="Email address of the watcher"),
):
    """Return all checklists watched by the given email."""
    return await get_watcher_checklists(app.state.db, email)


# ── Release Velocity Dashboard (v0.9.0) ───────────────────────────────────

@app.get("/analytics/velocity", response_model=ReleaseVelocity)
async def release_velocity(
    days: int = Query(30, ge=1, le=365, description="Rolling period in days (1-365)"),
):
    """Release velocity dashboard: completion rate, average duration, bottleneck categories,
    fastest/slowest releases, and breakdowns by service and environment."""
    return await get_release_velocity(app.state.db, days)


# ══════════════════════════════════════════════════════════════════════════
# Feature 1: Release Approvals (v1.0.0)
# ══════════════════════════════════════════════════════════════════════════

@app.post("/checklists/{checklist_id}/gates", response_model=ApprovalGateResponse, status_code=201)
async def create_gate(checklist_id: int, body: ApprovalGateCreate):
    """Create an approval gate for a release checklist."""
    result = await create_approval_gate(app.state.db, checklist_id, body.model_dump())
    if result is None:
        raise HTTPException(404, "Checklist not found")
    return result


@app.get("/checklists/{checklist_id}/gates", response_model=list[ApprovalGateResponse])
async def get_checklist_gates(checklist_id: int):
    """List all approval gates for a checklist."""
    c = await get_checklist(app.state.db, checklist_id)
    if not c:
        raise HTTPException(404, "Checklist not found")
    return await list_approval_gates(app.state.db, checklist_id)


@app.get("/gates/{gate_id}", response_model=ApprovalGateResponse)
async def get_gate(gate_id: int):
    """Get a single approval gate with its decisions."""
    result = await get_approval_gate(app.state.db, gate_id)
    if not result:
        raise HTTPException(404, "Approval gate not found")
    return result


@app.post("/gates/{gate_id}/decide", response_model=GateDecisionResponse, status_code=201)
async def decide_gate(gate_id: int, body: GateDecision):
    """Submit an approval or rejection decision for a gate."""
    result = await approve_or_reject_gate(app.state.db, gate_id, body.model_dump())
    if result is None:
        raise HTTPException(404, "Approval gate not found")
    if result == "invalid_role":
        raise HTTPException(
            422,
            f"Role '{body.approver_role}' is not in the gate's required_roles",
        )
    if result == "duplicate":
        raise HTTPException(
            409,
            f"'{body.approver_email}' has already submitted a decision for this gate",
        )
    if result == "gate_already_resolved":
        raise HTTPException(409, "This gate has already been approved or rejected")
    return result


@app.get("/checklists/{checklist_id}/gates/status", response_model=AllGatesStatus)
async def gates_status(checklist_id: int):
    """Check whether all approval gates for a checklist are approved."""
    c = await get_checklist(app.state.db, checklist_id)
    if not c:
        raise HTTPException(404, "Checklist not found")
    return await get_gates_status(app.state.db, checklist_id)


# ══════════════════════════════════════════════════════════════════════════
# Feature 2: Checklist Automation Rules (v1.0.0)
# ══════════════════════════════════════════════════════════════════════════

@app.post("/checklists/{checklist_id}/automations", response_model=AutomationRuleResponse, status_code=201)
async def create_automation(checklist_id: int, body: AutomationRuleCreate):
    """Create an automation rule for a check item in a checklist."""
    result = await create_automation_rule(app.state.db, checklist_id, body.model_dump())
    if result is None:
        raise HTTPException(404, "Checklist not found")
    if result == "invalid_rule_type":
        raise HTTPException(
            422,
            f"Invalid rule_type '{body.rule_type}'. Valid types: {sorted(VALID_RULE_TYPES)}",
        )
    if result == "item_not_in_checklist":
        raise HTTPException(
            422,
            f"Check item {body.item_id} does not belong to checklist {checklist_id}",
        )
    return result


@app.get("/checklists/{checklist_id}/automations", response_model=list[AutomationRuleResponse])
async def get_automations(checklist_id: int):
    """List all automation rules for a checklist."""
    c = await get_checklist(app.state.db, checklist_id)
    if not c:
        raise HTTPException(404, "Checklist not found")
    return await list_automation_rules(app.state.db, checklist_id)


@app.patch("/automations/{rule_id}", response_model=AutomationRuleResponse)
async def patch_automation(rule_id: int, body: AutomationRuleUpdate):
    """Update an automation rule's condition or enabled state."""
    result = await update_automation_rule(app.state.db, rule_id, body.model_dump(exclude_none=True))
    if result is None:
        raise HTTPException(404, "Automation rule not found")
    return result


@app.delete("/automations/{rule_id}", status_code=204)
async def remove_automation(rule_id: int):
    """Delete an automation rule."""
    if not await delete_automation_rule(app.state.db, rule_id):
        raise HTTPException(404, "Automation rule not found")


@app.post("/checklists/{checklist_id}/automations/evaluate")
async def evaluate_automations(checklist_id: int):
    """Evaluate all enabled automation rules for a checklist and apply actions."""
    c = await get_checklist(app.state.db, checklist_id)
    if not c:
        raise HTTPException(404, "Checklist not found")
    return await evaluate_automation_rules(app.state.db, checklist_id)


# ══════════════════════════════════════════════════════════════════════════
# Feature 3: Release Calendar (v1.0.0)
# ══════════════════════════════════════════════════════════════════════════

@app.post("/calendar/events", response_model=ReleaseEventResponse, status_code=201)
async def create_event(body: ReleaseEventCreate):
    """Schedule a release event on the calendar."""
    result = await create_release_event(app.state.db, body.model_dump())
    if result == "invalid_checklist":
        raise HTTPException(404, "Associated checklist not found")
    return result


@app.get("/calendar/events", response_model=list[ReleaseEventResponse])
async def get_events(
    environment: str | None = Query(None, description="Filter by environment"),
    status: str | None = Query(None, description="Filter by status: planned | confirmed | in_progress | completed | cancelled"),
    from_date: str | None = Query(None, alias="from", description="Start of date range (ISO format)"),
    to_date: str | None = Query(None, alias="to", description="End of date range (ISO format)"),
):
    """List release calendar events with optional filters."""
    return await list_release_events(
        app.state.db, environment=environment, status=status,
        from_date=from_date, to_date=to_date,
    )


@app.get("/calendar/events/{event_id}", response_model=ReleaseEventResponse)
async def get_event(event_id: int):
    """Get a single release calendar event."""
    result = await get_release_event(app.state.db, event_id)
    if not result:
        raise HTTPException(404, "Release event not found")
    return result


@app.patch("/calendar/events/{event_id}", response_model=ReleaseEventResponse)
async def patch_event(event_id: int, body: ReleaseEventUpdate):
    """Update a release calendar event."""
    result = await update_release_event(app.state.db, event_id, body.model_dump(exclude_none=True))
    if result is None:
        raise HTTPException(404, "Release event not found")
    if result == "invalid_status":
        raise HTTPException(
            422,
            f"Invalid status. Valid statuses: {sorted(VALID_EVENT_STATUSES)}",
        )
    return result


@app.delete("/calendar/events/{event_id}", status_code=204)
async def remove_event(event_id: int):
    """Delete a release calendar event."""
    if not await delete_release_event(app.state.db, event_id):
        raise HTTPException(404, "Release event not found")


@app.get("/calendar/conflicts", response_model=list[CalendarConflict])
async def calendar_conflicts(
    environment: str | None = Query(None, description="Filter by environment"),
    from_date: str | None = Query(None, alias="from", description="Start of date range (ISO format)"),
    to_date: str | None = Query(None, alias="to", description="End of date range (ISO format)"),
):
    """Detect scheduling conflicts (overlapping events in the same environment)."""
    return await detect_conflicts(
        app.state.db, environment=environment,
        from_date=from_date, to_date=to_date,
    )


@app.get("/calendar/view", response_model=CalendarView)
async def calendar_view(
    from_date: str = Query(..., alias="from", description="Start of date range (ISO format)"),
    to_date: str = Query(..., alias="to", description="End of date range (ISO format)"),
    environment: str | None = Query(None, description="Filter by environment"),
):
    """Full calendar view with events, conflicts, and statistics."""
    return await get_calendar_view(
        app.state.db, from_date=from_date, to_date=to_date,
        environment=environment,
    )

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
        "checklist comments, environment promotion, and CSV export."
    ),
    version="0.7.0",
    lifespan=lifespan,
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/health")
async def health():
    return {"status": "ok", "version": "0.7.0"}


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


@app.get("/checklists", response_model=list[ChecklistResponse])
async def list_all(
    environment: str | None = Query(None, description="staging | production | canary"),
    status: str | None = Query(None, description="in_progress | ready | blocked | completed"),
):
    return await list_checklists(app.state.db, environment, status)


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
    """Promote a checklist to a different environment (e.g. staging → production).
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
    """Export checklist items and sign-offs as CSV for audit/compliance."""
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
    """Automated risk scoring based on check failures, environment, and configuration."""
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

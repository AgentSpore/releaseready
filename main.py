from __future__ import annotations
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from models import (
    ChecklistCreate, ChecklistResponse,
    CheckItemCreate, CheckItemResponse, CheckItemUpdate,
    RollbackPlanCreate, RollbackPlanResponse,
    ReadinessReport,
)
from engine import (
    init_db, create_checklist, list_checklists, get_checklist,
    list_check_items, update_check_item, add_check_item,
    create_rollback_plan, get_readiness_report,
)

DB_PATH = os.getenv("DB_PATH", "releaseready.db")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = await init_db(DB_PATH)
    yield
    await app.state.db.close()


app = FastAPI(
    title="ReleaseReady",
    description="Production release readiness checklist. Pre-flight checks, rollback playbooks, deployment gates.",
    version="0.1.0",
    lifespan=lifespan,
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/health")
async def health():
    return {"status": "ok", "version": "0.1.0"}


# ── Checklists ────────────────────────────────────────────────────────────

@app.post("/checklists", response_model=ChecklistResponse, status_code=201)
async def create(body: ChecklistCreate):
    """
    Create a release checklist. Auto-seeds 15 default checks across
    infra, code, data, security, comms, and rollback categories.
    """
    return await create_checklist(app.state.db, body.model_dump())


@app.get("/checklists", response_model=list[ChecklistResponse])
async def list_all(
    environment: str | None = Query(None, description="staging | production | canary"),
    status: str | None = Query(None, description="in_progress | ready | blocked | completed"),
):
    """List release checklists with readiness scores."""
    return await list_checklists(app.state.db, environment, status)


@app.get("/checklists/{checklist_id}", response_model=ChecklistResponse)
async def get_one(checklist_id: int):
    """Get checklist details with readiness score."""
    c = await get_checklist(app.state.db, checklist_id)
    if not c:
        raise HTTPException(404, "Checklist not found")
    return c


# ── Check Items ───────────────────────────────────────────────────────────

@app.get("/checklists/{checklist_id}/items", response_model=list[CheckItemResponse])
async def get_items(checklist_id: int):
    """List all check items for a checklist, grouped by category."""
    c = await get_checklist(app.state.db, checklist_id)
    if not c:
        raise HTTPException(404, "Checklist not found")
    return await list_check_items(app.state.db, checklist_id)


@app.post("/checklists/{checklist_id}/items", response_model=CheckItemResponse, status_code=201)
async def add_item(checklist_id: int, body: CheckItemCreate):
    """Add a custom check item to an existing checklist."""
    c = await get_checklist(app.state.db, checklist_id)
    if not c:
        raise HTTPException(404, "Checklist not found")
    body.checklist_id = checklist_id
    return await add_check_item(app.state.db, body.model_dump())


@app.patch("/items/{item_id}", response_model=CheckItemResponse)
async def update_item(item_id: int, body: CheckItemUpdate):
    """
    Update a check item status: pass | fail | skip | na
    Add notes and checked_by for audit trail.
    """
    result = await update_check_item(
        app.state.db, item_id, body.status, body.notes, body.checked_by)
    if not result:
        raise HTTPException(404, "Check item not found")
    return result


# ── Rollback Plans ────────────────────────────────────────────────────────

@app.post("/checklists/{checklist_id}/rollback", response_model=RollbackPlanResponse, status_code=201)
async def add_rollback(checklist_id: int, body: RollbackPlanCreate):
    """Add or update rollback plan for a checklist."""
    c = await get_checklist(app.state.db, checklist_id)
    if not c:
        raise HTTPException(404, "Checklist not found")
    body.checklist_id = checklist_id
    return await create_rollback_plan(app.state.db, body.model_dump())


# ── Readiness Report ──────────────────────────────────────────────────────

@app.get("/checklists/{checklist_id}/report", response_model=ReadinessReport)
async def readiness_report(checklist_id: int):
    """
    Generate readiness report: score, blocking failures, rollback status,
    and a go/no-go recommendation for the release.
    """
    report = await get_readiness_report(app.state.db, checklist_id)
    if not report:
        raise HTTPException(404, "Checklist not found")
    return report

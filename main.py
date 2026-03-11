from __future__ import annotations
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from models import (
    ChecklistCreate, ChecklistResponse,
    CheckItemCreate, CheckItemResponse, CheckItemUpdate,
    RollbackPlanCreate, RollbackPlanResponse,
    ReadinessReport, BulkItemUpdate,
    SignOffCreate, SignOffResponse,
)
from engine import (
    init_db, create_checklist, list_checklists, get_checklist, delete_checklist,
    list_check_items, update_check_item, add_check_item,
    create_rollback_plan, get_readiness_report, get_aggregate_stats,
    clone_checklist, bulk_update_items,
    add_sign_off, list_sign_offs, complete_checklist,
)

DB_PATH = os.getenv("DB_PATH", "releaseready.db")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = await init_db(DB_PATH)
    yield
    await app.state.db.close()


app = FastAPI(
    title="ReleaseReady",
    description="Production release readiness checklist with sign-off workflow, rollback playbooks, and deployment gates.",
    version="0.4.0",
    lifespan=lifespan,
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/health")
async def health():
    return {"status": "ok", "version": "0.4.0"}


# ── Checklists ────────────────────────────────────────────────────────────

@app.post("/checklists", response_model=ChecklistResponse, status_code=201)
async def create(body: ChecklistCreate):
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
    if not result:
        raise HTTPException(404, "Check item not found")
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

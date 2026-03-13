from __future__ import annotations
from pydantic import BaseModel, Field
from typing import Optional


class ChecklistCreate(BaseModel):
    name: str = Field(..., description="Release checklist name, e.g. v2.3.0-backend")
    service: str = Field(..., description="Service or component being released")
    version: str = Field(..., description="Version tag or commit SHA")
    environment: str = Field("production", description="Target environment: staging | production | canary")
    owner_email: Optional[str] = None
    description: Optional[str] = None
    template_id: Optional[int] = Field(None, description="Create from template instead of default checks")


class ChecklistResponse(BaseModel):
    id: int
    name: str
    service: str
    version: str
    environment: str
    owner_email: Optional[str]
    description: Optional[str]
    status: str
    readiness_score: int
    total_checks: int
    passed_checks: int
    failed_checks: int
    blocked_checks: int
    created_at: str
    completed_at: Optional[str]


class CheckItemCreate(BaseModel):
    checklist_id: int
    category: str = Field(..., description="Category: infra | code | data | security | comms | rollback")
    title: str
    description: Optional[str] = None
    is_blocking: bool = Field(True, description="If True, failing this check blocks release")
    owner_email: Optional[str] = None
    depends_on: Optional[int] = Field(None, description="ID of check item this depends on")


class CheckItemResponse(BaseModel):
    id: int
    checklist_id: int
    category: str
    title: str
    description: Optional[str]
    status: str
    is_blocking: bool
    owner_email: Optional[str]
    notes: Optional[str]
    checked_at: Optional[str]
    checked_by: Optional[str]
    depends_on: Optional[int]


class CheckItemUpdate(BaseModel):
    status: str = Field(..., description="pass | fail | skip | na")
    notes: Optional[str] = None
    checked_by: Optional[str] = None


class RollbackPlanCreate(BaseModel):
    checklist_id: int
    steps: list[str] = Field(..., description="Ordered rollback steps")
    estimated_minutes: int = Field(15, ge=1)
    contacts: list[str] = Field(default_factory=list, description="On-call contacts for rollback")


class RollbackPlanResponse(BaseModel):
    id: int
    checklist_id: int
    steps: list[str]
    estimated_minutes: int
    contacts: list[str]
    created_at: str


class ReadinessReport(BaseModel):
    checklist_id: int
    service: str
    version: str
    environment: str
    status: str
    readiness_score: int
    passed: int
    failed: int
    blocked: int
    skipped: int
    blocking_failures: list[dict]
    has_rollback_plan: bool
    sign_off_count: int
    recommendation: str


class BulkItemUpdateItem(BaseModel):
    item_id: int
    status: str = Field(..., description="pass | fail | skip | na")
    notes: Optional[str] = None
    checked_by: Optional[str] = None


class BulkItemUpdate(BaseModel):
    updates: list[BulkItemUpdateItem] = Field(min_length=1, max_length=100)


class SignOffCreate(BaseModel):
    name: str = Field(min_length=1, max_length=80, description="Approver name")
    role: str = Field(min_length=1, max_length=80, description="Approver role, e.g. Engineering Lead")
    comment: Optional[str] = Field(None, max_length=500)


class SignOffResponse(BaseModel):
    id: int
    checklist_id: int
    name: str
    role: str
    comment: Optional[str]
    signed_at: str


# ── Templates ─────────────────────────────────────────────────────────────

class TemplateCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=120, description="Template name")
    description: Optional[str] = None
    from_checklist_id: Optional[int] = Field(None, description="Copy checks from existing checklist")


class TemplateResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    check_count: int
    created_at: str


class TemplateItemCreate(BaseModel):
    category: str
    title: str
    description: Optional[str] = None
    is_blocking: bool = True


# ── Timeline ─────────────────────────────────────────────────────────────

class TimelineEvent(BaseModel):
    type: str
    timestamp: str
    actor: str
    detail: str


class ReleaseTimeline(BaseModel):
    checklist_id: int
    service: str
    version: str
    total_events: int
    events: list[TimelineEvent]


# ── Service Releases ─────────────────────────────────────────────────────

class ServiceReleaseEntry(BaseModel):
    id: int
    version: str
    environment: str
    status: str
    readiness_score: int
    total_checks: int
    passed_checks: int
    blocking_failures: int
    created_at: str
    completed_at: Optional[str]


class ServiceReleases(BaseModel):
    service: str
    total_releases: int
    completed: int
    in_progress: int
    avg_readiness_score: float
    releases: list[ServiceReleaseEntry]


# ── Risk Assessment ──────────────────────────────────────────────────────

class RiskFactor(BaseModel):
    factor: str
    severity: str
    detail: str
    impact: int


class RiskAssessment(BaseModel):
    checklist_id: int
    service: str
    version: str
    environment: str
    risk_score: int
    risk_level: str
    readiness_score: int
    total_factors: int
    factors: list[RiskFactor]

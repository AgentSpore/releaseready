# ReleaseReady — Architecture (DEEP.md)

## Overview
Production release readiness checklist with templates, check dependencies,
sign-off workflow, rollback playbooks, deployment gates, release timeline,
service release cadence, and automated risk assessment.

## Stack
- **Runtime**: Python 3.11+ / FastAPI / uvicorn
- **Database**: aiosqlite (SQLite WAL mode, foreign keys ON)
- **Models**: Pydantic v2 with Field validation

## Database Schema

### templates / template_items
Templates for reusable checklists. Items have category, title, description, blocking flag.

### checklists
| Column | Type | Notes |
|--------|------|-------|
| id | INTEGER PK | auto |
| name | TEXT | release name |
| service | TEXT | service/app name |
| version | TEXT | semver |
| environment | TEXT | staging/production/canary |
| status | TEXT | in_progress/ready/blocked/completed |
| readiness_score | REAL | 0-100 |
| owner_email | TEXT NULL | release owner |
| completed_at | TEXT NULL | ISO timestamp |
| created_at | TEXT | ISO timestamp |

### check_items
Per-checklist checks with category, title, status (pending/pass/fail/skip),
blocking flag, depends_on chain, checked_by, checked_at, notes.

### rollback_plans
Steps, estimated_minutes, trigger_conditions per checklist.

### sign_offs
Name, role, comment, signed_at per checklist.

## API Endpoints (v0.6.0) — 22 endpoints

### Templates
- POST /templates — create (optionally from existing checklist)
- GET /templates — list all
- GET /templates/{id} — detail
- GET /templates/{id}/items — template items
- POST /templates/{id}/items — add item
- DELETE /templates/{id} — remove

### Checklists
- POST /checklists — create (optionally from template)
- GET /checklists — list (filter by environment, status)
- GET /checklists/stats — aggregate statistics
- GET /checklists/{id} — detail
- DELETE /checklists/{id} — remove
- POST /checklists/{id}/clone — clone with new version

### Timeline & Risk
- GET /checklists/{id}/timeline — chronological event log
- GET /checklists/{id}/risk — automated risk assessment (7 factors)
- GET /services/{name}/releases — release history with cadence metrics

### Sign-Off & Completion
- POST /checklists/{id}/sign-off — add approval
- GET /checklists/{id}/sign-offs — list approvals
- POST /checklists/{id}/complete — mark completed (requires sign-off)

### Check Items
- GET /checklists/{id}/items — list checks
- POST /checklists/{id}/items — add check
- PATCH /checklists/{id}/items/bulk — bulk update
- PATCH /items/{id} — update single check

### Rollback & Readiness
- POST /checklists/{id}/rollback — add rollback plan
- GET /checklists/{id}/report — readiness report

## Key Features
- **Release Timeline**: Chronological events (creation, checks, sign-offs, rollback plans, completion)
- **Risk Assessment**: 7 factors (blocking failures, low readiness, production env, no rollback, no sign-offs, pending checks, security failures), scored 0-100 with level (low/medium/high/critical)
- **Service Releases**: Release history per service with cadence metrics (avg score, completion rate)
- **Templates**: Create from scratch or convert existing checklist
- **Check Dependencies**: depends_on chain with circular reference detection
- **Sign-Off Workflow**: Role-based approvals with blocking failure gate
- **Readiness Score**: Auto-calculated from check results

## Version History
- v0.1.0: Basic checklists + check items
- v0.2.0: Rollback plans, readiness reports
- v0.3.0: Templates, clone, bulk update
- v0.4.0: Sign-off workflow, completion gates
- v0.5.0: Check dependencies, aggregate stats, env/status filters
- v0.6.0: Release timeline, service cadence, risk assessment

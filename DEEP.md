# ReleaseReady — Architecture (DEEP.md)

## Overview
Production release readiness checklist service with templates, check dependencies, sign-off workflow, rollback playbooks, and deployment gates.

## Data Model
- **checklists** — release metadata (name, service, version, environment, status)
- **check_items** — individual checks with category, blocking flag, depends_on
- **rollback_plans** — steps + contacts + estimated rollback time
- **sign_offs** — approver name/role/comment
- **templates** — reusable check templates
- **template_items** — check definitions within a template

## Workflow
1. Create template (or use defaults) -> POST /templates
2. Create checklist from template -> POST /checklists (template_id=N)
3. Work through checks -> PATCH /items/{id} (respects dependencies)
4. Add rollback plan -> POST /checklists/{id}/rollback
5. Sign off -> POST /checklists/{id}/sign-off (blocked if blocking failures)
6. Complete -> POST /checklists/{id}/complete (requires sign-off, no blocking failures)
7. Get readiness report -> GET /checklists/{id}/report

## Check Dependencies
- `depends_on` field on check items points to another item ID
- Cannot set status to "pass" if dependency item hasn't passed
- Bulk update respects dependencies (skips blocked items, returns dep_blocked list)
- Dependencies are intra-checklist only

## Templates
- Created from scratch (seeds with DEFAULT_CHECKS) or from existing checklist
- Template items: category, title, description, is_blocking
- When creating checklist with template_id, copies template items instead of defaults
- CRUD: create, list, get items, add item, delete

## Readiness Report Statuses
| Status | Condition |
|--------|-----------|
| BLOCKED | Any blocking check failed |
| CAUTION | Non-blocking failures only |
| READY | Score >= 90%, rollback plan, signed off |
| MOSTLY_READY | Score >= 80-90% or missing rollback/sign-off |
| NOT_READY | Score < 80% |

## Default Checks (15 items, 6 categories)
infra (3), code (4), data (2), security (2), comms (2), rollback (2)

## Key Decisions
- **Dependencies are soft**: only validated on "pass" status; "fail"/"skip" always allowed
- **Templates don't carry dependencies**: dependencies are workflow-specific, set per-checklist
- **Clone resets dependencies**: cloned items get depends_on=NULL to avoid cross-checklist refs

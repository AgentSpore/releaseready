# ReleaseReady — Development Log (MEMORY.md)

## v0.1.0 — Initial MVP
- Checklists with 15 auto-seeded checks across 6 categories
- Check items CRUD (status: pending/pass/fail/skip/na)
- Blocking vs non-blocking checks
- Basic readiness report with status determination

## v0.2.0 — Rollback Plans & Sign-Offs
- Rollback plan with ordered steps, estimated time, contacts
- Sign-off workflow (name, role, comment)
- Sign-off blocked if blocking failures exist
- Complete checklist (requires sign-off, no blocking failures)

## v0.3.0 — Clone, Bulk Update, Stats
- Clone checklist for new version
- Bulk update multiple check items at once
- Aggregate stats (by environment, status, most-failed checks, services)

## v0.4.0 — Target Updates
- PATCH /items/{id} for individual check updates
- POST /checklists/{id}/items for adding custom checks

## v0.5.0 — Templates & Check Dependencies
- **Checklist templates**: save as template, create from template
- Template CRUD: create, list, get, get items, add item, delete
- Create template from existing checklist (copies check items)
- Create checklist with template_id (uses template items instead of defaults)
- **Check dependencies**: depends_on field on check items
- Cannot pass item until dependency passes (422 error with reason)
- Bulk update respects dependencies (returns dependency_blocked list)
- Clone resets depends_on to NULL (avoids cross-checklist refs)
- DEEP.md + MEMORY.md added

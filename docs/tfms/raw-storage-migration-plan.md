# TFMS Raw Storage Migration Runbook (0-8 + Tests)

This document is the step-by-step implementation guide to:

1. Keep `raw_xml` as the canonical raw source.
2. Store compact JSON in DB (no embedded `raw` branches).
3. Expose `raw=true/false` in TFMS REST APIs.

## Goal State

- DB:
  - `raw_xml` = full original XML
  - indexed columns for search (`msg_type`, `acid`, `gufi`, etc.)
  - `parsed_json` / `data` compact (no embedded `raw`)
- API:
  - `raw=false` (default) => compact response
  - `raw=true` => raw-only response

---

## Phase 0 - Compatibility/Safety

### Tasks
- Keep existing endpoints and schemas stable.
- Add feature flag `TFMS_RAW_RESPONSE_FROM_XML=true`.
- Do not remove DB columns.

### Done When
- App boots with old clients unchanged.
- Flag visible in container env.

---

## Phase 1 - Shared Payload Utilities

### Tasks
- Create `python-app/app/tfms_payload_utils.py` with:
  - `strip_raw_fields(value)`
  - `only_raw_fields(value)`
  - `projection_raw_by_key_from_xml(xml_text, projection_key)`

### Done When
- Utility functions are imported by service/endpoints.

---

## Phase 2 - REST `raw=true/false`

### Tasks
- Add `raw: bool = False` to:
  - `GET /tfms/events`
  - `GET /tfms/projections`
  - `GET /tfms/projections/{projection_key}`
- Behavior:
  - `raw=false` => return compact
  - `raw=true` => return raw-only

### Done When
- `raw=false` responses have no `"raw"` keys.
- `raw=true` responses contain raw payload.

---

## Phase 3 - Persist Compact JSON

### Tasks
- In `ingest_tfms_xml`:
  - save `raw_xml` unchanged
  - save `parsed_json = strip_raw_fields(parsed)`
  - save `projection.data = strip_raw_fields(projection_data)`

### Done When
- New rows in `tfms_events.parsed_json` and `tfms_projections.data` contain no embedded `raw`.

---

## Phase 4 - Historical Backfill

### Tasks
- Add backfill script: `python-app/scripts/backfill_tfms_compact_payloads.py`
- Process in chunks:
  - events by `id`
  - projections by `projection_key`
- Support incremental runs:
  - `--batch-size`
  - `--max-batches`

### Run Commands
```bash
docker compose exec -T fastapi python scripts/backfill_tfms_compact_payloads.py --batch-size 500
docker compose exec -T fastapi python scripts/backfill_tfms_compact_payloads.py --batch-size 500 --max-batches 20
```

### Done When
- Backfill can run repeatedly without errors.
- Updated rows lose embedded `raw` branches.

---

## Phase 5 - Index and Query Health

### Tasks
- Create/verify:
  - `ix_tfms_events_created_msg_acid_gufi`
  - `ix_tfms_projections_updated_type_msg_acid_gufi`

### SQL
```sql
CREATE INDEX IF NOT EXISTS ix_tfms_events_created_msg_acid_gufi
ON tfms_events (created_at DESC, msg_type, acid, gufi);

CREATE INDEX IF NOT EXISTS ix_tfms_projections_updated_type_msg_acid_gufi
ON tfms_projections (updated_at DESC, projection_type, msg_type, acid, gufi);
```

### Done When
- Both indexes exist in `tfms` DB.

---

## Phase 6 - Schema/Response Cleanup

### Tasks
- Keep current response shape for compatibility.
- Optional later: introduce explicit `raw_payload` field.

### Done When
- No breaking API changes for existing clients.

---

## Phase 7 - Documentation

### Tasks
- Update `README.md` to document:
  - `/tfms/events?raw=false|true`
  - `/tfms/projections?raw=false|true`
  - `/tfms/projections/{key}?raw=false|true`
- Document backfill command(s).

### Done When
- Operators can run migration using docs only.

---

## Phase 8 - Validation

### Functional Checks
- `raw=false` has no `"raw"` branch.
- `raw=true` returns raw-only branch.
- Filtering behavior (`acid`, `msg_type`, `gufi`) unchanged.

### Suggested Checks
```bash
curl -s "http://localhost:8000/tfms/events?limit=5&raw=false"
curl -s "http://localhost:8000/tfms/events?limit=5&raw=true"
curl -s "http://localhost:8000/tfms/projections?limit=5&raw=false"
curl -s "http://localhost:8000/tfms/projections?limit=5&raw=true"
```

---

## Tests

### Unit Tests
```bash
docker compose exec -T fastapi python -m pip install pytest
docker compose exec -T fastapi python -m pytest tests/tfms/test_tfms_payload_utils.py -q
```

### Runtime Smoke
1. Ingest one TFMS sample.
2. Verify DB compact for new row.
3. Verify API raw toggle on events/projections.

---

## Rollout Order

1. Phase 0
2. Phase 1
3. Phase 2
4. Phase 3
5. Phase 5
6. Phase 7
7. Phase 8 + tests
8. Phase 4 (backfill, can run incrementally over time)

Backfill is intentionally last in rollout because it is operationally heavy and not required to make new writes correct.

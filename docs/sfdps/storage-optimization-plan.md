# SFDPS Storage Optimization Plan

## Context

SFDPS does not currently store duplicated `raw` branches inside JSON payloads like TFMS/TBFM did.
The main growth driver is table size in `faa_swim`, especially `raw_events`.

Current observed footprint (example snapshot):

- `raw_events`: ~13 GB
- `flights_current`: ~367 MB

## Objective

Reduce SFDPS storage growth and keep query performance stable without breaking current API behavior.

## Scope

- Database retention and lifecycle for `raw_events`
- Optional compression/archival strategy for `raw_xml`
- Query/index tuning for common access paths
- Operational runbook and validation

---

## Phase 0 - Safety and Baseline

### Tasks
- Capture baseline metrics:
  - table sizes
  - row counts/day
  - ingest rate
  - API latency for `/events` and `/flights/current`
- Confirm backup and restore procedure for `faa_swim`.

### Commands
```bash
docker compose exec -T postgres psql -U postgres -d faa_swim -c "\
SELECT relname, pg_size_pretty(pg_total_relation_size(relid)) AS size\
FROM pg_catalog.pg_statio_user_tables\
ORDER BY pg_total_relation_size(relid) DESC;"
```

---

## Phase 1 - Retention Policy

### Recommendation
- Keep hot history in `raw_events` for a fixed window (example: 30-90 days).
- Keep `flights_current` fully online (it is current-state projection and relatively small).

### Tasks
- Define retention window with stakeholders.
- Add scheduled cleanup job by `received_at`.

### Example cleanup SQL
```sql
DELETE FROM raw_events
WHERE received_at < now() - interval '60 days';
```

---

## Phase 2 - Archival Strategy (Optional but Recommended)

### Goal
- Preserve long-term `raw_xml` outside hot Postgres while keeping operational DB lean.

### Options
1. Archive to object storage (preferred): parquet/jsonl + gzip/zstd.
2. Archive to separate cold Postgres schema/database.

### Tasks
- Export old rows (`stream_id`, `received_at`, `payload_type`, `raw_xml`, `parsed_json` minimal metadata).
- Verify archive integrity (counts + checksums).
- Delete archived rows from hot DB.

---

## Phase 3 - Index and Query Health

### Tasks
- Verify indexes for SFDPS hot queries:
  - `raw_events(received_at DESC)`
  - `raw_events(payload_type, received_at DESC)`
  - `raw_events(gufi)`
  - `raw_events(flight_id)`
- Keep `flights_current` indexes aligned with filter usage (`arrival_airport`, `departure_airport`, `source_timestamp`, `updated_at`).

### Example
```sql
CREATE INDEX IF NOT EXISTS ix_raw_events_received_at_desc
ON raw_events (received_at DESC);
```

---

## Phase 4 - Partitioning (If Growth Remains High)

### Trigger
- If retention + archive still leaves large maintenance windows or poor VACUUM behavior.

### Plan
- Partition `raw_events` by month on `received_at`.
- Drop old partitions instead of large DELETE operations.

### Note
- This is a bigger migration; execute only after retention policy is stable.

---

## Phase 5 - Operational Automation

### Tasks
- Add scheduled jobs:
  - archive job
  - retention delete/drop partition job
  - periodic `VACUUM (ANALYZE)` on affected tables
- Add alerts on:
  - DB size growth
  - long-running cleanup jobs
  - table bloat indicators

---

## Phase 6 - Documentation

### Tasks
- Update `README.md` with:
  - retention window
  - archive command/job
  - recovery instructions from archive
- Update `AGENTS.md` with operational commands.

---

## Phase 7 - Validation

### Checks
- Size reduction trend on `raw_events`.
- `/events` and `/flights/current` latency unchanged or improved.
- No ingestion loss (stream IDs monotonic, row counts expected).
- Restore sample from archive works.

---

## Phase 8 - Rollout Sequence

1. Baseline metrics and backup validation
2. Retention cleanup in dry-run/staging
3. Production retention rollout (small window first)
4. Add archival pipeline
5. Tighten retention to target window
6. Optional partitioning if needed

---

## Suggested First Implementation (Low Risk)

1. Start with 60-day retention on `raw_events`.
2. Run nightly cleanup during low traffic.
3. Re-measure table size and endpoint latency after one week.
4. Add archival only if historical retention beyond 60 days is required.

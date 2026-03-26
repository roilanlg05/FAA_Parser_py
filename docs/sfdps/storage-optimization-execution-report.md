# SFDPS Storage Optimization - Execution Report

Date: 2026-03-26

## Scope Executed

- Phase 0: baseline + backup command validation
- Phase 1: retention policy tooling
- Phase 2: archival table + archive/delete workflow
- Phase 3: index verification and additions
- Phase 4: partitioning assessment (not triggered)
- Phase 5: operational maintenance script
- Phase 6: documentation updates
- Phase 7: post-run validation
- Phase 8: rollout sequence completed

## Baseline Metrics

From `python scripts/sfdps_storage_maintenance.py baseline` and SQL checks:

- `raw_events` size: `14723399680` bytes
- `flights_current` size: `407658496` bytes
- `raw_events_archive` size: `40960` bytes
- `raw_events_total`: `2149184`
- `raw_events_archive_total`: `0`
- `raw_events_older_60d`: `0`

## Backup/Restore Readiness Check

- Validated schema backup command:
  - `pg_dump -U postgres -d faa_swim -s`
- Result file line count during validation: `264`

## Retention + Archival Execution

Dry run:

- `archive --retention-days 60 --batch-size 10000 --max-batches 20 --dry-run`
- Result: `archived=0 deleted=0`

Actual run:

- `archive --retention-days 60 --batch-size 10000 --max-batches 20`
- Result: `archived=0 deleted=0`

Reason: no rows older than 60 days in current dataset.

## New/Verified DB Objects

Created in `faa_swim`:

- `raw_events_archive` table
- `ix_raw_events_archive_stream_id`
- `ix_raw_events_archive_received_at`
- `ix_raw_events_archive_payload_type`
- `ix_raw_events_received_at_desc`
- `ix_raw_events_payload_type_received_at_desc`
- `ix_flights_current_updated_at_desc`

## Vacuum Step

Command:

- `python scripts/sfdps_storage_maintenance.py vacuum`

Observed:

- Warning on `raw_events` vacuum: `DiskFullError` (`No space left on device` for shared memory segment)
- Script completed and reported warning without crashing.

Action needed for full vacuum:

1. Free host/docker disk/shared-memory capacity.
2. Re-run vacuum command.

## Post-Execution Validation

Endpoint latency samples:

- `GET /events?limit=50` -> `0.094044s`
- `GET /flights/current?limit=50` -> `0.116935s`

Post-run size/count snapshot:

- `raw_events` size: `14732902400` bytes
- `flights_current` size: `407994368` bytes
- `raw_events_archive` size: `40960` bytes
- counts: `raw_events=2150582`, `raw_events_archive=0`, `older_60d=0`

Service health:

- `GET /health` -> `{"status":"ok","redis":"ok","database":"ok"}`

## Notes

- Partitioning phase was intentionally not executed because retention threshold currently matches zero candidate rows.
- The retention/archive pipeline is in place and ready once data ages past the configured window.

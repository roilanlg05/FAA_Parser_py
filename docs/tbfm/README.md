# TBFM Integration Notes

This repository runs TBFM in the same unified architecture used by SFDPS and TFMS:

`JMS/Solace -> Redis Streams -> Python worker -> Postgres -> FastAPI + WebSocket`

## Runtime services

- `redis-tbfm` stores raw TBFM stream and pub/sub channels.
- `tbfm-jms-bridge` consumes Solace queue and writes to `tbfm.raw.xml`.
- `fastapi` runs `TbfmStreamWorker` and exposes `/tbfm/*` + `WS /ws/tbfm`.

## Key environment variables

- `TBFM_REDIS_URL`
- `TBFM_DATABASE_URL`
- `TBFM_QUEUE_NAME`
- `TBFM_RAW_STREAM_NAME`
- `TBFM_CONSUMER_GROUP`
- `TBFM_CONSUMER_NAME`
- `TBFM_SOLACE_HOST`
- `TBFM_SOLACE_VPN`
- `TBFM_SOLACE_USERNAME`
- `TBFM_SOLACE_PASSWORD`
- `TBFM_RAW_RESPONSE_FROM_XML`

## API endpoints

- `POST /ingest/tbfm/raw` (debug/manual injection only)
- `GET /tbfm/ingest/stats`
- `GET /tbfm/events?raw=false`
- `GET /tbfm/projections?raw=false`
- `GET /tbfm/projections/{projection_key}?raw=false`
- `WS /ws/tbfm`

`raw=false` returns compact payloads; `raw=true` returns raw-only payloads.

## Advanced filters

Apply to `GET /tbfm/events` and `GET /tbfm/projections`:

- Pagination/order: `limit`, `offset`, `sort=asc|desc`
- Identity: `acid|callsign`, `gufi`, `tma_id`, `flight_ref`, `msg_type`, `source_facility`, `queue_name`
- Status: `status`, `status_any`, `status_all`, `status_field`
  - `status_field` values: `acs`, `fps`
  - alias supported: `ARRIVED`
- Airport: `airport`, `departure`
- Time: `date`, `from_date`, `to_date`, `from_ts`, `to_ts`, `time_basis=source`

Common validation errors:

- `invalid_time_basis`
- `invalid_status_field`
- `invalid_sort`
- `invalid_date_format`
- `invalid_time_range`

`WS /ws/tbfm` supports the same business filters (identity/status/airport/time) for initial subscription and `action=subscribe` updates.

## Backfill (historical compacting)

```bash
docker compose exec -T fastapi python scripts/backfill_tbfm_compact_payloads.py --batch-size 500
docker compose exec -T fastapi python scripts/backfill_tbfm_compact_payloads.py --batch-size 500 --max-batches 20
```

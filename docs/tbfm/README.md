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

## Backfill (historical compacting)

```bash
docker compose exec -T fastapi python scripts/backfill_tbfm_compact_payloads.py --batch-size 500
docker compose exec -T fastapi python scripts/backfill_tbfm_compact_payloads.py --batch-size 500 --max-batches 20
```

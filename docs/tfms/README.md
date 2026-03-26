# TFMS Integration Notes

TFMS corre en la misma arquitectura unificada del stack:

`JMS/Solace -> Redis Streams -> Python worker -> Postgres -> FastAPI + WebSocket`

## Servicios runtime

- `redis-tfms`: stream `tfms.raw.xml` y canales `tfms.events` / `tfms.projections`.
- `tfms-jms-bridge`: consume la cola de Solace y publica XML crudo al stream.
- `fastapi`: ejecuta `TfmsStreamWorker` y expone endpoints `/tfms/*` + `WS /ws/tfms`.

## Variables clave

- `TFMS_REDIS_URL`
- `TFMS_DATABASE_URL`
- `TFMS_QUEUE_NAME`
- `TFMS_RAW_STREAM_NAME`
- `TFMS_CONSUMER_GROUP`
- `TFMS_CONSUMER_NAME`
- `TFMS_EVENTS_CHANNEL_NAME`
- `TFMS_PROJECTIONS_CHANNEL_NAME`
- `TFMS_RAW_RESPONSE_FROM_XML`
- `TFMS_PARSER_PATH`

## Endpoints REST

- `POST /ingest/tfms/raw` (debug/manual injection)
- `GET /tfms/ingest/stats`
- `GET /tfms/events?raw=false`
- `GET /tfms/projections?raw=false`
- `GET /tfms/projections/{projection_key}?raw=false`

`raw=false` devuelve payload compacto; `raw=true` devuelve payload raw-only.

## Filtros avanzados

Aplican en `GET /tfms/events` y `GET /tfms/projections`:

- Paginacion/orden: `limit`, `offset`, `sort=asc|desc`
- Identidad: `acid|callsign`, `gufi`, `flight_ref`, `msg_type`, `source_facility`
- Estado: `status`, `status_any`, `status_all`, `status_field`
  - `status_field` valido: `flight_status`, `tmi_status`, `service_state`
  - alias soportado: `ARRIVED`
- Aeropuerto: `airport`, `departure`
- Tiempo: `date`, `from_date`, `to_date`, `from_ts`, `to_ts`, `time_basis=source`

Errores de validacion comunes:

- `invalid_time_basis`
- `invalid_status_field`
- `invalid_sort`
- `invalid_date_format`
- `invalid_time_range`

## WebSocket

- `WS /ws/tfms`
- Snapshot inicial + updates realtime (`event` y `projection`).
- Soporta los mismos filtros de negocio que REST (identidad/estado/aeropuerto/tiempo) para suscripcion inicial y resuscripcion (`action=subscribe`).

## Backfill de compactacion

```bash
docker compose exec -T fastapi python scripts/backfill_tfms_compact_payloads.py --batch-size 500
docker compose exec -T fastapi python scripts/backfill_tfms_compact_payloads.py --batch-size 500 --max-batches 20
```

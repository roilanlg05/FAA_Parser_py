# FAA SWIM stack: JMS/Solace -> Python parser -> FastAPI/Postgres/Redis

## Que incluye

- **`jms-bridge/`**: servicio Java que consume FAA SWIM/SCDS por **Solace JMS** y escribe XML crudo en un **Redis Stream**.
- **`python-app/`**: servicio Python con:
  - parser XML -> JSON para SFDPS, ASDE-X y track records
  - worker que consume `faa.raw.xml` desde Redis
  - persistencia en Postgres
  - publicación de JSONs parseados a Redis (`faa.parsed.json`, `faa.parsed.events`)
  - API en FastAPI para consultar salud, eventos y vuelos actuales
- **`docker-compose.yml`**: Postgres + Redis + FastAPI + JMS bridge

## Flujo

1. FAA SWIM/SCDS publica XML por Solace JMS.
2. `jms-bridge` lo consume y lo mete en Redis Stream `faa.raw.xml`.
3. `python-app` lee ese stream, parsea XML, guarda el payload crudo + JSON en Postgres y proyecta vuelos actuales en `flights_current`.
4. El JSON normalizado se republica a Redis para websockets, workers o notificaciones.
5. FastAPI expone endpoints para consultar los datos.

## Arranque rápido local

### 1) Infra

```bash
cp .env.example .env
docker compose up -d postgres redis-sfdps redis-tfms redis-tbfm
```

### 2) FastAPI + worker

```bash
cd python-app
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/faa_swim
export SFDPS_REDIS_URL=redis://localhost:6379/0
export TFMS_REDIS_URL=redis://localhost:6380/0
export TBFM_REDIS_URL=redis://localhost:6381/0
python run_dev.py
```

### 3) Java JMS bridge

```bash
cd jms-bridge
mvn package
export SOLACE_HOST=tcps://...
export SOLACE_VPN=...
export SOLACE_USERNAME=...
export SOLACE_PASSWORD=...
export SOLACE_QUEUE_NAME='your.queue.name.OUT'
export REDIS_URL=redis://localhost:6379
java -jar target/faa-swim-jms-bridge-0.1.0.jar
```

## Ingesta manual para pruebas

```bash
curl -X POST http://localhost:8000/ingest/raw \
  -H 'content-type: application/json' \
  -d @<(python - <<'PY'
import json, pathlib
xml = pathlib.Path('/mnt/data/sfdps_sample.xml').read_text()
print(json.dumps({'xml': xml, 'source': 'manual-test'}))
PY
)
```

## Endpoints

- `GET /health`
- `POST /ingest/raw`
- `GET /events?limit=50`
- `GET /flights/current?limit=50`
- `GET /flights/current/{gufi}`
- `WS /ws/flights` (filtros: `callsign`, `gufi`, `airport`, `destination`, `departure`, `date`, `limit`)
- `WS /ws/tfms` (snapshot + realtime con filtros avanzados de identidad/estado/tiempo/aeropuerto)
- `POST /ingest/tfms/raw` (debug/manual inject; pipeline productivo usa stream)
- `GET /tfms/ingest/stats`
- `GET /tfms/events?limit=100&raw=false`
- `GET /tfms/projections?limit=100&raw=false`
- `GET /tfms/projections/{projection_key}?raw=false`
- `WS /ws/tbfm` (snapshot + realtime con filtros avanzados de identidad/estado/tiempo/aeropuerto)
- `POST /ingest/tbfm/raw` (debug/manual inject; pipeline productivo usa stream)
- `GET /tbfm/ingest/stats`
- `GET /tbfm/events?limit=100&raw=false`
- `GET /tbfm/projections?limit=100&raw=false`
- `GET /tbfm/projections/{projection_key}?raw=false`

## WebSocket de vuelos

Conecta a:

- `ws://localhost:8000/ws/flights`
- `ws://localhost:8000/ws/flights?callsign=NKS585`
- `ws://localhost:8000/ws/flights?gufi=14dcdcc2-adc1-40ab-888c-153043fb1aeb`
- `ws://localhost:8000/ws/flights?destination=KSDF`
- `ws://localhost:8000/ws/flights?airport=KSDF`
- `ws://localhost:8000/ws/flights?departure=KSDF`
- `ws://localhost:8000/ws/flights?date=2026-03-26&limit=25`

Reglas de filtro:

- `destination`: coincide solo con `arrival.airport`.
- `airport`: coincide con `arrival.airport` o `departure.airport`.
- `departure`: coincide solo con `departure.airport`.
- `date`: formato `YYYY-MM-DD`, aplicado sobre `source_timestamp` y con fallback a `updated_at`.

Al conectar, el servidor envia un mensaje `snapshot` con vuelos actuales que cumplen el filtro.
Despues envia mensajes `event` en tiempo real cuando llega un payload que coincide.
En los eventos, el campo `flights` contiene vuelos canonicos filtrados (mismo esquema que `/flights/current`).
En `GET /events`, cada item ahora incluye `flights` con vuelos canonicos extraidos del payload parseado.

Tambien puedes cambiar la suscripcion en caliente enviando JSON por el socket:

```json
{
  "action": "subscribe",
  "callsign": "NKS585",
  "gufi": "14dcdcc2-adc1-40ab-888c-153043fb1aeb",
  "destination": "KSDF",
  "airport": "KSDF",
  "departure": "KSDF",
  "date": "2026-03-26",
  "limit": 25,
  "snapshot": true
}
```

## WebSocket TFMS

Conecta a:

- `ws://localhost:8000/ws/tfms`
- `ws://localhost:8000/ws/tfms?acid=NKS585`
- `ws://localhost:8000/ws/tfms?gufi=14dcdcc2-adc1-40ab-888c-153043fb1aeb`
- `ws://localhost:8000/ws/tfms?msg_type=FlightSectors&source_facility=TFMS`

Filtros soportados:

- `payload_type`
- `source_facility`
- `msg_type`
- `flight_ref`
- `acid`
- `gufi`
- `projection_type`
- `projection_key`
- `queue_name`
- `airport`
- `departure`
- `status`
- `status_any`
- `status_all`
- `status_field` (`flight_status`, `tmi_status`, `service_state`)
- `date`, `from_date`, `to_date` (`YYYY-MM-DD`)
- `from_ts`, `to_ts` (ISO 8601)
- `time_basis=source`
- `limit` (1..500)

Mensajes del servidor:

- `snapshot` (proyecciones actuales filtradas)
- `event` (evento TFMS del canal `tfms.events`)
- `projection` (actualizacion de proyeccion del canal `tfms.projections`)

`WS /ws/tfms` publica eventos compactos (sin ramas `raw`) para reducir latencia y consumo de memoria.

Toggle raw en REST TFMS:

- `raw=false` (default): payload compacto, sin duplicados `raw`.
- `raw=true`: vista raw-only.
- Endpoints: `/tfms/events`, `/tfms/projections`, `/tfms/projections/{projection_key}`.

Ejemplos:

```bash
curl -s "http://localhost:8000/tfms/events?limit=5&raw=false"
curl -s "http://localhost:8000/tfms/events?limit=5&raw=true"
curl -s "http://localhost:8000/tfms/projections?limit=5&raw=true"
curl -s "http://localhost:8000/tfms/events?acid=NKS585&status_any=ARRIVED&time_basis=source"
curl -s "http://localhost:8000/tfms/projections?airport=KJFK&from_date=2026-03-01&to_date=2026-03-31&sort=asc"
```

Cambio de suscripcion en caliente:

```json
{
  "action": "subscribe",
  "acid": "NKS585",
  "msg_type": "FlightSectors",
  "source_facility": "TFMS",
  "status_any": "ARRIVED,ACTIVE",
  "airport": "KSDF",
  "from_ts": "2026-03-26T00:00:00Z",
  "to_ts": "2026-03-26T23:59:59Z",
  "time_basis": "source",
  "limit": 100,
  "snapshot": true
}
```

Filtros REST TFMS (`/tfms/events` y `/tfms/projections`):

- Paginacion/orden: `limit`, `offset`, `sort=asc|desc`
- Identidad: `acid|callsign`, `gufi`, `flight_ref`, `msg_type`, `source_facility`
- Estado: `status`, `status_any`, `status_all`, `status_field`
- Aeropuerto: `airport`, `departure`
- Tiempo: `date`, `from_date`, `to_date`, `from_ts`, `to_ts`, `time_basis=source`

## WebSocket TBFM

Conecta a:

- `ws://localhost:8000/ws/tbfm`
- `ws://localhost:8000/ws/tbfm?acid=UPS1326`
- `ws://localhost:8000/ws/tbfm?msg_type=arrival&source_facility=TBFM`

Filtros soportados:

- `payload_type`
- `source_facility`
- `msg_type`
- `flight_ref`
- `acid`
- `gufi`
- `tma_id`
- `projection_type`
- `projection_key`
- `queue_name`
- `airport`
- `departure`
- `status`
- `status_any`
- `status_all`
- `status_field` (`acs`, `fps`)
- `date`, `from_date`, `to_date` (`YYYY-MM-DD`)
- `from_ts`, `to_ts` (ISO 8601)
- `time_basis=source`
- `limit` (1..500)

`WS /ws/tbfm` publica eventos compactos (sin ramas `raw`) para reducir latencia y consumo de memoria.

Toggle raw en REST TBFM:

- `raw=false` (default): payload compacto, sin duplicados `raw`.
- `raw=true`: vista raw-only.
- Endpoints: `/tbfm/events`, `/tbfm/projections`, `/tbfm/projections/{projection_key}`.

Ejemplos:

```bash
curl -s "http://localhost:8000/tbfm/events?limit=5&raw=false"
curl -s "http://localhost:8000/tbfm/events?limit=5&raw=true"
curl -s "http://localhost:8000/tbfm/projections?limit=5&raw=true"
curl -s "http://localhost:8000/tbfm/events?tma_id=DFW&status_field=acs&status_any=LANDED"
curl -s "http://localhost:8000/tbfm/projections?airport=KATL&from_ts=2026-03-26T00:00:00Z&to_ts=2026-03-26T23:59:59Z"
```

Filtros REST TBFM (`/tbfm/events` y `/tbfm/projections`):

- Paginacion/orden: `limit`, `offset`, `sort=asc|desc`
- Identidad: `acid|callsign`, `gufi`, `tma_id`, `flight_ref`, `msg_type`, `source_facility`, `queue_name`
- Estado: `status`, `status_any`, `status_all`, `status_field`
- Aeropuerto: `airport`, `departure`
- Tiempo: `date`, `from_date`, `to_date`, `from_ts`, `to_ts`, `time_basis=source`

## Operacion diaria (runbook rapido)

### Arranque

```bash
cd /Users/hashdown/Projects/FAA_Parser_py
docker compose up -d
```

### Validacion rapida

```bash
curl -s http://localhost:8000/health
curl -s "http://localhost:8000/events?limit=3"
curl -s "http://localhost:8000/flights/current?limit=3"
curl -s "http://localhost:8000/tfms/ingest/stats"
docker compose exec -T redis-sfdps redis-cli XLEN faa.raw.xml
docker compose exec -T redis-tfms redis-cli XLEN tfms.raw.xml
docker compose exec -T redis-tbfm redis-cli XLEN tbfm.raw.xml
```

### Monitoreo

```bash
docker compose ps
docker compose logs -f jms-bridge
docker compose logs -f tfms-jms-bridge
docker compose logs -f tbfm-jms-bridge
docker compose logs -f fastapi
```

### Parada

```bash
docker compose down
```

### Reinicio de un servicio puntual

```bash
docker compose restart jms-bridge
docker compose restart fastapi
```

### Troubleshooting comun

- Si `health` marca `database: error`, revisa logs de `postgres` y `fastapi`:
  - `docker compose logs postgres --no-color`
  - `docker compose logs fastapi --no-color`
- Si no llegan mensajes al stream (`XLEN` no sube), revisa `jms-bridge`:
  - `docker compose logs jms-bridge --no-color`
  - confirma `.env` con `SOLACE_HOST`, `SOLACE_VPN`, `SOLACE_USERNAME`, `SOLACE_PASSWORD`, `SOLACE_QUEUE_NAME`
- Si TFMS no crece en DB, valida stream TFMS y bridge:
  - `docker compose exec -T redis-tfms redis-cli XLEN tfms.raw.xml`
  - `docker compose logs tfms-jms-bridge --no-color`
- Si TBFM no crece en DB, valida stream TBFM y bridge:
  - `docker compose exec -T redis-tbfm redis-cli XLEN tbfm.raw.xml`
  - `docker compose logs tbfm-jms-bridge --no-color`
  - si ves `TBFM bridge idle; set TBFM_SOLACE env vars`, faltan credenciales/host TBFM en `.env`

Backfill recomendado para limpiar JSON historico TFMS (remover ramas `raw` embebidas):

```bash
docker compose exec -T fastapi python scripts/backfill_tfms_compact_payloads.py --batch-size 500
docker compose exec -T fastapi python scripts/backfill_tfms_compact_payloads.py --batch-size 500 --max-batches 20
docker compose exec -T fastapi python scripts/backfill_tbfm_compact_payloads.py --batch-size 500
docker compose exec -T fastapi python scripts/backfill_tbfm_compact_payloads.py --batch-size 500 --max-batches 20
```

Mantenimiento de almacenamiento SFDPS (`raw_events`):

```bash
docker compose exec -T fastapi python scripts/sfdps_storage_maintenance.py baseline
docker compose exec -T fastapi python scripts/sfdps_storage_maintenance.py archive --retention-days 60 --batch-size 10000 --dry-run
docker compose exec -T fastapi python scripts/sfdps_storage_maintenance.py archive --retention-days 60 --batch-size 10000
docker compose exec -T fastapi python scripts/sfdps_storage_maintenance.py vacuum
```

- Si `vacuum` reporta `DiskFullError` por shared memory, primero libera espacio del host/docker y reintenta.
- Si el puerto 5432 esta ocupado por otro contenedor local, detenlo antes de levantar este stack.

## Diseño de tablas

- `raw_events`: XML crudo + JSON parseado + metadatos de stream
- `flights_current`: última proyección conocida por `gufi`

## Nota importante

FAA SCDS exige **JMS** como API autorizada para clientes estándar. Por eso la pieza de conexión al broker la dejo en Java/JMS, y Python queda como parser, persistencia y API.

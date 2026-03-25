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
- `WS /ws/flights` (filtro por `callsign`, `gufi`, `destination`)
- `WS /ws/tfms` (filtro TFMS por `acid`, `gufi`, `msg_type`, etc.)
- `POST /ingest/tfms/raw` (debug/manual inject; pipeline productivo usa stream)
- `GET /tfms/ingest/stats`
- `GET /tfms/events?limit=100`
- `GET /tfms/projections?limit=100`
- `GET /tfms/projections/{projection_key}`
- `WS /ws/tbfm` (filtro TBFM por `acid`, `tma_id`, `msg_type`, etc.)
- `POST /ingest/tbfm/raw` (debug/manual inject; pipeline productivo usa stream)
- `GET /tbfm/ingest/stats`
- `GET /tbfm/events?limit=100`
- `GET /tbfm/projections?limit=100`
- `GET /tbfm/projections/{projection_key}`

## WebSocket de vuelos

Conecta a:

- `ws://localhost:8000/ws/flights`
- `ws://localhost:8000/ws/flights?callsign=NKS585`
- `ws://localhost:8000/ws/flights?gufi=14dcdcc2-adc1-40ab-888c-153043fb1aeb`
- `ws://localhost:8000/ws/flights?destination=KSDF`
- `ws://localhost:8000/ws/flights?airport=KSDF`

Reglas de filtro para aeropuerto:

- `destination` / `destino`: coincide solo con `arrival.airport`.
- `airport` / `aeropuerto` / `aereopuerto`: coincide con `arrival.airport` o `departure.airport`.

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

Mensajes del servidor:

- `snapshot` (proyecciones actuales filtradas)
- `event` (evento TFMS del canal `tfms.events`)
- `projection` (actualizacion de proyeccion del canal `tfms.projections`)

Cambio de suscripcion en caliente:

```json
{
  "action": "subscribe",
  "acid": "NKS585",
  "msg_type": "FlightSectors",
  "source_facility": "TFMS",
  "snapshot": true
}
```

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
- Si el puerto 5432 esta ocupado por otro contenedor local, detenlo antes de levantar este stack.

## Diseño de tablas

- `raw_events`: XML crudo + JSON parseado + metadatos de stream
- `flights_current`: última proyección conocida por `gufi`

## Nota importante

FAA SCDS exige **JMS** como API autorizada para clientes estándar. Por eso la pieza de conexión al broker la dejo en Java/JMS, y Python queda como parser, persistencia y API.

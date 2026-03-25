# TFMS JMS/Solace -> Python Parser -> FastAPI/Postgres/Redis

Este paquete está orientado a los tipos de mensajes TFMS que compartiste:

- `fltdOutput / fdm:fltdMessage / msgType=FlightSectors`
- `fltdOutput / fdm:fltdMessage / msgType=trackInformation`
  - con `ncsmTrackData`
  - con `ncsmRouteData`
- `fiOutput / fiMessage / msgType=TMI_FLIGHT_LIST`
- `tfmsStatusOutput`

La queue de ejemplo ya viene configurada por default en todos los componentes:

`landgbnb.gmail.com.TFMS.3b02c856-4246-4fa3-8c92-a7c2a37d2176.OUT`

## Estructura

- `jms-bridge/` Java consumer para Solace JMS, suscrito a una queue y reenviando cada XML a FastAPI.
- `python-app/app/tfms_parser.py` parser TFMS completo.
- `python-app/app/main.py` API FastAPI para ingestar XML, parsearlo, guardar en Postgres y publicar en Redis.
- `docker-compose.yml` Postgres + Redis + FastAPI.
- `examples_*.xml` ejemplos de prueba.
- `*.parsed.json` salidas JSON generadas en la validación local.

## Qué devuelve el parser

Cada payload se devuelve con dos capas:

1. **Normalizada**: campos prácticos (`msgType`, `qualifiedAircraftId`, `flightTraversalData2`, `eta`, `rvsmData`, etc.).
2. **Lossless/raw**: árbol JSON del XML completo (`raw`) para no omitir nada del mensaje original.

Eso te permite:

- usar el JSON normalizado para lógica de negocio,
- conservar todo el XML en forma JSON si mañana quieres extraer más campos.

## Uso rápido del parser

```bash
cd python-app
python tfms_consumer.py --input ../examples_flight_sectors.xml --pretty
python tfms_consumer.py --input ../examples_tmi_flight_list.xml --pretty
python tfms_consumer.py --input "/mnt/data/Pasted text (2).txt" --pretty
python tfms_consumer.py --input "/mnt/data/Pasted text (3).txt" --pretty
```

## Levantar FastAPI + Postgres + Redis

```bash
docker compose up --build
```

API:

- `GET /health`
- `POST /ingest/raw`
- `GET /events`
- `GET /projections`
- `GET /projections/{projection_key}`

Ejemplo de ingestión manual:

```bash
curl -X POST http://localhost:8000/ingest/raw \
  -H 'Content-Type: application/json' \
  -d @- <<'JSON'
{
  "queue_name": "landgbnb.gmail.com.TFMS.3b02c856-4246-4fa3-8c92-a7c2a37d2176.OUT",
  "xml": "<?xml version=\"1.0\" encoding=\"UTF-8\"?><root/>"
}
JSON
```

## Bridge JMS/Solace

Variables principales:

- `SOLACE_HOST`
- `SOLACE_VPN`
- `SOLACE_USERNAME`
- `SOLACE_PASSWORD`
- `TFMS_QUEUE_NAME`
- `FASTAPI_INGEST_URL`

Compilar:

```bash
cd jms-bridge
mvn package
```

Ejecutar:

```bash
java -jar target/tfms-jms-bridge-1.0.0-jar-with-dependencies.jar
```

## Modelo de datos

### `tfms_events`
Guarda el XML original y el JSON parseado completo.

### `tfms_projections`
Guarda la última proyección por:

- vuelo (`gufi` o `acid`)
- vuelo TMI
- status de servicio

## Validación local hecha

- parser probado con `FlightSectors`
- parser probado con `trackInformation`
- parser probado con `TMI_FLIGHT_LIST`
- parser probado con `tfmsStatusOutput`

Ver `validation_results.json`.

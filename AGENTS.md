# AGENTS.md
Instructions for coding agents working in this repository.

## Scope
- Monorepo with two services:
  - `python-app/` (FastAPI + XML parser + Redis worker + Postgres persistence)
  - `jms-bridge/` (Java JMS consumer forwarding to Redis Stream)
- Local infra/services are in top-level `docker-compose.yml`.

## Key Paths
- `README.md`: startup flow and endpoint overview.
- `python-app/app/main.py`: FastAPI app + lifespan wiring.
- `python-app/app/worker.py`: stream consumer loop.
- `python-app/app/parser.py`: XML parsing logic.
- `python-app/app/projection.py`: parsed payload projection.
- `python-app/app/service.py`: persist + publish orchestration.
- `python-app/app/models.py`: SQLAlchemy models.
- `python-app/app/schemas.py`: Pydantic schemas.
- `python-app/app/tfms_service.py`: TFMS persist + publish orchestration.
- `python-app/app/tbfm_service.py`: TBFM persist + publish orchestration.
- `python-app/app/tfms_parser_adapter.py`: TFMS parser adapter.
- `python-app/app/tbfm_parser_adapter.py`: TBFM parser adapter.
- `python-app/app/tfms_payload_utils.py`: TFMS raw/compact payload helpers.
- `python-app/app/tbfm_payload_utils.py`: TBFM raw/compact payload helpers.
- `python-app/tests/test_pipeline.py`: pytest tests.
- `python-app/tests/tbfm/test_tbfm_parser.py`: TBFM parser tests.
- `python-app/tests/tbfm/test_tbfm_payload_utils.py`: TBFM payload helper tests.
- `python-app/scripts/sfdps_storage_maintenance.py`: SFDPS retention/archive maintenance.
- `jms-bridge/pom.xml`: Maven build config (Java 17, shade plugin).
- `jms-bridge/tfms/pom.xml`: Maven build config for TFMS bridge.
- `jms-bridge/tbfm/pom.xml`: Maven build config for TBFM bridge.

## Build / Run Commands
Run from repo root unless noted.

### Infra
- Start only Postgres + Redis: `docker compose up -d postgres redis-sfdps redis-tfms redis-tbfm`
- Start full stack: `docker compose up -d`
- Start bridge in compose: `docker compose up -d jms-bridge`
- Start TFMS bridge in compose: `docker compose up -d tfms-jms-bridge`
- Start TBFM bridge in compose: `docker compose up -d tbfm-jms-bridge`

### Python app (`python-app/`)
- Setup:
  - `python -m venv .venv`
  - `source .venv/bin/activate`
  - `pip install -r requirements.txt`
- Run dev entrypoint: `python run_dev.py`
- Or run directly: `uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload`
- Local env vars:
  - `DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/faa_swim`
  - `SFDPS_REDIS_URL=redis://localhost:6379/0`
  - `TFMS_REDIS_URL=redis://localhost:6380/0`
  - `TBFM_REDIS_URL=redis://localhost:6381/0`
  - `ENABLE_WORKER=true`

### Java bridge (`jms-bridge/`)
- Build jar: `mvn package`
- Run jar: `java -jar target/faa-swim-jms-bridge-0.1.0.jar`
- Run as compose service: `docker compose up -d jms-bridge`
- Runtime env vars:
  - Required: `SOLACE_HOST`, `SOLACE_VPN`, `SOLACE_USERNAME`, `SOLACE_PASSWORD`
  - Required: `SOLACE_QUEUE_NAME` (queue to consume)
  - Optional: `SOLACE_CLIENT_NAME`
  - Optional (has default): `REDIS_URL`, `RAW_STREAM_NAME`, `TFMS_RAW_STREAM_NAME`, `TBFM_RAW_STREAM_NAME`

## Test Commands
Python tests are pytest-style functions in `python-app/tests/`.
- Install pytest if missing: `pip install pytest`
- Run all Python tests: `python -m pytest`
- Run one test file: `python -m pytest tests/test_pipeline.py`
- Run one test function: `python -m pytest tests/test_pipeline.py::test_sfdps_projection`
- Run by keyword: `python -m pytest -k sfdps`
- Run TBFM payload tests: `python -m pytest tests/tbfm/test_tbfm_payload_utils.py -q`
- Run SFDPS storage baseline script: `python scripts/sfdps_storage_maintenance.py baseline`
- Fast single-test loop tip: use `file::test_name` and optionally `-q`.
- Caveat: current tests read XML from absolute `/mnt/data/...` paths.
- In clean environments, tests may fail unless those files exist.

Java tests:
- No test sources currently exist under `jms-bridge/src/test`.
- Standard command (when tests are added): `mvn test`
- Single Java test class: `mvn -Dtest=ClassNameTest test`

## Lint / Format / Type Commands
Current repository state:
- No configured lint/type tools found (`ruff`, `black`, `isort`, `mypy`, `flake8`).
- No Makefile or task-runner command defines lint.

Safe checks available now:
- Python syntax compile check: `python -m compileall app tests`
- Maven compile check: `mvn -q -DskipTests compile`
- If you add lint/type tooling, commit config files and update this section.

## Code Style Guidelines
Use existing code patterns; avoid introducing a different style unless requested.

### Imports
- Keep `from __future__ import annotations` at top of Python modules.
- Order imports as: stdlib, third-party, local package imports.
- Prefer explicit imports; avoid wildcard imports.

### Formatting
- Use 4-space indentation in Python.
- Match prevailing single-quote string style.
- Keep multiline dict/list literals readable and vertically aligned.
- Add comments only for non-obvious behavior.

### Types
- Add argument and return annotations for new/changed functions.
- Prefer built-in generics: `list[T]`, `dict[K, V]`.
- In core modules use `T | None`; in schemas `Optional[T]` is acceptable.
- Keep API/service return shapes explicit (`dict[str, Any]`, etc.).

### Naming
- Python functions/variables/modules: `snake_case`.
- Python classes: `PascalCase`.
- Environment variable names: `UPPER_SNAKE_CASE`.
- Keep payload keys/schema fields in existing snake_case conventions.

### Async, DB, and Redis patterns
- Preserve async flow in FastAPI handlers and service methods.
- Use dependency-injected `AsyncSession` / `AsyncSessionLocal` patterns.
- Reuse `RedisManager`; do not create ad-hoc global Redis clients.
- Keep worker semantics (`xreadgroup` + `xack`) unless intentionally redesigning.

### Error handling
- Prefer targeted exceptions where practical.
- Maintain resilience in worker and health-check paths.
- When catching broad exceptions in loops, log with context (`stream_id`).
- Use `HTTPException` for API not-found/error responses.

### Parser and projection rules
- Parser must stay defensive with missing XML nodes and malformed values.
- Reuse helper converters (`to_int`, `to_float`, `text_of`).
- Unknown payloads should return structured fallback info, not crash.

### Java bridge style
- Keep code Java 17 compatible.
- Preserve message-acknowledgement and Redis field contract unless coordinated.
- Keep env-driven configuration centralized in `BridgeConfig`.

## API and Data Contracts
- FastAPI endpoints currently exposed:
  - `GET /health`
  - `POST /ingest/raw`
- `GET /events?limit=50`
- `GET /flights/current?limit=50`
- `GET /flights/current/{gufi}`
- `WS /ws/flights`
- `POST /ingest/tfms/raw`
- `GET /tfms/ingest/stats`
- `GET /tfms/events?limit=100`
- `GET /tfms/projections?limit=100`
- `GET /tfms/projections/{projection_key}`
- `WS /ws/tfms`
- `POST /ingest/tbfm/raw`
- `GET /tbfm/ingest/stats`
- `GET /tbfm/events?limit=100`
- `GET /tbfm/projections?limit=100`
- `GET /tbfm/projections/{projection_key}`
- `WS /ws/tbfm`
- Keep response schema fields in sync with `app/schemas.py`.
- Avoid changing stream/table names without coordinated config updates.
- Preserve `raw_events` and `flights_current` semantics for backward compatibility.

## Cursor / Copilot Rules
Checked and not found in this repository:
- `.cursorrules`
- `.cursor/rules/`
- `.github/copilot-instructions.md`
- If these are added later, treat them as higher-priority instructions.

## Agent Checklist
- Read `README.md` and this file before coding.
- Target the smallest possible change.
- Run the narrowest relevant test (single test when possible).
- Avoid broad refactors unless explicitly requested.
- Update this file when commands or conventions change.

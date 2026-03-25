from __future__ import annotations

import asyncio
import base64
import binascii
import json
import logging
from contextlib import asynccontextmanager
from collections import Counter, deque
from datetime import datetime
from typing import Any
from uuid import uuid4
from xml.etree.ElementTree import ParseError

from fastapi import Depends, FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from sqlalchemy import func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from .config import settings
from .db import Base, AsyncSessionLocal, engine, get_db_session
from .models import FlightCurrent
from .redis_client import RedisManager
from .schemas import EventResponse, HealthResponse, IngestRequest
from .service import get_current_flights, get_recent_events, ingest_xml
from .tfms_db import TfmsAsyncSessionLocal, tfms_engine
from .tfms_models import TfmsBase, TfmsProjection
from .tfms_schemas import TfmsEventResponse, TfmsIngestRequest, TfmsProjectionResponse
from .tfms_service import get_tfms_events, get_tfms_projections, ingest_tfms_xml
from .tbfm_db import TbfmAsyncSessionLocal, tbfm_engine
from .tbfm_models import TbfmBase, TbfmProjection
from .tbfm_schemas import TbfmEventResponse, TbfmIngestRequest, TbfmProjectionResponse
from .tbfm_service import get_tbfm_events, get_tbfm_projections, ingest_tbfm_xml
from .worker import StreamWorker, TbfmStreamWorker, TfmsStreamWorker

redis_manager = RedisManager()
worker = StreamWorker(redis_manager)
tfms_worker = TfmsStreamWorker(redis_manager)
tbfm_worker = TbfmStreamWorker(redis_manager)
logger = logging.getLogger(__name__)

_tfms_ingest_counters: Counter[str] = Counter()
_tfms_ingest_recent: deque[dict[str, Any]] = deque(maxlen=200)
_tbfm_ingest_counters: Counter[str] = Counter()
_tbfm_ingest_recent: deque[dict[str, Any]] = deque(maxlen=200)


def _record_tfms_ingest(
    *,
    status: str,
    reason: str | None = None,
    queue_name: str | None = None,
    payload_type: str | None = None,
    xml_length: int | None = None,
) -> None:
    counter_key = f'{status}:{reason}' if reason else status
    _tfms_ingest_counters[counter_key] += 1
    _tfms_ingest_recent.append(
        {
            'timestamp': datetime.utcnow().isoformat(),
            'status': status,
            'reason': reason,
            'queue_name': queue_name,
            'payload_type': payload_type,
            'xml_length': xml_length,
        }
    )


def _record_tbfm_ingest(
    *,
    status: str,
    reason: str | None = None,
    queue_name: str | None = None,
    payload_type: str | None = None,
    xml_length: int | None = None,
) -> None:
    counter_key = f'{status}:{reason}' if reason else status
    _tbfm_ingest_counters[counter_key] += 1
    _tbfm_ingest_recent.append(
        {
            'timestamp': datetime.utcnow().isoformat(),
            'status': status,
            'reason': reason,
            'queue_name': queue_name,
            'payload_type': payload_type,
            'xml_length': xml_length,
        }
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    async with tfms_engine.begin() as conn:
        await conn.run_sync(TfmsBase.metadata.create_all)
    async with tbfm_engine.begin() as conn:
        await conn.run_sync(TbfmBase.metadata.create_all)
    await redis_manager.connect_sfdps()
    await redis_manager.connect_tfms()
    await redis_manager.connect_tbfm()
    if settings.enable_worker:
        await worker.start()
        await tfms_worker.start()
        await tbfm_worker.start()
    try:
        yield
    finally:
        await tbfm_worker.stop()
        await tfms_worker.stop()
        await worker.stop()
        await redis_manager.close()
        await engine.dispose()
        await tfms_engine.dispose()
        await tbfm_engine.dispose()


app = FastAPI(title='FAA SWIM Stack', lifespan=lifespan)


def _matches_filter(value: str | None, expected: str | None, *, case_insensitive: bool = False) -> bool:
    if expected is None:
        return True
    if value is None:
        return False
    if case_insensitive:
        return value.upper() == expected.upper()
    return value == expected


def _normalize_msg_type(value: Any) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    normalized = ''.join(ch for ch in value.lower() if ch.isalnum())
    return normalized or None


def _matches_msg_type(value: str | None, expected: str | None) -> bool:
    if expected is None:
        return True
    return _normalize_msg_type(value) == _normalize_msg_type(expected)


def _flight_matches_subscription(
    flight: dict[str, Any], *, callsign: str | None, gufi: str | None, destination: str | None, airport: str | None
) -> bool:
    flight_gufi = flight.get('gufi')
    flight_destination = (flight.get('arrival') or {}).get('airport')
    flight_departure = (flight.get('departure') or {}).get('airport')
    flight_identification = flight.get('flight_identification') or {}
    aircraft_identification = flight_identification.get('aircraft_identification')
    airport_matches = True
    if airport is not None:
        airport_matches = (
            _matches_filter(flight_destination, airport, case_insensitive=True)
            or _matches_filter(flight_departure, airport, case_insensitive=True)
        )
    return (
        _matches_filter(aircraft_identification, callsign, case_insensitive=True)
        and _matches_filter(flight_gufi, gufi)
        and _matches_filter(flight_destination, destination, case_insensitive=True)
        and airport_matches
    )


def _iso_or_none(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _canonical_flight(
    flight: dict[str, Any],
    *,
    payload_type: str | None,
    stream_id: str | None,
    source: str | None,
    updated_at: datetime | None,
) -> dict[str, Any]:
    meta = flight.get('meta') or {}
    arrival = flight.get('arrival') or {}
    departure = flight.get('departure') or {}
    enroute = flight.get('enroute') or {}
    flight_identification = flight.get('flight_identification') or {}
    return {
        'gufi': flight.get('gufi'),
        'gufi_code_space': flight.get('gufi_code_space'),
        'flight_plan_identifier': flight.get('flight_plan_identifier'),
        'flight_status': flight.get('flight_status'),
        'operator': flight.get('operator'),
        'meta': {
            'centre': meta.get('centre'),
            'source': meta.get('source'),
            'system': meta.get('system'),
            'timestamp': meta.get('timestamp'),
        },
        'flight_identification': {
            'computer_id': flight_identification.get('computer_id'),
            'site_specific_plan_id': flight_identification.get('site_specific_plan_id'),
            'aircraft_identification': flight_identification.get('aircraft_identification'),
        },
        'arrival': {
            'airport': arrival.get('airport'),
            'estimated_runway_time': arrival.get('estimated_runway_time'),
        },
        'departure': {
            'airport': departure.get('airport'),
            'actual_runway_time': departure.get('actual_runway_time'),
        },
        'enroute': enroute,
        'controlling_unit': flight.get('controlling_unit'),
        'assigned_altitude': flight.get('assigned_altitude'),
        'supplemental_data': flight.get('supplemental_data') or {},
        'payload_type': payload_type,
        'stream_id': stream_id,
        'source': source,
        'source_timestamp': meta.get('timestamp'),
        'updated_at': _iso_or_none(updated_at),
    }


def _normalize_destination_filter(payload: dict[str, Any]) -> str | None:
    destination = payload.get('destination') or payload.get('destino')
    if isinstance(destination, str) and destination:
        return destination.upper()
    return None


def _normalize_airport_filter(payload: dict[str, Any]) -> str | None:
    airport = payload.get('airport') or payload.get('aeropuerto') or payload.get('aereopuerto')
    if isinstance(airport, str) and airport:
        return airport.upper()
    return None


def _normalize_tfms_subscription(payload: dict[str, Any]) -> dict[str, Any]:
    def _upper(value: Any) -> str | None:
        if isinstance(value, str) and value:
            return value.upper()
        return None

    def _text(value: Any) -> str | None:
        if isinstance(value, str) and value:
            return value
        return None

    return {
        'payload_type': _upper(payload.get('payload_type')),
        'source_facility': _upper(payload.get('source_facility')),
        'msg_type': _text(payload.get('msg_type')),
        'flight_ref': _text(payload.get('flight_ref')),
        'acid': _upper(payload.get('acid')),
        'gufi': _text(payload.get('gufi')),
        'projection_type': _upper(payload.get('projection_type')),
        'projection_key': _text(payload.get('projection_key')),
        'queue_name': _text(payload.get('queue_name')),
    }


def _normalize_tbfm_subscription(payload: dict[str, Any]) -> dict[str, Any]:
    def _upper(value: Any) -> str | None:
        if isinstance(value, str) and value:
            return value.upper()
        return None

    def _text(value: Any) -> str | None:
        if isinstance(value, str) and value:
            return value
        return None

    return {
        'payload_type': _upper(payload.get('payload_type')),
        'source_facility': _upper(payload.get('source_facility')),
        'msg_type': _text(payload.get('msg_type')),
        'flight_ref': _text(payload.get('flight_ref')),
        'acid': _upper(payload.get('acid')),
        'gufi': _text(payload.get('gufi')),
        'tma_id': _text(payload.get('tma_id')),
        'projection_type': _upper(payload.get('projection_type')),
        'projection_key': _text(payload.get('projection_key')),
        'queue_name': _text(payload.get('queue_name')),
    }


def _tfms_event_fields(event_payload: dict[str, Any]) -> dict[str, Any]:
    parsed = event_payload.get('parsed') if isinstance(event_payload.get('parsed'), dict) else {}
    payload_type = event_payload.get('payload_type')
    source_facility = None
    msg_type = None
    flight_ref = None
    acid = None
    gufi = None

    if payload_type == 'tfms_flight_data_output':
        first = (parsed.get('messages') or [None])[0] or {}
        body = first.get('body') or {}
        qid = body.get('qualifiedAircraftId') or {}
        source_facility = first.get('sourceFacility')
        msg_type = first.get('msgType')
        flight_ref = first.get('flightRef')
        acid = qid.get('aircraftId') or first.get('acid')
    elif payload_type == 'tfms_flow_information_output':
        first = (parsed.get('messages') or [None])[0] or {}
        tmi_first = (first.get('tmiFlightDataList') or [None])[0] or {}
        flight = tmi_first.get('flight') or {}
        source_facility = first.get('sourceFacility')
        msg_type = first.get('msgType')
        flight_ref = tmi_first.get('flightReference')
        acid = flight.get('aircraftId')
        gufi = tmi_first.get('gufi')
    elif payload_type == 'tfms_status_output':
        first = (parsed.get('statuses') or [None])[0] or {}
        source_facility = first.get('facility')
        msg_type = 'status'

    if not gufi:
        gufi = event_payload.get('gufi')

    return {
        'payload_type': payload_type,
        'source_facility': source_facility,
        'msg_type': msg_type,
        'flight_ref': flight_ref,
        'acid': acid,
        'gufi': gufi,
        'queue_name': event_payload.get('queue_name'),
    }


def _tfms_projection_fields(projection_payload: dict[str, Any]) -> dict[str, Any]:
    return {
        'projection_type': projection_payload.get('projection_type'),
        'projection_key': projection_payload.get('projection_key'),
        'acid': projection_payload.get('acid'),
        'gufi': projection_payload.get('gufi'),
        'flight_ref': projection_payload.get('flight_ref'),
        'msg_type': projection_payload.get('msg_type'),
        'source_facility': projection_payload.get('source_facility'),
    }


def _tfms_matches_subscription(fields: dict[str, Any], subscription: dict[str, Any]) -> bool:
    return (
        _matches_filter(fields.get('payload_type'), subscription.get('payload_type'), case_insensitive=True)
        and _matches_filter(fields.get('source_facility'), subscription.get('source_facility'), case_insensitive=True)
        and _matches_msg_type(fields.get('msg_type'), subscription.get('msg_type'))
        and _matches_filter(fields.get('flight_ref'), subscription.get('flight_ref'))
        and _matches_filter(fields.get('acid'), subscription.get('acid'), case_insensitive=True)
        and _matches_filter(fields.get('gufi'), subscription.get('gufi'))
        and _matches_filter(fields.get('projection_type'), subscription.get('projection_type'), case_insensitive=True)
        and _matches_filter(fields.get('projection_key'), subscription.get('projection_key'))
        and _matches_filter(fields.get('queue_name'), subscription.get('queue_name'))
    )


def _tbfm_event_fields(event_payload: dict[str, Any]) -> dict[str, Any]:
    parsed = event_payload.get('parsed') if isinstance(event_payload.get('parsed'), dict) else {}
    first_doc = ((parsed.get('documents') or [None])[0] or {}) if isinstance(parsed, dict) else {}
    env_attrs = (first_doc.get('attributes') or {}) if isinstance(first_doc, dict) else {}
    first_tma = ((first_doc.get('tma') or [None])[0] or {}) if isinstance(first_doc, dict) else {}
    tma_attrs = (first_tma.get('attributes') or {}) if isinstance(first_tma, dict) else {}
    first_air = ((first_tma.get('air') or [None])[0] or {}) if isinstance(first_tma, dict) else {}
    air_attrs = (first_air.get('attributes') or {}) if isinstance(first_air, dict) else {}
    first_flt = (first_air.get('flt') or {}) if isinstance(first_air, dict) else {}
    return {
        'payload_type': event_payload.get('payload_type'),
        'source_facility': env_attrs.get('envSrce'),
        'msg_type': air_attrs.get('airType'),
        'flight_ref': tma_attrs.get('msgId'),
        'acid': air_attrs.get('aid') or first_flt.get('aid'),
        'gufi': event_payload.get('gufi'),
        'tma_id': air_attrs.get('tmaId'),
        'queue_name': event_payload.get('queue_name'),
    }


def _tbfm_projection_fields(projection_payload: dict[str, Any]) -> dict[str, Any]:
    return {
        'projection_type': projection_payload.get('projection_type'),
        'projection_key': projection_payload.get('projection_key'),
        'acid': projection_payload.get('acid'),
        'gufi': projection_payload.get('gufi'),
        'tma_id': projection_payload.get('tma_id'),
        'flight_ref': projection_payload.get('flight_ref'),
        'msg_type': projection_payload.get('msg_type'),
        'source_facility': projection_payload.get('source_facility'),
    }


def _tbfm_matches_subscription(fields: dict[str, Any], subscription: dict[str, Any]) -> bool:
    return (
        _matches_filter(fields.get('payload_type'), subscription.get('payload_type'), case_insensitive=True)
        and _matches_filter(fields.get('source_facility'), subscription.get('source_facility'), case_insensitive=True)
        and _matches_msg_type(fields.get('msg_type'), subscription.get('msg_type'))
        and _matches_filter(fields.get('flight_ref'), subscription.get('flight_ref'))
        and _matches_filter(fields.get('acid'), subscription.get('acid'), case_insensitive=True)
        and _matches_filter(fields.get('gufi'), subscription.get('gufi'))
        and _matches_filter(fields.get('tma_id'), subscription.get('tma_id'))
        and _matches_filter(fields.get('projection_type'), subscription.get('projection_type'), case_insensitive=True)
        and _matches_filter(fields.get('projection_key'), subscription.get('projection_key'))
        and _matches_filter(fields.get('queue_name'), subscription.get('queue_name'))
    )


async def _fetch_tfms_snapshot(subscription: dict[str, Any], limit: int = 100) -> list[dict[str, Any]]:
    if subscription.get('payload_type') or subscription.get('queue_name'):
        return []

    stmt = select(TfmsProjection)

    if subscription.get('source_facility'):
        stmt = stmt.where(func.upper(TfmsProjection.source_facility) == subscription['source_facility'])
    normalized_msg_type = _normalize_msg_type(subscription.get('msg_type'))
    if normalized_msg_type:
        stmt = stmt.where(
            func.lower(func.regexp_replace(func.coalesce(TfmsProjection.msg_type, ''), '[^A-Za-z0-9]', '', 'g'))
            == normalized_msg_type
        )
    if subscription.get('flight_ref'):
        stmt = stmt.where(TfmsProjection.flight_ref == subscription['flight_ref'])
    if subscription.get('acid'):
        stmt = stmt.where(func.upper(TfmsProjection.acid) == subscription['acid'])
    if subscription.get('gufi'):
        stmt = stmt.where(TfmsProjection.gufi == subscription['gufi'])
    if subscription.get('projection_type'):
        stmt = stmt.where(func.upper(TfmsProjection.projection_type) == subscription['projection_type'])
    if subscription.get('projection_key'):
        stmt = stmt.where(TfmsProjection.projection_key == subscription['projection_key'])

    stmt = stmt.order_by(TfmsProjection.updated_at.desc()).limit(limit)

    async with TfmsAsyncSessionLocal() as session:
        rows = list((await session.execute(stmt)).scalars())
    snapshot: list[dict[str, Any]] = []
    for row in rows:
        projection = {
            'projection_key': row.projection_key,
            'projection_type': row.projection_type,
            'acid': row.acid,
            'gufi': row.gufi,
            'flight_ref': row.flight_ref,
            'msg_type': row.msg_type,
            'source_facility': row.source_facility,
            'source_timestamp': row.source_timestamp,
            'data': row.data,
            'updated_at': _iso_or_none(row.updated_at),
        }
        snapshot.append(projection)
    return snapshot


async def _fetch_tbfm_snapshot(subscription: dict[str, Any], limit: int = 100) -> list[dict[str, Any]]:
    if subscription.get('payload_type') or subscription.get('queue_name'):
        return []

    stmt = select(TbfmProjection)

    if subscription.get('source_facility'):
        stmt = stmt.where(func.upper(TbfmProjection.source_facility) == subscription['source_facility'])
    normalized_msg_type = _normalize_msg_type(subscription.get('msg_type'))
    if normalized_msg_type:
        stmt = stmt.where(
            func.lower(func.regexp_replace(func.coalesce(TbfmProjection.msg_type, ''), '[^A-Za-z0-9]', '', 'g'))
            == normalized_msg_type
        )
    if subscription.get('flight_ref'):
        stmt = stmt.where(TbfmProjection.flight_ref == subscription['flight_ref'])
    if subscription.get('acid'):
        stmt = stmt.where(func.upper(TbfmProjection.acid) == subscription['acid'])
    if subscription.get('gufi'):
        stmt = stmt.where(TbfmProjection.gufi == subscription['gufi'])
    if subscription.get('tma_id'):
        stmt = stmt.where(TbfmProjection.tma_id == subscription['tma_id'])
    if subscription.get('projection_type'):
        stmt = stmt.where(func.upper(TbfmProjection.projection_type) == subscription['projection_type'])
    if subscription.get('projection_key'):
        stmt = stmt.where(TbfmProjection.projection_key == subscription['projection_key'])

    stmt = stmt.order_by(TbfmProjection.updated_at.desc()).limit(limit)

    async with TbfmAsyncSessionLocal() as session:
        rows = list((await session.execute(stmt)).scalars())
    snapshot: list[dict[str, Any]] = []
    for row in rows:
        projection = {
            'projection_key': row.projection_key,
            'projection_type': row.projection_type,
            'acid': row.acid,
            'gufi': row.gufi,
            'tma_id': row.tma_id,
            'flight_ref': row.flight_ref,
            'msg_type': row.msg_type,
            'source_facility': row.source_facility,
            'source_time': row.source_time,
            'data': row.data,
            'updated_at': _iso_or_none(row.updated_at),
        }
        snapshot.append(projection)
    return snapshot


def _extract_canonical_flights(
    parsed_payload: dict[str, Any],
    *,
    payload_type: str | None,
    stream_id: str | None,
    source: str | None,
    updated_at: datetime | None,
    callsign: str | None,
    gufi: str | None,
    destination: str | None,
    airport: str | None,
) -> list[dict[str, Any]]:
    messages = parsed_payload.get('messages')
    if not isinstance(messages, list):
        return []
    matches: list[dict[str, Any]] = []
    for message in messages:
        if not isinstance(message, dict):
            continue
        flight = message.get('flight')
        if not isinstance(flight, dict):
            continue
        if not _flight_matches_subscription(
            flight,
            callsign=callsign,
            gufi=gufi,
            destination=destination,
            airport=airport,
        ):
            continue
        matches.append(
            _canonical_flight(
                flight,
                payload_type=payload_type,
                stream_id=stream_id,
                source=source,
                updated_at=updated_at,
            )
        )
    return matches


def _filter_sfdps_parsed_messages(
    parsed_payload: dict[str, Any], *, callsign: str | None, gufi: str | None, destination: str | None, airport: str | None
) -> dict[str, Any] | None:
    messages = parsed_payload.get('messages')
    if not isinstance(messages, list):
        return None

    filtered_messages: list[dict[str, Any]] = []
    for message in messages:
        if not isinstance(message, dict):
            continue
        flight = message.get('flight')
        if not isinstance(flight, dict):
            continue
        if _flight_matches_subscription(
            flight,
            callsign=callsign,
            gufi=gufi,
            destination=destination,
            airport=airport,
        ):
            filtered_messages.append(message)

    if not filtered_messages:
        return None

    filtered_payload = dict(parsed_payload)
    filtered_payload['messages'] = filtered_messages
    filtered_payload['message_count'] = len(filtered_messages)
    return filtered_payload


async def _fetch_snapshot(
    session: AsyncSession,
    *,
    callsign: str | None,
    gufi: str | None,
    destination: str | None,
    airport: str | None,
    limit: int = 25,
) -> list[dict[str, Any]]:
    stmt = select(FlightCurrent).order_by(FlightCurrent.updated_at.desc()).limit(limit)
    if gufi:
        stmt = stmt.where(FlightCurrent.gufi == gufi)
    if callsign:
        stmt = stmt.where(FlightCurrent.flight_id == callsign.upper())
    if destination:
        stmt = stmt.where(FlightCurrent.arrival_airport == destination.upper())
    if airport:
        stmt = stmt.where(
            or_(
                FlightCurrent.arrival_airport == airport.upper(),
                FlightCurrent.departure_airport == airport.upper(),
            )
        )
    result = await session.execute(stmt)
    rows = list(result.scalars())
    flights: list[dict[str, Any]] = []
    for row in rows:
        base_flight: dict[str, Any] = row.last_payload if isinstance(row.last_payload, dict) else {}
        if not base_flight:
            base_flight = {
                'gufi': row.gufi,
                'operator': row.operator,
                'flight_status': row.status,
                'arrival': {
                    'airport': row.arrival_airport,
                    'estimated_runway_time': _iso_or_none(row.arrival_estimated_time),
                },
                'departure': {
                    'airport': row.departure_airport,
                    'actual_runway_time': _iso_or_none(row.departure_actual_time),
                },
                'meta': {'timestamp': _iso_or_none(row.source_timestamp)},
                'flight_identification': {'aircraft_identification': row.flight_id},
            }
        flights.append(
            _canonical_flight(
                base_flight,
                payload_type=row.payload_type,
                stream_id=row.last_stream_id,
                source='solace-jms',
                updated_at=row.updated_at,
            )
        )

    return [
        flight
        for flight in flights
        if _flight_matches_subscription(
            flight,
            callsign=callsign,
            gufi=gufi,
            destination=destination,
            airport=airport,
        )
    ]


@app.get('/health', response_model=HealthResponse)
async def health() -> HealthResponse:
    db_status = 'ok'
    redis_status = 'ok'
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(text('SELECT 1'))
    except Exception:
        db_status = 'error'
    try:
        sfdps_redis = await redis_manager.connect_sfdps()
        tfms_redis = await redis_manager.connect_tfms()
        tbfm_redis = await redis_manager.connect_tbfm()
        await sfdps_redis.ping()
        await tfms_redis.ping()
        await tbfm_redis.ping()
    except Exception:
        redis_status = 'error'
    return HealthResponse(status='ok' if db_status == redis_status == 'ok' else 'degraded', redis=redis_status, database=db_status)


@app.post('/ingest/raw')
async def ingest_raw(request: IngestRequest, session: AsyncSession = Depends(get_db_session)) -> dict:
    stream_id = f'manual-{uuid4()}'
    return await ingest_xml(session, redis_manager, stream_id=stream_id, source=request.source, xml_text=request.xml)


@app.post('/ingest/tfms/raw')
async def ingest_tfms_raw(request: Request) -> dict[str, Any]:
    content_type = (request.headers.get('content-type') or '').lower()
    queue_header = request.headers.get('x-queue-name')
    metadata_header = request.headers.get('x-metadata-json')

    xml_text: str
    queue_name: str | None = queue_header
    metadata: dict[str, Any] = {}

    if 'application/json' in content_type:
        raw_body = await request.body()
        try:
            body = json.loads(raw_body)
        except json.JSONDecodeError:
            xml_text = raw_body.decode('utf-8', errors='replace')
        else:
            if isinstance(body, dict) and isinstance(body.get('xml_b64'), str):
                try:
                    xml_text = base64.b64decode(body['xml_b64'], validate=True).decode('utf-8', errors='replace')
                except (ValueError, binascii.Error):
                    _record_tfms_ingest(status='ignored', reason='invalid_base64', queue_name=queue_name)
                    logger.warning('Ignoring TFMS payload with invalid base64 queue=%s', queue_name)
                    return {'status': 'ignored', 'reason': 'invalid_base64'}
                queue_name = body.get('queue_name') or queue_name
                maybe_metadata = body.get('metadata')
                metadata = maybe_metadata if isinstance(maybe_metadata, dict) else {}
            else:
                payload = TfmsIngestRequest.model_validate(body)
                xml_text = payload.xml
                queue_name = payload.queue_name or queue_name
                metadata = payload.metadata
    else:
        raw_body = await request.body()
        xml_text = raw_body.decode('utf-8', errors='replace')
        if metadata_header:
            try:
                parsed = json.loads(metadata_header)
                if isinstance(parsed, dict):
                    metadata = parsed
            except json.JSONDecodeError:
                metadata = {'metadata_parse_error': True}

    if not xml_text.strip():
        _record_tfms_ingest(status='ignored', reason='empty_xml', queue_name=queue_name, xml_length=len(xml_text))
        logger.warning('Ignoring empty TFMS XML queue=%s', queue_name)
        return {'status': 'ignored', 'reason': 'empty_xml'}

    if not xml_text.lstrip().startswith('<'):
        _record_tfms_ingest(status='ignored', reason='non_xml_payload', queue_name=queue_name, xml_length=len(xml_text))
        logger.warning('Ignoring non-XML TFMS payload queue=%s prefix=%r', queue_name, xml_text[:80])
        return {'status': 'ignored', 'reason': 'non_xml_payload'}

    async with TfmsAsyncSessionLocal() as session:
        try:
            result = await ingest_tfms_xml(
                session,
                redis_manager,
                xml_text=xml_text,
                queue_name=queue_name,
                metadata=metadata,
            )
            _record_tfms_ingest(
                status='accepted',
                queue_name=queue_name,
                payload_type=result.get('payload_type'),
                xml_length=len(xml_text),
            )
            return result
        except ParseError:
            _record_tfms_ingest(status='ignored', reason='xml_parse_error', queue_name=queue_name, xml_length=len(xml_text))
            logger.warning('Ignoring TFMS XML parse error queue=%s', queue_name)
            return {'status': 'ignored', 'reason': 'xml_parse_error'}


@app.post('/ingest/tbfm/raw')
async def ingest_tbfm_raw(request: TbfmIngestRequest) -> dict[str, Any]:
    xml_text = request.xml
    if not xml_text.strip():
        _record_tbfm_ingest(status='ignored', reason='empty_xml', queue_name=request.queue_name, xml_length=len(xml_text))
        return {'status': 'ignored', 'reason': 'empty_xml'}
    if not xml_text.lstrip().startswith('<'):
        _record_tbfm_ingest(status='ignored', reason='non_xml_payload', queue_name=request.queue_name, xml_length=len(xml_text))
        return {'status': 'ignored', 'reason': 'non_xml_payload'}
    async with TbfmAsyncSessionLocal() as session:
        try:
            result = await ingest_tbfm_xml(
                session,
                redis_manager,
                xml_text=xml_text,
                queue_name=request.queue_name,
                metadata=request.metadata,
            )
            _record_tbfm_ingest(
                status='accepted',
                queue_name=request.queue_name,
                payload_type=result.get('payload_type'),
                xml_length=len(xml_text),
            )
            return result
        except (ParseError, ValueError):
            _record_tbfm_ingest(status='ignored', reason='xml_parse_error', queue_name=request.queue_name, xml_length=len(xml_text))
            return {'status': 'ignored', 'reason': 'xml_parse_error'}


@app.get('/tfms/ingest/stats')
async def tfms_ingest_stats() -> dict[str, Any]:
    return {
        'counters': dict(_tfms_ingest_counters),
        'recent': list(_tfms_ingest_recent),
    }


@app.get('/tbfm/ingest/stats')
async def tbfm_ingest_stats() -> dict[str, Any]:
    return {
        'counters': dict(_tbfm_ingest_counters),
        'recent': list(_tbfm_ingest_recent),
    }


@app.get('/events', response_model=list[EventResponse])
async def list_events(limit: int = 50, session: AsyncSession = Depends(get_db_session)) -> list[EventResponse]:
    rows = await get_recent_events(session, limit)
    return [
        EventResponse(
            stream_id=row.stream_id,
            payload_type=row.payload_type,
            source=row.source,
            gufi=row.gufi,
            flight_id=row.flight_id,
            parsed_json=row.parsed_json,
            flights=_extract_canonical_flights(
                row.parsed_json,
                payload_type=row.payload_type,
                stream_id=row.stream_id,
                source=row.source,
                updated_at=row.received_at,
                callsign=None,
                gufi=None,
                destination=None,
                airport=None,
            ),
            received_at=row.received_at,
        )
        for row in rows
    ]


@app.get('/flights/current', response_model=list[dict[str, Any]])
async def list_current_flights(
    limit: int = 50,
    callsign: str | None = None,
    gufi: str | None = None,
    destination: str | None = None,
    destino: str | None = None,
    airport: str | None = None,
    aeropuerto: str | None = None,
    aereopuerto: str | None = None,
    session: AsyncSession = Depends(get_db_session),
) -> list[dict[str, Any]]:
    destination_filter = _normalize_destination_filter(
        {
            'destination': destination,
            'destino': destino,
        }
    )
    airport_filter = _normalize_airport_filter(
        {
            'airport': airport,
            'aeropuerto': aeropuerto,
            'aereopuerto': aereopuerto,
        }
    )
    callsign_filter = callsign.upper() if isinstance(callsign, str) and callsign else None
    gufi_filter = gufi if isinstance(gufi, str) and gufi else None

    rows = await get_current_flights(session, limit)
    flights: list[dict[str, Any]] = []
    for row in rows:
        base_flight: dict[str, Any] = row.last_payload if isinstance(row.last_payload, dict) else {}
        if not base_flight:
            base_flight = {
                'gufi': row.gufi,
                'operator': row.operator,
                'flight_status': row.status,
                'arrival': {
                    'airport': row.arrival_airport,
                    'estimated_runway_time': _iso_or_none(row.arrival_estimated_time),
                },
                'departure': {
                    'airport': row.departure_airport,
                    'actual_runway_time': _iso_or_none(row.departure_actual_time),
                },
                'meta': {'timestamp': _iso_or_none(row.source_timestamp)},
                'flight_identification': {'aircraft_identification': row.flight_id},
            }
        flights.append(
            _canonical_flight(
                base_flight,
                payload_type=row.payload_type,
                stream_id=row.last_stream_id,
                source='solace-jms',
                updated_at=row.updated_at,
            )
        )
    return [
        flight
        for flight in flights
        if _flight_matches_subscription(
            flight,
            callsign=callsign_filter,
            gufi=gufi_filter,
            destination=destination_filter,
            airport=airport_filter,
        )
    ]


@app.get('/flights/current/{gufi}', response_model=dict[str, Any])
async def get_current_flight(gufi: str, session: AsyncSession = Depends(get_db_session)) -> dict[str, Any]:
    row = await session.get(FlightCurrent, gufi)
    if row is None:
        raise HTTPException(status_code=404, detail='Flight not found')
    base_flight: dict[str, Any] = row.last_payload if isinstance(row.last_payload, dict) else {}
    if not base_flight:
        base_flight = {
            'gufi': row.gufi,
            'operator': row.operator,
            'flight_status': row.status,
            'arrival': {
                'airport': row.arrival_airport,
                'estimated_runway_time': _iso_or_none(row.arrival_estimated_time),
            },
            'departure': {
                'airport': row.departure_airport,
                'actual_runway_time': _iso_or_none(row.departure_actual_time),
            },
            'meta': {'timestamp': _iso_or_none(row.source_timestamp)},
            'flight_identification': {'aircraft_identification': row.flight_id},
        }
    return _canonical_flight(
        base_flight,
        payload_type=row.payload_type,
        stream_id=row.last_stream_id,
        source='solace-jms',
        updated_at=row.updated_at,
    )


@app.get('/tfms/events', response_model=list[TfmsEventResponse])
async def list_tfms_events(limit: int = 100) -> list[TfmsEventResponse]:
    async with TfmsAsyncSessionLocal() as session:
        rows = await get_tfms_events(session, limit)
    return [
        TfmsEventResponse(
            id=row.id,
            queue_name=row.queue_name,
            payload_type=row.payload_type,
            root_tag=row.root_tag,
            source_facility=row.source_facility,
            msg_type=row.msg_type,
            flight_ref=row.flight_ref,
            acid=row.acid,
            gufi=row.gufi,
            parsed_json=row.parsed_json,
            created_at=row.created_at,
        )
        for row in rows
    ]


@app.get('/tfms/projections', response_model=list[TfmsProjectionResponse])
async def list_tfms_projections(limit: int = 100) -> list[TfmsProjectionResponse]:
    async with TfmsAsyncSessionLocal() as session:
        rows = await get_tfms_projections(session, limit)
    return [
        TfmsProjectionResponse(
            projection_key=row.projection_key,
            projection_type=row.projection_type,
            acid=row.acid,
            gufi=row.gufi,
            flight_ref=row.flight_ref,
            msg_type=row.msg_type,
            source_facility=row.source_facility,
            source_timestamp=row.source_timestamp,
            data=row.data,
            updated_at=row.updated_at,
        )
        for row in rows
    ]


@app.get('/tfms/projections/{projection_key}', response_model=TfmsProjectionResponse)
async def get_tfms_projection(projection_key: str) -> TfmsProjectionResponse:
    async with TfmsAsyncSessionLocal() as session:
        row = await session.get(TfmsProjection, projection_key)
    if row is None:
        raise HTTPException(status_code=404, detail='TFMS projection not found')
    return TfmsProjectionResponse(
        projection_key=row.projection_key,
        projection_type=row.projection_type,
        acid=row.acid,
        gufi=row.gufi,
        flight_ref=row.flight_ref,
        msg_type=row.msg_type,
        source_facility=row.source_facility,
        source_timestamp=row.source_timestamp,
        data=row.data,
        updated_at=row.updated_at,
    )


@app.get('/tbfm/events', response_model=list[TbfmEventResponse])
async def list_tbfm_events(limit: int = 100) -> list[TbfmEventResponse]:
    async with TbfmAsyncSessionLocal() as session:
        rows = await get_tbfm_events(session, limit)
    return [
        TbfmEventResponse(
            id=row.id,
            queue_name=row.queue_name,
            payload_type=row.payload_type,
            root_tag=row.root_tag,
            source_facility=row.source_facility,
            msg_type=row.msg_type,
            flight_ref=row.flight_ref,
            acid=row.acid,
            gufi=row.gufi,
            tma_id=row.tma_id,
            source_time=row.source_time,
            parsed_json=row.parsed_json,
            created_at=row.created_at,
        )
        for row in rows
    ]


@app.get('/tbfm/projections', response_model=list[TbfmProjectionResponse])
async def list_tbfm_projections(limit: int = 100) -> list[TbfmProjectionResponse]:
    async with TbfmAsyncSessionLocal() as session:
        rows = await get_tbfm_projections(session, limit)
    return [
        TbfmProjectionResponse(
            projection_key=row.projection_key,
            projection_type=row.projection_type,
            acid=row.acid,
            gufi=row.gufi,
            tma_id=row.tma_id,
            flight_ref=row.flight_ref,
            msg_type=row.msg_type,
            source_facility=row.source_facility,
            source_time=row.source_time,
            data=row.data,
            updated_at=row.updated_at,
        )
        for row in rows
    ]


@app.get('/tbfm/projections/{projection_key}', response_model=TbfmProjectionResponse)
async def get_tbfm_projection(projection_key: str) -> TbfmProjectionResponse:
    async with TbfmAsyncSessionLocal() as session:
        row = await session.get(TbfmProjection, projection_key)
    if row is None:
        raise HTTPException(status_code=404, detail='TBFM projection not found')
    return TbfmProjectionResponse(
        projection_key=row.projection_key,
        projection_type=row.projection_type,
        acid=row.acid,
        gufi=row.gufi,
        tma_id=row.tma_id,
        flight_ref=row.flight_ref,
        msg_type=row.msg_type,
        source_facility=row.source_facility,
        source_time=row.source_time,
        data=row.data,
        updated_at=row.updated_at,
    )


@app.websocket('/ws/flights')
async def flights_websocket(websocket: WebSocket) -> None:
    await websocket.accept()

    callsign = websocket.query_params.get('callsign')
    gufi = websocket.query_params.get('gufi')
    destination = _normalize_destination_filter(websocket.query_params)
    airport = _normalize_airport_filter(websocket.query_params)

    if callsign:
        callsign = callsign.upper()
    pubsub = None

    async def _reconnect_pubsub() -> Any:
        redis = await redis_manager.connect_sfdps()
        new_pubsub = redis.pubsub()
        await new_pubsub.subscribe(settings.parsed_channel_name)
        return new_pubsub

    try:
        pubsub = await _reconnect_pubsub()
    except Exception as exc:
        await websocket.send_json(
            {
                'type': 'error',
                'error': 'redis_unavailable',
                'detail': str(exc),
            }
        )
        await websocket.close(code=1011)
        return

    async with AsyncSessionLocal() as session:
        snapshot = await _fetch_snapshot(
            session,
            callsign=callsign,
            gufi=gufi,
            destination=destination,
            airport=airport,
        )
    await websocket.send_json(
        {
            'type': 'snapshot',
            'subscription': {'callsign': callsign, 'gufi': gufi, 'destination': destination, 'airport': airport},
            'count': len(snapshot),
            'flights': snapshot,
        }
    )

    try:
        while True:
            try:
                message = await asyncio.wait_for(websocket.receive_json(), timeout=0.2)
                if isinstance(message, dict) and message.get('action') == 'subscribe':
                    callsign = message.get('callsign')
                    gufi = message.get('gufi')
                    destination = _normalize_destination_filter(message)
                    airport = _normalize_airport_filter(message)
                    callsign = callsign.upper() if isinstance(callsign, str) and callsign else None
                    gufi = gufi if isinstance(gufi, str) and gufi else None
                    if message.get('snapshot', True):
                        async with AsyncSessionLocal() as session:
                            snapshot = await _fetch_snapshot(
                                session,
                                callsign=callsign,
                                gufi=gufi,
                                destination=destination,
                                airport=airport,
                            )
                        await websocket.send_json(
                            {
                                'type': 'snapshot',
                                'subscription': {
                                    'callsign': callsign,
                                    'gufi': gufi,
                                    'destination': destination,
                                    'airport': airport,
                                },
                                'count': len(snapshot),
                                'flights': snapshot,
                            }
                        )
                    else:
                        await websocket.send_json(
                            {
                                'type': 'subscribed',
                                'subscription': {
                                    'callsign': callsign,
                                    'gufi': gufi,
                                    'destination': destination,
                                    'airport': airport,
                                },
                            }
                        )
            except asyncio.TimeoutError:
                pass
            except json.JSONDecodeError:
                pass

            drained = False
            for _ in range(200):
                try:
                    payload = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.0)
                except Exception:
                    logger.exception('Flights websocket pubsub read failed; reconnecting')
                    try:
                        if pubsub is not None:
                            await pubsub.close()
                    except Exception:
                        pass
                    try:
                        pubsub = await _reconnect_pubsub()
                        await websocket.send_json({'type': 'warning', 'warning': 'redis_reconnected'})
                    except Exception as exc:
                        await websocket.send_json({'type': 'error', 'error': 'redis_unavailable', 'detail': str(exc)})
                        await asyncio.sleep(1)
                    break

                if not payload:
                    break
                drained = True

                raw_data = payload.get('data')
                if not isinstance(raw_data, str):
                    continue

                try:
                    parsed_event = json.loads(raw_data)
                except json.JSONDecodeError:
                    continue
                parsed_payload = parsed_event.get('parsed') if isinstance(parsed_event, dict) else None
                if not isinstance(parsed_payload, dict):
                    continue

                flights = _extract_canonical_flights(
                    parsed_payload,
                    payload_type=parsed_event.get('payload_type') if isinstance(parsed_event, dict) else None,
                    stream_id=parsed_event.get('stream_id') if isinstance(parsed_event, dict) else None,
                    source=parsed_event.get('source') if isinstance(parsed_event, dict) else None,
                    updated_at=None,
                    callsign=callsign,
                    gufi=gufi,
                    destination=destination,
                    airport=airport,
                )
                if flights:
                    filtered_payload = _filter_sfdps_parsed_messages(
                        parsed_payload,
                        callsign=callsign,
                        gufi=gufi,
                        destination=destination,
                        airport=airport,
                    )
                    filtered_event = dict(parsed_event)
                    if filtered_payload is not None:
                        filtered_event['parsed'] = filtered_payload
                    await websocket.send_json(
                        {
                            'type': 'event',
                            'subscription': {
                                'callsign': callsign,
                                'gufi': gufi,
                                'destination': destination,
                                'airport': airport,
                            },
                            'event': filtered_event,
                            'flights': flights,
                        }
                    )

            if not drained:
                await asyncio.sleep(0.05)
    except WebSocketDisconnect:
        return
    finally:
        if pubsub is not None:
            try:
                await pubsub.unsubscribe(settings.parsed_channel_name)
            finally:
                await pubsub.close()


@app.websocket('/ws/tfms')
async def tfms_websocket(websocket: WebSocket) -> None:
    await websocket.accept()

    subscription = _normalize_tfms_subscription(dict(websocket.query_params))
    limit_raw = websocket.query_params.get('limit')
    limit = int(limit_raw) if isinstance(limit_raw, str) and limit_raw.isdigit() else 100

    pubsub = None

    async def _reconnect_pubsub() -> Any:
        redis = await redis_manager.connect_tfms()
        new_pubsub = redis.pubsub()
        await new_pubsub.subscribe(settings.tfms_events_channel_name, settings.tfms_projections_channel_name)
        return new_pubsub

    try:
        pubsub = await _reconnect_pubsub()
    except Exception as exc:
        await websocket.send_json({'type': 'error', 'error': 'redis_unavailable', 'detail': str(exc)})
        await websocket.close(code=1011)
        return

    snapshot = await _fetch_tfms_snapshot(subscription, limit=limit)
    await websocket.send_json(
        {
            'type': 'snapshot',
            'subscription': subscription,
            'count': len(snapshot),
            'projections': snapshot,
        }
    )

    try:
        while True:
            try:
                message = await asyncio.wait_for(websocket.receive_json(), timeout=0.2)
                if isinstance(message, dict) and message.get('action') == 'subscribe':
                    subscription = _normalize_tfms_subscription(message)
                    if message.get('snapshot', True):
                        snapshot = await _fetch_tfms_snapshot(subscription, limit=limit)
                        await websocket.send_json(
                            {
                                'type': 'snapshot',
                                'subscription': subscription,
                                'count': len(snapshot),
                                'projections': snapshot,
                            }
                        )
                    else:
                        await websocket.send_json({'type': 'subscribed', 'subscription': subscription})
            except asyncio.TimeoutError:
                pass
            except json.JSONDecodeError:
                pass

            drained = False
            for _ in range(300):
                try:
                    payload = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.0)
                except Exception:
                    logger.exception('TFMS websocket pubsub read failed; reconnecting')
                    try:
                        if pubsub is not None:
                            await pubsub.close()
                    except Exception:
                        pass
                    try:
                        pubsub = await _reconnect_pubsub()
                        await websocket.send_json({'type': 'warning', 'warning': 'redis_reconnected'})
                    except Exception as exc:
                        await websocket.send_json({'type': 'error', 'error': 'redis_unavailable', 'detail': str(exc)})
                        await asyncio.sleep(1)
                    break

                if not payload:
                    break
                drained = True

                channel_raw = payload.get('channel')
                channel = channel_raw.decode() if isinstance(channel_raw, bytes) else channel_raw
                raw_data = payload.get('data')
                if not isinstance(raw_data, str):
                    continue
                try:
                    obj = json.loads(raw_data)
                except json.JSONDecodeError:
                    continue
                if not isinstance(obj, dict):
                    continue

                if channel == settings.tfms_events_channel_name:
                    fields = _tfms_event_fields(obj)
                    if _tfms_matches_subscription(fields, subscription):
                        await websocket.send_json({'type': 'event', 'subscription': subscription, 'event': obj})
                elif channel == settings.tfms_projections_channel_name:
                    fields = _tfms_projection_fields(obj)
                    if _tfms_matches_subscription(fields, subscription):
                        await websocket.send_json({'type': 'projection', 'subscription': subscription, 'projection': obj})

            if not drained:
                await asyncio.sleep(0.05)
    except WebSocketDisconnect:
        return
    finally:
        if pubsub is not None:
            try:
                await pubsub.unsubscribe(settings.tfms_events_channel_name, settings.tfms_projections_channel_name)
            finally:
                await pubsub.close()


@app.websocket('/ws/tbfm')
async def tbfm_websocket(websocket: WebSocket) -> None:
    await websocket.accept()

    subscription = _normalize_tbfm_subscription(dict(websocket.query_params))
    limit_raw = websocket.query_params.get('limit')
    limit = int(limit_raw) if isinstance(limit_raw, str) and limit_raw.isdigit() else 100

    pubsub = None

    async def _reconnect_pubsub() -> Any:
        redis = await redis_manager.connect_tbfm()
        new_pubsub = redis.pubsub()
        await new_pubsub.subscribe(settings.tbfm_events_channel_name, settings.tbfm_projections_channel_name)
        return new_pubsub

    try:
        pubsub = await _reconnect_pubsub()
    except Exception as exc:
        await websocket.send_json({'type': 'error', 'error': 'redis_unavailable', 'detail': str(exc)})
        await websocket.close(code=1011)
        return

    snapshot = await _fetch_tbfm_snapshot(subscription, limit=limit)
    await websocket.send_json(
        {
            'type': 'snapshot',
            'subscription': subscription,
            'count': len(snapshot),
            'projections': snapshot,
        }
    )

    try:
        while True:
            try:
                message = await asyncio.wait_for(websocket.receive_json(), timeout=0.2)
                if isinstance(message, dict) and message.get('action') == 'subscribe':
                    subscription = _normalize_tbfm_subscription(message)
                    if message.get('snapshot', True):
                        snapshot = await _fetch_tbfm_snapshot(subscription, limit=limit)
                        await websocket.send_json(
                            {
                                'type': 'snapshot',
                                'subscription': subscription,
                                'count': len(snapshot),
                                'projections': snapshot,
                            }
                        )
                    else:
                        await websocket.send_json({'type': 'subscribed', 'subscription': subscription})
            except asyncio.TimeoutError:
                pass
            except json.JSONDecodeError:
                pass

            drained = False
            for _ in range(300):
                try:
                    payload = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.0)
                except Exception:
                    logger.exception('TBFM websocket pubsub read failed; reconnecting')
                    try:
                        if pubsub is not None:
                            await pubsub.close()
                    except Exception:
                        pass
                    try:
                        pubsub = await _reconnect_pubsub()
                        await websocket.send_json({'type': 'warning', 'warning': 'redis_reconnected'})
                    except Exception as exc:
                        await websocket.send_json({'type': 'error', 'error': 'redis_unavailable', 'detail': str(exc)})
                        await asyncio.sleep(1)
                    break

                if not payload:
                    break
                drained = True

                channel_raw = payload.get('channel')
                channel = channel_raw.decode() if isinstance(channel_raw, bytes) else channel_raw
                raw_data = payload.get('data')
                if not isinstance(raw_data, str):
                    continue
                try:
                    obj = json.loads(raw_data)
                except json.JSONDecodeError:
                    continue
                if not isinstance(obj, dict):
                    continue

                if channel == settings.tbfm_events_channel_name:
                    fields = _tbfm_event_fields(obj)
                    if _tbfm_matches_subscription(fields, subscription):
                        await websocket.send_json({'type': 'event', 'subscription': subscription, 'event': obj})
                elif channel == settings.tbfm_projections_channel_name:
                    fields = _tbfm_projection_fields(obj)
                    if _tbfm_matches_subscription(fields, subscription):
                        await websocket.send_json({'type': 'projection', 'subscription': subscription, 'projection': obj})

            if not drained:
                await asyncio.sleep(0.05)
    except WebSocketDisconnect:
        return
    finally:
        if pubsub is not None:
            try:
                await pubsub.unsubscribe(settings.tbfm_events_channel_name, settings.tbfm_projections_channel_name)
            finally:
                await pubsub.close()

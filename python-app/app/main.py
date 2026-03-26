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
from sqlalchemy import Date, cast, func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from .config import settings
from .db import Base, AsyncSessionLocal, engine, get_db_session
from .models import FlightCurrent
from .redis_client import RedisManager
from .schemas import EventResponse, HealthResponse, IngestRequest
from .service import get_recent_events, ingest_xml
from .tfms_db import TfmsAsyncSessionLocal, tfms_engine
from .tfms_models import TfmsBase, TfmsEvent, TfmsProjection
from .tfms_parser_adapter import parse_tfms_xml
from .tfms_payload_utils import only_raw_fields, projection_raw_by_key_from_xml, strip_raw_fields
from .tfms_schemas import TfmsEventResponse, TfmsIngestRequest, TfmsProjectionResponse
from .tfms_service import get_tfms_events, get_tfms_projections, ingest_tfms_xml
from .tbfm_db import TbfmAsyncSessionLocal, tbfm_engine
from .tbfm_models import TbfmBase, TbfmEvent, TbfmProjection
from .tbfm_parser_adapter import parse_tbfm_xml
from .tbfm_payload_utils import only_raw_fields as only_tbfm_raw_fields
from .tbfm_payload_utils import projection_raw_by_key_from_xml as tbfm_projection_raw_by_key_from_xml
from .tbfm_payload_utils import strip_raw_fields as strip_tbfm_raw_fields
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
        await conn.execute(text('ALTER TABLE tfms_events ADD COLUMN IF NOT EXISTS source_timestamp TIMESTAMPTZ'))
        await conn.execute(text('CREATE INDEX IF NOT EXISTS ix_tfms_events_source_timestamp ON tfms_events (source_timestamp)'))
        await conn.execute(text('CREATE INDEX IF NOT EXISTS ix_tfms_projections_source_timestamp ON tfms_projections (source_timestamp)'))
    async with tbfm_engine.begin() as conn:
        await conn.run_sync(TbfmBase.metadata.create_all)
        await conn.execute(text('CREATE INDEX IF NOT EXISTS ix_tbfm_events_source_time ON tbfm_events (source_time)'))
        await conn.execute(text('CREATE INDEX IF NOT EXISTS ix_tbfm_projections_source_time ON tbfm_projections (source_time)'))
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
    flight: dict[str, Any],
    *,
    callsign: str | None,
    gufi: str | None,
    destination: str | None,
    airport: str | None,
    departure: str | None,
    date: str | None,
) -> bool:
    flight_gufi = flight.get('gufi')
    flight_destination = (flight.get('arrival') or {}).get('airport')
    flight_departure = (flight.get('departure') or {}).get('airport')
    flight_identification = flight.get('flight_identification') or {}
    aircraft_identification = flight_identification.get('aircraft_identification')
    timestamp = (flight.get('meta') or {}).get('timestamp') or flight.get('source_timestamp') or flight.get('updated_at')
    airport_matches = True
    departure_matches = True
    date_matches = True
    if airport is not None:
        airport_matches = (
            _matches_filter(flight_destination, airport, case_insensitive=True)
            or _matches_filter(flight_departure, airport, case_insensitive=True)
        )
    if departure is not None:
        departure_matches = _matches_filter(flight_departure, departure, case_insensitive=True)
    if date is not None:
        date_matches = isinstance(timestamp, str) and timestamp.startswith(date)
    return (
        _matches_filter(aircraft_identification, callsign, case_insensitive=True)
        and _matches_filter(flight_gufi, gufi)
        and _matches_filter(flight_destination, destination, case_insensitive=True)
        and airport_matches
        and departure_matches
        and date_matches
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
    destination = payload.get('destination')
    if isinstance(destination, str) and destination:
        return destination.upper()
    return None


def _normalize_airport_filter(payload: dict[str, Any]) -> str | None:
    airport = payload.get('airport')
    if isinstance(airport, str) and airport:
        return airport.upper()
    return None


def _normalize_departure_filter(payload: dict[str, Any]) -> str | None:
    departure = payload.get('departure')
    if isinstance(departure, str) and departure:
        return departure.upper()
    return None


def _normalize_date_filter(value: Any) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    return datetime.strptime(value, '%Y-%m-%d').date().isoformat()


def _split_csv_values(value: str | None, *, upper: bool = False) -> set[str]:
    if not value:
        return set()
    items = [item.strip() for item in value.split(',') if item.strip()]
    if upper:
        return {item.upper() for item in items}
    return set(items)


def _parse_iso_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        return None


def _matches_set(value: str | None, allowed: set[str], *, case_insensitive: bool = False) -> bool:
    if not allowed:
        return True
    if value is None:
        return False
    if case_insensitive:
        return value.upper() in {item.upper() for item in allowed}
    return value in allowed


def _validate_time_basis(value: str | None) -> None:
    if value is None or value == 'source':
        return
    raise HTTPException(status_code=400, detail='invalid_time_basis')


def _resolve_sort(value: str) -> bool:
    normalized = value.lower()
    if normalized == 'desc':
        return True
    if normalized == 'asc':
        return False
    raise HTTPException(status_code=400, detail='invalid_sort')


def _resolve_time_bounds(
    *,
    date: str | None,
    from_date: str | None,
    to_date: str | None,
    from_ts: str | None,
    to_ts: str | None,
) -> tuple[datetime | None, datetime | None]:
    start: datetime | None = None
    end: datetime | None = None

    try:
        if date:
            base = datetime.strptime(date, '%Y-%m-%d').date()
            start = datetime.combine(base, datetime.min.time()).astimezone()
            end = datetime.combine(base, datetime.max.time()).astimezone()

        if from_date:
            d = datetime.strptime(from_date, '%Y-%m-%d').date()
            start = datetime.combine(d, datetime.min.time()).astimezone()
        if to_date:
            d = datetime.strptime(to_date, '%Y-%m-%d').date()
            end = datetime.combine(d, datetime.max.time()).astimezone()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail='invalid_date_format') from exc

    ts_start = _parse_iso_timestamp(from_ts)
    ts_end = _parse_iso_timestamp(to_ts)
    if from_ts and ts_start is None:
        raise HTTPException(status_code=400, detail='invalid_date_format')
    if to_ts and ts_end is None:
        raise HTTPException(status_code=400, detail='invalid_date_format')

    if ts_start is not None:
        start = ts_start
    if ts_end is not None:
        end = ts_end

    if start and end and start > end:
        raise HTTPException(status_code=400, detail='invalid_time_range')

    return start, end


TFMS_STATUS_FIELDS = {'flight_status', 'tmi_status', 'service_state'}
TBFM_STATUS_FIELDS = {'acs', 'fps'}


def _normalize_status_value(value: str) -> str:
    return value.strip().upper()


def _status_aliases(service: str, value: str) -> set[str]:
    normalized = _normalize_status_value(value)
    if normalized == 'ARRIVED':
        return {'COMPLETED'} if service == 'tfms' else {'LANDED'}
    return {normalized}


def _status_targets(service: str, values: set[str]) -> set[str]:
    targets: set[str] = set()
    for value in values:
        targets.update(_status_aliases(service, value))
    return targets


def _status_matches_any(current_values: list[str | None], targets: set[str]) -> bool:
    if not targets:
        return True
    normalized_current = {_normalize_status_value(value) for value in current_values if isinstance(value, str) and value.strip()}
    return bool(normalized_current.intersection(targets))


def _status_matches_all(current_values: list[str | None], targets: set[str]) -> bool:
    if not targets:
        return True
    normalized_current = {_normalize_status_value(value) for value in current_values if isinstance(value, str) and value.strip()}
    return targets.issubset(normalized_current)


def _paginate_list(items: list[Any], *, offset: int, limit: int) -> list[Any]:
    if offset >= len(items):
        return []
    return items[offset : offset + limit]


def _tfms_status_values_from_event_payload(payload: dict[str, Any]) -> dict[str, str | None]:
    first_msg = (payload.get('messages') or [None])[0] if isinstance(payload, dict) else None
    tmi_list = (first_msg or {}).get('tmiFlightDataList') if isinstance(first_msg, dict) else None
    statuses_list = payload.get('statuses') if isinstance(payload, dict) else None
    return {
        'flight_status': (((first_msg or {}).get('body') or {}).get('flightStatus') if isinstance(first_msg, dict) else None),
        'tmi_status': ((tmi_list or [{}])[0].get('status') if isinstance(tmi_list, list) and tmi_list else None),
        'service_state': ((statuses_list or [{}])[0].get('state') if isinstance(statuses_list, list) and statuses_list else None),
    }


def _tfms_status_values_from_projection_data(data: dict[str, Any]) -> dict[str, str | None]:
    body = data.get('body') if isinstance(data, dict) else None
    tmi = data.get('tmi') if isinstance(data, dict) else None
    status = data.get('status') if isinstance(data, dict) else None
    return {
        'flight_status': body.get('flightStatus') if isinstance(body, dict) else None,
        'tmi_status': tmi.get('status') if isinstance(tmi, dict) else None,
        'service_state': status.get('state') if isinstance(status, dict) else None,
    }


def _tbfm_status_values_from_payload(payload: dict[str, Any]) -> dict[str, str | None]:
    documents = payload.get('documents') if isinstance(payload, dict) else None
    first_doc = (documents or [None])[0] if isinstance(documents, list) and documents else None
    first_tma = (((first_doc or {}).get('tma') or [None])[0] if isinstance(first_doc, dict) else None)
    first_air = (((first_tma or {}).get('air') or [None])[0] if isinstance(first_tma, dict) else None)
    first_flt = (first_air.get('flt') if isinstance(first_air, dict) else None) or {}
    return {
        'acs': first_flt.get('acs') if isinstance(first_flt, dict) else None,
        'fps': first_flt.get('fps') if isinstance(first_flt, dict) else None,
    }


def _tbfm_status_values_from_projection_data(data: dict[str, Any]) -> dict[str, str | None]:
    air = data.get('air') if isinstance(data, dict) else None
    flt = air.get('flt') if isinstance(air, dict) else None
    return {
        'acs': flt.get('acs') if isinstance(flt, dict) else None,
        'fps': flt.get('fps') if isinstance(flt, dict) else None,
    }


def _tfms_event_airports(payload: dict[str, Any]) -> tuple[str | None, str | None]:
    first_msg = (payload.get('messages') or [None])[0] if isinstance(payload, dict) else None
    body = first_msg.get('body') if isinstance(first_msg, dict) else None
    qid = body.get('qualifiedAircraftId') if isinstance(body, dict) else None
    dep = ((qid.get('departurePoint') or {}).get('airport')) if isinstance(qid, dict) else None
    arr = ((qid.get('arrivalPoint') or {}).get('airport')) if isinstance(qid, dict) else None
    return dep, arr


def _tbfm_event_airports(payload: dict[str, Any]) -> tuple[str | None, str | None]:
    documents = payload.get('documents') if isinstance(payload, dict) else None
    first_doc = (documents or [None])[0] if isinstance(documents, list) and documents else None
    first_tma = (((first_doc or {}).get('tma') or [None])[0] if isinstance(first_doc, dict) else None)
    first_air = (((first_tma or {}).get('air') or [None])[0] if isinstance(first_tma, dict) else None)
    first_flt = (first_air.get('flt') if isinstance(first_air, dict) else None) or {}
    dep = first_flt.get('dap') if isinstance(first_flt, dict) else None
    arr = first_flt.get('apt') if isinstance(first_flt, dict) else None
    return dep, arr


def _tfms_event_source_timestamp(payload: dict[str, Any]) -> datetime | None:
    first_msg = (payload.get('messages') or [None])[0] if isinstance(payload, dict) else None
    if isinstance(first_msg, dict):
        ts = _parse_iso_timestamp(first_msg.get('sourceTimeStamp'))
        if ts is not None:
            return ts
    statuses = payload.get('statuses') if isinstance(payload, dict) else None
    first_status = (statuses or [None])[0] if isinstance(statuses, list) and statuses else None
    if isinstance(first_status, dict):
        return _parse_iso_timestamp(first_status.get('timeStamp'))
    return None


def _tbfm_event_source_timestamp(payload: dict[str, Any]) -> datetime | None:
    documents = payload.get('documents') if isinstance(payload, dict) else None
    first_doc = (documents or [None])[0] if isinstance(documents, list) and documents else None
    attrs = (first_doc.get('attributes') if isinstance(first_doc, dict) else None) or {}
    first_tma = (((first_doc or {}).get('tma') or [None])[0] if isinstance(first_doc, dict) else None)
    tma_attrs = (first_tma.get('attributes') if isinstance(first_tma, dict) else None) or {}
    return _parse_iso_timestamp(tma_attrs.get('msgTime') or attrs.get('envTime'))


def _airport_matches(*, departure_airport: str | None, arrival_airport: str | None, departure_values: set[str], airport_values: set[str]) -> bool:
    if departure_values and not _matches_set(departure_airport, departure_values, case_insensitive=True):
        return False
    if airport_values and not (
        _matches_set(departure_airport, airport_values, case_insensitive=True)
        or _matches_set(arrival_airport, airport_values, case_insensitive=True)
    ):
        return False
    return True


def _extract_tfms_projection_airports(data: dict[str, Any]) -> tuple[str | None, str | None]:
    body = data.get('body') if isinstance(data, dict) else None
    qid = body.get('qualifiedAircraftId') if isinstance(body, dict) else None
    if isinstance(qid, dict):
        dep = ((qid.get('departurePoint') or {}).get('airport')) if isinstance(qid.get('departurePoint'), dict) else None
        arr = ((qid.get('arrivalPoint') or {}).get('airport')) if isinstance(qid.get('arrivalPoint'), dict) else None
        return dep, arr
    flight = data.get('flight') if isinstance(data, dict) else None
    if isinstance(flight, dict):
        dep = ((flight.get('departurePoint') or {}).get('airport')) if isinstance(flight.get('departurePoint'), dict) else None
        arr = ((flight.get('arrivalPoint') or {}).get('airport')) if isinstance(flight.get('arrivalPoint'), dict) else None
        return dep, arr
    return None, None


def _extract_tbfm_projection_airports(data: dict[str, Any]) -> tuple[str | None, str | None]:
    dep = data.get('origin') if isinstance(data, dict) else None
    arr = data.get('destination') if isinstance(data, dict) else None
    if dep is None:
        dep = (((data.get('air') or {}).get('flt') or {}).get('dap')) if isinstance(data, dict) else None
    if arr is None:
        arr = (((data.get('air') or {}).get('flt') or {}).get('apt')) if isinstance(data, dict) else None
    return dep, arr


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
        'airport': _upper(payload.get('airport')),
        'departure': _upper(payload.get('departure')),
        'status': _text(payload.get('status')),
        'status_any': _text(payload.get('status_any')),
        'status_all': _text(payload.get('status_all')),
        'status_field': _text(payload.get('status_field')),
        'date': _text(payload.get('date')),
        'from_date': _text(payload.get('from_date')),
        'to_date': _text(payload.get('to_date')),
        'from_ts': _text(payload.get('from_ts')),
        'to_ts': _text(payload.get('to_ts')),
        'time_basis': _text(payload.get('time_basis')),
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
        'airport': _upper(payload.get('airport')),
        'departure': _upper(payload.get('departure')),
        'status': _text(payload.get('status')),
        'status_any': _text(payload.get('status_any')),
        'status_all': _text(payload.get('status_all')),
        'status_field': _text(payload.get('status_field')),
        'date': _text(payload.get('date')),
        'from_date': _text(payload.get('from_date')),
        'to_date': _text(payload.get('to_date')),
        'from_ts': _text(payload.get('from_ts')),
        'to_ts': _text(payload.get('to_ts')),
        'time_basis': _text(payload.get('time_basis')),
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


def _tfms_realtime_filters_match(payload: dict[str, Any], subscription: dict[str, Any], *, projection: bool) -> bool:
    try:
        _validate_time_basis(subscription.get('time_basis'))
        start, end = _resolve_time_bounds(
            date=subscription.get('date'),
            from_date=subscription.get('from_date'),
            to_date=subscription.get('to_date'),
            from_ts=subscription.get('from_ts'),
            to_ts=subscription.get('to_ts'),
        )
    except HTTPException:
        return False

    if subscription.get('status_field') and subscription.get('status_field') not in TFMS_STATUS_FIELDS:
        return False

    airport_values = _split_csv_values(subscription.get('airport'), upper=True)
    departure_values = _split_csv_values(subscription.get('departure'), upper=True)
    status_any_values = {
        _normalize_status_value(v) for v in _split_csv_values(subscription.get('status_any') or subscription.get('status'))
    }
    status_all_values = {_normalize_status_value(v) for v in _split_csv_values(subscription.get('status_all'))}
    any_targets = _status_targets('tfms', status_any_values)
    all_targets = _status_targets('tfms', status_all_values)

    if projection:
        source_time = _parse_iso_timestamp(payload.get('source_timestamp'))
        data = payload.get('data') if isinstance(payload.get('data'), dict) else {}
        statuses = _tfms_status_values_from_projection_data(data)
        dep_airport, arr_airport = _extract_tfms_projection_airports(data)
    else:
        parsed = payload.get('parsed') if isinstance(payload.get('parsed'), dict) else {}
        source_time = _tfms_event_source_timestamp(parsed)
        statuses = _tfms_status_values_from_event_payload(parsed)
        dep_airport, arr_airport = _tfms_event_airports(parsed)

    if start is not None and (source_time is None or source_time < start):
        return False
    if end is not None and (source_time is None or source_time > end):
        return False

    fields = [subscription.get('status_field')] if subscription.get('status_field') else ['flight_status', 'tmi_status', 'service_state']
    current_values = [statuses.get(field) for field in fields]
    if any_targets and not _status_matches_any(current_values, any_targets):
        return False
    if all_targets and not _status_matches_all(current_values, all_targets):
        return False

    return _airport_matches(
        departure_airport=dep_airport,
        arrival_airport=arr_airport,
        departure_values=departure_values,
        airport_values=airport_values,
    )


def _tbfm_realtime_filters_match(payload: dict[str, Any], subscription: dict[str, Any], *, projection: bool) -> bool:
    try:
        _validate_time_basis(subscription.get('time_basis'))
        start, end = _resolve_time_bounds(
            date=subscription.get('date'),
            from_date=subscription.get('from_date'),
            to_date=subscription.get('to_date'),
            from_ts=subscription.get('from_ts'),
            to_ts=subscription.get('to_ts'),
        )
    except HTTPException:
        return False

    if subscription.get('status_field') and subscription.get('status_field') not in TBFM_STATUS_FIELDS:
        return False

    airport_values = _split_csv_values(subscription.get('airport'), upper=True)
    departure_values = _split_csv_values(subscription.get('departure'), upper=True)
    status_any_values = {
        _normalize_status_value(v) for v in _split_csv_values(subscription.get('status_any') or subscription.get('status'))
    }
    status_all_values = {_normalize_status_value(v) for v in _split_csv_values(subscription.get('status_all'))}
    any_targets = _status_targets('tbfm', status_any_values)
    all_targets = _status_targets('tbfm', status_all_values)

    if projection:
        source_time = _parse_iso_timestamp(payload.get('source_time'))
        data = payload.get('data') if isinstance(payload.get('data'), dict) else {}
        statuses = _tbfm_status_values_from_projection_data(data)
        dep_airport, arr_airport = _extract_tbfm_projection_airports(data)
    else:
        parsed = payload.get('parsed') if isinstance(payload.get('parsed'), dict) else {}
        source_time = _tbfm_event_source_timestamp(parsed)
        statuses = _tbfm_status_values_from_payload(parsed)
        dep_airport, arr_airport = _tbfm_event_airports(parsed)

    if start is not None and (source_time is None or source_time < start):
        return False
    if end is not None and (source_time is None or source_time > end):
        return False

    fields = [subscription.get('status_field')] if subscription.get('status_field') else ['acs', 'fps']
    current_values = [statuses.get(field) for field in fields]
    if any_targets and not _status_matches_any(current_values, any_targets):
        return False
    if all_targets and not _status_matches_all(current_values, all_targets):
        return False

    return _airport_matches(
        departure_airport=dep_airport,
        arrival_airport=arr_airport,
        departure_values=departure_values,
        airport_values=airport_values,
    )


async def _fetch_tfms_snapshot(subscription: dict[str, Any], limit: int = 100) -> list[dict[str, Any]]:
    if subscription.get('payload_type') or subscription.get('queue_name'):
        return []

    _validate_time_basis(subscription.get('time_basis'))
    if subscription.get('status_field') and subscription.get('status_field') not in TFMS_STATUS_FIELDS:
        raise HTTPException(status_code=400, detail='invalid_status_field')
    start, end = _resolve_time_bounds(
        date=subscription.get('date'),
        from_date=subscription.get('from_date'),
        to_date=subscription.get('to_date'),
        from_ts=subscription.get('from_ts'),
        to_ts=subscription.get('to_ts'),
    )

    airport_values = _split_csv_values(subscription.get('airport'), upper=True)
    departure_values = _split_csv_values(subscription.get('departure'), upper=True)
    status_any_values = {
        _normalize_status_value(v) for v in _split_csv_values(subscription.get('status_any') or subscription.get('status'))
    }
    status_all_values = {_normalize_status_value(v) for v in _split_csv_values(subscription.get('status_all'))}
    any_targets = _status_targets('tfms', status_any_values)
    all_targets = _status_targets('tfms', status_all_values)

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

    stmt = stmt.order_by(TfmsProjection.updated_at.desc())

    async with TfmsAsyncSessionLocal() as session:
        rows = list((await session.execute(stmt)).scalars())

    filtered_rows: list[TfmsProjection] = []
    for row in rows:
        ts = _parse_iso_timestamp(row.source_timestamp)
        if start is not None and (ts is None or ts < start):
            continue
        if end is not None and (ts is None or ts > end):
            continue

        data = row.data or {}
        statuses = _tfms_status_values_from_projection_data(data)
        fields = [subscription.get('status_field')] if subscription.get('status_field') else ['flight_status', 'tmi_status', 'service_state']
        current_values = [statuses.get(field) for field in fields]
        if any_targets and not _status_matches_any(current_values, any_targets):
            continue
        if all_targets and not _status_matches_all(current_values, all_targets):
            continue

        if airport_values or departure_values:
            dep_airport, arr_airport = _extract_tfms_projection_airports(data)
            if not _airport_matches(
                departure_airport=dep_airport,
                arrival_airport=arr_airport,
                departure_values=departure_values,
                airport_values=airport_values,
            ):
                continue

        filtered_rows.append(row)

    rows = filtered_rows[:limit]

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

    _validate_time_basis(subscription.get('time_basis'))
    if subscription.get('status_field') and subscription.get('status_field') not in TBFM_STATUS_FIELDS:
        raise HTTPException(status_code=400, detail='invalid_status_field')
    start, end = _resolve_time_bounds(
        date=subscription.get('date'),
        from_date=subscription.get('from_date'),
        to_date=subscription.get('to_date'),
        from_ts=subscription.get('from_ts'),
        to_ts=subscription.get('to_ts'),
    )

    airport_values = _split_csv_values(subscription.get('airport'), upper=True)
    departure_values = _split_csv_values(subscription.get('departure'), upper=True)
    status_any_values = {
        _normalize_status_value(v) for v in _split_csv_values(subscription.get('status_any') or subscription.get('status'))
    }
    status_all_values = {_normalize_status_value(v) for v in _split_csv_values(subscription.get('status_all'))}
    any_targets = _status_targets('tbfm', status_any_values)
    all_targets = _status_targets('tbfm', status_all_values)

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

    stmt = stmt.order_by(TbfmProjection.updated_at.desc())

    async with TbfmAsyncSessionLocal() as session:
        rows = list((await session.execute(stmt)).scalars())

    filtered_rows: list[TbfmProjection] = []
    for row in rows:
        ts = _parse_iso_timestamp(row.source_time)
        if start is not None and (ts is None or ts < start):
            continue
        if end is not None and (ts is None or ts > end):
            continue

        data = row.data or {}
        statuses = _tbfm_status_values_from_projection_data(data)
        fields = [subscription.get('status_field')] if subscription.get('status_field') else ['acs', 'fps']
        current_values = [statuses.get(field) for field in fields]
        if any_targets and not _status_matches_any(current_values, any_targets):
            continue
        if all_targets and not _status_matches_all(current_values, all_targets):
            continue

        if airport_values or departure_values:
            dep_airport, arr_airport = _extract_tbfm_projection_airports(data)
            if not _airport_matches(
                departure_airport=dep_airport,
                arrival_airport=arr_airport,
                departure_values=departure_values,
                airport_values=airport_values,
            ):
                continue

        filtered_rows.append(row)

    rows = filtered_rows[:limit]

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
    departure: str | None,
    date: str | None,
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
            departure=departure,
            date=date,
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
    parsed_payload: dict[str, Any],
    *,
    callsign: str | None,
    gufi: str | None,
    destination: str | None,
    airport: str | None,
    departure: str | None,
    date: str | None,
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
            departure=departure,
            date=date,
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
    departure: str | None,
    date: str | None,
    limit: int = 25,
) -> list[dict[str, Any]]:
    stmt = select(FlightCurrent)
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
    if departure:
        stmt = stmt.where(FlightCurrent.departure_airport == departure.upper())
    if date:
        parsed_date = datetime.strptime(date, '%Y-%m-%d').date()
        stmt = stmt.where(func.coalesce(cast(FlightCurrent.source_timestamp, Date), cast(FlightCurrent.updated_at, Date)) == parsed_date)
    stmt = stmt.order_by(FlightCurrent.updated_at.desc()).limit(limit)
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
            departure=departure,
            date=date,
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
                departure=None,
                date=None,
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
    airport: str | None = None,
    departure: str | None = None,
    date: str | None = None,
    session: AsyncSession = Depends(get_db_session),
) -> list[dict[str, Any]]:
    destination_filter = _normalize_destination_filter({'destination': destination})
    airport_filter = _normalize_airport_filter({'airport': airport})
    departure_filter = _normalize_departure_filter({'departure': departure})
    try:
        date_filter = _normalize_date_filter(date)
    except ValueError:
        raise HTTPException(status_code=400, detail='Invalid date format. Use YYYY-MM-DD')
    callsign_filter = callsign.upper() if isinstance(callsign, str) and callsign else None
    gufi_filter = gufi if isinstance(gufi, str) and gufi else None
    return await _fetch_snapshot(
        session,
        callsign=callsign_filter,
        gufi=gufi_filter,
        destination=destination_filter,
        airport=airport_filter,
        departure=departure_filter,
        date=date_filter,
        limit=limit,
    )


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
async def list_tfms_events(
    limit: int = 100,
    offset: int = 0,
    sort: str = 'desc',
    raw: bool = False,
    acid: str | None = None,
    callsign: str | None = None,
    gufi: str | None = None,
    flight_ref: str | None = None,
    msg_type: str | None = None,
    source_facility: str | None = None,
    queue_name: str | None = None,
    status: str | None = None,
    status_any: str | None = None,
    status_all: str | None = None,
    status_field: str | None = None,
    airport: str | None = None,
    departure: str | None = None,
    date: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    from_ts: str | None = None,
    to_ts: str | None = None,
    time_basis: str | None = None,
) -> list[TfmsEventResponse]:
    _validate_time_basis(time_basis)
    start, end = _resolve_time_bounds(date=date, from_date=from_date, to_date=to_date, from_ts=from_ts, to_ts=to_ts)
    acid_values = _split_csv_values(acid or callsign, upper=True)
    gufi_values = _split_csv_values(gufi)
    flight_ref_values = _split_csv_values(flight_ref)
    msg_type_values = _split_csv_values(msg_type)
    source_values = _split_csv_values(source_facility, upper=True)
    queue_values = _split_csv_values(queue_name)
    status_any_values = {_normalize_status_value(v) for v in _split_csv_values(status_any or status)}
    status_all_values = {_normalize_status_value(v) for v in _split_csv_values(status_all)}
    airport_values = _split_csv_values(airport, upper=True)
    departure_values = _split_csv_values(departure, upper=True)

    if status_field and status_field not in TFMS_STATUS_FIELDS:
        raise HTTPException(status_code=400, detail='invalid_status_field')

    limit = max(1, min(limit, 500))
    offset = max(0, offset)
    direction_desc = _resolve_sort(sort)
    any_targets = _status_targets('tfms', status_any_values)
    all_targets = _status_targets('tfms', status_all_values)

    async with TfmsAsyncSessionLocal() as session:
        stmt = select(TfmsEvent)
        if acid_values:
            stmt = stmt.where(func.upper(TfmsEvent.acid).in_(acid_values))
        if gufi_values:
            stmt = stmt.where(TfmsEvent.gufi.in_(gufi_values))
        if flight_ref_values:
            stmt = stmt.where(TfmsEvent.flight_ref.in_(flight_ref_values))
        if msg_type_values:
            stmt = stmt.where(func.lower(TfmsEvent.msg_type).in_({m.lower() for m in msg_type_values}))
        if source_values:
            stmt = stmt.where(func.upper(TfmsEvent.source_facility).in_(source_values))
        if queue_values:
            stmt = stmt.where(TfmsEvent.queue_name.in_(queue_values))
        if start is not None:
            stmt = stmt.where(TfmsEvent.source_timestamp >= start)
        if end is not None:
            stmt = stmt.where(TfmsEvent.source_timestamp <= end)
        stmt = stmt.order_by(
            TfmsEvent.source_timestamp.desc() if direction_desc else TfmsEvent.source_timestamp.asc(),
            TfmsEvent.id.desc() if direction_desc else TfmsEvent.id.asc(),
        )
        rows = list((await session.execute(stmt)).scalars())

    filtered_rows: list[TfmsEvent] = []
    for row in rows:
        payload = row.parsed_json or {}
        statuses = _tfms_status_values_from_event_payload(payload)
        fields = [status_field] if status_field else ['flight_status', 'tmi_status', 'service_state']
        current_values = [statuses.get(field) for field in fields]
        if any_targets and not _status_matches_any(current_values, any_targets):
            continue
        if all_targets and not _status_matches_all(current_values, all_targets):
            continue

        if airport_values or departure_values:
            dep_airport, arr_airport = _tfms_event_airports(payload)
            if not _airport_matches(
                departure_airport=dep_airport,
                arrival_airport=arr_airport,
                departure_values=departure_values,
                airport_values=airport_values,
            ):
                continue

        filtered_rows.append(row)

    rows = _paginate_list(filtered_rows, offset=offset, limit=limit)

    output: list[TfmsEventResponse] = []
    for row in rows:
        output.append(
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
                source_timestamp=row.source_timestamp,
                parsed_json=(
                    (
                        only_raw_fields(parse_tfms_xml(row.raw_xml))
                        if settings.tfms_raw_response_from_xml
                        else only_raw_fields(row.parsed_json or {})
                    )
                    if raw
                    else strip_raw_fields(row.parsed_json or {})
                ),
                created_at=row.created_at,
            )
        )
    return output


@app.get('/tfms/projections', response_model=list[TfmsProjectionResponse])
async def list_tfms_projections(
    limit: int = 100,
    offset: int = 0,
    sort: str = 'desc',
    raw: bool = False,
    acid: str | None = None,
    callsign: str | None = None,
    gufi: str | None = None,
    flight_ref: str | None = None,
    msg_type: str | None = None,
    source_facility: str | None = None,
    projection_type: str | None = None,
    projection_key: str | None = None,
    airport: str | None = None,
    departure: str | None = None,
    status: str | None = None,
    status_any: str | None = None,
    status_all: str | None = None,
    status_field: str | None = None,
    date: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    from_ts: str | None = None,
    to_ts: str | None = None,
    time_basis: str | None = None,
) -> list[TfmsProjectionResponse]:
    _validate_time_basis(time_basis)
    start, end = _resolve_time_bounds(date=date, from_date=from_date, to_date=to_date, from_ts=from_ts, to_ts=to_ts)
    if status_field and status_field not in TFMS_STATUS_FIELDS:
        raise HTTPException(status_code=400, detail='invalid_status_field')

    limit = max(1, min(limit, 500))
    offset = max(0, offset)
    direction_desc = _resolve_sort(sort)

    acid_values = _split_csv_values(acid or callsign, upper=True)
    gufi_values = _split_csv_values(gufi)
    flight_ref_values = _split_csv_values(flight_ref)
    msg_type_values = _split_csv_values(msg_type)
    source_values = _split_csv_values(source_facility, upper=True)
    projection_type_values = _split_csv_values(projection_type, upper=True)
    projection_key_values = _split_csv_values(projection_key)
    airport_values = _split_csv_values(airport, upper=True)
    departure_values = _split_csv_values(departure, upper=True)
    status_any_values = {_normalize_status_value(v) for v in _split_csv_values(status_any or status)}
    status_all_values = {_normalize_status_value(v) for v in _split_csv_values(status_all)}
    any_targets = _status_targets('tfms', status_any_values)
    all_targets = _status_targets('tfms', status_all_values)

    async with TfmsAsyncSessionLocal() as session:
        stmt = select(TfmsProjection)
        if acid_values:
            stmt = stmt.where(func.upper(TfmsProjection.acid).in_(acid_values))
        if gufi_values:
            stmt = stmt.where(TfmsProjection.gufi.in_(gufi_values))
        if flight_ref_values:
            stmt = stmt.where(TfmsProjection.flight_ref.in_(flight_ref_values))
        if msg_type_values:
            normalized_msg_types = {_normalize_msg_type(v) for v in msg_type_values}
            normalized_msg_types.discard(None)
            if normalized_msg_types:
                stmt = stmt.where(
                    func.lower(func.regexp_replace(func.coalesce(TfmsProjection.msg_type, ''), '[^A-Za-z0-9]', '', 'g')).in_(
                        normalized_msg_types
                    )
                )
        if source_values:
            stmt = stmt.where(func.upper(TfmsProjection.source_facility).in_(source_values))
        if projection_type_values:
            stmt = stmt.where(func.upper(TfmsProjection.projection_type).in_(projection_type_values))
        if projection_key_values:
            stmt = stmt.where(TfmsProjection.projection_key.in_(projection_key_values))

        stmt = stmt.order_by(
            TfmsProjection.updated_at.desc() if direction_desc else TfmsProjection.updated_at.asc(),
            TfmsProjection.projection_key.desc() if direction_desc else TfmsProjection.projection_key.asc(),
        )
        rows = list((await session.execute(stmt)).scalars())

        filtered_rows: list[TfmsProjection] = []
        for row in rows:
            ts = _parse_iso_timestamp(row.source_timestamp)
            if start is not None and (ts is None or ts < start):
                continue
            if end is not None and (ts is None or ts > end):
                continue

            data = row.data or {}
            statuses = _tfms_status_values_from_projection_data(data)
            fields = [status_field] if status_field else ['flight_status', 'tmi_status', 'service_state']
            current_values = [statuses.get(field) for field in fields]
            if any_targets and not _status_matches_any(current_values, any_targets):
                continue
            if all_targets and not _status_matches_all(current_values, all_targets):
                continue

            if airport_values or departure_values:
                dep_airport, arr_airport = _extract_tfms_projection_airports(data)
                if not _airport_matches(
                    departure_airport=dep_airport,
                    arrival_airport=arr_airport,
                    departure_values=departure_values,
                    airport_values=airport_values,
                ):
                    continue

            filtered_rows.append(row)

        rows = _paginate_list(filtered_rows, offset=offset, limit=limit)

        raw_by_key: dict[str, dict[str, Any]] = {}
        if raw and rows and settings.tfms_raw_response_from_xml:
            candidates = await session.execute(
                select(TfmsEvent).order_by(TfmsEvent.id.desc()).limit(max(limit * 5, 200))
            )
            recent_events = list(candidates.scalars())
            for row in rows:
                value: dict[str, Any] | None = None
                for event in recent_events:
                    if event.msg_type and row.msg_type and event.msg_type != row.msg_type:
                        continue
                    if event.acid and row.acid and event.acid != row.acid:
                        continue
                    if event.gufi and row.gufi and event.gufi != row.gufi:
                        continue
                    if event.flight_ref and row.flight_ref and event.flight_ref != row.flight_ref:
                        continue
                    value = projection_raw_by_key_from_xml(event.raw_xml, row.projection_key)
                    if value:
                        break
                raw_by_key[row.projection_key] = value or {}
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
            data=(
                raw_by_key.get(row.projection_key, {})
                if raw and settings.tfms_raw_response_from_xml
                else (only_raw_fields(row.data or {}) if raw else strip_raw_fields(row.data or {}))
            ),
            updated_at=row.updated_at,
        )
        for row in rows
    ]


@app.get('/tfms/projections/{projection_key}', response_model=TfmsProjectionResponse)
async def get_tfms_projection(projection_key: str, raw: bool = False) -> TfmsProjectionResponse:
    async with TfmsAsyncSessionLocal() as session:
        row = await session.get(TfmsProjection, projection_key)
        raw_data: dict[str, Any] = {}
        if raw and row is not None and settings.tfms_raw_response_from_xml:
            stmt = select(TfmsEvent)
            if row.msg_type:
                stmt = stmt.where(TfmsEvent.msg_type == row.msg_type)
            if row.acid:
                stmt = stmt.where(TfmsEvent.acid == row.acid)
            if row.gufi:
                stmt = stmt.where(TfmsEvent.gufi == row.gufi)
            if row.flight_ref:
                stmt = stmt.where(TfmsEvent.flight_ref == row.flight_ref)
            candidates = await session.execute(stmt.order_by(TfmsEvent.id.desc()).limit(5000))
            for event in list(candidates.scalars()):
                if event.msg_type and row.msg_type and event.msg_type != row.msg_type:
                    continue
                if event.acid and row.acid and event.acid != row.acid:
                    continue
                if event.gufi and row.gufi and event.gufi != row.gufi:
                    continue
                if event.flight_ref and row.flight_ref and event.flight_ref != row.flight_ref:
                    continue
                raw_data = projection_raw_by_key_from_xml(event.raw_xml, projection_key) or {}
                if raw_data:
                    break
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
        data=(
            raw_data
            if raw and settings.tfms_raw_response_from_xml
            else (only_raw_fields(row.data or {}) if raw else strip_raw_fields(row.data or {}))
        ),
        updated_at=row.updated_at,
    )


@app.get('/tbfm/events', response_model=list[TbfmEventResponse])
async def list_tbfm_events(
    limit: int = 100,
    offset: int = 0,
    sort: str = 'desc',
    raw: bool = False,
    acid: str | None = None,
    callsign: str | None = None,
    gufi: str | None = None,
    flight_ref: str | None = None,
    msg_type: str | None = None,
    source_facility: str | None = None,
    tma_id: str | None = None,
    queue_name: str | None = None,
    status: str | None = None,
    status_any: str | None = None,
    status_all: str | None = None,
    status_field: str | None = None,
    airport: str | None = None,
    departure: str | None = None,
    date: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    from_ts: str | None = None,
    to_ts: str | None = None,
    time_basis: str | None = None,
) -> list[TbfmEventResponse]:
    _validate_time_basis(time_basis)
    start, end = _resolve_time_bounds(date=date, from_date=from_date, to_date=to_date, from_ts=from_ts, to_ts=to_ts)
    if status_field and status_field not in TBFM_STATUS_FIELDS:
        raise HTTPException(status_code=400, detail='invalid_status_field')

    limit = max(1, min(limit, 500))
    offset = max(0, offset)
    direction_desc = _resolve_sort(sort)

    acid_values = _split_csv_values(acid or callsign, upper=True)
    gufi_values = _split_csv_values(gufi)
    flight_ref_values = _split_csv_values(flight_ref)
    msg_type_values = _split_csv_values(msg_type)
    source_values = _split_csv_values(source_facility, upper=True)
    tma_values = _split_csv_values(tma_id)
    queue_values = _split_csv_values(queue_name)
    airport_values = _split_csv_values(airport, upper=True)
    departure_values = _split_csv_values(departure, upper=True)
    status_any_values = {_normalize_status_value(v) for v in _split_csv_values(status_any or status)}
    status_all_values = {_normalize_status_value(v) for v in _split_csv_values(status_all)}
    any_targets = _status_targets('tbfm', status_any_values)
    all_targets = _status_targets('tbfm', status_all_values)

    async with TbfmAsyncSessionLocal() as session:
        stmt = select(TbfmEvent)
        if acid_values:
            stmt = stmt.where(func.upper(TbfmEvent.acid).in_(acid_values))
        if gufi_values:
            stmt = stmt.where(TbfmEvent.gufi.in_(gufi_values))
        if flight_ref_values:
            stmt = stmt.where(TbfmEvent.flight_ref.in_(flight_ref_values))
        if msg_type_values:
            normalized_msg_types = {_normalize_msg_type(v) for v in msg_type_values}
            normalized_msg_types.discard(None)
            if normalized_msg_types:
                stmt = stmt.where(
                    func.lower(func.regexp_replace(func.coalesce(TbfmEvent.msg_type, ''), '[^A-Za-z0-9]', '', 'g')).in_(
                        normalized_msg_types
                    )
                )
        if source_values:
            stmt = stmt.where(func.upper(TbfmEvent.source_facility).in_(source_values))
        if tma_values:
            stmt = stmt.where(TbfmEvent.tma_id.in_(tma_values))
        if queue_values:
            stmt = stmt.where(TbfmEvent.queue_name.in_(queue_values))
        stmt = stmt.order_by(
            TbfmEvent.created_at.desc() if direction_desc else TbfmEvent.created_at.asc(),
            TbfmEvent.id.desc() if direction_desc else TbfmEvent.id.asc(),
        )
        rows = list((await session.execute(stmt)).scalars())

    filtered_rows: list[TbfmEvent] = []
    for row in rows:
        ts = _parse_iso_timestamp(row.source_time)
        if start is not None and (ts is None or ts < start):
            continue
        if end is not None and (ts is None or ts > end):
            continue

        payload = row.parsed_json or {}
        statuses = _tbfm_status_values_from_payload(payload)
        fields = [status_field] if status_field else ['acs', 'fps']
        current_values = [statuses.get(field) for field in fields]
        if any_targets and not _status_matches_any(current_values, any_targets):
            continue
        if all_targets and not _status_matches_all(current_values, all_targets):
            continue

        if airport_values or departure_values:
            dep_airport, arr_airport = _tbfm_event_airports(payload)
            if not _airport_matches(
                departure_airport=dep_airport,
                arrival_airport=arr_airport,
                departure_values=departure_values,
                airport_values=airport_values,
            ):
                continue

        filtered_rows.append(row)

    rows = _paginate_list(filtered_rows, offset=offset, limit=limit)

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
            parsed_json=(
                (
                    only_tbfm_raw_fields(parse_tbfm_xml(row.raw_xml))
                    if settings.tbfm_raw_response_from_xml
                    else only_tbfm_raw_fields(row.parsed_json or {})
                )
                if raw
                else strip_tbfm_raw_fields(row.parsed_json or {})
            ),
            created_at=row.created_at,
        )
        for row in rows
    ]


@app.get('/tbfm/projections', response_model=list[TbfmProjectionResponse])
async def list_tbfm_projections(
    limit: int = 100,
    offset: int = 0,
    sort: str = 'desc',
    raw: bool = False,
    acid: str | None = None,
    callsign: str | None = None,
    gufi: str | None = None,
    tma_id: str | None = None,
    flight_ref: str | None = None,
    msg_type: str | None = None,
    source_facility: str | None = None,
    projection_type: str | None = None,
    projection_key: str | None = None,
    airport: str | None = None,
    departure: str | None = None,
    status: str | None = None,
    status_any: str | None = None,
    status_all: str | None = None,
    status_field: str | None = None,
    date: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    from_ts: str | None = None,
    to_ts: str | None = None,
    time_basis: str | None = None,
) -> list[TbfmProjectionResponse]:
    _validate_time_basis(time_basis)
    start, end = _resolve_time_bounds(date=date, from_date=from_date, to_date=to_date, from_ts=from_ts, to_ts=to_ts)
    if status_field and status_field not in TBFM_STATUS_FIELDS:
        raise HTTPException(status_code=400, detail='invalid_status_field')

    limit = max(1, min(limit, 500))
    offset = max(0, offset)
    direction_desc = _resolve_sort(sort)

    acid_values = _split_csv_values(acid or callsign, upper=True)
    gufi_values = _split_csv_values(gufi)
    tma_values = _split_csv_values(tma_id)
    flight_ref_values = _split_csv_values(flight_ref)
    msg_type_values = _split_csv_values(msg_type)
    source_values = _split_csv_values(source_facility, upper=True)
    projection_type_values = _split_csv_values(projection_type, upper=True)
    projection_key_values = _split_csv_values(projection_key)
    airport_values = _split_csv_values(airport, upper=True)
    departure_values = _split_csv_values(departure, upper=True)
    status_any_values = {_normalize_status_value(v) for v in _split_csv_values(status_any or status)}
    status_all_values = {_normalize_status_value(v) for v in _split_csv_values(status_all)}
    any_targets = _status_targets('tbfm', status_any_values)
    all_targets = _status_targets('tbfm', status_all_values)

    async with TbfmAsyncSessionLocal() as session:
        stmt = select(TbfmProjection)
        if acid_values:
            stmt = stmt.where(func.upper(TbfmProjection.acid).in_(acid_values))
        if gufi_values:
            stmt = stmt.where(TbfmProjection.gufi.in_(gufi_values))
        if tma_values:
            stmt = stmt.where(TbfmProjection.tma_id.in_(tma_values))
        if flight_ref_values:
            stmt = stmt.where(TbfmProjection.flight_ref.in_(flight_ref_values))
        if msg_type_values:
            normalized_msg_types = {_normalize_msg_type(v) for v in msg_type_values}
            normalized_msg_types.discard(None)
            if normalized_msg_types:
                stmt = stmt.where(
                    func.lower(func.regexp_replace(func.coalesce(TbfmProjection.msg_type, ''), '[^A-Za-z0-9]', '', 'g')).in_(
                        normalized_msg_types
                    )
                )
        if source_values:
            stmt = stmt.where(func.upper(TbfmProjection.source_facility).in_(source_values))
        if projection_type_values:
            stmt = stmt.where(func.upper(TbfmProjection.projection_type).in_(projection_type_values))
        if projection_key_values:
            stmt = stmt.where(TbfmProjection.projection_key.in_(projection_key_values))

        stmt = stmt.order_by(
            TbfmProjection.updated_at.desc() if direction_desc else TbfmProjection.updated_at.asc(),
            TbfmProjection.projection_key.desc() if direction_desc else TbfmProjection.projection_key.asc(),
        )
        rows = list((await session.execute(stmt)).scalars())

        filtered_rows: list[TbfmProjection] = []
        for row in rows:
            ts = _parse_iso_timestamp(row.source_time)
            if start is not None and (ts is None or ts < start):
                continue
            if end is not None and (ts is None or ts > end):
                continue

            data = row.data or {}
            statuses = _tbfm_status_values_from_projection_data(data)
            fields = [status_field] if status_field else ['acs', 'fps']
            current_values = [statuses.get(field) for field in fields]
            if any_targets and not _status_matches_any(current_values, any_targets):
                continue
            if all_targets and not _status_matches_all(current_values, all_targets):
                continue

            if airport_values or departure_values:
                dep_airport, arr_airport = _extract_tbfm_projection_airports(data)
                if not _airport_matches(
                    departure_airport=dep_airport,
                    arrival_airport=arr_airport,
                    departure_values=departure_values,
                    airport_values=airport_values,
                ):
                    continue

            filtered_rows.append(row)

        rows = _paginate_list(filtered_rows, offset=offset, limit=limit)

        raw_by_key: dict[str, dict[str, Any]] = {}
        if raw and rows and settings.tbfm_raw_response_from_xml:
            candidates = await session.execute(
                select(TbfmEvent).order_by(TbfmEvent.id.desc()).limit(max(limit * 5, 200))
            )
            recent_events = list(candidates.scalars())
            for row in rows:
                value: dict[str, Any] | None = None
                for event in recent_events:
                    if event.msg_type and row.msg_type and event.msg_type != row.msg_type:
                        continue
                    if event.acid and row.acid and event.acid != row.acid:
                        continue
                    if event.gufi and row.gufi and event.gufi != row.gufi:
                        continue
                    if event.flight_ref and row.flight_ref and event.flight_ref != row.flight_ref:
                        continue
                    if event.tma_id and row.tma_id and event.tma_id != row.tma_id:
                        continue
                    value = tbfm_projection_raw_by_key_from_xml(event.raw_xml, row.projection_key)
                    if value:
                        break
                raw_by_key[row.projection_key] = value or {}
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
            data=(
                raw_by_key.get(row.projection_key, {})
                if raw and settings.tbfm_raw_response_from_xml
                else (only_tbfm_raw_fields(row.data or {}) if raw else strip_tbfm_raw_fields(row.data or {}))
            ),
            updated_at=row.updated_at,
        )
        for row in rows
    ]


@app.get('/tbfm/projections/{projection_key}', response_model=TbfmProjectionResponse)
async def get_tbfm_projection(projection_key: str, raw: bool = False) -> TbfmProjectionResponse:
    async with TbfmAsyncSessionLocal() as session:
        row = await session.get(TbfmProjection, projection_key)
        raw_data: dict[str, Any] = {}
        if raw and row is not None and settings.tbfm_raw_response_from_xml:
            stmt = select(TbfmEvent)
            if row.msg_type:
                stmt = stmt.where(TbfmEvent.msg_type == row.msg_type)
            if row.acid:
                stmt = stmt.where(TbfmEvent.acid == row.acid)
            if row.gufi:
                stmt = stmt.where(TbfmEvent.gufi == row.gufi)
            if row.flight_ref:
                stmt = stmt.where(TbfmEvent.flight_ref == row.flight_ref)
            if row.tma_id:
                stmt = stmt.where(TbfmEvent.tma_id == row.tma_id)
            candidates = await session.execute(stmt.order_by(TbfmEvent.id.desc()).limit(5000))
            for event in list(candidates.scalars()):
                if event.msg_type and row.msg_type and event.msg_type != row.msg_type:
                    continue
                if event.acid and row.acid and event.acid != row.acid:
                    continue
                if event.gufi and row.gufi and event.gufi != row.gufi:
                    continue
                if event.flight_ref and row.flight_ref and event.flight_ref != row.flight_ref:
                    continue
                if event.tma_id and row.tma_id and event.tma_id != row.tma_id:
                    continue
                raw_data = tbfm_projection_raw_by_key_from_xml(event.raw_xml, projection_key) or {}
                if raw_data:
                    break
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
        data=(
            raw_data
            if raw and settings.tbfm_raw_response_from_xml
            else (only_tbfm_raw_fields(row.data or {}) if raw else strip_tbfm_raw_fields(row.data or {}))
        ),
        updated_at=row.updated_at,
    )


@app.websocket('/ws/flights')
async def flights_websocket(websocket: WebSocket) -> None:
    await websocket.accept()

    callsign = websocket.query_params.get('callsign')
    gufi = websocket.query_params.get('gufi')
    destination = _normalize_destination_filter(websocket.query_params)
    airport = _normalize_airport_filter(websocket.query_params)
    departure = _normalize_departure_filter(websocket.query_params)
    limit_raw = websocket.query_params.get('limit')
    limit = int(limit_raw) if isinstance(limit_raw, str) and limit_raw.isdigit() else 100
    try:
        date = _normalize_date_filter(websocket.query_params.get('date'))
    except ValueError:
        await websocket.send_json({'type': 'error', 'error': 'invalid_date', 'detail': 'Use YYYY-MM-DD'})
        await websocket.close(code=1008)
        return

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
            departure=departure,
            date=date,
            limit=limit,
        )
    await websocket.send_json(
        {
            'type': 'snapshot',
            'subscription': {
                'callsign': callsign,
                'gufi': gufi,
                'destination': destination,
                'airport': airport,
                'departure': departure,
                'date': date,
                'limit': limit,
            },
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
                    departure = _normalize_departure_filter(message)
                    if isinstance(message.get('limit'), int) and message.get('limit') > 0:
                        limit = int(message.get('limit'))
                    try:
                        date = _normalize_date_filter(message.get('date'))
                    except ValueError:
                        await websocket.send_json({'type': 'error', 'error': 'invalid_date', 'detail': 'Use YYYY-MM-DD'})
                        continue
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
                                departure=departure,
                                date=date,
                                limit=limit,
                            )
                        await websocket.send_json(
                            {
                                'type': 'snapshot',
                                'subscription': {
                                    'callsign': callsign,
                                    'gufi': gufi,
                                    'destination': destination,
                                    'airport': airport,
                                    'departure': departure,
                                    'date': date,
                                    'limit': limit,
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
                                    'departure': departure,
                                    'date': date,
                                    'limit': limit,
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
                    departure=departure,
                    date=date,
                )
                if flights:
                    filtered_payload = _filter_sfdps_parsed_messages(
                        parsed_payload,
                        callsign=callsign,
                        gufi=gufi,
                        destination=destination,
                        airport=airport,
                        departure=departure,
                        date=date,
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
                                'departure': departure,
                                'date': date,
                                'limit': limit,
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

    try:
        snapshot = await _fetch_tfms_snapshot(subscription, limit=limit)
    except HTTPException as exc:
        await websocket.send_json({'type': 'error', 'error': exc.detail})
        await websocket.close(code=1008)
        return
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
                    if isinstance(message.get('limit'), int) and message.get('limit') > 0:
                        limit = max(1, min(int(message.get('limit')), 500))
                    if message.get('snapshot', True):
                        try:
                            snapshot = await _fetch_tfms_snapshot(subscription, limit=limit)
                        except HTTPException as exc:
                            await websocket.send_json({'type': 'error', 'error': exc.detail})
                            continue
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
                    if _tfms_matches_subscription(fields, subscription) and _tfms_realtime_filters_match(
                        obj, subscription, projection=False
                    ):
                        await websocket.send_json({'type': 'event', 'subscription': subscription, 'event': obj})
                elif channel == settings.tfms_projections_channel_name:
                    fields = _tfms_projection_fields(obj)
                    if _tfms_matches_subscription(fields, subscription) and _tfms_realtime_filters_match(
                        obj, subscription, projection=True
                    ):
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

    try:
        snapshot = await _fetch_tbfm_snapshot(subscription, limit=limit)
    except HTTPException as exc:
        await websocket.send_json({'type': 'error', 'error': exc.detail})
        await websocket.close(code=1008)
        return
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
                    if isinstance(message.get('limit'), int) and message.get('limit') > 0:
                        limit = max(1, min(int(message.get('limit')), 500))
                    if message.get('snapshot', True):
                        try:
                            snapshot = await _fetch_tbfm_snapshot(subscription, limit=limit)
                        except HTTPException as exc:
                            await websocket.send_json({'type': 'error', 'error': exc.detail})
                            continue
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
                    if _tbfm_matches_subscription(fields, subscription) and _tbfm_realtime_filters_match(
                        obj, subscription, projection=False
                    ):
                        await websocket.send_json({'type': 'event', 'subscription': subscription, 'event': obj})
                elif channel == settings.tbfm_projections_channel_name:
                    fields = _tbfm_projection_fields(obj)
                    if _tbfm_matches_subscription(fields, subscription) and _tbfm_realtime_filters_match(
                        obj, subscription, projection=True
                    ):
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

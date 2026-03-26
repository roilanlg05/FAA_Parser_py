from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .config import settings
from .redis_client import RedisManager
from .tfms_models import TfmsEvent, TfmsProjection
from .tfms_parser_adapter import build_tfms_projections, parse_tfms_xml
from .tfms_payload_utils import strip_raw_fields

logger = logging.getLogger(__name__)

def _parse_source_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        return None


def _root_facility_and_msg(parsed: dict[str, Any]) -> tuple[str | None, str | None, str | None, str | None, datetime | None]:
    payload_type = parsed.get('payload_type')
    if payload_type == 'tfms_flight_data_output':
        source_facility: str | None = None
        msg_type: str | None = None
        flight_ref: str | None = None
        acid: str | None = None
        source_timestamp: datetime | None = None

        for message in parsed.get('messages') or []:
            if not isinstance(message, dict):
                continue

            body = message.get('body') or {}
            qid = body.get('qualifiedAircraftId') if isinstance(body, dict) else None
            if not isinstance(qid, dict):
                qid = {}

            if source_facility is None:
                source_facility = message.get('sourceFacility')
            if msg_type is None:
                msg_type = message.get('msgType')
            if flight_ref is None:
                flight_ref = message.get('flightRef')
            if acid is None:
                acid = qid.get('aircraftId') or message.get('acid')
            if source_timestamp is None:
                source_timestamp = _parse_source_timestamp(message.get('sourceTimeStamp'))

            if source_facility and msg_type and flight_ref and acid and source_timestamp:
                break

        return (source_facility, msg_type, flight_ref, acid, source_timestamp)
    if payload_type == 'tfms_flow_information_output':
        first = (parsed.get('messages') or [None])[0] or {}
        tmi_first = (first.get('tmiFlightDataList') or [None])[0] or {}
        flight = tmi_first.get('flight') or {}
        return (
            first.get('sourceFacility'),
            first.get('msgType'),
            tmi_first.get('flightReference'),
            flight.get('aircraftId'),
            _parse_source_timestamp(first.get('sourceTimeStamp')),
        )
    if payload_type == 'tfms_status_output':
        first = (parsed.get('statuses') or [None])[0] or {}
        return (first.get('facility'), 'status', None, None, _parse_source_timestamp(first.get('time')))
    return (None, None, None, None, None)


async def ingest_tfms_xml(
    session: AsyncSession,
    redis_manager: RedisManager,
    *,
    xml_text: str,
    queue_name: str | None,
    metadata: dict[str, Any] | None,
) -> dict[str, Any]:
    parsed = parse_tfms_xml(xml_text)
    source_facility, msg_type, flight_ref, acid, source_timestamp = _root_facility_and_msg(parsed)
    projections = build_tfms_projections(parsed)
    gufi = next((projection.get('gufi') for projection in projections if projection.get('gufi')), None)

    compact_parsed = strip_raw_fields(parsed)

    event = TfmsEvent(
        queue_name=queue_name or settings.tfms_queue_name,
        payload_type=parsed.get('payload_type') or 'unknown',
        root_tag=parsed.get('root_tag'),
        source_facility=source_facility,
        msg_type=msg_type,
        flight_ref=flight_ref,
        acid=acid,
        gufi=gufi,
        source_timestamp=source_timestamp,
        raw_xml=xml_text,
        parsed_json=compact_parsed,
    )
    session.add(event)

    for projection in projections:
        key = projection.get('key')
        if not key:
            continue
        existing = await session.get(TfmsProjection, key)
        if existing is None:
            existing = TfmsProjection(projection_key=key)
            session.add(existing)
        existing.projection_type = projection.get('projection_type') or 'unknown'
        existing.acid = projection.get('acid')
        existing.gufi = projection.get('gufi')
        existing.flight_ref = projection.get('flightRef')
        existing.msg_type = projection.get('msgType')
        existing.source_facility = projection.get('sourceFacility')
        existing.source_timestamp = projection.get('sourceTimeStamp')
        existing.data = strip_raw_fields(projection.get('data') or {})

    await session.commit()
    await session.refresh(event)

    try:
        redis = await redis_manager.connect_tfms()
        event_payload = {
            'event_id': event.id,
            'queue_name': event.queue_name,
            'payload_type': event.payload_type,
            'parsed': compact_parsed,
            'metadata': metadata or {},
        }
        await redis.publish(settings.tfms_events_channel_name, json.dumps(event_payload, ensure_ascii=False))

        for projection in projections:
            data = projection.get('data') or {}
            projection_payload = {
                'projection_type': projection.get('projection_type'),
                'projection_key': projection.get('key'),
                'acid': projection.get('acid'),
                'gufi': projection.get('gufi'),
                'flight_ref': projection.get('flightRef'),
                'msg_type': projection.get('msgType'),
                'source_facility': projection.get('sourceFacility'),
                'source_timestamp': projection.get('sourceTimeStamp'),
                'data': strip_raw_fields(data),
            }
            await redis.publish(settings.tfms_projections_channel_name, json.dumps(projection_payload, ensure_ascii=False))
    except Exception:
        logger.exception('Failed to publish TFMS redis notifications event_id=%s', event.id)

    return {
        'status': 'accepted',
        'event_id': event.id,
        'payload_type': event.payload_type,
        'projection_count': len(projections),
        'queue_name': event.queue_name,
        'parsed': compact_parsed,
    }


async def get_tfms_events(session: AsyncSession, limit: int = 100) -> list[TfmsEvent]:
    stmt = select(TfmsEvent).order_by(TfmsEvent.created_at.desc()).limit(limit)
    result = await session.execute(stmt)
    return list(result.scalars())


async def get_tfms_projections(session: AsyncSession, limit: int = 100) -> list[TfmsProjection]:
    stmt = select(TfmsProjection).order_by(TfmsProjection.updated_at.desc()).limit(limit)
    result = await session.execute(stmt)
    return list(result.scalars())

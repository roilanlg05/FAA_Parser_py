from __future__ import annotations

import json
import logging
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .config import settings
from .redis_client import RedisManager
from .tbfm_models import TbfmEvent, TbfmProjection
from .tbfm_parser_adapter import build_tbfm_projections, parse_tbfm_xml
from .tbfm_payload_utils import strip_raw_fields

logger = logging.getLogger(__name__)

def _tbfm_summary(parsed: dict[str, Any]) -> tuple[str | None, str | None, str | None, str | None, str | None, str | None]:
    first_doc = ((parsed.get('documents') or [None])[0] or {}) if isinstance(parsed, dict) else {}
    env_attrs = (first_doc.get('attributes') or {}) if isinstance(first_doc, dict) else {}
    first_tma = ((first_doc.get('tma') or [None])[0] or {}) if isinstance(first_doc, dict) else {}
    tma_attrs = (first_tma.get('attributes') or {}) if isinstance(first_tma, dict) else {}
    first_air = ((first_tma.get('air') or [None])[0] or {}) if isinstance(first_tma, dict) else {}
    air_attrs = (first_air.get('attributes') or {}) if isinstance(first_air, dict) else {}
    first_flt = (first_air.get('flt') or {}) if isinstance(first_air, dict) else {}
    return (
        env_attrs.get('envSrce'),
        air_attrs.get('airType'),
        tma_attrs.get('msgId'),
        air_attrs.get('aid') or first_flt.get('aid'),
        None,
        air_attrs.get('tmaId'),
    )


async def ingest_tbfm_xml(
    session: AsyncSession,
    redis_manager: RedisManager,
    *,
    xml_text: str,
    queue_name: str | None,
    metadata: dict[str, Any] | None,
) -> dict[str, Any]:
    parsed = parse_tbfm_xml(xml_text)
    source_facility, msg_type, flight_ref, acid, gufi, tma_id = _tbfm_summary(parsed)
    projections = build_tbfm_projections(parsed)
    source_time = None
    first_doc = ((parsed.get('documents') or [None])[0] or {}) if isinstance(parsed, dict) else {}
    env_attrs = (first_doc.get('attributes') or {}) if isinstance(first_doc, dict) else {}
    first_tma = ((first_doc.get('tma') or [None])[0] or {}) if isinstance(first_doc, dict) else {}
    tma_attrs = (first_tma.get('attributes') or {}) if isinstance(first_tma, dict) else {}
    source_time = tma_attrs.get('msgTime') or env_attrs.get('envTime')

    compact_parsed = strip_raw_fields(parsed)

    event = TbfmEvent(
        queue_name=queue_name or settings.tbfm_queue_name,
        payload_type='tbfm_metering_publication',
        root_tag='env',
        source_facility=source_facility,
        msg_type=msg_type,
        flight_ref=flight_ref,
        acid=acid,
        gufi=gufi,
        tma_id=tma_id,
        source_time=source_time,
        raw_xml=xml_text,
        parsed_json=compact_parsed,
    )
    session.add(event)

    for projection in projections:
        key = projection.get('projection_key') or projection.get('key')
        if not key:
            continue
        existing = await session.get(TbfmProjection, key)
        if existing is None:
            existing = TbfmProjection(projection_key=key)
            session.add(existing)
        existing.projection_type = projection.get('projection_type') or 'unknown'
        existing.acid = projection.get('acid')
        existing.gufi = projection.get('gufi')
        existing.tma_id = projection.get('tma_id')
        existing.flight_ref = projection.get('flight_ref')
        existing.msg_type = projection.get('msg_type')
        existing.source_facility = projection.get('source_facility')
        existing.source_time = projection.get('source_time')
        existing.data = strip_raw_fields(projection.get('data') or {})

    await session.commit()
    await session.refresh(event)

    try:
        redis = await redis_manager.connect_tbfm()
        event_payload = {
            'event_id': event.id,
            'queue_name': event.queue_name,
            'payload_type': event.payload_type,
            'parsed': compact_parsed,
            'metadata': metadata or {},
        }
        await redis.publish(settings.tbfm_events_channel_name, json.dumps(event_payload, ensure_ascii=False))

        for projection in projections:
            projection_payload = {
                'projection_type': projection.get('projection_type'),
                'projection_key': projection.get('projection_key') or projection.get('key'),
                'acid': projection.get('acid'),
                'gufi': projection.get('gufi'),
                'tma_id': projection.get('tma_id'),
                'flight_ref': projection.get('flight_ref'),
                'msg_type': projection.get('msg_type'),
                'source_facility': projection.get('source_facility'),
                'source_time': projection.get('source_time'),
                'data': strip_raw_fields(projection.get('data') or {}),
            }
            await redis.publish(settings.tbfm_projections_channel_name, json.dumps(projection_payload, ensure_ascii=False))
    except Exception:
        logger.exception('Failed to publish TBFM redis notifications event_id=%s', event.id)

    return {
        'status': 'accepted',
        'event_id': event.id,
        'payload_type': event.payload_type,
        'projection_count': len(projections),
        'queue_name': event.queue_name,
        'parsed': compact_parsed,
    }


async def get_tbfm_events(session: AsyncSession, limit: int = 100) -> list[TbfmEvent]:
    stmt = select(TbfmEvent).order_by(TbfmEvent.created_at.desc()).limit(limit)
    result = await session.execute(stmt)
    return list(result.scalars())


async def get_tbfm_projections(session: AsyncSession, limit: int = 100) -> list[TbfmProjection]:
    stmt = select(TbfmProjection).order_by(TbfmProjection.updated_at.desc()).limit(limit)
    result = await session.execute(stmt)
    return list(result.scalars())

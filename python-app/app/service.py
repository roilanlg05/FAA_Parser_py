from __future__ import annotations

from typing import Any

import logging
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .models import FlightCurrent, RawEvent
from .parser import parse_faa_xml
from .projection import extract_projected_flights
from .redis_client import RedisManager

logger = logging.getLogger(__name__)


async def persist_parsed_payload(
    session: AsyncSession,
    redis_manager: RedisManager,
    *,
    stream_id: str,
    source: str,
    raw_xml: str,
    parsed: dict[str, Any],
) -> dict[str, Any]:
    flights = extract_projected_flights(parsed)
    first_flight = flights[0] if flights else {}

    raw_event = RawEvent(
        stream_id=stream_id,
        source=source,
        payload_type=parsed.get('payload_type', 'unknown'),
        gufi=first_flight.get('gufi'),
        flight_id=first_flight.get('flight_id'),
        raw_xml=raw_xml,
        parsed_json=parsed,
    )
    session.add(raw_event)

    for projected in flights:
        existing = await session.get(FlightCurrent, projected['gufi'])
        if existing is None:
            existing = FlightCurrent(gufi=projected['gufi'])
            session.add(existing)
        existing.flight_id = projected['flight_id']
        existing.operator = projected['operator']
        existing.status = projected['status']
        existing.departure_airport = projected['departure_airport']
        existing.arrival_airport = projected['arrival_airport']
        existing.departure_actual_time = projected['departure_actual_time']
        existing.arrival_estimated_time = projected['arrival_estimated_time']
        existing.source_timestamp = projected['source_timestamp']
        existing.payload_type = projected['payload_type']
        existing.last_stream_id = stream_id
        existing.last_payload = projected['last_payload']

    await session.commit()
    try:
        await redis_manager.publish_parsed(
            {
                'stream_id': stream_id,
                'source': source,
                'payload_type': parsed.get('payload_type', 'unknown'),
                'flight_count': len(flights),
                'parsed': parsed,
            }
        )
    except Exception:
        logger.exception('Failed to publish parsed payload stream_id=%s', stream_id)
    return {'stream_id': stream_id, 'payload_type': parsed.get('payload_type', 'unknown'), 'flight_count': len(flights)}


async def ingest_xml(
    session: AsyncSession,
    redis_manager: RedisManager,
    *,
    stream_id: str,
    source: str,
    xml_text: str,
) -> dict[str, Any]:
    parsed = parse_faa_xml(xml_text)
    return await persist_parsed_payload(
        session,
        redis_manager,
        stream_id=stream_id,
        source=source,
        raw_xml=xml_text,
        parsed=parsed,
    )


async def get_recent_events(session: AsyncSession, limit: int = 50) -> list[RawEvent]:
    stmt = select(RawEvent).order_by(RawEvent.id.desc()).limit(limit)
    result = await session.execute(stmt)
    return list(result.scalars())


async def get_current_flights(session: AsyncSession, limit: int = 50) -> list[FlightCurrent]:
    stmt = select(FlightCurrent).order_by(FlightCurrent.updated_at.desc()).limit(limit)
    result = await session.execute(stmt)
    return list(result.scalars())

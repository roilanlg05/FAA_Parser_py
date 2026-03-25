from __future__ import annotations

from datetime import datetime
from typing import Any


def parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    if value.endswith('Z'):
        value = value.replace('Z', '+00:00')
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def extract_projected_flights(parsed: dict[str, Any]) -> list[dict[str, Any]]:
    if parsed.get('payload_type') != 'sfdps_message_collection':
        return []
    projected: list[dict[str, Any]] = []
    for message in parsed.get('messages', []):
        flight = message.get('flight') or {}
        gufi = flight.get('gufi')
        if not gufi:
            continue
        projected.append(
            {
                'gufi': gufi,
                'flight_id': ((flight.get('flight_identification') or {}).get('aircraft_identification')),
                'operator': flight.get('operator'),
                'status': flight.get('flight_status'),
                'departure_airport': ((flight.get('departure') or {}).get('airport')),
                'arrival_airport': ((flight.get('arrival') or {}).get('airport')),
                'departure_actual_time': parse_dt((flight.get('departure') or {}).get('actual_runway_time')),
                'arrival_estimated_time': parse_dt((flight.get('arrival') or {}).get('estimated_runway_time')),
                'source_timestamp': parse_dt((flight.get('meta') or {}).get('timestamp')),
                'payload_type': parsed.get('payload_type', 'sfdps_message_collection'),
                'last_payload': flight,
            }
        )
    return projected

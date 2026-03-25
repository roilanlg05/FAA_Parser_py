from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str
    redis: str
    database: str


class IngestRequest(BaseModel):
    xml: str
    source: str = 'manual'


class EventResponse(BaseModel):
    stream_id: str
    payload_type: str
    source: Optional[str] = None
    gufi: Optional[str] = None
    flight_id: Optional[str] = None
    parsed_json: dict[str, Any]
    flights: Optional[list[dict[str, Any]]] = None
    received_at: Optional[datetime] = None


class FlightCurrentResponse(BaseModel):
    gufi: str
    flight_id: Optional[str] = None
    operator: Optional[str] = None
    status: Optional[str] = None
    departure_airport: Optional[str] = None
    arrival_airport: Optional[str] = None
    departure_actual_time: Optional[datetime] = None
    arrival_estimated_time: Optional[datetime] = None
    source_timestamp: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    payload_type: str
    last_payload: Optional[dict[str, Any]] = None

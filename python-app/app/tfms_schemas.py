from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class TfmsIngestRequest(BaseModel):
    xml: str
    queue_name: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class TfmsEventResponse(BaseModel):
    id: int
    queue_name: str | None = None
    payload_type: str
    root_tag: str | None = None
    source_facility: str | None = None
    msg_type: str | None = None
    flight_ref: str | None = None
    acid: str | None = None
    gufi: str | None = None
    parsed_json: dict[str, Any]
    created_at: datetime | None = None


class TfmsProjectionResponse(BaseModel):
    projection_key: str
    projection_type: str
    acid: str | None = None
    gufi: str | None = None
    flight_ref: str | None = None
    msg_type: str | None = None
    source_facility: str | None = None
    source_timestamp: str | None = None
    data: dict[str, Any]
    updated_at: datetime | None = None

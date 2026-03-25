from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.types import JSON
from sqlalchemy.orm import Mapped, mapped_column

from .db import Base


JSONType = JSON().with_variant(JSONB, 'postgresql')


class RawEvent(Base):
    __tablename__ = 'raw_events'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    stream_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    source: Mapped[str | None] = mapped_column(String(128), nullable=True)
    payload_type: Mapped[str] = mapped_column(String(64), index=True)
    gufi: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    flight_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    raw_xml: Mapped[str] = mapped_column(Text)
    parsed_json: Mapped[dict] = mapped_column(JSONType)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)


class FlightCurrent(Base):
    __tablename__ = 'flights_current'

    gufi: Mapped[str] = mapped_column(String(128), primary_key=True)
    flight_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    operator: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    departure_airport: Mapped[str | None] = mapped_column(String(16), nullable=True, index=True)
    arrival_airport: Mapped[str | None] = mapped_column(String(16), nullable=True, index=True)
    departure_actual_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    arrival_estimated_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    source_timestamp: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    payload_type: Mapped[str] = mapped_column(String(64), default='sfdps_message_collection')
    last_stream_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    last_payload: Mapped[dict | None] = mapped_column(JSONType, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

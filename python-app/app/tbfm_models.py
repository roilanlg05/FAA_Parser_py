from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import JSON

from .tbfm_db import TbfmBase


JSONType = JSON().with_variant(JSONB, 'postgresql')


class TbfmEvent(TbfmBase):
    __tablename__ = 'tbfm_events'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    queue_name: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    payload_type: Mapped[str] = mapped_column(String(128), index=True)
    root_tag: Mapped[str | None] = mapped_column(String(128), nullable=True)
    source_facility: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    msg_type: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    flight_ref: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    acid: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    gufi: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    tma_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    source_time: Mapped[str | None] = mapped_column(String(64), nullable=True)
    raw_xml: Mapped[str] = mapped_column(Text)
    parsed_json: Mapped[dict] = mapped_column(JSONType)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)


class TbfmProjection(TbfmBase):
    __tablename__ = 'tbfm_projections'

    projection_key: Mapped[str] = mapped_column(String(255), primary_key=True)
    projection_type: Mapped[str] = mapped_column(String(64), index=True)
    acid: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    gufi: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    tma_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    flight_ref: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    msg_type: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    source_facility: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    source_time: Mapped[str | None] = mapped_column(String(64), nullable=True)
    data: Mapped[dict] = mapped_column(JSONType)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

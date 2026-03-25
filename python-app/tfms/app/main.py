from __future__ import annotations

import json
import os
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

import redis
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .db import fetch_events, fetch_projection, fetch_projections, init_db, insert_event, upsert_projection
from .tfms_parser import build_projections, parse_tfms_xml


REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
REDIS_EVENTS_CHANNEL = os.getenv("REDIS_EVENTS_CHANNEL", "tfms.events")
REDIS_PROJECTIONS_CHANNEL = os.getenv("REDIS_PROJECTIONS_CHANNEL", "tfms.projections")
QUEUE_NAME = os.getenv(
    "TFMS_QUEUE_NAME",
    "landgbnb.gmail.com.TFMS.3b02c856-4246-4fa3-8c92-a7c2a37d2176.OUT",
)


class RawIngestRequest(BaseModel):
    xml: str = Field(..., description="Raw XML payload")
    queue_name: Optional[str] = Field(default=None)
    metadata: Dict[str, Any] = Field(default_factory=dict)



def _root_facility_and_msg(parsed: Dict[str, Any]) -> tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    payload_type = parsed.get("payload_type")
    if payload_type == "tfms_flight_data_output":
        first = (parsed.get("messages") or [None])[0] or {}
        body = first.get("body") or {}
        qid = body.get("qualifiedAircraftId") or {}
        return (
            first.get("sourceFacility"),
            first.get("msgType"),
            first.get("flightRef"),
            qid.get("aircraftId") or first.get("acid"),
        )
    if payload_type == "tfms_flow_information_output":
        first = (parsed.get("messages") or [None])[0] or {}
        tmi_first = (first.get("tmiFlightDataList") or [None])[0] or {}
        flight = tmi_first.get("flight") or {}
        return (
            first.get("sourceFacility"),
            first.get("msgType"),
            tmi_first.get("flightReference"),
            flight.get("aircraftId"),
        )
    if payload_type == "tfms_status_output":
        first = (parsed.get("statuses") or [None])[0] or {}
        return (first.get("facility"), "status", None, None)
    return (None, None, None, None)


@asynccontextmanager
def lifespan(app: FastAPI):
    init_db()
    app.state.redis = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    yield
    app.state.redis.close()


app = FastAPI(title="TFMS Consumer API", lifespan=lifespan)


@app.get("/health")
def health() -> Dict[str, Any]:
    return {"ok": True, "queue_name": QUEUE_NAME}


@app.post("/ingest/raw")
def ingest_raw(request: RawIngestRequest) -> Dict[str, Any]:
    parsed = parse_tfms_xml(request.xml)
    source_facility, msg_type, flight_ref, acid = _root_facility_and_msg(parsed)
    projections = build_projections(parsed)
    gufi = None
    if projections:
        gufi = projections[0].get("gufi")

    event_id = insert_event(
        queue_name=request.queue_name or QUEUE_NAME,
        payload_type=parsed.get("payload_type") or "unknown",
        root_tag=parsed.get("root_tag"),
        source_facility=source_facility,
        msg_type=msg_type,
        flight_ref=flight_ref,
        acid=acid,
        gufi=gufi,
        raw_xml=request.xml,
        parsed_json=parsed,
    )

    redis_client: redis.Redis = app.state.redis
    redis_client.publish(
        REDIS_EVENTS_CHANNEL,
        json.dumps(
            {
                "event_id": event_id,
                "queue_name": request.queue_name or QUEUE_NAME,
                "payload_type": parsed.get("payload_type"),
                "parsed": parsed,
                "metadata": request.metadata,
            },
            ensure_ascii=False,
        ),
    )

    for projection in projections:
        upsert_projection(projection)
        redis_client.set(f"tfms:projection:{projection['key']}", json.dumps(projection, ensure_ascii=False))
        redis_client.publish(REDIS_PROJECTIONS_CHANNEL, json.dumps(projection, ensure_ascii=False))

    return {
        "event_id": event_id,
        "payload_type": parsed.get("payload_type"),
        "projection_count": len(projections),
        "queue_name": request.queue_name or QUEUE_NAME,
        "parsed": parsed,
    }


@app.get("/events")
def get_events(limit: int = 100) -> Dict[str, Any]:
    return {"items": fetch_events(limit=limit)}


@app.get("/projections")
def get_projections(limit: int = 100) -> Dict[str, Any]:
    return {"items": fetch_projections(limit=limit)}


@app.get("/projections/{projection_key}")
def get_projection(projection_key: str) -> Dict[str, Any]:
    item = fetch_projection(projection_key)
    if not item:
        raise HTTPException(status_code=404, detail="projection not found")
    return item

from __future__ import annotations

import json
import os
from contextlib import contextmanager
from typing import Any, Dict, Generator, Optional

import psycopg
from psycopg.rows import dict_row


DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/tfms")


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS tfms_events (
  id BIGSERIAL PRIMARY KEY,
  queue_name TEXT,
  payload_type TEXT NOT NULL,
  root_tag TEXT,
  source_facility TEXT,
  msg_type TEXT,
  flight_ref TEXT,
  acid TEXT,
  gufi TEXT,
  raw_xml TEXT NOT NULL,
  parsed_json JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tfms_projections (
  projection_key TEXT PRIMARY KEY,
  projection_type TEXT NOT NULL,
  acid TEXT,
  gufi TEXT,
  flight_ref TEXT,
  msg_type TEXT,
  source_facility TEXT,
  source_timestamp TEXT,
  data JSONB NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tfms_events_created_at ON tfms_events (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_tfms_events_gufi ON tfms_events (gufi);
CREATE INDEX IF NOT EXISTS idx_tfms_events_acid ON tfms_events (acid);
CREATE INDEX IF NOT EXISTS idx_tfms_events_msg_type ON tfms_events (msg_type);
"""


@contextmanager
def get_conn() -> Generator[psycopg.Connection, None, None]:
    conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)
    try:
        yield conn
    finally:
        conn.close()



def init_db() -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
        conn.commit()



def insert_event(
    queue_name: Optional[str],
    payload_type: str,
    root_tag: Optional[str],
    source_facility: Optional[str],
    msg_type: Optional[str],
    flight_ref: Optional[str],
    acid: Optional[str],
    gufi: Optional[str],
    raw_xml: str,
    parsed_json: Dict[str, Any],
) -> int:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO tfms_events (
                  queue_name, payload_type, root_tag, source_facility, msg_type, flight_ref,
                  acid, gufi, raw_xml, parsed_json
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING id
                """,
                (
                    queue_name,
                    payload_type,
                    root_tag,
                    source_facility,
                    msg_type,
                    flight_ref,
                    acid,
                    gufi,
                    raw_xml,
                    json.dumps(parsed_json),
                ),
            )
            row = cur.fetchone()
        conn.commit()
        return int(row["id"])



def upsert_projection(projection: Dict[str, Any]) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO tfms_projections (
                  projection_key, projection_type, acid, gufi, flight_ref, msg_type,
                  source_facility, source_timestamp, data
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (projection_key) DO UPDATE SET
                  projection_type = EXCLUDED.projection_type,
                  acid = EXCLUDED.acid,
                  gufi = EXCLUDED.gufi,
                  flight_ref = EXCLUDED.flight_ref,
                  msg_type = EXCLUDED.msg_type,
                  source_facility = EXCLUDED.source_facility,
                  source_timestamp = EXCLUDED.source_timestamp,
                  data = EXCLUDED.data,
                  updated_at = NOW()
                """,
                (
                    projection["key"],
                    projection["projection_type"],
                    projection.get("acid"),
                    projection.get("gufi"),
                    projection.get("flightRef"),
                    projection.get("msgType"),
                    projection.get("sourceFacility"),
                    projection.get("sourceTimeStamp"),
                    json.dumps(projection["data"]),
                ),
            )
        conn.commit()



def fetch_events(limit: int = 100) -> list[dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM tfms_events ORDER BY created_at DESC LIMIT %s", (limit,))
            rows = cur.fetchall()
    return list(rows)



def fetch_projection(key: str) -> Optional[dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM tfms_projections WHERE projection_key=%s", (key,))
            row = cur.fetchone()
    return dict(row) if row else None



def fetch_projections(limit: int = 100) -> list[dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM tfms_projections ORDER BY updated_at DESC LIMIT %s", (limit,))
            rows = cur.fetchall()
    return list(rows)

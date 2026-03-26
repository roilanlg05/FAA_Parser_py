\connect faa_swim;

CREATE TABLE IF NOT EXISTS raw_events_archive (
  id INTEGER PRIMARY KEY,
  stream_id VARCHAR(64) NOT NULL,
  source VARCHAR(128),
  payload_type VARCHAR(64) NOT NULL,
  gufi VARCHAR(128),
  flight_id VARCHAR(64),
  raw_xml TEXT NOT NULL,
  parsed_json JSONB NOT NULL,
  received_at TIMESTAMPTZ,
  archived_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS ix_raw_events_archive_stream_id
  ON raw_events_archive (stream_id);

CREATE INDEX IF NOT EXISTS ix_raw_events_archive_received_at
  ON raw_events_archive (received_at);

CREATE INDEX IF NOT EXISTS ix_raw_events_archive_payload_type
  ON raw_events_archive (payload_type);

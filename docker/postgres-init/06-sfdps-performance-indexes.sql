\connect faa_swim;

CREATE INDEX IF NOT EXISTS ix_raw_events_received_at_desc
  ON raw_events (received_at DESC);

CREATE INDEX IF NOT EXISTS ix_raw_events_payload_type_received_at_desc
  ON raw_events (payload_type, received_at DESC);

CREATE INDEX IF NOT EXISTS ix_flights_current_updated_at_desc
  ON flights_current (updated_at DESC);

\connect faa_swim;

DO $$
BEGIN
  IF to_regclass('public.raw_events') IS NOT NULL THEN
    EXECUTE 'CREATE INDEX IF NOT EXISTS ix_raw_events_received_at_desc ON raw_events (received_at DESC)';
    EXECUTE 'CREATE INDEX IF NOT EXISTS ix_raw_events_payload_type_received_at_desc ON raw_events (payload_type, received_at DESC)';
  END IF;

  IF to_regclass('public.flights_current') IS NOT NULL THEN
    EXECUTE 'CREATE INDEX IF NOT EXISTS ix_flights_current_updated_at_desc ON flights_current (updated_at DESC)';
  END IF;
END
$$;

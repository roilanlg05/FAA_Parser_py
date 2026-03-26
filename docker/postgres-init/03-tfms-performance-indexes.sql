\connect tfms;

DO $$
BEGIN
  IF to_regclass('public.tfms_events') IS NOT NULL THEN
    EXECUTE 'CREATE INDEX IF NOT EXISTS ix_tfms_events_created_msg_acid_gufi ON tfms_events (created_at DESC, msg_type, acid, gufi)';
  END IF;

  IF to_regclass('public.tfms_projections') IS NOT NULL THEN
    EXECUTE 'CREATE INDEX IF NOT EXISTS ix_tfms_projections_updated_type_msg_acid_gufi ON tfms_projections (updated_at DESC, projection_type, msg_type, acid, gufi)';
  END IF;
END
$$;

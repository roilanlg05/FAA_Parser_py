\connect tbfm;

DO $$
BEGIN
  IF to_regclass('public.tbfm_events') IS NOT NULL THEN
    EXECUTE 'CREATE INDEX IF NOT EXISTS ix_tbfm_events_created_msg_acid_gufi_tma ON tbfm_events (created_at DESC, msg_type, acid, gufi, tma_id)';
  END IF;

  IF to_regclass('public.tbfm_projections') IS NOT NULL THEN
    EXECUTE 'CREATE INDEX IF NOT EXISTS ix_tbfm_projections_updated_type_msg_acid_gufi_tma ON tbfm_projections (updated_at DESC, projection_type, msg_type, acid, gufi, tma_id)';
  END IF;
END
$$;

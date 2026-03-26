\connect tbfm;

CREATE INDEX IF NOT EXISTS ix_tbfm_events_created_msg_acid_gufi_tma
  ON tbfm_events (created_at DESC, msg_type, acid, gufi, tma_id);

CREATE INDEX IF NOT EXISTS ix_tbfm_projections_updated_type_msg_acid_gufi_tma
  ON tbfm_projections (updated_at DESC, projection_type, msg_type, acid, gufi, tma_id);

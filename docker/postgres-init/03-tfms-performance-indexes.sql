\connect tfms;

CREATE INDEX IF NOT EXISTS ix_tfms_events_created_msg_acid_gufi
  ON tfms_events (created_at DESC, msg_type, acid, gufi);

CREATE INDEX IF NOT EXISTS ix_tfms_projections_updated_type_msg_acid_gufi
  ON tfms_projections (updated_at DESC, projection_type, msg_type, acid, gufi);

ALTER TABLE jobs ADD COLUMN priority SMALLINT NOT NULL DEFAULT 0 CHECK (priority BETWEEN 0 AND 9);
ALTER TABLE jobs ADD COLUMN scheduled_at TIMESTAMPTZ;

ALTER TABLE job_outbox ADD COLUMN priority SMALLINT NOT NULL DEFAULT 0 CHECK (priority BETWEEN 0 AND 9);
ALTER TABLE job_outbox ADD COLUMN available_at TIMESTAMPTZ NOT NULL DEFAULT now();
DROP INDEX job_outbox_created_idx;
CREATE INDEX job_outbox_dispatch_idx ON job_outbox (priority DESC, available_at, created_at, job_id);

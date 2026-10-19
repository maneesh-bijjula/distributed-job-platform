ALTER TABLE jobs DROP CONSTRAINT jobs_status_check;
ALTER TABLE jobs DROP CONSTRAINT jobs_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_status_check
    CHECK (status IN ('QUEUED', 'SUCCEEDED', 'FAILED', 'CANCELLED'));
ALTER TABLE jobs ADD CONSTRAINT jobs_terminal_state_check CHECK (
    (status = 'QUEUED' AND result IS NULL AND error IS NULL AND completed_at IS NULL)
    OR (status = 'SUCCEEDED' AND result IS NOT NULL AND error IS NULL AND completed_at IS NOT NULL)
    OR (status = 'FAILED' AND result IS NULL AND error IS NOT NULL AND completed_at IS NOT NULL)
    OR (status = 'CANCELLED' AND result IS NULL AND error IS NULL AND completed_at IS NOT NULL)
);
ALTER TABLE jobs ADD COLUMN retry_of UUID REFERENCES jobs(id);
CREATE INDEX jobs_retry_of_idx ON jobs (retry_of) WHERE retry_of IS NOT NULL;

CREATE TABLE job_events (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    event_type VARCHAR(32) NOT NULL CHECK (event_type IN
        ('BASELINE', 'SUBMITTED', 'SUCCEEDED', 'FAILED', 'CANCELLED', 'RETRY_REQUESTED')),
    status VARCHAR(16) NOT NULL CHECK (status IN ('QUEUED', 'SUCCEEDED', 'FAILED', 'CANCELLED')),
    related_job_id UUID REFERENCES jobs(id),
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX job_events_job_id_idx ON job_events (job_id, id);

-- Existing jobs have no historical audit trail. Record their known state at
-- migration time rather than inventing submission or execution events.
INSERT INTO job_events (job_id, event_type, status)
SELECT id, 'BASELINE', status FROM jobs;

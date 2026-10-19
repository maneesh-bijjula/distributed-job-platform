ALTER TABLE jobs ADD COLUMN max_attempts INTEGER NOT NULL DEFAULT 4 CHECK (max_attempts BETWEEN 1 AND 25);
ALTER TABLE jobs ADD COLUMN initial_backoff_seconds INTEGER NOT NULL DEFAULT 1 CHECK (initial_backoff_seconds BETWEEN 1 AND 3600);
ALTER TABLE jobs ADD COLUMN max_backoff_seconds INTEGER NOT NULL DEFAULT 60 CHECK (max_backoff_seconds BETWEEN 1 AND 3600);
ALTER TABLE jobs ADD CONSTRAINT jobs_backoff_order CHECK (initial_backoff_seconds <= max_backoff_seconds);
ALTER TABLE jobs ADD COLUMN retry_at TIMESTAMPTZ;
ALTER TABLE jobs ADD CONSTRAINT jobs_retry_wait_check CHECK (retry_at IS NULL OR status='QUEUED');
ALTER TABLE jobs ADD COLUMN legacy_retry_policy BOOLEAN NOT NULL DEFAULT FALSE;
-- Existing in-flight jobs must retain enough attempts for the old recovery limit (up to 20).
UPDATE jobs SET max_attempts = 25, legacy_retry_policy = TRUE;
ALTER TABLE job_attempts ADD COLUMN failure_kind VARCHAR(32)
    CHECK (failure_kind IN ('RETRYABLE_INFRASTRUCTURE','PERMANENT_HANDLER'));

ALTER TABLE job_events DROP CONSTRAINT job_events_event_type_check;
ALTER TABLE job_events ADD CONSTRAINT job_events_event_type_check CHECK (event_type IN
    ('BASELINE','SUBMITTED','STARTED','SUCCEEDED','FAILED','CANCELLED','RETRY_REQUESTED',
     'LEASE_EXPIRED','TIMED_OUT','RECOVERY_EXHAUSTED','RETRY_SCHEDULED','RETRIES_EXHAUSTED',
     'DEAD_LETTER_RECOVERED','DEAD_LETTER_DISCARDED'));

CREATE TABLE dead_letters (
    id UUID PRIMARY KEY,
    dedup_key VARCHAR(160) NOT NULL UNIQUE,
    source VARCHAR(16) NOT NULL CHECK (source IN ('BROKER','EXECUTION')),
    reason VARCHAR(32) NOT NULL CHECK (reason IN ('DELIVERY_FAILED','MALFORMED_MESSAGE','UNKNOWN_JOB',
        'RETRIES_EXHAUSTED','PERMANENT_HANDLER','DEADLINE_EXCEEDED','RECOVERY_EXHAUSTED')),
    job_id UUID REFERENCES jobs(id),
    generation INTEGER CHECK (generation >= 0),
    payload_sha256 VARCHAR(64),
    payload_bytes INTEGER CHECK (payload_bytes >= 0),
    captured_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    state VARCHAR(16) NOT NULL DEFAULT 'OPEN' CHECK (state IN ('OPEN','RECOVERED','DISCARDED')),
    resolution_key VARCHAR(128) UNIQUE,
    resolved_at TIMESTAMPTZ,
    recovery_job_id UUID REFERENCES jobs(id),
    CHECK ((state='OPEN' AND resolution_key IS NULL AND resolved_at IS NULL AND recovery_job_id IS NULL)
        OR (state='RECOVERED' AND resolution_key IS NOT NULL AND resolved_at IS NOT NULL AND recovery_job_id IS NOT NULL)
        OR (state='DISCARDED' AND resolution_key IS NOT NULL AND resolved_at IS NOT NULL AND recovery_job_id IS NULL))
);
CREATE INDEX dead_letters_listing_idx ON dead_letters(captured_at,id);
CREATE INDEX dead_letters_open_idx ON dead_letters(captured_at,id) WHERE state='OPEN';

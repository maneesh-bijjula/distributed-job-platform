ALTER TABLE jobs DROP CONSTRAINT jobs_status_check;
ALTER TABLE jobs DROP CONSTRAINT jobs_terminal_state_check;
ALTER TABLE jobs ADD COLUMN timeout_seconds INTEGER NOT NULL DEFAULT 30 CHECK (timeout_seconds BETWEEN 1 AND 3600);
ALTER TABLE jobs ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0);
ALTER TABLE jobs ADD COLUMN recovery_count INTEGER NOT NULL DEFAULT 0 CHECK (recovery_count >= 0);
ALTER TABLE jobs ADD COLUMN generation INTEGER NOT NULL DEFAULT 0 CHECK (generation >= 0);
ALTER TABLE jobs ADD COLUMN active_attempt_id UUID;
ALTER TABLE jobs ADD COLUMN deadline_at TIMESTAMPTZ;
ALTER TABLE jobs ADD CONSTRAINT jobs_status_check
    CHECK (status IN ('QUEUED','RUNNING','SUCCEEDED','FAILED','CANCELLED'));
ALTER TABLE jobs ADD CONSTRAINT jobs_terminal_state_check CHECK (
    (status = 'QUEUED' AND result IS NULL AND error IS NULL AND completed_at IS NULL AND active_attempt_id IS NULL)
    OR (status = 'RUNNING' AND result IS NULL AND error IS NULL AND completed_at IS NULL AND active_attempt_id IS NOT NULL AND deadline_at IS NOT NULL)
    OR (status = 'SUCCEEDED' AND result IS NOT NULL AND error IS NULL AND completed_at IS NOT NULL AND active_attempt_id IS NULL)
    OR (status = 'FAILED' AND result IS NULL AND error IS NOT NULL AND completed_at IS NOT NULL AND active_attempt_id IS NULL)
    OR (status = 'CANCELLED' AND result IS NULL AND error IS NULL AND completed_at IS NOT NULL AND active_attempt_id IS NULL)
);

CREATE TABLE job_attempts (
    id UUID PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(id),
    attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
    generation INTEGER NOT NULL CHECK (generation >= 0),
    claim_token UUID NOT NULL UNIQUE,
    worker_id VARCHAR(128) NOT NULL,
    status VARCHAR(16) NOT NULL CHECK (status IN ('RUNNING','SUCCEEDED','FAILED','CANCELLED','TIMED_OUT','ABANDONED')),
    started_at TIMESTAMPTZ NOT NULL,
    heartbeat_at TIMESTAMPTZ NOT NULL,
    lease_until TIMESTAMPTZ NOT NULL,
    deadline_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    error VARCHAR(256),
    UNIQUE(job_id, attempt_number),
    UNIQUE(job_id, generation),
    UNIQUE(id, job_id),
    CHECK (lease_until <= deadline_at),
    CHECK ((status = 'RUNNING' AND finished_at IS NULL AND error IS NULL)
        OR (status IN ('SUCCEEDED','CANCELLED') AND finished_at IS NOT NULL AND error IS NULL)
        OR (status IN ('FAILED','TIMED_OUT','ABANDONED') AND finished_at IS NOT NULL AND error IS NOT NULL))
);
ALTER TABLE jobs ADD CONSTRAINT jobs_active_attempt_fk
    FOREIGN KEY (active_attempt_id,id) REFERENCES job_attempts(id,job_id);
CREATE INDEX job_attempts_job_order_idx ON job_attempts(job_id,attempt_number);
CREATE INDEX job_attempts_active_lease_idx ON job_attempts(lease_until) WHERE status = 'RUNNING';
CREATE INDEX jobs_queued_deadline_idx ON jobs(deadline_at) WHERE status = 'QUEUED' AND deadline_at IS NOT NULL;

ALTER TABLE job_outbox ADD COLUMN generation INTEGER NOT NULL DEFAULT 0 CHECK (generation >= 0);
ALTER TABLE job_events DROP CONSTRAINT job_events_event_type_check;
ALTER TABLE job_events DROP CONSTRAINT job_events_status_check;
ALTER TABLE job_events ADD CONSTRAINT job_events_event_type_check CHECK (event_type IN
    ('BASELINE','SUBMITTED','STARTED','SUCCEEDED','FAILED','CANCELLED','RETRY_REQUESTED',
     'LEASE_EXPIRED','TIMED_OUT','RECOVERY_EXHAUSTED'));
ALTER TABLE job_events ADD CONSTRAINT job_events_status_check
    CHECK (status IN ('QUEUED','RUNNING','SUCCEEDED','FAILED','CANCELLED'));
ALTER TABLE job_events ADD COLUMN attempt_id UUID REFERENCES job_attempts(id);

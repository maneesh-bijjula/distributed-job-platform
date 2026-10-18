CREATE TABLE jobs (
    id UUID PRIMARY KEY,
    type VARCHAR(32) NOT NULL CHECK (type IN ('WORD_COUNT', 'SHA256', 'UPPERCASE')),
    input_text TEXT NOT NULL CHECK (length(input_text) BETWEEN 1 AND 16000),
    status VARCHAR(16) NOT NULL DEFAULT 'QUEUED' CHECK (status IN ('QUEUED', 'SUCCEEDED', 'FAILED')),
    result JSONB,
    error VARCHAR(256),
    idempotency_key VARCHAR(128) UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    CHECK (
        (status = 'QUEUED' AND result IS NULL AND error IS NULL AND completed_at IS NULL)
        OR (status = 'SUCCEEDED' AND result IS NOT NULL AND error IS NULL AND completed_at IS NOT NULL)
        OR (status = 'FAILED' AND result IS NULL AND error IS NOT NULL AND completed_at IS NOT NULL)
    )
);
CREATE INDEX jobs_created_idx ON jobs (created_at DESC, id DESC);
CREATE INDEX jobs_status_created_idx ON jobs (status, created_at DESC, id DESC);

CREATE TABLE job_outbox (
    job_id UUID PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX job_outbox_created_idx ON job_outbox (created_at, job_id);

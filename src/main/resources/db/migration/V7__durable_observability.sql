-- Counters begin at this migration; earlier transitions are not reconstructed.
CREATE TABLE job_metric_counts (
    event VARCHAR(32) NOT NULL,
    job_type VARCHAR(32) NOT NULL CHECK (job_type IN ('WORD_COUNT','SHA256','UPPERCASE')),
    value BIGINT NOT NULL CHECK (value >= 0),
    PRIMARY KEY(event,job_type)
);
CREATE TABLE job_metric_timings (
    kind VARCHAR(16) NOT NULL CHECK (kind IN ('QUEUE_DELAY','EXECUTION')),
    job_type VARCHAR(32) NOT NULL CHECK (job_type IN ('WORD_COUNT','SHA256','UPPERCASE')),
    outcome VARCHAR(16) NOT NULL CHECK (outcome IN ('STARTED','SUCCEEDED','FAILED','CANCELLED','TIMED_OUT','ABANDONED')),
    sample_count BIGINT NOT NULL CHECK (sample_count > 0),
    total_seconds DOUBLE PRECISION NOT NULL CHECK (total_seconds >= 0),
    PRIMARY KEY(kind,job_type,outcome)
);
ALTER TABLE jobs ADD COLUMN eligible_at TIMESTAMPTZ;
ALTER TABLE job_attempts ADD COLUMN queue_delay_seconds DOUBLE PRECISION CHECK (queue_delay_seconds >= 0);
-- Null eligibility/delay on legacy rows avoids inventing historical queue measurements.
ALTER TABLE api_keys DROP CONSTRAINT api_keys_role_check;
ALTER TABLE api_keys ADD CONSTRAINT api_keys_role_check CHECK (role IN ('USER','OPERATOR','METRICS'));

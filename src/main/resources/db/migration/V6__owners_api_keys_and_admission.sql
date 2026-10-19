CREATE TABLE owners (
    id UUID PRIMARY KEY,
    name VARCHAR(80) NOT NULL CHECK (length(name) BETWEEN 1 AND 80),
    submission_limit_per_minute INTEGER NOT NULL DEFAULT 120 CHECK (submission_limit_per_minute BETWEEN 1 AND 10000),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
INSERT INTO owners(id,name) VALUES ('00000000-0000-0000-0000-000000000001','Legacy local jobs');

ALTER TABLE jobs ADD COLUMN owner_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES owners(id);
ALTER TABLE jobs DROP CONSTRAINT jobs_idempotency_key_key;
ALTER TABLE jobs ADD CONSTRAINT jobs_owner_idempotency_key UNIQUE(owner_id,idempotency_key);
ALTER TABLE jobs ADD CONSTRAINT jobs_id_owner_key UNIQUE(id,owner_id);
ALTER TABLE jobs ADD CONSTRAINT jobs_retry_owner_fk FOREIGN KEY(retry_of,owner_id) REFERENCES jobs(id,owner_id);
CREATE INDEX jobs_owner_created_idx ON jobs(owner_id,created_at DESC,id DESC);
CREATE INDEX jobs_owner_status_idx ON jobs(owner_id,status,created_at DESC,id DESC);

CREATE TABLE api_keys (
    id UUID PRIMARY KEY,
    owner_id UUID NOT NULL REFERENCES owners(id),
    name VARCHAR(80) NOT NULL CHECK (length(name) BETWEEN 1 AND 80),
    role VARCHAR(16) NOT NULL CHECK (role IN ('USER','OPERATOR')),
    token_hash VARCHAR(64) NOT NULL UNIQUE CHECK (token_hash ~ '^[a-f0-9]{64}$'),
    prefix VARCHAR(12) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    revoked_at TIMESTAMPTZ,
    created_by UUID REFERENCES api_keys(id)
);
CREATE INDEX api_keys_owner_idx ON api_keys(owner_id,created_at,id);

CREATE TABLE access_audit (
    id BIGSERIAL PRIMARY KEY,
    actor_key_id UUID REFERENCES api_keys(id),
    action VARCHAR(32) NOT NULL CHECK (action IN ('BOOTSTRAP_KEY_CREATED','OWNER_CREATED','KEY_CREATED','KEY_REVOKED','LIMIT_CHANGED')),
    owner_id UUID NOT NULL REFERENCES owners(id),
    key_id UUID REFERENCES api_keys(id),
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE submission_usage (
    owner_id UUID PRIMARY KEY REFERENCES owners(id),
    window_start TIMESTAMPTZ NOT NULL,
    used INTEGER NOT NULL CHECK (used > 0)
);
-- Preserve old resolution audit rows; new actions also record the authenticated operator key.
ALTER TABLE dead_letters ADD COLUMN resolved_by_key_id UUID REFERENCES api_keys(id);

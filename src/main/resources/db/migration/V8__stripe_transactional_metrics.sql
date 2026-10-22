-- Keep existing durable totals in stripe zero. All stripes are summed at scrape time.
-- Bounded stripes remove a global per-type write hotspot without relaxing atomicity.
ALTER TABLE job_metric_counts ADD COLUMN stripe SMALLINT NOT NULL DEFAULT 0 CHECK (stripe BETWEEN 0 AND 31);
ALTER TABLE job_metric_counts DROP CONSTRAINT job_metric_counts_pkey;
ALTER TABLE job_metric_counts ADD PRIMARY KEY(event,job_type,stripe);
ALTER TABLE job_metric_timings ADD COLUMN stripe SMALLINT NOT NULL DEFAULT 0 CHECK (stripe BETWEEN 0 AND 31);
ALTER TABLE job_metric_timings DROP CONSTRAINT job_metric_timings_pkey;
ALTER TABLE job_metric_timings ADD PRIMARY KEY(kind,job_type,outcome,stripe);

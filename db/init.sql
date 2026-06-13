-- ============================================================
-- Video Events Data Quality Pipeline — Schema
-- Idempotent: safe to run multiple times (IF NOT EXISTS / CREATE OR REPLACE)
-- ============================================================

-- Safe casts: return NULL instead of raising on unparseable input, so a
-- single bad value can be flagged as a DQ issue rather than aborting the
-- whole validation query/transaction.

CREATE OR REPLACE FUNCTION safe_to_timestamptz(val TEXT)
RETURNS TIMESTAMPTZ AS $$
BEGIN
    RETURN val::TIMESTAMPTZ;
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION safe_to_int(val TEXT)
RETURNS INTEGER AS $$
BEGIN
    RETURN val::INTEGER;
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- RAW_LOAD: scratch table shaped exactly like the source CSV.
-- COPY target for server-side bulk load; truncated at the start of
-- each run before loading the next file.
CREATE TABLE IF NOT EXISTS raw_load (
    event_id        TEXT,
    event_type      TEXT,
    session_id      TEXT,
    platform        TEXT,
    content_id      TEXT,
    timestamp       TEXT,
    duration_ms     TEXT,
    device_id       TEXT,
    firmware_version TEXT,
    error_code      TEXT
);

-- STAGING: transient, run-tagged working table. Within a pipeline run,
-- rows for that run_id are inserted, validated, routed, then deleted —
-- staging holds no data between runs.
CREATE TABLE IF NOT EXISTS staging (
    run_id          UUID NOT NULL,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    event_id        TEXT,
    event_type      TEXT,
    session_id      TEXT,
    platform        TEXT,
    content_id      TEXT,
    timestamp       TEXT,
    duration_ms     TEXT,
    device_id       TEXT,
    firmware_version TEXT,
    error_code      TEXT
);

-- LANDING: clean, typed, validated records only
CREATE TABLE IF NOT EXISTS landing (
    run_id          UUID NOT NULL,
    event_id        TEXT NOT NULL,
    event_type      TEXT NOT NULL CHECK (event_type IN ('playback_start','playback_end','buffer_start','buffer_end','error')),
    session_id      TEXT NOT NULL,
    platform        TEXT NOT NULL CHECK (platform IN ('roku','stva','specguide','odn','tve')),
    content_id      TEXT,
    timestamp       TIMESTAMPTZ NOT NULL,
    duration_ms     INTEGER CHECK (duration_ms >= 0),
    device_id       TEXT NOT NULL,
    firmware_version TEXT NOT NULL CHECK (firmware_version ~ '^\d+\.\d+\.\d+$'),
    error_code      TEXT CHECK (error_code IN ('e001','e002','e003','e004','e005','e006','e007','e008','e009','e010')),
    UNIQUE (event_id)
);

-- DQ_ISSUES: one row per violation (quarantine/audit)
CREATE TABLE IF NOT EXISTS dq_issues (
    id          SERIAL PRIMARY KEY,
    run_id      UUID NOT NULL,
    event_id    TEXT,
    issue_type  TEXT NOT NULL,
    detail      TEXT,
    raw_row     JSONB
);

CREATE INDEX IF NOT EXISTS idx_dq_issues_run_id ON dq_issues(run_id);
CREATE INDEX IF NOT EXISTS idx_dq_issues_issue_type ON dq_issues(issue_type);
CREATE INDEX IF NOT EXISTS idx_landing_run_id ON landing(run_id);
CREATE INDEX IF NOT EXISTS idx_staging_run_id ON staging(run_id);
"""
Video Events Data Quality Pipeline
-----------------------------------
Single atomic transaction per run:
  1. Client-side COPY of the CSV into a scratch `raw_load` table
     (Python streams the file to Postgres via COPY FROM STDIN).
  2. SQL generates a fresh run_id (gen_random_uuid()) and stamps it onto
     rows moved from raw_load into `staging`.
  3. Validation SQL routes clean rows -> landing, violations -> dq_issues.
  4. staging rows for this run_id are deleted (staging is transient —
     holds no data between runs).
  5. Commit. On any error, the whole transaction rolls back (raw_load
     truncated, nothing written to landing/dq_issues/staging).

Findings report JSON is written to ./output/ after commit.

Usage:
    python pipeline/pipeline.py --input data/video_events_sample.csv

Re-runnable: each invocation gets a fresh run_id and is append-only with
respect to landing/dq_issues.
"""

import argparse
import csv
import json
import os
from datetime import datetime, timezone

import psycopg


DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/video_dq"
)

_HERE = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", os.path.join(_HERE, "..", "output"))

STAGING_COLUMNS = [
    "event_id",
    "event_type",
    "session_id",
    "platform",
    "content_id",
    "timestamp",
    "duration_ms",
    "device_id",
    "firmware_version",
    "error_code",
]

VALID_EVENT_TYPES = (
    "playback_start",
    "playback_end",
    "buffer_start",
    "buffer_end",
    "error",
)

VALID_PLATFORMS = ("roku", "stva", "specguide", "odn", "tve")

VALID_ERROR_CODES = (
    "e001", "e002", "e003", "e004", "e005",
    "e006", "e007", "e008", "e009", "e010",
)


def get_connection():
    db_url = os.environ.get("DATABASE_URL", DATABASE_URL)
    return psycopg.connect(db_url)


def load_csv_to_staging(conn, input_path):
    """
    Client-side bulk load, within the caller's open transaction:

      0. Verify the CSV header matches STAGING_COLUMNS exactly (order
         and names) — fail fast rather than let COPY silently load
         columns into the wrong positions.
      1. TRUNCATE raw_load (scratch table from any prior failed run)
      2. Stream the CSV from Python into Postgres via COPY FROM STDIN
         (copy_expert) — no server-side file access required.
      3. Generate a fresh run_id via gen_random_uuid()
      4. INSERT INTO staging SELECT run_id, raw_load.* (single
         set-based statement)
      5. TRUNCATE raw_load

    Returns (run_id, n_rows_loaded). Does not commit — caller controls
    the transaction boundary.
    """
    # ------------------------------------------------------------
    # Header check. COPY's HEADER option only discards the first line;
    # it does NOT validate column names/order, so a CSV with columns
    # in the wrong order would silently load into the wrong raw_load
    # columns.
    # ------------------------------------------------------------
    with open(input_path, newline="") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            raise ValueError(f"{input_path} is empty (no header row)")

    if header != STAGING_COLUMNS:
        raise ValueError(
            f"CSV header mismatch in {input_path}.\n"
            f"  expected: {STAGING_COLUMNS}\n"
            f"  found:    {header}"
        )

    with conn.cursor() as cur:
        cur.execute("TRUNCATE raw_load")

        with open(input_path, "r") as f:
            with cur.copy(
                f"COPY raw_load ({', '.join(STAGING_COLUMNS)}) FROM STDIN"
                " WITH (FORMAT csv, HEADER true, NULL '')"
            ) as copy:
                copy.write(f.read())

        # gen_random_uuid() is built into Postgres 13+ (no extension needed)
        cur.execute("SELECT gen_random_uuid()")
        run_id = cur.fetchone()[0]

        cur.execute(
            f"""
            INSERT INTO staging (run_id, {", ".join(STAGING_COLUMNS)})
            SELECT %(run_id)s, {", ".join(STAGING_COLUMNS)}
            FROM raw_load
            """,
            {"run_id": run_id},
        )
        n_rows = cur.rowcount

        cur.execute("TRUNCATE raw_load")

    return run_id, n_rows


def run_validation(conn, run_id):
    """
    Evaluate validation rules for this run's staging rows, insert clean
    rows into landing, and insert one row per violation into dq_issues.
    Returns a summary dict of counts per issue_type.
    """
    with conn.cursor() as cur:
        # ------------------------------------------------------------
        # Build a "validated" view as a CTE-backed temp table for this run
        # ------------------------------------------------------------
        cur.execute(
            """
            CREATE TEMP TABLE validated AS
            SELECT
                s.*,

                -- normalized (lowercased/trimmed) enum-like columns, used
                -- for all comparisons below and written to landing, so
                -- casing differences (e.g. "Roku" vs "roku") don't cause
                -- false INVALID_* flags
                LOWER(TRIM(s.event_type)) AS norm_event_type,
                LOWER(TRIM(s.platform)) AS norm_platform,
                LOWER(TRIM(s.error_code)) AS norm_error_code,

                -- duplicate event_id: within this run OR already in landing from a prior run
                (
                    COUNT(*) OVER (PARTITION BY s.run_id, s.event_id) > 1
                    OR EXISTS (SELECT 1 FROM landing l WHERE l.event_id = s.event_id)
                ) AS is_duplicate,

                (s.event_type IS NULL
                    OR LOWER(TRIM(s.event_type)) <> ALL(%(event_types)s)) AS bad_event_type,

                (s.platform IS NULL
                    OR LOWER(TRIM(s.platform)) <> ALL(%(platforms)s)) AS bad_platform,

                -- content_id required only for playback events per data
                -- dictionary ("Should not be null for playback events")
                ((s.content_id IS NULL OR s.content_id = '')
                    AND LOWER(TRIM(s.event_type)) IN ('playback_start', 'playback_end')
                ) AS null_content_id,

                (s.device_id IS NULL OR s.device_id = '') AS null_device_id,

                (s.firmware_version IS NULL
                    OR s.firmware_version !~ '^\\d+\\.\\d+\\.\\d+$') AS bad_firmware,

                -- timestamp castability + staleness (safe_to_timestamptz
                -- returns NULL for null input or unparseable text, instead
                -- of raising and aborting the query)
                (safe_to_timestamptz(s.timestamp) IS NULL) AS invalid_timestamp_format,

                -- NOTE: the data dictionary uses "should" (not "must") for the 30-day
                -- window, but we treat it as a hard exclusion here. In practice this
                -- rejects a large portion of records — consider downgrading to a
                -- warning (flag in dq_issues but still route to landing) if legitimate
                -- historical or delayed-delivery events are being lost.
                CASE
                    WHEN safe_to_timestamptz(s.timestamp) IS NOT NULL
                    THEN (safe_to_timestamptz(s.timestamp) < now() - INTERVAL '30 days')
                    ELSE FALSE
                END AS stale_timestamp,

                CASE
                    WHEN safe_to_timestamptz(s.timestamp) IS NOT NULL
                    THEN (safe_to_timestamptz(s.timestamp) > now())
                    ELSE FALSE
                END AS future_timestamp,

                -- duration_ms castability + value checks
                (s.duration_ms IS NOT NULL
                    AND s.duration_ms != ''
                    AND safe_to_int(s.duration_ms) IS NULL) AS invalid_duration_format,

                CASE
                    WHEN safe_to_int(s.duration_ms) IS NOT NULL
                    THEN (safe_to_int(s.duration_ms) < 0)
                    ELSE FALSE
                END AS negative_duration,

                (s.duration_ms IS NULL
                    AND LOWER(TRIM(s.event_type)) NOT IN ('playback_start', 'buffer_start')
                ) AS missing_duration,

                (s.duration_ms IS NOT NULL
                    AND s.duration_ms != ''
                    AND LOWER(TRIM(s.event_type)) IN ('playback_start', 'buffer_start')
                ) AS unexpected_duration,

                -- error_code conditional logic
                (LOWER(TRIM(s.event_type)) = 'error' AND
                    (s.error_code IS NULL OR s.error_code = '')) AS missing_error_code,

                (LOWER(TRIM(s.event_type)) IS DISTINCT FROM 'error'
                    AND s.error_code IS NOT NULL
                    AND s.error_code != '') AS unexpected_error_code,

                (s.error_code IS NOT NULL
                    AND s.error_code != ''
                    AND LOWER(TRIM(s.error_code)) <> ALL(%(error_codes)s)) AS invalid_error_code

            FROM staging s
            WHERE s.run_id = %(run_id)s
            """,
            {
                "run_id": run_id,
                "event_types": list(VALID_EVENT_TYPES),
                "platforms": list(VALID_PLATFORMS),
                "error_codes": list(VALID_ERROR_CODES),
            },
        )

        # Partial indexes on each flag column so the 16 dq_issues INSERTs
        # can use index scans instead of full sequential scans of validated.
        # Each index covers only the TRUE rows (the minority at healthy
        # violation rates), keeping index builds fast.
        for flag_col in [
            "is_duplicate", "bad_event_type", "bad_platform", "null_content_id",
            "null_device_id", "bad_firmware", "invalid_timestamp_format",
            "stale_timestamp", "future_timestamp", "invalid_duration_format",
            "negative_duration", "missing_duration", "unexpected_duration",
            "missing_error_code", "unexpected_error_code", "invalid_error_code",
        ]:
            cur.execute(f"CREATE INDEX ON validated ({flag_col}) WHERE {flag_col}")

        # ------------------------------------------------------------
        # Insert clean rows into landing
        # A row is "clean" only if NONE of the issue flags are true,
        # AND timestamp/duration are castable (required for typed landing).
        # ------------------------------------------------------------
        cur.execute(
            """
            INSERT INTO landing (
                run_id, event_id, event_type, session_id, platform,
                content_id, timestamp, duration_ms, device_id,
                firmware_version, error_code
            )
            SELECT
                run_id, event_id, norm_event_type, session_id, norm_platform,
                content_id, safe_to_timestamptz(timestamp),
                safe_to_int(duration_ms),
                device_id, firmware_version,
                CASE WHEN error_code IS NOT NULL AND error_code != ''
                     THEN norm_error_code ELSE error_code END
            FROM validated
            WHERE NOT (
                is_duplicate
                OR bad_event_type
                OR bad_platform
                OR null_content_id
                OR null_device_id
                OR bad_firmware
                OR invalid_timestamp_format
                OR stale_timestamp
                OR future_timestamp
                OR invalid_duration_format
                OR negative_duration
                OR missing_duration
                OR unexpected_duration
                OR missing_error_code
                OR unexpected_error_code
                OR invalid_error_code
            )
            ON CONFLICT (event_id) DO NOTHING
            """
        )

        # ------------------------------------------------------------
        # Insert one dq_issues row per (row, violated rule)
        # ------------------------------------------------------------
        issue_branches = [
            ("DUPLICATE_EVENT_ID", "is_duplicate", "Duplicate event_id within this run or already exists in landing"),
            ("INVALID_EVENT_TYPE", "bad_event_type", "event_type not in allowed set"),
            ("INVALID_PLATFORM", "bad_platform", "platform not in allowed set"),
            ("NULL_CONTENT_ID", "null_content_id", "content_id is null or empty for a playback event"),
            ("NULL_DEVICE_ID", "null_device_id", "device_id is null or empty"),
            ("INVALID_FIRMWARE_FORMAT", "bad_firmware", "firmware_version does not match X.Y.Z"),
            ("INVALID_TIMESTAMP_FORMAT", "invalid_timestamp_format", "timestamp not parseable"),
            ("STALE_TIMESTAMP", "stale_timestamp", "timestamp older than 30 days"),
            ("FUTURE_TIMESTAMP", "future_timestamp", "timestamp is in the future"),
            ("INVALID_DURATION_FORMAT", "invalid_duration_format", "duration_ms not an integer"),
            ("NEGATIVE_DURATION", "negative_duration", "duration_ms is negative"),
            ("MISSING_DURATION", "missing_duration", "duration_ms missing for non-start event"),
            ("UNEXPECTED_DURATION", "unexpected_duration", "duration_ms must be null for playback_start and buffer_start events"),
            ("MISSING_ERROR_CODE", "missing_error_code", "error event missing error_code"),
            ("UNEXPECTED_ERROR_CODE", "unexpected_error_code", "non-error event has an error_code"),
            ("INVALID_ERROR_CODE", "invalid_error_code", "error_code not in allowed set"),
        ]

        for issue_type, flag_col, detail in issue_branches:
            cur.execute(
                f"""
                INSERT INTO dq_issues (run_id, event_id, issue_type, detail, raw_row)
                SELECT
                    run_id,
                    event_id,
                    %(issue_type)s,
                    %(detail)s,
                    to_jsonb(v) - 'norm_event_type' - 'norm_platform' - 'norm_error_code'
                        - 'is_duplicate' - 'bad_event_type' - 'bad_platform'
                        - 'null_content_id' - 'null_device_id' - 'bad_firmware'
                        - 'invalid_timestamp_format' - 'stale_timestamp' - 'future_timestamp'
                        - 'invalid_duration_format' - 'negative_duration'
                        - 'missing_duration' - 'unexpected_duration' - 'missing_error_code'
                        - 'unexpected_error_code' - 'invalid_error_code'
                FROM validated v
                WHERE {flag_col}
                """,
                {"issue_type": issue_type, "detail": detail},
            )

        cur.execute("DROP TABLE validated")

    # ------------------------------------------------------------
    # staging is transient: remove this run's rows now that landing/
    # dq_issues have been populated. Still inside the open transaction —
    # if anything above failed, none of this commits.
    # ------------------------------------------------------------
    with conn.cursor() as cur:
        cur.execute("DELETE FROM staging WHERE run_id = %s", (run_id,))

    # ------------------------------------------------------------
    # Summary counts for the findings report (computed from landing/
    # dq_issues, since staging has just been cleared)
    # ------------------------------------------------------------
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM landing WHERE run_id = %s", (run_id,)
        )
        total_clean = cur.fetchone()[0]

        cur.execute(
            """
            SELECT issue_type, COUNT(*)
            FROM dq_issues
            WHERE run_id = %s
            GROUP BY issue_type
            ORDER BY issue_type
            """,
            (run_id,),
        )
        issue_counts = {row[0]: row[1] for row in cur.fetchall()}

        cur.execute(
            "SELECT COUNT(DISTINCT event_id) FROM dq_issues WHERE run_id = %s",
            (run_id,),
        )
        total_flagged_rows = cur.fetchone()[0]

    return {
        "total_clean": total_clean,
        "total_flagged_rows": total_flagged_rows,
        "issue_counts": issue_counts,
    }


def write_findings_report(run_id, input_filename, total_staged, summary):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    report = {
        "run_id": str(run_id),
        "input_file": input_filename,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_staged": total_staged,
        **summary,
    }
    out_path = os.path.join(OUTPUT_DIR, f"findings_{run_id}.json")
    with open(out_path, "w") as f:
        json.dump(report, f, indent=2)
    return out_path


def main():
    parser = argparse.ArgumentParser(description="Video Events DQ Pipeline")
    parser.add_argument(
        "--input",
        required=True,
        help="Path to input CSV file (absolute or relative to cwd)",
    )
    args = parser.parse_args()

    input_path = os.path.abspath(args.input)

    conn = get_connection()
    try:
        try:
            run_id, n_loaded = load_csv_to_staging(conn, input_path)
            print(f"[pipeline] run_id={run_id}")
            print(f"[pipeline] loaded {n_loaded} rows into staging")

            summary = run_validation(conn, run_id)
            print(f"[pipeline] validation complete: {summary}")

            conn.commit()
            print("[pipeline] transaction committed")
        except Exception:
            conn.rollback()
            print("[pipeline] transaction rolled back due to error")
            raise

        report_path = write_findings_report(
            run_id, os.path.basename(input_path), n_loaded, summary
        )
        print(f"[pipeline] findings report written to {report_path}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
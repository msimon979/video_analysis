import csv
import os
import psycopg
import pytest
from unittest.mock import patch

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pipeline.pipeline import main, DATABASE_URL, STAGING_COLUMNS

TEST_DB_NAME = "test_db"
_PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")

# Use localhost for local development; override via env var for CI/Docker
_PG_HOST = os.environ.get("PG_HOST", "localhost")
ADMIN_DB_URL = f"postgresql://postgres:postgres@{_PG_HOST}:5432/postgres"
TEST_DB_URL = f"postgresql://postgres:postgres@{_PG_HOST}:5432/{TEST_DB_NAME}"

TEST_DATA_DIR = os.path.abspath(os.path.join(_PROJECT_ROOT, "data"))
TEST_CSV_NAME = "video_events_test.csv"
TEST_CSV_PATH = os.path.join(TEST_DATA_DIR, TEST_CSV_NAME)


@pytest.fixture(scope="session")
def setup_test_database():
    """
    Creates a dedicated test database and initializes the schema using init.sql
    """
    with psycopg.connect(ADMIN_DB_URL, autocommit=True) as admin_conn:
        with admin_conn.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {TEST_DB_NAME}")
            cur.execute(f"CREATE DATABASE {TEST_DB_NAME}")

    schema_path = os.path.join(_PROJECT_ROOT, "db", "init.sql")
    with open(schema_path, "r") as f:
        schema_sql = f.read()

    with psycopg.connect(TEST_DB_URL) as test_conn:
        with test_conn.cursor() as cur:
            cur.execute(schema_sql)
        test_conn.commit()

    yield

    with psycopg.connect(ADMIN_DB_URL, autocommit=True) as admin_conn:
        with admin_conn.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {TEST_DB_NAME}")


@pytest.fixture
def generate_sample_csv(setup_test_database):
    """
    Generates a sample CSV incorporating 2 pass cases and all 14 error paths.
    """
    os.makedirs(TEST_DATA_DIR, exist_ok=True)

    rows = [
        # 1. PASS: Perfectly clean playback event (duration_ms null — required for start events)
        ["clean_1", "playback_start", "sess_1", "roku", "cont_100", "2026-06-01T12:00:00Z", "", "dev_1", "1.0.0", ""],
        # 2. PASS: Perfectly clean non-playback event (no content_id or duration required)
        ["clean_2", "buffer_start", "sess_2", "stva", "", "2026-06-01T12:01:00Z", "", "dev_2", "2.4.12", ""],
        # 3. PASS: Valid error event with a recognised error code (E001–E010 per data dictionary)
        ["clean_3", "error", "sess_3", "specguide", "", "2026-06-01T12:01:30Z", "250", "dev_3", "3.1.0", "e007"],

        # 4. FAIL: DUPLICATE_EVENT_ID (Reuses 'clean_1' event_id)
        ["clean_1", "playback_end", "sess_4", "odn", "cont_100", "2026-06-01T12:02:00Z", "5000", "dev_1", "1.0.0", ""],
        # 4. FAIL: INVALID_EVENT_TYPE
        ["err_type", "clicked_button", "sess_4", "tve", "", "2026-06-01T12:03:00Z", "", "dev_3", "1.0.0", ""],
        # 5. FAIL: INVALID_PLATFORM
        ["err_plat", "playback_start", "sess_5", "apple_tv", "cont_101", "2026-06-01T12:04:00Z", "0", "dev_4", "1.0.0", ""],
        # 6. FAIL: NULL_CONTENT_ID (playback event lacks content id)
        ["err_cont", "playback_start", "sess_6", "roku", "", "2026-06-01T12:05:00Z", "0", "dev_5", "1.0.0", ""],
        # 7. FAIL: NULL_DEVICE_ID
        ["err_dev", "playback_end", "sess_7", "stva", "cont_102", "2026-06-01T12:06:00Z", "1200", "", "1.0.0", ""],
        # 8. FAIL: INVALID_FIRMWARE_FORMAT
        ["err_firm", "playback_end", "sess_8", "specguide", "cont_103", "2026-06-01T12:07:00Z", "4500", "dev_6", "v1.0", ""],
        # 9. FAIL: INVALID_TIMESTAMP_FORMAT
        ["err_time", "playback_end", "sess_9", "odn", "cont_104", "not-a-date", "3000", "dev_7", "1.0.0", ""],
        # 10. FAIL: STALE_TIMESTAMP (Older than 30 days)
        ["err_stale", "playback_end", "sess_10", "tve", "cont_105", "2020-01-01T00:00:00Z", "3000", "dev_8", "1.0.0", ""],
        # 11. FAIL: INVALID_DURATION_FORMAT
        ["err_dur_fmt", "playback_end", "sess_11", "roku", "cont_106", "2026-06-01T12:08:00Z", "one_thousand", "dev_9", "1.0.0", ""],
        # 12. FAIL: NEGATIVE_DURATION
        ["err_dur_neg", "playback_end", "sess_12", "stva", "cont_107", "2026-06-01T12:09:00Z", "-500", "dev_10", "1.0.0", ""],
        # 13. FAIL: MISSING_DURATION (Required for non-start playback events like playback_end)
        ["err_dur_miss", "playback_end", "sess_13", "specguide", "cont_108", "2026-06-01T12:10:00Z", "", "dev_11", "1.0.0", ""],
        # 14. FAIL: MISSING_ERROR_CODE (Event type is 'error', but no error code supplied)
        ["err_code_miss", "error", "sess_14", "odn", "", "2026-06-01T12:11:00Z", "0", "dev_12", "1.0.0", ""],
        # 15. FAIL: UNEXPECTED_ERROR_CODE (Event type is playback, but carries an error code)
        ["err_code_unex", "playback_start", "sess_15", "tve", "cont_109", "2026-06-01T12:12:00Z", "0", "dev_13", "1.0.0", "e001"],
        # 16. FAIL: INVALID_ERROR_CODE (Event type is error, but code is not in allowed set)
        ["err_code_inv", "error", "sess_16", "roku", "", "2026-06-01T12:13:00Z", "0", "dev_14", "1.0.0", "e999"],
        # 17. FAIL: FUTURE_TIMESTAMP
        ["err_future", "buffer_end", "sess_17", "odn", "", "2035-01-01T00:00:00Z", "500", "dev_15", "1.0.0", ""],
        # 18. FAIL: UNEXPECTED_DURATION (start event must have null duration_ms per data dictionary)
        ["err_unex_dur", "buffer_start", "sess_18", "tve", "", "2026-06-01T12:14:00Z", "100", "dev_16", "1.0.0", ""],
    ]

    with open(TEST_CSV_PATH, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(STAGING_COLUMNS)
        writer.writerows(rows)

    yield TEST_CSV_PATH

    if os.path.exists(TEST_CSV_PATH):
        os.remove(TEST_CSV_PATH)


def test_pipeline_execution(generate_sample_csv):
    """
    Executes the main entry point of the pipeline pointing to our test database,
    and runs assertions on the data routing quality metrics.
    """
    input_path = generate_sample_csv

    with patch.dict(os.environ, {"DATABASE_URL": TEST_DB_URL}), \
         patch("sys.argv", ["pipeline.py", "--input", input_path]), \
         patch("pipeline.pipeline.write_findings_report"):

        main()

    with psycopg.connect(TEST_DB_URL) as conn, conn.cursor() as cur:

        # 1. Assert exactly 2 rows passed all validations: clean_2 (buffer_start) and
        #    clean_3 (error with valid e007 code). clean_1 appears twice in the file so
        #    both instances are flagged DUPLICATE_EVENT_ID and neither lands.
        cur.execute("SELECT COUNT(*) FROM landing;")
        landing_count = cur.fetchone()[0]
        assert landing_count == 2, f"Expected 2 clean records in landing, found {landing_count}"

        # Verify normalized values (LOWER) were committed to landing
        cur.execute("SELECT event_type, platform FROM landing WHERE event_id = 'clean_2';")
        clean_row = cur.fetchone()
        assert clean_row[0] == "buffer_start"
        assert clean_row[1] == "stva"

        # 2. Assert that our error rows were cleanly parsed into the data quality quarantine log
        cur.execute("SELECT issue_type, COUNT(*) FROM dq_issues GROUP BY issue_type;")
        issues_logged = dict(cur.fetchall())

        expected_issues = [
            "DUPLICATE_EVENT_ID", "INVALID_EVENT_TYPE", "INVALID_PLATFORM",
            "NULL_CONTENT_ID", "NULL_DEVICE_ID", "INVALID_FIRMWARE_FORMAT",
            "INVALID_TIMESTAMP_FORMAT", "STALE_TIMESTAMP", "FUTURE_TIMESTAMP",
            "INVALID_DURATION_FORMAT", "NEGATIVE_DURATION", "MISSING_DURATION",
            "UNEXPECTED_DURATION", "MISSING_ERROR_CODE", "UNEXPECTED_ERROR_CODE",
            "INVALID_ERROR_CODE"
        ]

        for issue in expected_issues:
            assert issue in issues_logged, f"Data Quality rule failed to catch or log: {issue}"
            assert issues_logged[issue] >= 1

        # 3. Confirm transient staging tables are entirely cleared after atomic runtime commit
        cur.execute("SELECT COUNT(*) FROM staging;")
        staging_count = cur.fetchone()[0]
        assert staging_count == 0, "Staging table was not purged post-run!"

        cur.execute("SELECT COUNT(*) FROM raw_load;")
        raw_load_count = cur.fetchone()[0]
        assert raw_load_count == 0, "Raw scratch table was not truncated post-run!"

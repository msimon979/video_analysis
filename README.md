# Video Events Data Quality Pipeline

## Architecture

1. **Postgres** — `raw_load` (COPY scratch table), `staging` (transient, run-tagged), `landing`, `dq_issues` tables (`db/init.sql`, idempotent).
2. **Pipeline** (`pipeline/pipeline.py`) — single atomic transaction: client-side `COPY` of the input CSV into `raw_load`, SQL generates a `run_id` (`gen_random_uuid()`) and moves rows into `staging`, validation routes clean rows → `landing`, violations → `dq_issues`, then `staging` rows for that run are deleted. Commits on success, rolls back on any error. Writes a findings report JSON to `output/` after commit. **Idempotent** — re-running with the same file detects already-loaded `event_id`s as cross-run duplicates and skips them in `landing`.
3. **MCP server** (`mcp_server/mcp_server.py`) — exposes `ask_about_errors(query, run_id)`, grounds an LLM call in `dq_issues`/findings data for natural-language Q&A.
4. **Flask API** (`api/app.py`) — `/ask` POST endpoint, MCP client, relays natural-language questions to the MCP server.

## Prerequisites

- Python 3.11+
- PostgreSQL 16 (`brew install postgresql@16 && brew services start postgresql@16`)

## Setup

```bash
# 1. Create and activate a virtual environment
python3 -m venv venv
source venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Copy and fill in environment variables
cp .env .env.local  # set ANTHROPIC_API_KEY

# 4. Create the database and apply schema (one-time)
make setup
```

## Commands

```bash
make run       # Run the pipeline against data/video_events_sample.csv
make test      # Run the test suite (uses an isolated test_db, auto-created and dropped)
make truncate  # Wipe all data from landing, dq_issues, staging, raw_load
make setup     # Create database and apply schema
make clean     # Remove output files and Python caches
make up        # Start MCP server (:8000) and Flask API (:5000)
```

## Run with a new file

```bash
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/video_dq \
  venv/bin/python3 pipeline/pipeline.py --input path/to/your_file.csv
```

## Query findings via the API

Start the services first (`make up`), then:

```bash
curl -X POST http://localhost:5000/ask \
  -H "Content-Type: application/json" \
  -d '{"query": "What are the most common data quality issues?"}'
```

## Fresh start

```bash
make truncate   # wipe data only
# or to fully reset the schema:
psql -U postgres -d video_dq -c "DROP TABLE IF EXISTS landing, dq_issues, staging, raw_load CASCADE;"
psql -U postgres -d video_dq -c "DROP FUNCTION IF EXISTS safe_to_timestamptz(text), safe_to_int(text) CASCADE;"
make setup
```

## Tradeoffs

- **No Docker**: the solution runs directly on the local machine rather than in containers. Docker was the original target environment but was not viable due to local machine constraints. PostgreSQL is installed via Homebrew and Python dependencies via a virtualenv — equivalent isolation for a demo context. Production would containerize all services.
- **Migrations**: `init.sql` uses idempotent `CREATE TABLE IF NOT EXISTS` — appropriate for a demo. Production would use versioned migrations (Alembic/Flyway).
- **Validation**: deterministic SQL-based rule checks keep the pipeline auditable and testable. The LLM layer (MCP server) is grounded in this structured output rather than reasoning freely over raw data.
- **Idempotency**: `event_id` has a `UNIQUE` constraint in `landing`. Re-running with the same file flags already-seen event_ids as `DUPLICATE_EVENT_ID` in `dq_issues` and skips them in `landing`.

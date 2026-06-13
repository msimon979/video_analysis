# ==============================================================================
# Local Makefile for Video Events DQ Pipeline (no Docker required)
# Prerequisites: PostgreSQL 16, Python 3.11+
#   brew install postgresql@16 && brew services start postgresql@16
#   pip install -r requirements.txt
# ==============================================================================

.PHONY: help setup db-create db-init run up down status test clean truncate

DB_NAME   := video_dq
DB_URL    := postgresql://postgres:postgres@localhost:5432/$(DB_NAME)
PSQL      := psql -U postgres
PYTHON    := venv/bin/python3

help:
	@echo "======================================================================"
	@echo " Video Events DQ Pipeline - Local Command Matrix"
	@echo "======================================================================"
	@echo "Prerequisites: PostgreSQL running locally, pip packages installed"
	@echo "  brew install postgresql@16 && brew services start postgresql@16"
	@echo "  pip install -r requirements.txt"
	@echo ""
	@echo "Available commands:"
	@echo "  make setup    - Create database and apply schema (run once)"
	@echo "  make run      - Execute the pipeline with the default sample data"
	@echo "  make up       - Start MCP server + Flask API (foreground)"
	@echo "  make test     - Run the pytest validation suite"
	@echo "  make clean    - Remove local runtime caches and test outputs"
	@echo "======================================================================"

setup: db-superuser db-create db-init

db-superuser:
	@echo "[setup] Ensuring postgres superuser role exists..."
	createuser -s postgres 2>/dev/null || echo "[setup] Role 'postgres' already exists, skipping."

db-create:
	@echo "[setup] Creating database '$(DB_NAME)'..."
	$(PSQL) -c "CREATE DATABASE $(DB_NAME);" 2>/dev/null || echo "[setup] Database already exists, skipping."

db-init:
	@echo "[setup] Applying schema..."
	$(PSQL) -d $(DB_NAME) -f db/init.sql

run:
	DATABASE_URL=$(DB_URL) $(PYTHON) pipeline/pipeline.py --input data/video_events_sample.csv

up:
	@echo "[up] Starting MCP server on :8000 and Flask API on :5000"
	DATABASE_URL=$(DB_URL) GROQ_API_KEY=$(GROQ_API_KEY) $(PYTHON) mcp_server/mcp_server.py &
	sleep 2
	MCP_SERVER_URL=http://localhost:8000 MCP_AUTH_TOKEN=$(MCP_AUTH_TOKEN) $(PYTHON) api/app.py

down:
	@pkill -f "mcp_server.py" && echo "[down] MCP server stopped" || echo "[down] MCP server was not running"
	@pkill -f "api/app.py" && echo "[down] Flask API stopped" || echo "[down] Flask API was not running"

status:
	@lsof -i :8000 -sTCP:LISTEN -t >/dev/null 2>&1 && echo "[status] MCP server: running on :8000" || echo "[status] MCP server: stopped"
	@lsof -i :5001 -sTCP:LISTEN -t >/dev/null 2>&1 && echo "[status] Flask API:  running on :5001" || echo "[status] Flask API:  stopped"

test:
	@echo "[test] Running pytest..."
	DATABASE_URL=$(DB_URL) venv/bin/pytest tests/test_pipeline.py -v

truncate:
	$(PSQL) -d $(DB_NAME) -c "TRUNCATE landing, dq_issues, staging, raw_load;"

clean:
	@echo "[clean] Stripping cache artifacts..."
	rm -f output/findings_*.json
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +

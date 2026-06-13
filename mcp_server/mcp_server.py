"""
MCP server — Video Events DQ.

Tools:
  list_recent_runs(limit)                      — recent runs with clean/flagged counts
  get_run_summary(run_id)                      — structured breakdown for a run (no LLM)
  get_issues_detail(issue_type, run_id, limit) — sample raw rows for an issue type
  ask_about_errors(query, run_id)              — natural-language Q&A grounded in DQ data
"""

import json
import os

import psycopg
import uvicorn
from groq import Groq
from mcp.server.fastmcp import FastMCP
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse


DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/video_dq"
)
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
MCP_AUTH_TOKEN = os.environ.get("MCP_AUTH_TOKEN", "dummy-demo-token")

mcp = FastMCP("video-dq-server")
groq_client = Groq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None


class BearerAuthMiddleware(BaseHTTPMiddleware):
    """Reject requests that don't present the expected bearer token."""

    async def dispatch(self, request, call_next):
        auth_header = request.headers.get("authorization", "")
        if auth_header != f"Bearer {MCP_AUTH_TOKEN}":
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        return await call_next(request)


def get_connection():
    return psycopg.connect(DATABASE_URL)


def resolve_run_id(conn, run_id: str):
    """Return run_id if provided, else the most recent run across landing/dq_issues."""
    if run_id:
        return run_id
    with conn.cursor() as cur:
        # dq_issues.id is a serial — highest id = most recent run
        cur.execute("SELECT run_id FROM dq_issues ORDER BY id DESC LIMIT 1")
        row = cur.fetchone()
        if row:
            return row[0]
        cur.execute("SELECT run_id FROM landing LIMIT 1")
        row = cur.fetchone()
        return row[0] if row else None


@mcp.tool()
def list_recent_runs(limit: int = 10) -> str:
    """
    Return recent pipeline runs with clean and flagged row counts, most recent first.

    Args:
        limit: Number of runs to return (default 10).
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH landing_runs AS (
                    SELECT run_id, COUNT(*) AS clean_rows
                    FROM landing
                    GROUP BY run_id
                ),
                issue_runs AS (
                    SELECT run_id, MAX(id) AS last_id, COUNT(DISTINCT event_id) AS flagged_rows
                    FROM dq_issues
                    GROUP BY run_id
                )
                SELECT
                    COALESCE(l.run_id::text, i.run_id::text) AS run_id,
                    COALESCE(l.clean_rows, 0)   AS clean_rows,
                    COALESCE(i.flagged_rows, 0) AS flagged_rows
                FROM landing_runs l
                FULL OUTER JOIN issue_runs i ON i.run_id = l.run_id
                ORDER BY i.last_id DESC NULLS LAST
                LIMIT %s
                """,
                (limit,),
            )
            cols = ["run_id", "clean_rows", "flagged_rows"]
            runs = [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()

    return json.dumps(runs, indent=2, default=str)


@mcp.tool()
def get_run_summary(run_id: str = "") -> str:
    """
    Return a structured summary for a pipeline run: clean/flagged counts,
    issue breakdown by type, platform, and event type. No LLM involved.

    Args:
        run_id: UUID of the run. Defaults to the most recent run.
    """
    conn = get_connection()
    try:
        target = resolve_run_id(conn, run_id)
        if not target:
            return json.dumps({"error": "No pipeline runs found."})

        summary = {"run_id": str(target)}

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM landing WHERE run_id = %s", (target,))
            summary["clean_rows"] = cur.fetchone()[0]

            cur.execute(
                "SELECT COUNT(DISTINCT event_id) FROM dq_issues WHERE run_id = %s",
                (target,),
            )
            summary["flagged_rows"] = cur.fetchone()[0]

            cur.execute(
                """
                SELECT issue_type, COUNT(*) AS cnt
                FROM dq_issues WHERE run_id = %s
                GROUP BY issue_type ORDER BY cnt DESC
                """,
                (target,),
            )
            summary["issues_by_type"] = {r[0]: r[1] for r in cur.fetchall()}

            cur.execute(
                """
                SELECT raw_row->>'platform' AS platform, COUNT(*) AS cnt
                FROM dq_issues
                WHERE run_id = %s AND raw_row->>'platform' IS NOT NULL
                GROUP BY platform ORDER BY cnt DESC
                """,
                (target,),
            )
            summary["issues_by_platform"] = {r[0]: r[1] for r in cur.fetchall()}

            cur.execute(
                """
                SELECT raw_row->>'event_type' AS event_type, COUNT(*) AS cnt
                FROM dq_issues
                WHERE run_id = %s AND raw_row->>'event_type' IS NOT NULL
                GROUP BY event_type ORDER BY cnt DESC
                """,
                (target,),
            )
            summary["issues_by_event_type"] = {r[0]: r[1] for r in cur.fetchall()}

    finally:
        conn.close()

    return json.dumps(summary, indent=2, default=str)


@mcp.tool()
def get_issues_detail(issue_type: str, run_id: str = "", limit: int = 10) -> str:
    """
    Return sample rows for a specific issue type, including the raw field
    values that caused the violation.

    Args:
        issue_type: E.g. "STALE_TIMESTAMP", "INVALID_FIRMWARE_FORMAT".
        run_id: UUID of the run. Defaults to the most recent run.
        limit: Max rows to return (default 10).
    """
    conn = get_connection()
    try:
        target = resolve_run_id(conn, run_id)
        if not target:
            return json.dumps({"error": "No pipeline runs found."})

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT event_id, detail, raw_row
                FROM dq_issues
                WHERE run_id = %s AND issue_type = %s
                ORDER BY id
                LIMIT %s
                """,
                (target, issue_type, limit),
            )
            cols = ["event_id", "detail", "raw_row"]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()

    return json.dumps(
        {"run_id": str(target), "issue_type": issue_type, "samples": rows},
        indent=2,
        default=str,
    )


def gather_context(conn, run_id):
    """Pull enriched context for the LLM: counts, breakdowns, and sample bad rows."""
    ctx = {"run_id": str(run_id)}

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM landing WHERE run_id = %s", (run_id,))
        ctx["clean_rows"] = cur.fetchone()[0]

        cur.execute(
            "SELECT COUNT(DISTINCT event_id) FROM dq_issues WHERE run_id = %s",
            (run_id,),
        )
        ctx["flagged_rows"] = cur.fetchone()[0]

        cur.execute(
            """
            SELECT issue_type, COUNT(*) AS cnt
            FROM dq_issues WHERE run_id = %s
            GROUP BY issue_type ORDER BY cnt DESC
            """,
            (run_id,),
        )
        ctx["issues_by_type"] = {r[0]: r[1] for r in cur.fetchall()}

        cur.execute(
            """
            SELECT raw_row->>'platform' AS platform, COUNT(*) AS cnt
            FROM dq_issues
            WHERE run_id = %s AND raw_row->>'platform' IS NOT NULL
            GROUP BY platform ORDER BY cnt DESC
            """,
            (run_id,),
        )
        ctx["issues_by_platform"] = {r[0]: r[1] for r in cur.fetchall()}

        cur.execute(
            """
            SELECT raw_row->>'event_type' AS event_type, COUNT(*) AS cnt
            FROM dq_issues
            WHERE run_id = %s AND raw_row->>'event_type' IS NOT NULL
            GROUP BY event_type ORDER BY cnt DESC
            """,
            (run_id,),
        )
        ctx["issues_by_event_type"] = {r[0]: r[1] for r in cur.fetchall()}

        cur.execute(
            """
            SELECT issue_type, event_id, detail, raw_row
            FROM dq_issues WHERE run_id = %s
            ORDER BY id LIMIT 10
            """,
            (run_id,),
        )
        cols = ["issue_type", "event_id", "detail", "raw_row"]
        ctx["sample_issues"] = [dict(zip(cols, row)) for row in cur.fetchall()]

    return ctx


@mcp.tool()
def ask_about_errors(query: str, run_id: str = "") -> str:
    """
    Answer a natural-language question about data quality issues found by
    the pipeline. Grounds the response in structured DQ findings data.

    Args:
        query: The natural-language question to answer.
        run_id: Optional run UUID. Defaults to the most recent run.
    """
    if groq_client is None:
        return "Error: GROQ_API_KEY is not configured on the MCP server."

    conn = get_connection()
    try:
        target = resolve_run_id(conn, run_id)
        if not target:
            return "No pipeline runs found. Run the pipeline first."
        ctx = gather_context(conn, target)
    finally:
        conn.close()

    system_prompt = (
        "You are a data quality assistant for a video events pipeline. "
        "Answer the user's question using ONLY the structured findings data "
        "provided below. Be concise, cite specific numbers, and call out "
        "patterns across platforms or event types where relevant. "
        "If the data doesn't contain enough information to answer, say so "
        "explicitly rather than guessing.\n\n"
        f"FINDINGS:\n{json.dumps(ctx, indent=2, default=str)}"
    )

    response = groq_client.chat.completions.create(
        model="llama-3.1-8b-instant",
        max_tokens=1000,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": query},
        ],
    )

    return response.choices[0].message.content


if __name__ == "__main__":
    app = mcp.streamable_http_app()
    app.user_middleware.insert(0, Middleware(BearerAuthMiddleware))
    uvicorn.run(app, host="0.0.0.0", port=8000)

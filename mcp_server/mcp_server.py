"""
MCP server exposing `ask_about_errors(query)`.

Grounds an LLM call (Anthropic API) in structured data pulled from
`dq_issues` / `landing` / `staging` for the latest run (or a specified
run_id), then answers natural-language questions about data quality
findings.

Run:
    python mcp_server/mcp_server.py
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

# Dummy hardcoded bearer token for demo purposes — shows auth is being
# considered at the transport boundary. In production this would be a
# real per-client credential (or OAuth token) validated against an
# identity provider, not a shared static secret.
MCP_AUTH_TOKEN = os.environ.get("MCP_AUTH_TOKEN", "dummy-demo-token")

mcp = FastMCP("video-dq-server")
groq_client = Groq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None


class BearerAuthMiddleware(BaseHTTPMiddleware):
    """Reject requests that don't present the expected bearer token."""

    async def dispatch(self, request, call_next):
        auth_header = request.headers.get("authorization", "")
        expected = f"Bearer {MCP_AUTH_TOKEN}"
        if auth_header != expected:
            return JSONResponse(
                {"error": "unauthorized"}, status_code=401
            )
        return await call_next(request)


def get_connection():
    return psycopg.connect(DATABASE_URL)


def get_latest_run_id(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT run_id FROM dq_issues ORDER BY id DESC LIMIT 1")
        row = cur.fetchone()
        return row[0] if row else None


def gather_context(conn, run_id):
    """Pull structured summary data for the run to ground the LLM."""
    context = {"run_id": str(run_id)}

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM landing WHERE run_id = %s", (run_id,))
        context["total_clean"] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT event_id) FROM dq_issues WHERE run_id = %s", (run_id,))
        context["total_flagged"] = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM landing WHERE run_id = %s", (run_id,))
        context["total_clean"] = cur.fetchone()[0]

        cur.execute(
            """
            SELECT issue_type, COUNT(*) AS cnt
            FROM dq_issues
            WHERE run_id = %s
            GROUP BY issue_type
            ORDER BY cnt DESC
            """,
            (run_id,),
        )
        context["issue_counts"] = {row[0]: row[1] for row in cur.fetchall()}

        # Sample of up to 5 dq_issues rows (exclude raw_row to keep context small)
        cur.execute(
            """
            SELECT issue_type, event_id, detail
            FROM dq_issues
            WHERE run_id = %s
            ORDER BY id
            LIMIT 5
            """,
            (run_id,),
        )
        cols = ["issue_type", "event_id", "detail"]
        context["sample_issues"] = [dict(zip(cols, row)) for row in cur.fetchall()]

    return context


@mcp.tool()
def ask_about_errors(query: str, run_id: str = "") -> str:
    """
    Answer a natural-language question about data quality issues found
    by the pipeline. If run_id is omitted, uses the most recent run.

    Args:
        query: The natural-language question to answer.
        run_id: Optional specific run_id (UUID string). Defaults to latest.
    """
    if groq_client is None:
        return "Error: GROQ_API_KEY is not configured on the MCP server."

    conn = get_connection()
    try:
        target_run_id = run_id or get_latest_run_id(conn)
        if target_run_id is None:
            return "No pipeline runs found. Run the pipeline first."

        context = gather_context(conn, target_run_id)
    finally:
        conn.close()

    system_prompt = (
        "You are a data quality assistant for a video events pipeline. "
        "Answer the user's question using ONLY the structured findings "
        "data provided below. Be concise and cite specific numbers from "
        "the data. If the data doesn't contain enough information to "
        "answer, say so explicitly rather than guessing.\n\n"
        f"FINDINGS DATA:\n{json.dumps(context, indent=2, default=str)}"
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
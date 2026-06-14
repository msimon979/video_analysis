"""Integration smoke tests for all four MCP tools. Skipped if server is not running."""

import asyncio
import json
import os
import socket

import pytest

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

MCP_ENDPOINT = "http://localhost:8000/mcp"
MCP_AUTH_TOKEN = os.environ.get("MCP_AUTH_TOKEN") or "dummy-demo-token"


def _server_running() -> bool:
    try:
        with socket.create_connection(("localhost", 8000), timeout=2):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(
    not _server_running(), reason="MCP server not running on :8000"
)


async def _call(session, tool, args=None):
    result = await session.call_tool(tool, args or {})
    text = "\n".join(b.text for b in result.content if hasattr(b, "text"))
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


async def _run_all_tools():
    headers = {"Authorization": f"Bearer {MCP_AUTH_TOKEN}"}
    async with streamablehttp_client(MCP_ENDPOINT, headers=headers) as (r, w, _):
        async with ClientSession(r, w) as session:
            await session.initialize()

            runs = await _call(session, "list_recent_runs", {"limit": 5})
            assert isinstance(runs, list), "list_recent_runs should return a list"
            assert len(runs) > 0, "list_recent_runs returned no runs"
            assert "run_id" in runs[0]
            assert "clean_rows" in runs[0]
            assert "flagged_rows" in runs[0]

            latest_run_id = runs[0]["run_id"]

            summary = await _call(session, "get_run_summary", {"run_id": latest_run_id})
            assert isinstance(summary, dict), "get_run_summary should return a dict"
            assert "clean_rows" in summary
            assert "flagged_rows" in summary
            assert "issues_by_type" in summary
            assert "issues_by_platform" in summary
            assert "issues_by_event_type" in summary

            first_issue_type = next(iter(summary["issues_by_type"]), "STALE_TIMESTAMP")

            detail = await _call(session, "get_issues_detail", {
                "issue_type": first_issue_type,
                "run_id": latest_run_id,
                "limit": 3,
            })
            assert isinstance(detail, dict), "get_issues_detail should return a dict"
            assert detail["issue_type"] == first_issue_type
            assert "samples" in detail

            answer = await _call(session, "ask_about_errors", {
                "query": "What is the most common issue type?",
                "run_id": latest_run_id,
            })
            assert isinstance(answer, str) and len(answer) > 0, "ask_about_errors should return a non-empty string"


def test_mcp_tools():
    asyncio.run(_run_all_tools())

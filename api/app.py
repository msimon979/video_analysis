"""
Flask API — single /ask POST endpoint.

Acts as an MCP client: relays natural-language questions to the MCP
server's `ask_about_errors` tool and returns the response.

Run:
    python api/app.py
"""

import os

from flask import Flask, jsonify, request
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client


MCP_SERVER_URL = os.environ.get("MCP_SERVER_URL", "http://localhost:8000")
MCP_ENDPOINT = f"{MCP_SERVER_URL}/mcp"

# Must match MCP_AUTH_TOKEN configured on the MCP server (dummy demo value)
MCP_AUTH_TOKEN = os.environ.get("MCP_AUTH_TOKEN", "dummy-demo-token")

app = Flask(__name__)


async def call_ask_about_errors(query: str, run_id: str = "") -> str:
    headers = {"Authorization": f"Bearer {MCP_AUTH_TOKEN}"}
    async with streamablehttp_client(MCP_ENDPOINT, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool(
                "ask_about_errors", {"query": query, "run_id": run_id}
            )
            text_parts = [
                block.text for block in result.content if hasattr(block, "text")
            ]
            return "\n".join(text_parts)


@app.route("/ask", methods=["POST"])
async def ask():
    body = request.get_json(silent=True) or {}
    query = body.get("query")
    run_id = body.get("run_id", "")

    if not query:
        return jsonify({"error": "Missing 'query' in request body"}), 400

    try:
        answer = await call_ask_about_errors(query, run_id)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 502

    return jsonify({"query": query, "answer": answer})


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
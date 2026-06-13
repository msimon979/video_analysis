"""
Flask API — /ask POST endpoint.

Discovers available tools from the MCP server at request time, uses an
LLM agent loop to select and chain tool calls until the query is answered.

Routes:
  POST /ask    { "query": "...", "run_id": "..." }
  GET  /tools  lists available MCP tools and their descriptions
  GET  /health
"""

import json
import logging
import os

from flask import Flask, jsonify, request
from groq import Groq
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client
from pydantic import BaseModel, ValidationError, field_validator


MCP_SERVER_URL = os.environ.get("MCP_SERVER_URL", "http://localhost:8000")
MCP_ENDPOINT = f"{MCP_SERVER_URL}/mcp"
MCP_AUTH_TOKEN = os.environ.get("MCP_AUTH_TOKEN", "dummy-demo-token")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
MAX_AGENT_ITERATIONS = 2

ROUTING_PROMPT = (
    "You are a tool-calling agent for a video data quality API. "
    "Given the user query and available tools, return a JSON object "
    "with exactly two keys:\n"
    '  "tool": the tool name to call\n'
    '  "args": a dict of arguments matching that tool\'s inputSchema\n'
    "Return only the JSON object — no explanation.\n\n"
    "Available tools:\n{tool_specs}"
)

CONTINUATION_PROMPT = (
    "You are a tool-calling agent for a video data quality API. "
    "A tool was just called. Decide if the result fully answers the original query.\n"
    "Return a JSON object:\n"
    '  If complete:               {{"done": true}}\n'
    '  If another tool is needed: {{"done": false, "tool": "<name>", "args": {{...}}}}\n'
    "Return only the JSON object — no explanation.\n\n"
    "Available tools:\n{tool_specs}"
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

app = Flask(__name__)
groq_client = Groq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None


class AskRequest(BaseModel):
    query: str
    run_id: str = ""

    @field_validator("query")
    @classmethod
    def query_must_not_be_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("query must not be blank")
        return v.strip()


def _mcp_headers():
    return {"Authorization": f"Bearer {MCP_AUTH_TOKEN}"}


async def list_mcp_tools() -> list[dict]:
    async with streamablehttp_client(MCP_ENDPOINT, headers=_mcp_headers()) as (r, w, _):
        async with ClientSession(r, w) as session:
            await session.initialize()
            result = await session.list_tools()
            return [
                {
                    "name": t.name,
                    "description": t.description,
                    "inputSchema": t.inputSchema,
                }
                for t in result.tools
            ]


def _llm(system: str, user: str) -> dict:
    response = groq_client.chat.completions.create(
        model="llama-3.1-8b-instant",
        max_tokens=300,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
    )
    return json.loads(response.choices[0].message.content)


async def route_and_call(query: str, run_id: str) -> tuple[str, list[str]]:
    """
    Agent loop:
    1. Fetch available tools from the MCP server.
    2. LLM picks the first tool and args.
    3. Call the tool.
    4. LLM decides if the result is sufficient or another tool is needed.
    5. Repeat up to MAX_AGENT_ITERATIONS.
    Returns (final_result, list_of_tools_called).
    """
    async with streamablehttp_client(MCP_ENDPOINT, headers=_mcp_headers()) as (r, w, _):
        async with ClientSession(r, w) as session:
            await session.initialize()

            tools = await session.list_tools()
            tool_specs = json.dumps(
                [
                    {
                        "name": t.name,
                        "description": t.description,
                        "inputSchema": t.inputSchema,
                    }
                    for t in tools.tools
                ],
                indent=2,
            )

            if groq_client is None:
                result = await session.call_tool(
                    "ask_about_errors", {"query": query, "run_id": run_id}
                )
                text = "\n".join(b.text for b in result.content if hasattr(b, "text"))
                return text, ["ask_about_errors"]

            routing = _llm(
                system=ROUTING_PROMPT.format(tool_specs=tool_specs),
                user=f'query: "{query}"\nrun_id: "{run_id}"',
            )
            tool_name = routing["tool"]
            tool_args = routing.get("args", {})
            log.info("routing: selected tool=%s args=%s", tool_name, tool_args)

            tools_called = []
            last_result = ""
            history = []

            for iteration in range(MAX_AGENT_ITERATIONS):
                log.info("iteration %d: calling tool=%s", iteration + 1, tool_name)
                mcp_result = await session.call_tool(tool_name, tool_args)
                last_result = "\n".join(
                    b.text for b in mcp_result.content if hasattr(b, "text")
                )
                tools_called.append(tool_name)
                history.append(f'Tool: {tool_name}\nResult: {last_result}')

                next_step = _llm(
                    system=CONTINUATION_PROMPT.format(tool_specs=tool_specs),
                    user=(
                        f'Original query: "{query}"\n'
                        f'run_id: "{run_id}"\n\n'
                        f'Steps so far:\n' + "\n\n".join(history)
                    ),
                )

                if next_step.get("done", True):
                    log.info("agent done after %d tool(s): %s", len(tools_called), tools_called)
                    break

                tool_name = next_step["tool"]
                tool_args = next_step.get("args", {})
                log.info("agent continuing: next tool=%s args=%s", tool_name, tool_args)

            return last_result, tools_called


@app.route("/ask", methods=["POST"])
async def ask():
    try:
        payload = AskRequest.model_validate(request.get_json(silent=True) or {})
    except ValidationError as exc:
        return jsonify({"error": exc.errors()}), 400

    try:
        answer, tools_used = await route_and_call(payload.query, payload.run_id)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 502

    return jsonify({"query": payload.query, "tools_used": tools_used, "answer": answer})


@app.route("/tools", methods=["GET"])
async def tools():
    try:
        tool_list = await list_mcp_tools()
    except Exception as exc:
        return jsonify({"error": str(exc)}), 502
    return jsonify({"tools": tool_list})


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)

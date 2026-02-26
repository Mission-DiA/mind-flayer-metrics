import json
import structlog
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from app.config import settings
from app.models.billing_models import (
    MCPToolCallRequest,
    MCPToolCallResponse,
    MCPToolsListResponse,
    HealthResponse,
)
from app.mcp.server import mcp_handler
from app.mcp.validators import validate_tool_arguments
from app.billing.query_engine import query_engine

log = structlog.get_logger()

# ── Rate limiter ──────────────────────────────────────────────────────────────
limiter = Limiter(key_func=get_remote_address)

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Mind Flayer Metrics — Billing Intelligence Backend",
    description="FastAPI + MCP server for multi-cloud billing intelligence",
    version="1.0.0",
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.origins_list,
    allow_credentials=True,
    allow_methods=["POST", "GET"],
    allow_headers=["*"],
)


# ── MCP endpoints ─────────────────────────────────────────────────────────────

@app.post("/mcp/tools/list", response_model=MCPToolsListResponse)
async def mcp_list_tools():
    """MCP: return all 13 billing tools with their JSON schemas."""
    return MCPToolsListResponse(tools=mcp_handler.get_tools_list())


@app.post("/mcp/tools/call", response_model=MCPToolCallResponse)
@limiter.limit(f"{settings.rate_limit_per_hour}/hour")
async def mcp_call_tool(request: Request, body: MCPToolCallRequest):
    """MCP: validate and execute a billing tool, return result in MCP format."""
    tool_name = body.name

    if not mcp_handler.tool_exists(tool_name):
        raise HTTPException(status_code=404, detail=f"Tool '{tool_name}' not found")

    validate_tool_arguments(tool_name, body.arguments)

    capability = mcp_handler.map_to_capability(tool_name)

    log.info("mcp_tool_call", tool=tool_name, capability=capability)

    # T15 fills in query_engine.execute — raises NotImplementedError until then
    result = await query_engine.execute(capability, body.arguments)

    return MCPToolCallResponse(
        content=[{"type": "text", "text": json.dumps(result, default=str)}]
    )


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health", response_model=HealthResponse)
async def health():
    return HealthResponse(
        status="healthy",
        service="billing-intelligence-backend",
    )


@app.get("/")
async def root():
    return {
        "service": "mind-flayer-metrics backend",
        "docs": "/docs",
        "health": "/health",
        "mcp_tools": "/mcp/tools/list",
    }

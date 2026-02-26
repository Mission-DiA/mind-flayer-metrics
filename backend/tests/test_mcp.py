import pytest
from httpx import AsyncClient, ASGITransport
from app.main import app


@pytest.mark.asyncio
async def test_health():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "healthy"


@pytest.mark.asyncio
async def test_mcp_tools_list_returns_13():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.post("/mcp/tools/list")
    assert r.status_code == 200
    tools = r.json()["tools"]
    assert len(tools) == 13
    names = [t["name"] for t in tools]
    assert "query_billing_costs" in names
    assert "find_untagged_resources" in names


@pytest.mark.asyncio
async def test_mcp_call_unknown_tool_returns_404():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.post("/mcp/tools/call", json={"name": "nonexistent_tool", "arguments": {}})
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_mcp_call_invalid_args_returns_400():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        # get_cost_by_service requires start_date and end_date
        r = await client.post("/mcp/tools/call", json={
            "name": "get_cost_by_service",
            "arguments": {}
        })
    assert r.status_code == 400

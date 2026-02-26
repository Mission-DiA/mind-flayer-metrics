from pydantic import BaseModel, Field
from typing import Any


class MCPToolCallRequest(BaseModel):
    name: str
    arguments: dict[str, Any] = Field(default_factory=dict)


class ChartSuggestion(BaseModel):
    type: str  # "pie" | "bar" | "line" | "table"
    reason: str


class ResultMetadata(BaseModel):
    query_timestamp: str
    total_count: int
    user_requested_chart: bool = False
    chart_type: str | None = None
    chart_suggestion: ChartSuggestion | None = None


class BillingResult(BaseModel):
    data: list[dict[str, Any]]
    summary: str
    metadata: ResultMetadata


class MCPToolCallResponse(BaseModel):
    content: list[dict[str, Any]]


class MCPToolsListResponse(BaseModel):
    tools: list[dict[str, Any]]


class HealthResponse(BaseModel):
    status: str
    service: str
    version: str = "1.0.0"

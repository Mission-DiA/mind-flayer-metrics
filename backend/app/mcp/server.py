from .tools import BILLING_TOOLS, TOOLS_BY_NAME

# Tool name → internal billing capability name.
# Filled out fully in T11.
TOOL_CAPABILITY_MAP: dict[str, str] = {
    "query_billing_costs":     "natural_language_query",
    "get_total_cost":          "total_cost",
    "get_cost_by_service":     "cost_by_service",
    "get_cost_by_provider":    "cost_by_provider",
    "get_cost_by_team":        "cost_by_team",
    "get_cost_by_environment": "cost_by_environment",
    "get_cost_by_region":      "cost_by_region",
    "get_cost_by_account":     "cost_by_account",
    "get_daily_trend":         "daily_trend",
    "get_weekly_trend":        "weekly_trend",
    "compare_month_over_month":"month_over_month",
    "get_top_resources":       "top_resources",
    "find_untagged_resources": "untagged_resources",
}


class MCPProtocolHandler:
    def get_tools_list(self) -> list[dict]:
        return BILLING_TOOLS

    def tool_exists(self, tool_name: str) -> bool:
        return tool_name in TOOLS_BY_NAME

    def map_to_capability(self, tool_name: str) -> str:
        return TOOL_CAPABILITY_MAP.get(tool_name, tool_name)


mcp_handler = MCPProtocolHandler()

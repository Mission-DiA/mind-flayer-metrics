# 13 MCP billing tool definitions with complete inputSchema per tool.

BILLING_TOOLS: list[dict] = [
    {
        "name": "query_billing_costs",
        "description": "Query cloud billing costs using natural language",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Natural language query, e.g. 'AWS costs last month'"
                },
                "providers": {
                    "type": "array",
                    "items": {"type": "string", "enum": ["GCP", "AWS", "Snowflake", "MongoDB"]},
                    "description": "Optional: filter by providers"
                }
            },
            "required": ["query"]
        }
    },
    {
        "name": "get_total_cost",
        "description": "Get total cloud spend summary for a date range",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "start_date": {"type": "string", "format": "date"},
                "end_date": {"type": "string", "format": "date"},
                "provider": {"type": "string", "enum": ["GCP", "AWS", "Snowflake", "MongoDB"]}
            },
            "required": ["start_date", "end_date"]
        }
    },
    {
        "name": "get_cost_by_service",
        "description": "Get cost breakdown by cloud service",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "start_date": {"type": "string", "format": "date"},
                "end_date": {"type": "string", "format": "date"},
                "provider": {"type": "string", "enum": ["GCP", "AWS", "Snowflake", "MongoDB"]},
                "limit": {"type": "integer", "minimum": 1, "maximum": 100, "default": 20}
            },
            "required": ["start_date", "end_date"]
        }
    },
    {
        "name": "get_cost_by_provider",
        "description": "Get cost breakdown and comparison across cloud providers",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "start_date": {"type": "string", "format": "date"},
                "end_date": {"type": "string", "format": "date"},
                "providers": {
                    "type": "array",
                    "items": {"type": "string", "enum": ["GCP", "AWS", "Snowflake", "MongoDB"]}
                }
            },
            "required": ["start_date", "end_date"]
        }
    },
    {
        "name": "get_cost_by_team",
        "description": "Get cost breakdown by team label",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "start_date": {"type": "string", "format": "date"},
                "end_date": {"type": "string", "format": "date"},
                "team": {"type": "string", "description": "Optional: filter to a specific team"}
            },
            "required": ["start_date", "end_date"]
        }
    },
    {
        "name": "get_cost_by_environment",
        "description": "Get cost breakdown by environment (production, staging, development)",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "start_date": {"type": "string", "format": "date"},
                "end_date": {"type": "string", "format": "date"},
                "environment": {
                    "type": "string",
                    "enum": ["production", "staging", "development"]
                }
            },
            "required": ["start_date", "end_date"]
        }
    },
    {
        "name": "get_cost_by_region",
        "description": "Get cost breakdown by geographic region",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "start_date": {"type": "string", "format": "date"},
                "end_date": {"type": "string", "format": "date"},
                "provider": {"type": "string", "enum": ["GCP", "AWS", "Snowflake", "MongoDB"]}
            },
            "required": ["start_date", "end_date"]
        }
    },
    {
        "name": "get_cost_by_account",
        "description": "Get cost breakdown and comparison across cloud accounts/projects",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "start_date": {"type": "string", "format": "date"},
                "end_date": {"type": "string", "format": "date"},
                "provider": {"type": "string", "enum": ["GCP", "AWS", "Snowflake", "MongoDB"]}
            },
            "required": ["start_date", "end_date"]
        }
    },
    {
        "name": "get_daily_trend",
        "description": "Get daily cost time series for trend analysis",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "days": {"type": "integer", "minimum": 1, "maximum": 90, "default": 30},
                "provider": {"type": "string", "enum": ["GCP", "AWS", "Snowflake", "MongoDB"]}
            },
            "required": []
        }
    },
    {
        "name": "get_weekly_trend",
        "description": "Get weekly cost time series for trend analysis",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "weeks": {"type": "integer", "minimum": 1, "maximum": 52, "default": 12},
                "provider": {"type": "string", "enum": ["GCP", "AWS", "Snowflake", "MongoDB"]}
            },
            "required": []
        }
    },
    {
        "name": "compare_month_over_month",
        "description": "Compare costs between two calendar months",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "current_month": {
                    "type": "string",
                    "format": "date",
                    "description": "First day of current month, e.g. '2026-02-01'"
                },
                "previous_month": {
                    "type": "string",
                    "format": "date",
                    "description": "First day of previous month, e.g. '2026-01-01'"
                },
                "provider": {"type": "string", "enum": ["GCP", "AWS", "Snowflake", "MongoDB"]}
            },
            "required": ["current_month", "previous_month"]
        }
    },
    {
        "name": "get_top_resources",
        "description": "Get the most expensive individual cloud resources",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "start_date": {"type": "string", "format": "date"},
                "end_date": {"type": "string", "format": "date"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 100, "default": 20},
                "provider": {"type": "string", "enum": ["GCP", "AWS", "Snowflake", "MongoDB"]}
            },
            "required": ["start_date", "end_date"]
        }
    },
    {
        "name": "find_untagged_resources",
        "description": "Find resources missing team/environment labels — cost allocation gaps",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "provider": {"type": "string", "enum": ["GCP", "AWS", "Snowflake", "MongoDB"]},
                "days": {"type": "integer", "minimum": 1, "maximum": 90, "default": 30}
            },
            "required": []
        }
    },
]

# Quick lookup by name
TOOLS_BY_NAME: dict[str, dict] = {t["name"]: t for t in BILLING_TOOLS}

from typing import Any
from .bigquery_client import bq_client
from .gemini_client import gemini_client
from .sql_templates import TEMPLATES
from .query_parser import parse_chart_request

# TODO T15: Implement the full billing query engine.
#
# Pipeline:
#   1. For natural_language_query → gemini_client.detect_intent → select template + params
#   2. For direct tool calls → use capability name to pick template
#   3. bq_client.run_query(template, params)
#   4. gemini_client.summarise(query, rows)
#   5. Return BillingResult


class BillingQueryEngine:
    async def execute(self, capability: str, arguments: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError("T15: implement billing query engine")


query_engine = BillingQueryEngine()

from typing import Any
from app.config import settings

# TODO T12: Implement BigQuery client with parameterized query execution.
#
# This module will:
#   - Initialise google.cloud.bigquery.Client using ADC
#   - Accept a SQL template string + parameter dict
#   - Execute the query with QueryJobConfig (parameterized, max_bytes_billed)
#   - Return rows as list[dict]


class BigQueryClient:
    def __init__(self):
        self.project = settings.gcp_project
        self.full_table = settings.bigquery_full_table
        # T12: self.client = bigquery.Client(project=self.project)

    async def run_query(self, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        raise NotImplementedError("T12: implement BigQuery parameterized query execution")


bq_client = BigQueryClient()

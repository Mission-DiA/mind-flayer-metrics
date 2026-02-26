from app.config import settings

# TODO T14: Implement Gemini API client.
#
# Two responsibilities:
#   1. Intent detection — given a natural language query, return which
#      SQL template to use + extracted parameters (dates, filters).
#   2. Summarisation — given raw BigQuery rows, return a 1-2 sentence
#      human-readable summary (e.g. "AWS costs 38% higher than GCP").


class GeminiClient:
    def __init__(self):
        self.model_name = settings.gemini_model
        # T14: configure genai with settings.gemini_api_key

    async def detect_intent(self, query: str) -> dict:
        """Return {"template": str, "params": dict} from a natural language query."""
        raise NotImplementedError("T14: implement Gemini intent detection")

    async def summarise(self, query: str, rows: list[dict]) -> str:
        """Return a concise AI summary of the query result rows."""
        raise NotImplementedError("T14: implement Gemini summarisation")


gemini_client = GeminiClient()

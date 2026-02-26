# 13 parameterized BigQuery SQL templates.
# All queries MUST include a billing_date filter (require_partition_filter = TRUE).
# TODO T13: Write each template in full.

TEMPLATES: dict[str, str] = {
    "total_cost":           "-- TODO T13",
    "cost_by_service":      "-- TODO T13",
    "cost_by_provider":     "-- TODO T13",
    "cost_by_team":         "-- TODO T13",
    "cost_by_environment":  "-- TODO T13",
    "cost_by_region":       "-- TODO T13",
    "cost_by_account":      "-- TODO T13",
    "daily_trend":          "-- TODO T13",
    "weekly_trend":         "-- TODO T13",
    "month_over_month":     "-- TODO T13",
    "top_resources":        "-- TODO T13",
    "untagged_resources":   "-- TODO T13",
    # natural_language_query handled by gemini_client → template selection
}

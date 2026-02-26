# TODO T16: Implement chart request parser.
#
# Detects whether the user's query includes chart keywords and
# determines the appropriate chart type.

CHART_KEYWORDS = ["chart", "graph", "visualize", "visualise", "plot", "show me a"]

CHART_TYPE_HINTS = {
    "pie":  ["pie", "proportion", "share", "percentage"],
    "line": ["line", "trend", "over time", "daily", "weekly", "monthly"],
    "bar":  ["bar", "compare", "breakdown", "by service", "by team"],
}


def parse_chart_request(query: str) -> dict:
    """
    Return {"show_chart": bool, "chart_type": str | None}.
    chart_type is one of "pie", "bar", "line", or "auto".
    """
    q = query.lower()

    if not any(kw in q for kw in CHART_KEYWORDS):
        return {"show_chart": False, "chart_type": None}

    for chart_type, hints in CHART_TYPE_HINTS.items():
        if any(h in q for h in hints):
            return {"show_chart": True, "chart_type": chart_type}

    return {"show_chart": True, "chart_type": "auto"}

#!/usr/bin/env python3
"""
Snowflake Billing Collector
Reads from Snowflake Account Usage → writes to billing.fact_cloud_costs.

Primary source  : SNOWFLAKE.ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY
                  (requires ORGADMIN role — gives direct USD cost per account + service)
Fallback source : SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY
                  (requires ACCOUNTADMIN role — credits × CREDIT_PRICE_USD)

Target : kf-dev-ops-p001.billing.fact_cloud_costs

Usage:
    python snowflake_collector.py                   # collects yesterday
    python snowflake_collector.py --date 2026-02-25 # collects specific date
    python snowflake_collector.py --backfill 30     # backfills last N days (max 90)

Env vars (required):
    SNOWFLAKE_ACCOUNT    e.g. xy12345.us-east-1
    SNOWFLAKE_USER
    SNOWFLAKE_PASSWORD

Env vars (optional):
    SNOWFLAKE_ROLE           default: ACCOUNTADMIN
    SNOWFLAKE_WAREHOUSE      default: COMPUTE_WH
    SNOWFLAKE_ENVIRONMENT    default: unknown  (set 'production' or 'development')
    SNOWFLAKE_REGION         default: unknown
    CREDIT_PRICE_USD         default: 4.0  (used only in fallback mode)
    TARGET_PROJECT           default: kf-dev-ops-p001
    TARGET_DATASET           default: billing
    TARGET_TABLE             default: fact_cloud_costs
    COLLECTOR_VERSION        default: 1.0.0
"""

import os
import re
import argparse
import logging
import time
from datetime import date, timedelta, datetime, timezone

import snowflake.connector
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
# NOTE: SNOWFLAKE_PASSWORD is intentionally NOT read at module level.
# It is read inside connect() — the only function that needs it — to limit
# the window during which the raw credential string exists as a named object.

SF_ACCOUNT    = os.environ["SNOWFLAKE_ACCOUNT"]
SF_USER       = os.environ["SNOWFLAKE_USER"]
SF_ROLE       = os.environ.get("SNOWFLAKE_ROLE",      "ACCOUNTADMIN")
SF_WAREHOUSE  = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SF_ENV        = os.environ.get("SNOWFLAKE_ENVIRONMENT", "unknown")
SF_REGION     = os.environ.get("SNOWFLAKE_REGION",    "unknown")
CREDIT_PRICE  = float(os.environ.get("CREDIT_PRICE_USD", "4.0"))

TARGET_PROJECT = os.environ.get("TARGET_PROJECT", "kf-dev-ops-p001")
TARGET_DATASET = os.environ.get("TARGET_DATASET", "billing")
TARGET_TABLE   = os.environ.get("TARGET_TABLE",   "fact_cloud_costs")
COLLECTOR_VER  = os.environ.get("COLLECTOR_VERSION", "1.0.0")

TARGET_FULL = f"`{TARGET_PROJECT}.{TARGET_DATASET}.{TARGET_TABLE}`"

# Backfill safety cap
MAX_BACKFILL_DAYS = 90

# ── Identifier validation ─────────────────────────────────────────────────────

_SAFE_ID = re.compile(r'^[a-zA-Z0-9_\-]+$')


def _check_ids(**ids: str) -> None:
    for name, value in ids.items():
        if not _SAFE_ID.match(value):
            raise ValueError(f"Unsafe BigQuery identifier {name}={value!r}")


_check_ids(
    TARGET_PROJECT=TARGET_PROJECT,
    TARGET_DATASET=TARGET_DATASET,
    TARGET_TABLE=TARGET_TABLE,
)

# ── SQL ───────────────────────────────────────────────────────────────────────

# Primary: org-level, direct USD, one row per account + service per day
ORG_USAGE_SQL = """
    SELECT
        ACCOUNT_NAME,
        ACCOUNT_LOCATOR,
        SERVICE_TYPE,
        USAGE,
        USAGE_UNIT,
        USAGE_IN_CURRENCY,
        CURRENCY
    FROM SNOWFLAKE.ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY
    WHERE USAGE_DATE = %(billing_date)s
      AND USAGE_IN_CURRENCY > 0
    ORDER BY USAGE_IN_CURRENCY DESC
"""

# Fallback: account-level, credits → USD, one row per service + warehouse per day
METERING_SQL = """
    SELECT
        SERVICE_TYPE,
        IFNULL(WAREHOUSE_NAME, SERVICE_TYPE)    AS WAREHOUSE_NAME,
        SUM(CREDITS_USED)                       AS CREDITS_USED
    FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY
    WHERE USAGE_DATE = %(billing_date)s
      AND CREDITS_USED > 0
    GROUP BY SERVICE_TYPE, WAREHOUSE_NAME
    ORDER BY CREDITS_USED DESC
"""


# ── Snowflake fetch ───────────────────────────────────────────────────────────

_MAX_RETRIES = 3
_RETRY_BACKOFF = 5  # seconds, doubles each attempt


def connect(retries: int = _MAX_RETRIES) -> snowflake.connector.SnowflakeConnection:
    """Connect to Snowflake with exponential backoff on transient failures."""
    delay = _RETRY_BACKOFF
    for attempt in range(1, retries + 1):
        try:
            return snowflake.connector.connect(
                account=SF_ACCOUNT,
                user=SF_USER,
                password=os.environ["SNOWFLAKE_PASSWORD"],  # read here, not at module level
                role=SF_ROLE,
                warehouse=SF_WAREHOUSE,
                database="SNOWFLAKE",
                schema="ORGANIZATION_USAGE",
            )
        except Exception as e:
            if attempt == retries:
                raise
            # Log only the exception type — Snowflake connector exceptions for auth
            # failures can include the username and connection metadata in their
            # string representation; never log str(e) for connection errors
            log.warning(
                f"[Snowflake] connect attempt {attempt} failed "
                f"({type(e).__name__}), retrying in {delay}s"
            )
            time.sleep(delay)
            delay *= 2


def fetch_org_usage(cursor, billing_date: date) -> list[dict]:
    """Primary: ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY (ORGADMIN required)."""
    now = datetime.now(timezone.utc).isoformat()  # timezone-aware UTC timestamp
    cursor.execute(ORG_USAGE_SQL, {"billing_date": billing_date.isoformat()})
    rows = []
    for account_name, account_locator, service_type, usage, usage_unit, cost_usd, currency in cursor:
        rows.append({
            "billing_date":      billing_date.isoformat(),
            "provider":          "Snowflake",
            "account_id":        account_locator or account_name,
            "account_name":      account_name,
            "project_id":        account_name,
            "service_name":      _friendly_service(service_type),
            "sku":               service_type,
            "sku_description":   service_type,
            "resource_id":       None,
            "resource_name":     None,
            "resource_type":     service_type,
            "cost":              round(float(cost_usd), 6),
            "currency":          currency or "USD",
            "original_cost":     round(float(cost_usd), 6),
            "usage_amount":      round(float(usage), 6) if usage else None,
            "usage_unit":        usage_unit,
            "team":              "unknown",
            "environment":       SF_ENV,
            "region":            SF_REGION,
            "tags":              None,
            "collected_at":      now,
            "processed_at":      now,
            "source_file":       "snowflake.organization_usage.usage_in_currency_daily",
            "collector_version": COLLECTOR_VER,
        })
    return rows


def fetch_metering(cursor, billing_date: date) -> list[dict]:
    """Fallback: ACCOUNT_USAGE.METERING_DAILY_HISTORY (credits × CREDIT_PRICE_USD)."""
    now = datetime.now(timezone.utc).isoformat()  # timezone-aware UTC timestamp
    cursor.execute(METERING_SQL, {"billing_date": billing_date.isoformat()})
    rows = []
    for service_type, warehouse_name, credits_used in cursor:
        cost_usd = round(float(credits_used) * CREDIT_PRICE, 6)
        rows.append({
            "billing_date":      billing_date.isoformat(),
            "provider":          "Snowflake",
            "account_id":        SF_ACCOUNT,
            "account_name":      SF_ACCOUNT,
            "project_id":        SF_ACCOUNT,
            "service_name":      _friendly_service(service_type),
            "sku":               service_type,
            "sku_description":   service_type,
            "resource_id":       warehouse_name,
            "resource_name":     warehouse_name,
            "resource_type":     service_type,
            "cost":              cost_usd,
            "currency":          "USD",
            "original_cost":     round(float(credits_used), 6),
            "usage_amount":      round(float(credits_used), 6),
            "usage_unit":        "credits",
            "team":              "unknown",
            "environment":       SF_ENV,
            "region":            SF_REGION,
            "tags":              None,
            "collected_at":      now,
            "processed_at":      now,
            "source_file":       "snowflake.account_usage.metering_daily_history",
            "collector_version": COLLECTOR_VER,
        })
    return rows


def _friendly_service(service_type: str) -> str:
    """Map Snowflake internal service codes to human-readable names."""
    mapping = {
        "WAREHOUSE_METERING":        "Warehouse Compute",
        "STORAGE":                   "Storage",
        "SERVERLESS_TASK":           "Serverless Tasks",
        "SNOWPIPE":                  "Snowpipe",
        "AUTOMATIC_CLUSTERING":      "Auto Clustering",
        "MATERIALIZED_VIEW":         "Materialized Views",
        "SEARCH_OPTIMIZATION":       "Search Optimization",
        "DATA_TRANSFER":             "Data Transfer",
        "REPLICATION":               "Replication",
        "CLOUD_SERVICES":            "Cloud Services",
    }
    return mapping.get(service_type, service_type.replace("_", " ").title())


# ── BigQuery write ────────────────────────────────────────────────────────────

def delete_existing(bq: bigquery.Client, billing_date: date) -> None:
    sql = f"""
        DELETE FROM {TARGET_FULL}
        WHERE billing_date = @billing_date
          AND provider = 'Snowflake'
    """
    cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("billing_date", "DATE", billing_date.isoformat())
    ])
    bq.query(sql, job_config=cfg).result()


def insert_rows(bq: bigquery.Client, rows: list[dict]) -> int:
    if not rows:
        return 0
    target = f"{TARGET_PROJECT}.{TARGET_DATASET}.{TARGET_TABLE}"
    errors = bq.insert_rows_json(target, rows)
    if errors:
        first_reason = errors[0][0].get('reason', 'unknown') if errors[0] else 'unknown'
        raise RuntimeError(
            f"BigQuery streaming insert failed: {len(errors)} row(s), "
            f"first error reason: {first_reason}"
        )
    return len(rows)


# ── Core ──────────────────────────────────────────────────────────────────────

def collect_for_date(sf_conn, bq: bigquery.Client, billing_date: date) -> int:
    log.info(f"[Snowflake] collecting {billing_date} ...")
    cursor = sf_conn.cursor()

    # Try primary source first (org-level, direct USD)
    rows = []
    try:
        rows = fetch_org_usage(cursor, billing_date)
        log.info(f"[Snowflake] {billing_date} — org usage: {len(rows)} rows")
    except Exception as e:
        log.warning(f"[Snowflake] org usage failed ({type(e).__name__}), falling back to metering history")

    # Fallback to metering history if org usage returned nothing
    if not rows:
        try:
            rows = fetch_metering(cursor, billing_date)
            log.info(f"[Snowflake] {billing_date} — metering fallback: {len(rows)} rows")
        except Exception as e:
            log.error(f"[Snowflake] metering fallback also failed: {type(e).__name__}")
            raise
    finally:
        cursor.close()

    if not rows:
        log.info(f"[Snowflake] {billing_date} — no data")
        return 0

    delete_existing(bq, billing_date)
    count = insert_rows(bq, rows)
    log.info(f"[Snowflake] {billing_date} — {count} rows inserted")
    return count


def main():
    parser = argparse.ArgumentParser(description="Snowflake billing → fact_cloud_costs")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--date",     help="Collect a specific date (YYYY-MM-DD)")
    group.add_argument("--backfill", type=int, metavar="N",
                       help=f"Backfill last N days (max {MAX_BACKFILL_DAYS})")
    args = parser.parse_args()

    sf_conn = connect()
    bq = bigquery.Client(project=TARGET_PROJECT)

    if args.date:
        try:
            parsed = date.fromisoformat(args.date)
        except ValueError:
            parser.error(f"Invalid date {args.date!r} — expected YYYY-MM-DD")
        today = datetime.now(timezone.utc).date()
        if parsed > today:
            parser.error("--date cannot be in the future")
        if parsed < date(2020, 1, 1):
            parser.error("--date is before 2020-01-01")
        dates = [parsed]
    elif args.backfill:
        if not (1 <= args.backfill <= MAX_BACKFILL_DAYS):
            parser.error(f"--backfill must be between 1 and {MAX_BACKFILL_DAYS}")
        today = datetime.now(timezone.utc).date()
        dates = [today - timedelta(days=i) for i in range(1, args.backfill + 1)]
    else:
        dates = [datetime.now(timezone.utc).date() - timedelta(days=1)]

    total = 0
    try:
        for d in dates:
            try:
                total += collect_for_date(sf_conn, bq, d)
            except Exception as e:
                log.error(f"[Snowflake] failed for {d}: {type(e).__name__}")
                raise
    finally:
        sf_conn.close()

    log.info(f"[Snowflake] done — {total} total rows across {len(dates)} day(s)")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
MongoDB Atlas Billing Collector
Reads from MongoDB Atlas Invoices API → writes to billing.fact_cloud_costs.

Source : MongoDB Atlas API v1.0 — /orgs/{orgId}/invoices (line items)
Target : kf-dev-ops-p001.billing.fact_cloud_costs

Strategy:
  1. Fetch the current pending invoice (this month's accumulating charges)
  2. Paginate through all closed invoices, include those whose period overlaps billing_date
  3. Filter all line items where startDate matches billing_date
  4. Each line item = one row in fact_cloud_costs

Usage:
    python mongodb_collector.py                   # collects yesterday
    python mongodb_collector.py --date 2026-02-25 # collects specific date
    python mongodb_collector.py --backfill 30     # backfills last N days (max 90)

Env vars (required):
    MONGODB_PUBLIC_KEY    Atlas API public key
    MONGODB_PRIVATE_KEY   Atlas API private key
    MONGODB_ORG_ID        Atlas organisation ID

Env vars (optional):
    MONGODB_ENVIRONMENT   default: unknown  (set 'production' or 'development')
    MONGODB_REGION        default: unknown
    TARGET_PROJECT        default: kf-dev-ops-p001
    TARGET_DATASET        default: billing
    TARGET_TABLE          default: fact_cloud_costs
    COLLECTOR_VERSION     default: 1.0.0
"""

import os
import re
import argparse
import logging
import time
from datetime import date, timedelta, datetime, timezone

import requests
from requests.auth import HTTPDigestAuth
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
# NOTE: MONGODB_PUBLIC_KEY and MONGODB_PRIVATE_KEY are intentionally NOT read
# at module level. They are read inside atlas_get() — the only function that
# uses them — to limit the window during which the raw credential strings exist
# as named Python objects accessible to tracebacks and debuggers.

ORG_ID  = os.environ["MONGODB_ORG_ID"]
MDB_ENV = os.environ.get("MONGODB_ENVIRONMENT", "unknown")
MDB_REGION = os.environ.get("MONGODB_REGION",   "unknown")

TARGET_PROJECT = os.environ.get("TARGET_PROJECT", "kf-dev-ops-p001")
TARGET_DATASET = os.environ.get("TARGET_DATASET", "billing")
TARGET_TABLE   = os.environ.get("TARGET_TABLE",   "fact_cloud_costs")
COLLECTOR_VER  = os.environ.get("COLLECTOR_VERSION", "1.0.0")

TARGET_FULL = f"`{TARGET_PROJECT}.{TARGET_DATASET}.{TARGET_TABLE}`"

BASE_URL  = "https://cloud.mongodb.com/api/atlas/v1.0"
HEADERS   = {"Accept": "application/json", "Content-Type": "application/json"}

_MAX_RETRIES   = 3
_RETRY_BACKOFF = 5   # seconds, doubles each attempt
_PAGE_SIZE     = 100

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

# ── Atlas API ─────────────────────────────────────────────────────────────────

def atlas_get(path: str, params: dict | None = None) -> dict | None:
    """GET from Atlas API with exponential backoff on 429/5xx."""
    # Read credentials here — not at module level — to avoid them persisting
    # as named globals accessible to tracebacks and exception handlers
    auth  = HTTPDigestAuth(
        os.environ["MONGODB_PUBLIC_KEY"],
        os.environ["MONGODB_PRIVATE_KEY"],
    )
    url   = f"{BASE_URL}{path}"
    delay = _RETRY_BACKOFF

    for attempt in range(1, _MAX_RETRIES + 1):
        resp = requests.get(url, auth=auth, headers=HEADERS, params=params, timeout=30)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code == 429 or resp.status_code >= 500:
            if attempt == _MAX_RETRIES:
                # Redact ORG_ID from path in logs — it is a Secret Manager secret
                safe_path = path.replace(ORG_ID, "***")
                log.error(f"[MongoDB] GET {safe_path} → {resp.status_code} after {_MAX_RETRIES} attempts")
                return None
            safe_path = path.replace(ORG_ID, "***")
            log.warning(f"[MongoDB] GET {safe_path} → {resp.status_code}, retrying in {delay}s")
            time.sleep(delay)
            delay *= 2
            continue

        # 4xx or unexpected status — log the Atlas error code only, never the response body
        safe_path = path.replace(ORG_ID, "***")
        try:
            err_json  = resp.json()
            err_code  = str(err_json.get('errorCode') or err_json.get('detail') or 'unknown_error')[:80]
        except Exception:
            err_code  = 'non-json-response'
        log.warning(f"[MongoDB] GET {safe_path} → {resp.status_code}: {err_code}")
        return None
    return None


def paginate_invoices() -> list[dict]:
    """
    Fetch all closed invoices from the Atlas invoices list API, paginating through
    all pages. Atlas uses pageNum (1-based) + itemsPerPage.
    """
    invoices = []
    page_num = 1
    while True:
        data = atlas_get(
            f"/orgs/{ORG_ID}/invoices",
            params={"pageNum": page_num, "itemsPerPage": _PAGE_SIZE},
        )
        if not data:
            break
        results = data.get("results", [])
        invoices.extend(results)
        # Stop when we've received fewer results than requested (last page)
        if len(results) < _PAGE_SIZE:
            break
        page_num += 1
    return invoices


def get_invoices_for_date(billing_date: date) -> list[dict]:
    """
    Return all invoices that may contain line items for billing_date.
    Covers: pending invoice + paginated closed invoices whose period overlaps billing_date.
    """
    invoices = []

    # Pending invoice (current month's accumulating charges)
    pending = atlas_get(f"/orgs/{ORG_ID}/invoices/pending")
    if pending:
        invoices.append(pending)

    # Closed invoices — paginate through all, filter by date range
    for inv in paginate_invoices():
        inv_id = inv.get("id", "")
        start  = inv.get("startDate", "")[:10]
        end    = inv.get("endDate",   "")[:10]
        if start <= billing_date.isoformat() <= end:
            detail = atlas_get(f"/orgs/{ORG_ID}/invoices/{inv_id}")
            if detail:
                invoices.append(detail)

    return invoices


def extract_line_items(invoices: list[dict], billing_date: date) -> list[dict]:
    """
    Pull all line items matching billing_date across all provided invoices.
    Deduplicates by (clusterName, sku, startDate).
    """
    date_str = billing_date.isoformat()
    seen     = set()
    items    = []

    for invoice in invoices:
        for item in invoice.get("lineItems", []):
            start = item.get("startDate", "")[:10]
            if start != date_str:
                continue

            key = (
                item.get("clusterName", ""),
                item.get("sku", ""),
                start,
            )
            if key in seen:
                continue
            seen.add(key)
            items.append(item)

    return items


# ── Transform ─────────────────────────────────────────────────────────────────

def to_rows(line_items: list[dict], billing_date: date) -> list[dict]:
    now      = datetime.now(timezone.utc).isoformat()
    date_str = billing_date.isoformat()
    rows     = []

    for item in line_items:
        cost_cents = float(item.get("totalPriceCents", 0))
        cost_usd   = round(cost_cents / 100, 6)

        if cost_usd == 0:
            continue

        cluster_name = item.get("clusterName") or item.get("sku", "unknown")
        sku          = item.get("sku", "")
        service_name = _friendly_service(sku)

        rows.append({
            "billing_date":      date_str,
            "provider":          "MongoDB",
            "account_id":        ORG_ID,
            "account_name":      item.get("groupName") or ORG_ID,
            "project_id":        item.get("groupId")   or ORG_ID,
            "service_name":      service_name,
            "sku":               sku,
            "sku_description":   item.get("note") or sku,
            "resource_id":       cluster_name,
            "resource_name":     cluster_name,
            "resource_type":     "Atlas Cluster",
            "cost":              cost_usd,
            "currency":          "USD",
            "original_cost":     cost_usd,
            "usage_amount":      float(item.get("quantity", 0) or 0),
            "usage_unit":        item.get("unit") or None,
            "team":              "unknown",
            "environment":       MDB_ENV,
            "region":            MDB_REGION,
            "tags":              None,
            "collected_at":      now,
            "processed_at":      now,
            "source_file":       "mongodb-atlas-invoices-api",
            "collector_version": COLLECTOR_VER,
        })

    return rows


def _friendly_service(sku: str) -> str:
    sku_upper = sku.upper()
    if "CLUSTER"    in sku_upper: return "Atlas Cluster"
    if "STORAGE"    in sku_upper: return "Storage"
    if "TRANSFER"   in sku_upper: return "Data Transfer"
    if "BACKUP"     in sku_upper: return "Backup"
    if "SEARCH"     in sku_upper: return "Atlas Search"
    if "SERVERLESS" in sku_upper: return "Serverless"
    if "STREAM"     in sku_upper: return "Atlas Stream"
    if "CHARTS"     in sku_upper: return "Charts"
    return sku.replace("_", " ").title() if sku else "MongoDB"


# ── BigQuery write ────────────────────────────────────────────────────────────

def delete_existing(bq: bigquery.Client, billing_date: date) -> None:
    sql = f"""
        DELETE FROM {TARGET_FULL}
        WHERE billing_date = @billing_date
          AND provider = 'MongoDB'
    """
    cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("billing_date", "DATE", billing_date.isoformat())
    ])
    bq.query(sql, cfg).result()


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

def collect_for_date(bq: bigquery.Client, billing_date: date) -> int:
    log.info(f"[MongoDB] collecting {billing_date} ...")

    invoices   = get_invoices_for_date(billing_date)
    line_items = extract_line_items(invoices, billing_date)

    if not line_items:
        log.info(f"[MongoDB] {billing_date} — no line items found")
        return 0

    rows = to_rows(line_items, billing_date)
    if not rows:
        log.info(f"[MongoDB] {billing_date} — all items were zero cost, skipping")
        return 0

    delete_existing(bq, billing_date)
    count = insert_rows(bq, rows)
    log.info(f"[MongoDB] {billing_date} — {count} rows inserted")
    return count


def main():
    parser = argparse.ArgumentParser(description="MongoDB billing → fact_cloud_costs")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--date",     help="Collect a specific date (YYYY-MM-DD)")
    group.add_argument("--backfill", type=int, metavar="N",
                       help=f"Backfill last N days (max {MAX_BACKFILL_DAYS})")
    args = parser.parse_args()

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
    for d in dates:
        try:
            total += collect_for_date(bq, d)
        except Exception as e:
            log.error(f"[MongoDB] failed for {d}: {type(e).__name__}")
            raise

    log.info(f"[MongoDB] done — {total} total rows across {len(dates)} day(s)")


if __name__ == "__main__":
    main()

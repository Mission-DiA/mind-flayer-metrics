#!/usr/bin/env python3
"""
MongoDB Atlas Billing Collector
Reads from MongoDB Atlas Invoices API → writes to billing.fact_cloud_costs.

Source : MongoDB Atlas API v1.0 — /orgs/{orgId}/invoices (line items)
Target : kf-dev-ops-p001.billing.fact_cloud_costs

Strategy:
  1. Fetch the current pending invoice (this month's accumulating charges)
  2. Also fetch the most recent closed invoice (covers last month)
  3. Filter all line items where startDate matches billing_date
  4. Each line item = one row in fact_cloud_costs

Usage:
    python mongodb_collector.py                   # collects yesterday
    python mongodb_collector.py --date 2026-02-25 # collects specific date
    python mongodb_collector.py --backfill 30     # backfills last N days

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
import argparse
import logging
from datetime import date, timedelta

import requests
from requests.auth import HTTPDigestAuth
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

PUBLIC_KEY  = os.environ["MONGODB_PUBLIC_KEY"]
PRIVATE_KEY = os.environ["MONGODB_PRIVATE_KEY"]
ORG_ID      = os.environ["MONGODB_ORG_ID"]
MDB_ENV     = os.environ.get("MONGODB_ENVIRONMENT", "unknown")
MDB_REGION  = os.environ.get("MONGODB_REGION",      "unknown")

TARGET_PROJECT = os.environ.get("TARGET_PROJECT", "kf-dev-ops-p001")
TARGET_DATASET = os.environ.get("TARGET_DATASET", "billing")
TARGET_TABLE   = os.environ.get("TARGET_TABLE",   "fact_cloud_costs")
COLLECTOR_VER  = os.environ.get("COLLECTOR_VERSION", "1.0.0")

TARGET_FULL = f"`{TARGET_PROJECT}.{TARGET_DATASET}.{TARGET_TABLE}`"

BASE_URL = "https://cloud.mongodb.com/api/atlas/v1.0"
AUTH     = HTTPDigestAuth(PUBLIC_KEY, PRIVATE_KEY)
HEADERS  = {"Accept": "application/json", "Content-Type": "application/json"}


# ── Atlas API ─────────────────────────────────────────────────────────────────

def atlas_get(path: str) -> dict | None:
    url = f"{BASE_URL}{path}"
    resp = requests.get(url, auth=AUTH, headers=HEADERS, timeout=30)
    if resp.status_code == 200:
        return resp.json()
    log.warning(f"[MongoDB] GET {path} → {resp.status_code}: {resp.text[:200]}")
    return None


def get_invoices_for_date(billing_date: date) -> list[dict]:
    """
    Return all invoices that may contain line items for billing_date.
    Covers: pending invoice + invoices from the same and previous month.
    """
    invoices = []

    # Pending invoice (current month's accumulating charges)
    pending = atlas_get(f"/orgs/{ORG_ID}/invoices/pending")
    if pending:
        invoices.append(pending)

    # Closed invoices — filter by date range covering billing_date's month
    # Atlas returns invoices sorted newest-first
    all_invoices = atlas_get(f"/orgs/{ORG_ID}/invoices")
    if all_invoices:
        for inv in all_invoices.get("results", []):
            inv_id = inv.get("id", "")
            start  = inv.get("startDate", "")[:10]
            end    = inv.get("endDate",   "")[:10]
            # Include invoice if its period overlaps billing_date
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
    now      = __import__("datetime").datetime.utcnow().isoformat() + "Z"
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
    bq.query(sql, job_config=cfg).result()


def insert_rows(bq: bigquery.Client, rows: list[dict]) -> int:
    if not rows:
        return 0
    target = f"{TARGET_PROJECT}.{TARGET_DATASET}.{TARGET_TABLE}"
    errors = bq.insert_rows_json(target, rows)
    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors}")
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
    group.add_argument("--backfill", type=int, metavar="N", help="Backfill last N days")
    args = parser.parse_args()

    bq = bigquery.Client(project=TARGET_PROJECT)

    if args.date:
        dates = [date.fromisoformat(args.date)]
    elif args.backfill:
        today = date.today()
        dates = [today - timedelta(days=i) for i in range(1, args.backfill + 1)]
    else:
        dates = [date.today() - timedelta(days=1)]

    total = 0
    for d in dates:
        try:
            total += collect_for_date(bq, d)
        except Exception as e:
            log.error(f"[MongoDB] failed for {d}: {e}")
            raise

    log.info(f"[MongoDB] done — {total} total rows across {len(dates)} day(s)")


if __name__ == "__main__":
    main()

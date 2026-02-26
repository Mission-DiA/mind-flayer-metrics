#!/usr/bin/env python3
"""
AWS Billing Collector
Reads from AWS Cost Explorer API → writes to billing.fact_cloud_costs.

Source : AWS Cost Explorer (boto3) — grouped by SERVICE + LINKED_ACCOUNT + REGION
Target : kf-dev-ops-p001.billing.fact_cloud_costs

Usage:
    python aws_collector.py                   # collects yesterday
    python aws_collector.py --date 2026-02-25 # collects specific date
    python aws_collector.py --backfill 30     # backfills last N days

Env vars (required):
    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY

Env vars (optional):
    AWS_REGION          default: us-east-1
    TARGET_PROJECT      default: kf-dev-ops-p001
    TARGET_DATASET      default: billing
    TARGET_TABLE        default: fact_cloud_costs
    COLLECTOR_VERSION   default: 1.0.0
"""

import os
import argparse
import logging
from datetime import date, timedelta

import boto3
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

AWS_ACCESS_KEY = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
AWS_REGION     = os.environ.get("AWS_REGION", "us-east-1")

TARGET_PROJECT = os.environ.get("TARGET_PROJECT", "kf-dev-ops-p001")
TARGET_DATASET = os.environ.get("TARGET_DATASET", "billing")
TARGET_TABLE   = os.environ.get("TARGET_TABLE",   "fact_cloud_costs")
COLLECTOR_VER  = os.environ.get("COLLECTOR_VERSION", "1.0.0")

TARGET_FULL = f"`{TARGET_PROJECT}.{TARGET_DATASET}.{TARGET_TABLE}`"


# ── AWS fetch ─────────────────────────────────────────────────────────────────

def fetch_daily_costs(ce_client, billing_date: date) -> list[dict]:
    """
    Fetch daily AWS costs from Cost Explorer grouped by SERVICE + LINKED_ACCOUNT.
    Returns a list of row dicts ready for BigQuery insertion.

    Note: Cost Explorer API end date is exclusive, so we add 1 day.
    """
    date_str  = billing_date.isoformat()
    next_date = (billing_date + timedelta(days=1)).isoformat()

    # Primary fetch: SERVICE + LINKED_ACCOUNT (max 2 GroupBy dimensions)
    response = ce_client.get_cost_and_usage(
        TimePeriod={"Start": date_str, "End": next_date},
        Granularity="DAILY",
        Metrics=["UnblendedCost", "UsageQuantity"],
        GroupBy=[
            {"Type": "DIMENSION", "Key": "SERVICE"},
            {"Type": "DIMENSION", "Key": "LINKED_ACCOUNT"},
        ],
    )

    rows = []
    now = __import__("datetime").datetime.utcnow().isoformat() + "Z"

    for result in response.get("ResultsByTime", []):
        for group in result.get("Groups", []):
            service_name = group["Keys"][0]
            account_id   = group["Keys"][1]
            cost         = float(group["Metrics"]["UnblendedCost"]["Amount"])
            usage_amount = float(group["Metrics"]["UsageQuantity"]["Amount"])
            usage_unit   = group["Metrics"]["UsageQuantity"].get("Unit", "")

            if cost == 0:
                continue

            rows.append({
                "billing_date":      date_str,
                "provider":          "AWS",
                "account_id":        account_id,
                "account_name":      None,   # enriched below if account map provided
                "project_id":        account_id,
                "service_name":      service_name,
                "sku":               None,
                "sku_description":   None,
                "resource_id":       None,
                "resource_name":     None,
                "resource_type":     None,
                "cost":              round(cost, 6),
                "currency":         "USD",
                "original_cost":     round(cost, 6),
                "usage_amount":      round(usage_amount, 6),
                "usage_unit":        usage_unit or None,
                "team":              "unknown",
                "environment":       "unknown",
                "region":            AWS_REGION,
                "tags":              None,
                "collected_at":      now,
                "processed_at":      now,
                "source_file":       "aws-cost-explorer",
                "collector_version": COLLECTOR_VER,
            })

    return rows


def fetch_tag_costs(ce_client, billing_date: date, tag_key: str) -> dict[tuple, str]:
    """
    Secondary fetch: get tag values per (service, account) for team/environment.
    Returns {(service, account): tag_value}.
    Cost Explorer only allows 2 GroupBy dimensions, so this is a separate call.
    """
    date_str  = billing_date.isoformat()
    next_date = (billing_date + timedelta(days=1)).isoformat()

    try:
        response = ce_client.get_cost_and_usage(
            TimePeriod={"Start": date_str, "End": next_date},
            Granularity="DAILY",
            Metrics=["UnblendedCost"],
            GroupBy=[
                {"Type": "DIMENSION", "Key": "SERVICE"},
                {"Type": "TAG",       "Key": tag_key},
            ],
        )
    except Exception as e:
        log.warning(f"[AWS] tag fetch for '{tag_key}' failed (tags may not be enabled): {e}")
        return {}

    mapping = {}
    for result in response.get("ResultsByTime", []):
        for group in result.get("Groups", []):
            service    = group["Keys"][0]
            tag_value  = group["Keys"][1].replace(f"{tag_key}$", "").lower().strip()
            if tag_value:
                mapping[service] = tag_value

    return mapping


# ── BigQuery write ────────────────────────────────────────────────────────────

def delete_existing(bq_client: bigquery.Client, billing_date: date) -> None:
    sql = f"""
        DELETE FROM {TARGET_FULL}
        WHERE billing_date = @billing_date
          AND provider = 'AWS'
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("billing_date", "DATE", billing_date.isoformat())
    ])
    bq_client.query(sql, job_config=job_config).result()


def insert_rows(bq_client: bigquery.Client, rows: list[dict]) -> int:
    if not rows:
        return 0
    target = f"{TARGET_PROJECT}.{TARGET_DATASET}.{TARGET_TABLE}"
    errors = bq_client.insert_rows_json(target, rows)
    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors}")
    return len(rows)


# ── Core ──────────────────────────────────────────────────────────────────────

def collect_for_date(
    ce_client: object,
    bq_client: bigquery.Client,
    billing_date: date,
) -> int:
    log.info(f"[AWS] collecting {billing_date} ...")

    rows = fetch_daily_costs(ce_client, billing_date)
    if not rows:
        log.info(f"[AWS] {billing_date} — no data")
        return 0

    # Enrich team/environment from cost allocation tags (best-effort)
    team_map = fetch_tag_costs(ce_client, billing_date, "team")
    env_map  = fetch_tag_costs(ce_client, billing_date, "environment")

    for row in rows:
        svc = row["service_name"]
        if svc in team_map:
            row["team"] = team_map[svc]
        if svc in env_map:
            row["environment"] = env_map[svc]

    # Idempotent: delete then insert
    delete_existing(bq_client, billing_date)
    count = insert_rows(bq_client, rows)
    log.info(f"[AWS] {billing_date} — {count} rows inserted")
    return count


def main():
    parser = argparse.ArgumentParser(description="AWS billing → fact_cloud_costs")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--date",     help="Collect a specific date (YYYY-MM-DD)")
    group.add_argument("--backfill", type=int, metavar="N", help="Backfill last N days")
    args = parser.parse_args()

    ce_client = boto3.client(
        "ce",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,
    )
    bq_client = bigquery.Client(project=TARGET_PROJECT)

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
            total += collect_for_date(ce_client, bq_client, d)
        except Exception as e:
            log.error(f"[AWS] failed for {d}: {e}")
            raise

    log.info(f"[AWS] done — {total} total rows across {len(dates)} day(s)")


if __name__ == "__main__":
    main()

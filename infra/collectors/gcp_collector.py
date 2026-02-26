#!/usr/bin/env python3
"""
GCP Billing Collector
Reads from GCP native BigQuery billing export → writes to billing.fact_cloud_costs.

Source : {SOURCE_PROJECT}.{SOURCE_DATASET}.{SOURCE_TABLE}  (GCP billing export)
Target : kf-dev-ops-p001.billing.fact_cloud_costs

Usage:
    python gcp_collector.py                  # collects yesterday
    python gcp_collector.py --date 2026-02-25  # collects specific date
    python gcp_collector.py --backfill 30    # backfills last N days

Env vars (required):
    SOURCE_PROJECT   GCP project that owns the billing export table
    SOURCE_DATASET   BigQuery dataset of the billing export
    SOURCE_TABLE     BigQuery table name of the billing export

Env vars (optional):
    TARGET_PROJECT   default: kf-dev-ops-p001
    TARGET_DATASET   default: billing
    TARGET_TABLE     default: fact_cloud_costs
    COLLECTOR_VERSION  default: 1.0.0
"""

import os
import argparse
import logging
from datetime import date, timedelta

from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

SOURCE_PROJECT = os.environ["SOURCE_PROJECT"]
SOURCE_DATASET = os.environ["SOURCE_DATASET"]
SOURCE_TABLE   = os.environ["SOURCE_TABLE"]

TARGET_PROJECT = os.environ.get("TARGET_PROJECT", "kf-dev-ops-p001")
TARGET_DATASET = os.environ.get("TARGET_DATASET", "billing")
TARGET_TABLE   = os.environ.get("TARGET_TABLE",   "fact_cloud_costs")
COLLECTOR_VER  = os.environ.get("COLLECTOR_VERSION", "1.0.0")

SOURCE_FULL = f"`{SOURCE_PROJECT}.{SOURCE_DATASET}.{SOURCE_TABLE}`"
TARGET_FULL = f"`{TARGET_PROJECT}.{TARGET_DATASET}.{TARGET_TABLE}`"


# ── SQL ───────────────────────────────────────────────────────────────────────

DELETE_SQL = f"""
DELETE FROM {TARGET_FULL}
WHERE billing_date = @billing_date
  AND provider = 'GCP'
"""

INSERT_SQL = f"""
INSERT INTO {TARGET_FULL} (
  billing_date, provider, account_id, account_name, project_id,
  service_name, sku, sku_description,
  resource_id, resource_name, resource_type,
  cost, currency, original_cost, usage_amount, usage_unit,
  team, environment, region, tags,
  collected_at, processed_at, source_file, collector_version
)
SELECT
  DATE(usage_start_time)                                              AS billing_date,
  'GCP'                                                               AS provider,
  project.id                                                          AS account_id,
  project.name                                                        AS account_name,
  project.id                                                          AS project_id,

  service.description                                                 AS service_name,
  sku.id                                                              AS sku,
  sku.description                                                     AS sku_description,

  resource.name                                                       AS resource_id,
  resource.global_name                                                AS resource_name,
  resource.type                                                       AS resource_type,

  -- Net cost = gross cost + credits (credits are negative amounts)
  ROUND(
    cost + IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) AS c), 0),
    6
  )                                                                   AS cost,
  currency,
  cost                                                                AS original_cost,
  usage.amount                                                        AS usage_amount,
  usage.unit                                                          AS usage_unit,

  -- Team: read from labels, fallback to 'unknown'
  LOWER(COALESCE(
    (SELECT value FROM UNNEST(labels) WHERE key = 'team'   LIMIT 1),
    (SELECT value FROM UNNEST(project.labels) WHERE key = 'team' LIMIT 1),
    'unknown'
  ))                                                                  AS team,

  -- Environment: read from labels, fallback to 'unknown'
  LOWER(COALESCE(
    (SELECT value FROM UNNEST(labels) WHERE key = 'environment' LIMIT 1),
    (SELECT value FROM UNNEST(labels) WHERE key = 'env'         LIMIT 1),
    (SELECT value FROM UNNEST(project.labels) WHERE key = 'environment' LIMIT 1),
    'unknown'
  ))                                                                  AS environment,

  location.region                                                     AS region,
  TO_JSON_STRING(labels)                                              AS tags,

  CURRENT_TIMESTAMP()                                                 AS collected_at,
  CURRENT_TIMESTAMP()                                                 AS processed_at,
  '{SOURCE_PROJECT}.{SOURCE_DATASET}.{SOURCE_TABLE}'                 AS source_file,
  @collector_version                                                  AS collector_version

FROM {SOURCE_FULL}
WHERE DATE(usage_start_time) = @billing_date
  AND cost != 0
"""


# ── Core ──────────────────────────────────────────────────────────────────────

def collect_for_date(client: bigquery.Client, billing_date: date) -> int:
    """
    ETL one day of GCP billing data into fact_cloud_costs.
    Idempotent: deletes existing rows for the date before inserting.
    Returns number of rows inserted.
    """
    date_str = billing_date.isoformat()
    params = [
        bigquery.ScalarQueryParameter("billing_date",      "DATE",   date_str),
        bigquery.ScalarQueryParameter("collector_version", "STRING", COLLECTOR_VER),
    ]
    job_config = bigquery.QueryJobConfig(query_parameters=params)

    log.info(f"[GCP] collecting {date_str} ...")

    # Step 1: delete existing rows for this date + provider
    client.query(DELETE_SQL, job_config=job_config).result()
    log.info(f"[GCP] {date_str} — existing rows cleared")

    # Step 2: insert fresh rows
    insert_job = client.query(INSERT_SQL, job_config=job_config)
    insert_job.result()

    rows_inserted = insert_job.num_dml_affected_rows or 0
    log.info(f"[GCP] {date_str} — {rows_inserted} rows inserted")
    return rows_inserted


def main():
    parser = argparse.ArgumentParser(description="GCP billing → fact_cloud_costs")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--date",     help="Collect a specific date (YYYY-MM-DD)")
    group.add_argument("--backfill", type=int, metavar="N", help="Backfill last N days")
    args = parser.parse_args()

    client = bigquery.Client(project=TARGET_PROJECT)

    if args.date:
        dates = [date.fromisoformat(args.date)]
    elif args.backfill:
        today = date.today()
        dates = [today - timedelta(days=i) for i in range(1, args.backfill + 1)]
    else:
        dates = [date.today() - timedelta(days=1)]  # default: yesterday

    total = 0
    for d in dates:
        try:
            total += collect_for_date(client, d)
        except Exception as e:
            log.error(f"[GCP] failed for {d}: {e}")
            raise

    log.info(f"[GCP] done — {total} total rows inserted across {len(dates)} day(s)")


if __name__ == "__main__":
    main()

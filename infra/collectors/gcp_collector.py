#!/usr/bin/env python3
"""
GCP Billing Collector
Reads from GCP native BigQuery billing export → writes to billing.fact_cloud_costs.

Source : {SOURCE_PROJECT}.{SOURCE_DATASET}.{SOURCE_TABLE}  (GCP billing export)
Target : kf-dev-ops-p001.billing.fact_cloud_costs

Usage:
    python gcp_collector.py                  # collects yesterday
    python gcp_collector.py --date 2026-02-25  # collects specific date
    python gcp_collector.py --backfill 30    # backfills last N days (max 90)

Env vars (required):
    SOURCE_PROJECT   GCP project that owns the billing export table
    SOURCE_DATASET   BigQuery dataset of the billing export
    SOURCE_TABLE     BigQuery table name of the billing export

Env vars (optional):
    TARGET_PROJECT   default: kf-dev-ops-p001
    TARGET_DATASET   default: billing
    TARGET_TABLE     default: fact_cloud_costs
    COLLECTOR_VERSION  default: 1.0.0
    MAX_BYTES_BILLED   default: 10 GB (cannot be disabled — always enforced)
"""

import os
import re
import argparse
import logging
from datetime import date, timedelta, datetime, timezone

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

# Always enforce a bytes-billed cap — cannot be disabled.
# Default: 10 GB per query job (~$0.05). Raise via env var only if justified.
MAX_BYTES_BILLED = max(1, int(os.environ.get("MAX_BYTES_BILLED", str(10 * 1024 ** 3))))

# Backfill safety cap — prevents runaway BQ spend from accidental large values
MAX_BACKFILL_DAYS = 90

# ── Identifier validation ─────────────────────────────────────────────────────
# Reject any env-var value that could break out of BigQuery backtick quoting
# and inject arbitrary SQL. Only alphanumerics, hyphens, underscores permitted.

_SAFE_ID = re.compile(r'^[a-zA-Z0-9_\-]+$')


def _check_ids(**ids: str) -> None:
    for name, value in ids.items():
        if not _SAFE_ID.match(value):
            raise ValueError(f"Unsafe BigQuery identifier {name}={value!r}")


_check_ids(
    SOURCE_PROJECT=SOURCE_PROJECT,
    SOURCE_DATASET=SOURCE_DATASET,
    SOURCE_TABLE=SOURCE_TABLE,
    TARGET_PROJECT=TARGET_PROJECT,
    TARGET_DATASET=TARGET_DATASET,
    TARGET_TABLE=TARGET_TABLE,
)

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
  PARSE_JSON(TO_JSON_STRING(labels))                                  AS tags,

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
    job_config = bigquery.QueryJobConfig(
        query_parameters=params,
        maximum_bytes_billed=MAX_BYTES_BILLED,  # always enforced — never None
    )

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
    group.add_argument("--backfill", type=int, metavar="N",
                       help=f"Backfill last N days (max {MAX_BACKFILL_DAYS})")
    args = parser.parse_args()

    client = bigquery.Client(project=TARGET_PROJECT)

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
            total += collect_for_date(client, d)
        except Exception as e:
            log.error(f"[GCP] failed for {d}: {type(e).__name__}")
            raise

    log.info(f"[GCP] done — {total} total rows inserted across {len(dates)} day(s)")


if __name__ == "__main__":
    main()

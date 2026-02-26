-- ============================================================
-- Billing Intelligence Platform — BigQuery Schema
-- Project  : kf-dev-ops-p001
-- Dataset  : billing
-- Run this in GCP Console → BigQuery → Query editor
-- ============================================================

-- Step 1: Create dataset (skip if already exists)
-- ============================================================
CREATE SCHEMA IF NOT EXISTS `kf-dev-ops-p001.billing`
OPTIONS (
  description = "Unified multi-cloud billing data warehouse",
  location    = "asia-south1"
);


-- Step 2: Create fact table
-- ============================================================
CREATE TABLE IF NOT EXISTS `kf-dev-ops-p001.billing.fact_cloud_costs` (

  -- ── Temporal (Partition Key) ────────────────────────────
  billing_date        DATE        NOT NULL,

  -- ── Provider ────────────────────────────────────────────
  provider            STRING      NOT NULL,   -- 'GCP' | 'AWS' | 'Snowflake' | 'MongoDB'
  account_id          STRING      NOT NULL,   -- GCP project ID / AWS account ID / etc.
  account_name        STRING,                 -- Human-readable account name
  project_id          STRING,                 -- GCP project or AWS account alias

  -- ── Service (Cluster Key) ───────────────────────────────
  service_name        STRING      NOT NULL,   -- 'Compute Engine' | 'EC2' | 'Warehouse' etc.
  sku                 STRING,                 -- SKU / pricing tier code
  sku_description     STRING,                 -- Human-readable SKU label

  -- ── Resource ────────────────────────────────────────────
  resource_id         STRING,                 -- Platform-specific resource ID
  resource_name       STRING,                 -- Human-readable resource name
  resource_type       STRING,                 -- Resource category

  -- ── Cost (Measures) ─────────────────────────────────────
  cost                FLOAT64     NOT NULL,   -- Cost in USD (normalised)
  currency            STRING      DEFAULT 'USD',
  original_cost       FLOAT64,               -- Cost in original currency before normalisation
  usage_amount        FLOAT64,               -- Quantity consumed (e.g. 100 GB, 730 hours)
  usage_unit          STRING,                -- Unit: 'hour' | 'GB' | 'requests' etc.

  -- ── Attribution ─────────────────────────────────────────
  team                STRING      NOT NULL,  -- Label: 'analytics' | 'platform' | 'unknown'
  environment         STRING      NOT NULL,  -- 'production' | 'staging' | 'development' | 'unknown'
  region              STRING,                -- 'us-central1' | 'us-east-1' etc.

  -- ── Raw Tags ────────────────────────────────────────────
  tags                JSON,                  -- Original platform labels (GCP) / tags (AWS)

  -- ── Audit ───────────────────────────────────────────────
  collected_at        TIMESTAMP   NOT NULL,  -- When the collector fetched the data
  processed_at        TIMESTAMP   NOT NULL,  -- When ETL loaded it into BigQuery
  source_file         STRING      NOT NULL,  -- Raw file path in Cloud Storage
  collector_version   STRING      NOT NULL   -- Collector script version

)
PARTITION BY billing_date
CLUSTER BY provider, account_id, service_name
OPTIONS (
  description               = "Unified multi-cloud billing data: GCP, AWS, Snowflake, MongoDB",
  partition_expiration_days = 1095,   -- 3 years retention
  require_partition_filter  = TRUE    -- Block full-table scans — all queries must filter by billing_date
);


-- Step 3: Daily summary materialized view (cached, $0 cost on repeat queries)
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS `kf-dev-ops-p001.billing.mv_daily_summary`
OPTIONS (
  enable_refresh       = TRUE,
  refresh_interval_minutes = 60
)
AS
SELECT
  billing_date,
  provider,
  team,
  environment,
  SUM(cost)                    AS total_cost,
  COUNT(DISTINCT resource_id)  AS resource_count,
  COUNT(DISTINCT account_id)   AS account_count
FROM `kf-dev-ops-p001.billing.fact_cloud_costs`
GROUP BY billing_date, provider, team, environment;


-- Step 4: Verify
-- ============================================================
-- After running Steps 1–3, confirm with:
--
--   SELECT table_name, row_count, size_bytes
--   FROM `kf-dev-ops-p001.billing.INFORMATION_SCHEMA.TABLES`;
--
-- Expected output:
--   fact_cloud_costs    0    0       ← empty until collectors run
--   mv_daily_summary    0    0

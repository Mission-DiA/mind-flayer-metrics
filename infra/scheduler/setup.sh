#!/usr/bin/env bash
# =============================================================================
# T8: Cloud Scheduler Setup — Billing Collectors
# Project  : kf-dev-ops-p001
# Region   : asia-south1
# SA       : kf-dev-jenkins-sa@kf-dev-ops-p001.iam.gserviceaccount.com
#            (pre-existing; already has run.admin, secretmanager.secretAccessor,
#             bigquery.jobUser, artifactregistry.admin)
#
# Schedule : Daily, staggered 15 min apart, Asia/Kolkata timezone
#   02:00 IST  →  GCP collector
#   02:15 IST  →  AWS collector
#   02:30 IST  →  Snowflake collector
#   02:45 IST  →  MongoDB collector
#
# Prerequisites (complete before running this script):
#
#   1. GCP admin grants ONE missing IAM role:
#        gcloud projects add-iam-policy-binding kf-dev-ops-p001 \
#          --member="serviceAccount:kf-dev-jenkins-sa@kf-dev-ops-p001.iam.gserviceaccount.com" \
#          --role="roles/bigquery.dataEditor"
#      (SA currently only has bigquery.dataViewer — collectors need dataEditor to write rows)
#
#   2. Create 10 Secret Manager secrets (you have secretmanager.admin, so you can do this):
#
#      Secret name                       Value
#      ─────────────────────────────────────────────────────────────────────────
#      billing-gcp-source-project        <GCP project owning the billing export>
#      billing-gcp-source-dataset        <BigQuery dataset of the billing export>
#      billing-gcp-source-table          <BigQuery table of the billing export>
#
#      billing-aws-access-key-id         <AWS_ACCESS_KEY_ID>
#      billing-aws-secret-access-key     <AWS_SECRET_ACCESS_KEY>
#
#      billing-snowflake-account         <e.g. xy12345.us-east-1>
#      billing-snowflake-user            <Snowflake username>
#      billing-snowflake-password        <Snowflake password>
#
#      billing-mongodb-public-key        <Atlas API public key>
#      billing-mongodb-private-key       <Atlas API private key>
#      billing-mongodb-org-id            <Atlas organisation ID>
#
#      To create a secret safely (value never appears in shell history):
#        read -rs -p "Enter value: " _v && echo
#        printf '%s' "${_v}" | gcloud secrets create SECRET_NAME \
#          --project=kf-dev-ops-p001 --data-file=- --replication-policy=automatic
#        unset _v
#
#      NEVER use: echo -n "VALUE" | gcloud secrets create ...
#      (echo writes the secret value into your shell history file permanently)
#
#   3. gcloud auth login && gcloud auth configure-docker asia-south1-docker.pkg.dev
#   4. Docker installed and running
#
# Usage:
#   chmod +x setup.sh && ./setup.sh
# =============================================================================
set -euo pipefail

# ── Variables ─────────────────────────────────────────────────────────────────

PROJECT="kf-dev-ops-p001"
REGION="asia-south1"
SA_EMAIL="kf-dev-jenkins-sa@kf-dev-ops-p001.iam.gserviceaccount.com"

AR_REPO="billing-collectors"
# Use a timestamp-based tag instead of :latest so every build is uniquely
# identifiable, rollback is possible, and audit trails are meaningful.
IMAGE_TAG="$(date +%Y%m%d%H%M%S)"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT}/${AR_REPO}/billing-collectors:${IMAGE_TAG}"

# Cloud Run Job settings
JOB_MEMORY="512Mi"
JOB_CPU="1"
JOB_MAX_RETRIES="2"
JOB_TIMEOUT="1800s"  # 30 min — sufficient for daily single-day collection

echo "============================================================"
echo "T8: Billing Collector Infrastructure Setup"
echo "Project : ${PROJECT} | Region : ${REGION}"
echo "SA      : ${SA_EMAIL}"
echo "Image   : ${IMAGE}"
echo "============================================================"
echo ""

# Guard: verify gcloud active project matches to prevent accidental cross-project deploys
CURRENT_PROJECT=$(gcloud config get-value project 2>/dev/null || echo "")
if [[ "${CURRENT_PROJECT}" != "${PROJECT}" ]]; then
  echo "ERROR: gcloud active project '${CURRENT_PROJECT}' does not match '${PROJECT}'"
  echo "       Run: gcloud config set project ${PROJECT}"
  exit 1
fi

# Interactive prompt — skipped in CI if SETUP_CONFIRMED=yes is set
if [[ -t 0 ]]; then
  read -p "Have you completed all prerequisites listed in this script's header? [y/N] " confirm
  [[ "${confirm}" =~ ^[Yy]$ ]] || { echo "Aborted. Complete prerequisites first."; exit 1; }
else
  [[ "${SETUP_CONFIRMED:-}" == "yes" ]] || {
    echo "ERROR: Non-interactive mode detected. Set SETUP_CONFIRMED=yes to proceed in CI."
    exit 1
  }
fi

# ── Step 1: Enable required APIs ──────────────────────────────────────────────
# SA has serviceusage.serviceUsageAdmin — can enable APIs

echo ""
echo "==> Step 1: Enabling required APIs..."
gcloud services enable \
  run.googleapis.com \
  cloudscheduler.googleapis.com \
  artifactregistry.googleapis.com \
  secretmanager.googleapis.com \
  --project="${PROJECT}" --quiet

# ── Step 2: Create Artifact Registry repository ───────────────────────────────
# SA has artifactregistry.admin

echo ""
echo "==> Step 2: Ensuring Artifact Registry repo exists..."
gcloud artifacts repositories describe "${AR_REPO}" \
  --location="${REGION}" --project="${PROJECT}" > /dev/null 2>&1 \
|| gcloud artifacts repositories create "${AR_REPO}" \
    --project="${PROJECT}" \
    --repository-format=docker \
    --location="${REGION}" \
    --description="Billing collector Docker images"

# ── Step 3: Build and push Docker image ───────────────────────────────────────
# SA has artifactregistry.admin — can push images

echo ""
echo "==> Step 3: Building and pushing Docker image..."
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COLLECTORS_DIR="$(dirname "${SCRIPT_DIR}")/collectors"

gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet
docker build -t "${IMAGE}" "${COLLECTORS_DIR}"
docker push "${IMAGE}"

# ── Step 4: Create Cloud Run Jobs ─────────────────────────────────────────────
# SA has run.admin — can create/update jobs

echo ""
echo "==> Step 4: Creating Cloud Run Jobs..."

# Helper — create if absent, update if exists
_job() {
  local name="$1"; shift
  echo "     Job: ${name}"
  if gcloud run jobs describe "${name}" \
       --region="${REGION}" --project="${PROJECT}" > /dev/null 2>&1; then
    gcloud run jobs update "${name}" \
      --region="${REGION}" --project="${PROJECT}" \
      --image="${IMAGE}" \
      --service-account="${SA_EMAIL}" \
      --max-retries="${JOB_MAX_RETRIES}" \
      --task-timeout="${JOB_TIMEOUT}" \
      --memory="${JOB_MEMORY}" \
      --cpu="${JOB_CPU}" \
      "$@"
  else
    gcloud run jobs create "${name}" \
      --region="${REGION}" --project="${PROJECT}" \
      --image="${IMAGE}" \
      --service-account="${SA_EMAIL}" \
      --max-retries="${JOB_MAX_RETRIES}" \
      --task-timeout="${JOB_TIMEOUT}" \
      --memory="${JOB_MEMORY}" \
      --cpu="${JOB_CPU}" \
      "$@"
  fi
}

# GCP: reads from GCP billing export BigQuery table → fact_cloud_costs
_job "billing-collector-gcp" \
  --args="gcp_collector.py" \
  --set-env-vars="TARGET_PROJECT=kf-dev-ops-p001,TARGET_DATASET=billing,TARGET_TABLE=fact_cloud_costs" \
  --set-secrets="SOURCE_PROJECT=billing-gcp-source-project:latest,SOURCE_DATASET=billing-gcp-source-dataset:latest,SOURCE_TABLE=billing-gcp-source-table:latest"

# AWS: reads from AWS Cost Explorer API → fact_cloud_costs
_job "billing-collector-aws" \
  --args="aws_collector.py" \
  --set-env-vars="TARGET_PROJECT=kf-dev-ops-p001,TARGET_DATASET=billing,TARGET_TABLE=fact_cloud_costs,AWS_REGION=us-east-1" \
  --set-secrets="AWS_ACCESS_KEY_ID=billing-aws-access-key-id:latest,AWS_SECRET_ACCESS_KEY=billing-aws-secret-access-key:latest"

# Snowflake: reads from SNOWFLAKE.ORGANIZATION_USAGE or ACCOUNT_USAGE → fact_cloud_costs
_job "billing-collector-snowflake" \
  --args="snowflake_collector.py" \
  --set-env-vars="TARGET_PROJECT=kf-dev-ops-p001,TARGET_DATASET=billing,TARGET_TABLE=fact_cloud_costs,SNOWFLAKE_ROLE=ACCOUNTADMIN,SNOWFLAKE_WAREHOUSE=COMPUTE_WH" \
  --set-secrets="SNOWFLAKE_ACCOUNT=billing-snowflake-account:latest,SNOWFLAKE_USER=billing-snowflake-user:latest,SNOWFLAKE_PASSWORD=billing-snowflake-password:latest"

# MongoDB: reads from Atlas Invoices API → fact_cloud_costs
_job "billing-collector-mongodb" \
  --args="mongodb_collector.py" \
  --set-env-vars="TARGET_PROJECT=kf-dev-ops-p001,TARGET_DATASET=billing,TARGET_TABLE=fact_cloud_costs" \
  --set-secrets="MONGODB_PUBLIC_KEY=billing-mongodb-public-key:latest,MONGODB_PRIVATE_KEY=billing-mongodb-private-key:latest,MONGODB_ORG_ID=billing-mongodb-org-id:latest"

# ── Step 5: Create Cloud Scheduler jobs ───────────────────────────────────────
# SA has run.admin — covers calling the Cloud Run Admin API to execute jobs
# Cloud Scheduler triggers via: POST /v2/projects/.../jobs/JOB_NAME:run (OAuth2)

echo ""
echo "==> Step 5: Creating Cloud Scheduler jobs..."

_schedule() {
  local name="$1"
  local cron="$2"
  local job="$3"
  local desc="$4"
  local uri="https://${REGION}-run.googleapis.com/v2/projects/${PROJECT}/locations/${REGION}/jobs/${job}:run"

  echo "     Schedule: ${name}  (${cron} IST)"
  if gcloud scheduler jobs describe "${name}" \
       --location="${REGION}" --project="${PROJECT}" > /dev/null 2>&1; then
    gcloud scheduler jobs update http "${name}" \
      --project="${PROJECT}" --location="${REGION}" \
      --schedule="${cron}" --time-zone="Asia/Kolkata" \
      --uri="${uri}" --message-body="{}" \
      --oauth-service-account-email="${SA_EMAIL}" \
      --oauth-token-scope="https://www.googleapis.com/auth/cloud-platform" \
      --description="${desc}"
  else
    gcloud scheduler jobs create http "${name}" \
      --project="${PROJECT}" --location="${REGION}" \
      --schedule="${cron}" --time-zone="Asia/Kolkata" \
      --uri="${uri}" --message-body="{}" \
      --oauth-service-account-email="${SA_EMAIL}" \
      --oauth-token-scope="https://www.googleapis.com/auth/cloud-platform" \
      --description="${desc}"
  fi
}

#  Name                          Cron (IST)    IST time    Job
#  ────────────────────────────  ────────────  ──────────  ─────────────────────────
_schedule "billing-schedule-gcp"       "0 2 * * *"  "billing-collector-gcp"       "Daily GCP billing collection (02:00 IST)"
_schedule "billing-schedule-aws"       "15 2 * * *" "billing-collector-aws"       "Daily AWS billing collection (02:15 IST)"
_schedule "billing-schedule-snowflake" "30 2 * * *" "billing-collector-snowflake" "Daily Snowflake billing collection (02:30 IST)"
_schedule "billing-schedule-mongodb"   "45 2 * * *" "billing-collector-mongodb"   "Daily MongoDB billing collection (02:45 IST)"

# ── Done ──────────────────────────────────────────────────────────────────────

echo ""
echo "============================================================"
echo "Setup complete!"
echo "============================================================"
echo ""
echo "Verify:"
echo "  Cloud Run Jobs  : https://console.cloud.google.com/run/jobs?project=${PROJECT}"
echo "  Cloud Scheduler : https://console.cloud.google.com/cloudscheduler?project=${PROJECT}"
echo ""
echo "Smoke-test (runs yesterday's date):"
echo "  gcloud run jobs execute billing-collector-gcp --region=${REGION} --project=${PROJECT}"
echo ""
echo "View logs:"
echo "  gcloud logging read 'resource.type=cloud_run_job' --project=${PROJECT} --limit=50"

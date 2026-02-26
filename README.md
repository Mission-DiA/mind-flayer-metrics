# mind-flayer-metrics

Billing Intelligence Platform for Kissflow — unified multi-cloud cost visibility across GCP, AWS, Snowflake, and MongoDB.

## Structure

```
mind-flayer-metrics/
├── backend/     FastAPI + MCP server — billing query engine (Cloud Run)
└── frontend/    Next.js 14 dashboard — MCP client + visual UI (Cloud Run)
```

## Architecture

```
Users (Finance + DevOps)
        │ HTTPS + Google IAP (@kissflow.com)
        ▼
frontend/   Next.js 14 (MCP Client)       Cloud Run — asia-south1
        │ MCP Protocol
        ▼
backend/    FastAPI + MCP + Query Engine  Cloud Run — asia-south1
        │                 │
   Gemini API         BigQuery
   (AI summaries)     billing.fact_cloud_costs (kf-dev-ops-p001)
```

## Services

| Service | Tech | Port | Deployment |
|---------|------|------|------------|
| Backend | FastAPI + Python 3.11 | 8000 | Cloud Run (min 1, max 10, 1 GB RAM) |
| Frontend | Next.js 14 | 3000 | Cloud Run (min 1, max 5, 512 MB RAM) |

## Prerequisites

- Python 3.11+
- Node.js 18+
- GCP project: `kf-dev-ops-p001`
- `gcloud auth application-default login` (local dev)

## Quick Start

```bash
# Backend
cd backend
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000

# Frontend
cd frontend
npm install
npm run dev
```

## Design Spec

See `shield-automation-specs/Kabilan-Custom billing dashboard.md` for full architecture, tool definitions, and SQL templates.

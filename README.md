# Codeace Intelligent Data Onboarding Platform

This project is a local-first implementation of the Codeace full-stack machine task. It lets a user upload a large e-commerce CSV, processes it asynchronously, stores isolated job data in DuckDB, streams progress over WebSockets, detects anomalies with IQR, and uses Ollama `qwen2.5:7b` for schema-aware dashboard queries and anomaly reporting.

## Architecture

```text
frontend/ React + Plotly
    |
    | REST + WebSocket
    v
backend/ FastAPI
    |
    | Celery task queue
    v
Redis  <---- job state, progress snapshots, anomaly payloads
    |
    v
Celery worker ----> DuckDB file: backend/data/ecommerce.duckdb
    |
    v
Ollama qwen2.5:7b for NL SQL plans and anomaly report text
```

## Why These Choices

- **FastAPI** keeps the API small, async-friendly, and easy to document.
- **Celery + Redis** moves CSV parsing, DuckDB insertion, and anomaly detection out of the request lifecycle, so large uploads do not lock the browser.
- **DuckDB** is a strong fit for local analytical workloads. It handles CSV-shaped data and aggregations without a separate database server.
- **`job_id` isolation** is implemented as a required `job_id` column on every event row. Each upload gets a UUID, and every dashboard query filters by that UUID.
- **WebSockets** stream job status snapshots from Redis so the frontend can show progress through upload, queueing, ingestion, anomaly detection, completion, and failure.
- **IQR anomaly detection** is deterministic and explainable. The app checks `price` with 1.5 * IQR bounds, then passes the anomaly summary and sample rows to Ollama for a concise business-facing report. Identifier fields such as `product_id` and `user_id` are intentionally excluded.
- **Anomaly report prompts** include up to 50 sampled anomaly rows with only crucial fields: `event_time`, `event_type`, `category_code`, `brand`, `price`, and `anomaly_value`. This gives the local LLM context while avoiding token waste on identifiers.
- **Ollama `qwen2.5:7b`** keeps AI local and account-free. The prompt includes the DuckDB schema and the current `job_id`, and the backend validates generated SQL before execution.

## Project Structure

```text
machine_task/
  backend/
    app/
      main.py                 FastAPI routes and WebSocket
      worker.py               Celery CSV ingestion task
      db.py                   DuckDB schema and connection helper
      state.py                Redis job/anomaly state helpers
      services/
        anomaly.py            IQR detection + LLM anomaly report
        nl_query.py           Prompt to SQL/chart planner
        ollama.py             Ollama HTTP client
    Dockerfile
    requirements.txt
  frontend/
    src/
      App.jsx                 Upload, dashboard, prompt, anomalies
      api.js
      styles.css
    package.json
  docker-compose.yml
```

## Prerequisites

- Python 3.11+
- Node.js 20+
- Docker Desktop for Redis and optional backend containers
- Ollama with the required model:

```bash
ollama pull qwen2.5:7b
ollama serve
```

## Run With Docker Compose

From `machine_task/`:

```bash
docker compose up --build
```

This starts Redis, the FastAPI API, and the Celery worker.

In another terminal, start the frontend:

```bash
cd frontend
npm install
npm run dev
```

Open:

```text
http://localhost:5173
```

## Run Manually

Start Redis:

```bash
docker run -d -p 6379:6379 redis:7-alpine
```

Start the backend API:

```bash
cd backend
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

Start the Celery worker in a second terminal:

```bash
cd backend
.venv\Scripts\activate
celery -A app.worker worker --loglevel=info --pool=solo
```

Start the frontend:

```bash
cd frontend
npm install
npm run dev
```

## API Overview

- `POST /api/uploads` uploads a CSV, creates a `job_id`, saves the file, and queues Celery processing.
- `GET /api/jobs/{job_id}` returns the latest job state.
- `WS /ws/jobs/{job_id}` streams progress snapshots.
- `GET /api/jobs/{job_id}/metrics` returns default dashboard metrics.
- `GET /api/jobs/{job_id}/events` returns server-side filtered and paginated rows for the data explorer. Supported filters include event type, brand, category, price range, and time range.
- `GET /api/jobs/{job_id}/anomalies` returns IQR anomaly rows and the LLM analysis report.
- `POST /api/jobs/{job_id}/ask` accepts `{ "prompt": "..." }`, asks Ollama for SQL and chart metadata, validates the SQL, runs it in DuckDB, and returns rows plus chart type.

## CSV Contract

The uploaded CSV must contain these columns:

```text
event_time,event_type,product_id,category_code,brand,price,user_id,user_session
```

The worker reads the file in chunks, normalizes types, adds `job_id`, and inserts into DuckDB. If a required column is missing, the job moves to `failed` and the error is visible in the UI.

During preprocessing:

- Blank or missing `category_code` values become `Unknown`.
- Blank or missing `brand` values become `Unknown`.
- Missing or invalid `price` values become `0`.
- The data explorer hides `user_session` to keep the matrix table focused on business-readable fields.

## Partial Failure Handling

- Upload failures are marked immediately in Redis.
- Worker exceptions mark the job as `failed` with the exception message.
- DuckDB rows for a retried `job_id` are deleted before reinsertion to avoid duplicate data.
- If Ollama is unavailable during anomaly reporting, deterministic IQR results still return and the report explains that AI text generation failed.
- If Ollama cannot generate a usable NL SQL plan, the backend falls back to a small set of safe heuristic queries.
- Generated SQL is rejected unless it is a `SELECT`, targets `events`, includes the active `job_id`, and avoids write/admin keywords.

## Trade-Offs

- Redis stores progress and anomaly JSON with a 24-hour TTL. For a production system, this would move to durable metadata tables.
- CSV upload is saved before Celery starts. For very large files, direct-to-object-storage upload plus resumable chunks would be stronger.
- DuckDB is excellent for local analytics, but concurrent writes should be serialized in production. This assessment worker is intended to run with limited concurrency.
- The NL query guard is intentionally conservative. Relative time prompts are anchored to the latest timestamp in the uploaded job because sample datasets are often historical. A production version would use a SQL parser such as `sqlglot`, stronger semantic validation, and query cost limits.
- Data explorer filtering is server-side for the core assessment fields. Sorting, saved dashboard cards, and filter presets would be natural next steps.

## Loom Walkthrough Outline

1. Upload CSV and show WebSocket progress.
2. Open dashboard metrics and charts once processing completes.
3. Ask a natural-language chart question.
4. Show generated SQL and rendered Plotly result.
5. Show anomaly panel with IQR counts, sample-backed report, and failure handling notes.

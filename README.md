# Codeace Intelligent Data Onboarding Platform

This project is a local-first implementation of the Codeace full-stack machine task. It lets a user upload a large e-commerce CSV, processes it asynchronously, stores isolated job data in DuckDB, streams progress over WebSockets, detects anomalies with IQR, and uses Ollama `qwen2.5:7b` for schema-aware dashboard queries and anomaly reporting.

---

## Features

### Interactive Dashboard
A fully interactive analytics dashboard renders automatically once CSV ingestion completes. It displays key business metrics — total events, revenue, top brands, and category breakdowns — as Plotly charts. All charts update in real time as new filters are applied, giving users an instant visual overview of their uploaded dataset.

### AI Assistant — Natural Language Queries
An integrated AI assistant lets users type plain-English questions about their data. The assistant translates each question into validated SQL using Ollama `qwen2.5:7b`, executes it against DuckDB, and returns the result in the most appropriate form — a bar chart, line chart, table, or scalar value — directly inside the dashboard. Users can ask questions like *"What are the top 10 brands by revenue?"* or *"Show me daily purchase trends"* without writing any SQL.

### AI-Integrated Anomaly Detection and Reporting
Price anomalies are detected automatically using IQR (Interquartile Range) statistical analysis after ingestion. Detected anomalies are then passed to the local LLM, which produces a concise, business-facing narrative report explaining the findings, their likely significance, and suggested next steps. Both the raw anomaly rows and the AI-generated report are available in the Anomalies panel of the dashboard.

### Automatic Data Flush After Ingestion
Uploaded CSV files are deleted from the `uploads/` directory immediately after the Celery worker finishes loading data into DuckDB. The raw file never persists on disk beyond ingestion — only the structured, queryable DuckDB representation is retained. This keeps storage footprint minimal without requiring any manual cleanup.

### Interactive Filters in the Data Explorer Table
The matrix table in the dashboard supports server-side filtering across multiple dimensions simultaneously. Users can filter rows by event type, brand, category code, price range, and time range. All filters are composable and applied server-side in DuckDB, so even large datasets remain fast and responsive.

### User Authentication with Token Expiry
The platform includes a user authentication system using JWT-based tokens. Tokens expire within 24 hours, requiring users to re-authenticate to maintain access. Passwords are stored as salted hashes and never transmitted or logged in plain text.

### User Account Management
Authenticated users can manage their accounts directly within the platform. This includes updating profile details, changing passwords, and viewing a history of their uploaded jobs. Administrators have additional controls to list, deactivate, or remove user accounts.

---

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

---

## Why These Choices

- **FastAPI** keeps the API small, async-friendly, and easy to document.
- **Celery + Redis** moves CSV parsing, DuckDB insertion, and anomaly detection out of the request lifecycle, so large uploads do not lock the browser.
- **DuckDB** is a strong fit for local analytical workloads. It handles CSV-shaped data and aggregations without a separate database server.
- **`job_id` isolation** is implemented as a required `job_id` column on every event row. Each upload gets a UUID, and every dashboard query filters by that UUID.
- **WebSockets** stream job status snapshots from Redis so the frontend can show progress through upload, queueing, ingestion, anomaly detection, completion, and failure.
- **IQR anomaly detection** is deterministic and explainable. The app checks `price` with 1.5 × IQR bounds, then passes the anomaly summary and sample rows to Ollama for a concise business-facing report. Identifier fields such as `product_id` and `user_id` are intentionally excluded.
- **Anomaly report prompts** include up to 50 sampled anomaly rows with only crucial fields: `event_time`, `event_type`, `category_code`, `brand`, `price`, and `anomaly_value`. This gives the local LLM context while avoiding token waste on identifiers.
- **Ollama `qwen2.5:7b`** keeps AI local and account-free. The prompt includes the DuckDB schema and the current `job_id`, and the backend validates generated SQL before execution.

---

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

---

## Prerequisites

- Python 3.11+
- Node.js 20+
- Docker Desktop for Redis and optional backend containers
- Ollama with the required model:

```bash
ollama pull qwen2.5:7b
ollama serve
```

---

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

```
http://localhost:5173
```

---

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

---

## API Overview

- `POST /api/uploads` — uploads a CSV, creates a `job_id`, saves the file, and queues Celery processing.
- `GET /api/jobs/{job_id}` — returns the latest job state.
- `WS /ws/jobs/{job_id}` — streams progress snapshots.
- `GET /api/jobs/{job_id}/metrics` — returns default dashboard metrics.
- `GET /api/jobs/{job_id}/events` — returns server-side filtered and paginated rows for the data explorer. Supported filters include event type, brand, category, price range, and time range.
- `GET /api/jobs/{job_id}/anomalies` — returns IQR anomaly rows and the LLM analysis report.
- `POST /api/jobs/{job_id}/ask` — accepts `{ "prompt": "..." }`, asks Ollama for SQL and chart metadata, validates the SQL, runs it in DuckDB, and returns rows plus chart type.

---

## CSV Contract

The uploaded CSV must contain these columns:

```
event_time,event_type,product_id,category_code,brand,price,user_id,user_session
```

The worker reads the file in chunks, normalizes types, adds `job_id`, and inserts into DuckDB. If a required column is missing, the job moves to `failed` and the error is visible in the UI.

During preprocessing:

- Blank or missing `category_code` values become `Unknown`.
- Blank or missing `brand` values become `Unknown`.
- Missing or invalid `price` values become `0`.
- The data explorer hides `user_session` to keep the matrix table focused on business-readable fields.

---

## Storage Efficiency

Uploaded CSV files are automatically deleted from the `uploads/` folder once the Celery worker finishes loading data into DuckDB. This means the raw file never lingers on disk after ingestion — only the structured, queryable DuckDB representation is retained. This keeps the storage footprint minimal by design, without any manual cleanup step required.

---

## Partial Failure Handling

- Upload failures are marked immediately in Redis.
- Worker exceptions mark the job as `failed` with the exception message.
- DuckDB rows for a retried `job_id` are deleted before reinsertion to avoid duplicate data.
- If Ollama is unavailable during anomaly reporting, deterministic IQR results still return and the report explains that AI text generation failed.
- If Ollama cannot generate a usable NL SQL plan, the backend falls back to a small set of safe heuristic queries.
- Generated SQL is rejected unless it is a `SELECT`, targets `events`, includes the active `job_id`, and avoids write/admin keywords.

---

## Future Enhancements

> **Note:** Given more time beyond the 24-hour machine task window, the following enhancements would be the next priorities.

### Microservices Architecture

Evolve the monolithic FastAPI backend into a set of independently deployable services behind an **API Gateway**:

- **API Gateway** — A single entry point (e.g., Kong or a custom FastAPI gateway) that handles routing, rate limiting, authentication verification, and request logging before forwarding traffic to downstream services. This removes cross-cutting concerns from individual services.
- **User Service** — An isolated service responsible for registration, login, JWT issuance, token refresh and revocation, password management, and account lifecycle (activation, deactivation, deletion). Backed by its own PostgreSQL instance so user data is never co-located with analytical workloads.
- **ETL Service** — A dedicated service that owns the full ingestion pipeline: CSV validation, chunked parsing, DuckDB or warehouse loading, anomaly detection, and status reporting over Redis. Decoupling ETL from the API means ingestion workers can scale horizontally without touching the query or auth layers.

### Authentication & Security

- **OAuth 2.0 / Social Login** — Replace the current username/password flow with OAuth providers (Google, GitHub, Microsoft) using an OpenID Connect library such as `authlib`. This removes credential management from the app entirely and improves security posture.

### Database

- **Migrate from DuckDB to PostgreSQL (or a cloud warehouse)** — DuckDB is an excellent fit for local analytical workloads. Moving to PostgreSQL (with `asyncpg`) or a managed cloud data warehouse (BigQuery, Snowflake, Redshift) would unlock multi-user concurrency, durable storage, row-level permissions, and horizontal scaling without changing the SQL interface significantly.

### AI & Analytics

- **Conversational Chatbot Assistant** — Evolve the current one-shot NL-to-SQL prompt into a stateful multi-turn chat assistant with conversation history, follow-up clarification, and context-aware suggestions. This would use a message thread per job stored in Redis or PostgreSQL.
- **Per-Chart AI Assistant** — Add an inline "Ask about this chart" button to every Plotly chart card. Clicking it would open a focused prompt panel pre-seeded with the chart's SQL and data context, letting users ask for explanations, comparisons, or drill-downs without leaving the chart.
- **Richer Insight Charts** — Add funnel charts for the view → cart → purchase conversion path, cohort retention heatmaps, time-of-day heatmaps, geographic maps if location data is present, and RFM (recency, frequency, monetary) scatter plots for customer segmentation.

---

## Loom Walkthrough Outline

1. Upload CSV and show WebSocket progress.
2. Open dashboard metrics and charts once processing completes.
3. Use interactive filters in the data explorer table.
4. Ask a natural-language chart question via the AI assistant.
5. Show generated SQL and rendered Plotly result.
6. Show anomaly panel with IQR counts, sample-backed AI report, and failure handling notes.
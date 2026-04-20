from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid4

import aiofiles
from fastapi import Depends, FastAPI, File, HTTPException, Query, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from duckdb import Error as DuckDBException

from app.auth import authenticate_ws_token, get_current_user
from app.config import get_settings
from app.db import duckdb_connection
from app.routers.auth import router as auth_router
from app.services.nl_query import answer_prompt
from app.state import get_anomaly_state, get_job_state, set_job_state
from app.userdb import ensure_user_schema
from app.worker import process_upload

settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    ensure_user_schema()
    yield


app = FastAPI(title=settings.app_name, lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth_router)


class PromptRequest(BaseModel):
    prompt: str


@app.get("/api/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/api/uploads")
async def upload_csv(
    file: UploadFile = File(...),
    _: dict = Depends(get_current_user),
) -> dict[str, str]:
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Please upload a CSV file.")

    job_id = str(uuid4())
    destination = settings.upload_dir / f"{job_id}.csv"
    set_job_state(job_id, status="uploading", stage="Saving upload", progress=1, filename=file.filename)

    try:
        async with aiofiles.open(destination, "wb") as out_file:
            while chunk := await file.read(1024 * 1024):
                await out_file.write(chunk)
    except Exception as exc:
        set_job_state(job_id, status="failed", stage="Upload failed", progress=100, error=str(exc))
        raise HTTPException(status_code=500, detail="Upload failed.") from exc

    set_job_state(job_id, status="queued", stage="Queued for processing", progress=3, file_path=str(destination))
    process_upload.delay(job_id, str(destination))
    return {"job_id": job_id, "status": "queued"}


@app.get("/api/jobs/{job_id}")
async def job_status(
    job_id: str,
    _: dict = Depends(get_current_user),
) -> dict:
    state = get_job_state(job_id)
    if not state:
        raise HTTPException(status_code=404, detail="Job not found.")
    return state


@app.websocket("/ws/jobs/{job_id}")
async def job_progress(
    websocket: WebSocket,
    job_id: str,
    token: str = Query(default=""),
) -> None:
    if not authenticate_ws_token(token):
        await websocket.close(code=4001)
        return
    await websocket.accept()
    previous = None
    try:
        while True:
            state = get_job_state(job_id) or {"job_id": job_id, "status": "unknown"}
            encoded = json.dumps(state, default=str)
            if encoded != previous:
                await websocket.send_text(encoded)
                previous = encoded
            if state.get("status") in {"completed", "failed"}:
                await asyncio.sleep(1)
            await asyncio.sleep(0.75)
    except WebSocketDisconnect:
        return


@app.get("/api/jobs/{job_id}/metrics")
async def metrics(
    job_id: str,
    _: dict = Depends(get_current_user),
) -> dict:
    require_completed_or_processing(job_id)
    with duckdb_connection() as conn:
        totals = conn.execute(
            """
            SELECT
                COUNT(*) AS total_events,
                COUNT(DISTINCT user_id) AS users,
                COUNT(DISTINCT user_session) AS sessions,
                ROUND(SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END), 2) AS revenue,
                COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchases
            FROM events
            WHERE job_id = ?
            """,
            [job_id],
        ).fetchdf().to_dict(orient="records")[0]
        date_range = conn.execute(
            """
            SELECT
                CAST(MIN(event_time) AS VARCHAR) AS min_event_time,
                CAST(MAX(event_time) AS VARCHAR) AS max_event_time
            FROM events
            WHERE job_id = ? AND event_time IS NOT NULL
            """,
            [job_id],
        ).fetchdf().to_dict(orient="records")[0]
        by_type = conn.execute(
            """
            SELECT event_type, COUNT(*) AS events
            FROM events
            WHERE job_id = ?
            GROUP BY event_type
            ORDER BY events DESC
            """,
            [job_id],
        ).fetchdf().to_dict(orient="records")
        top_brands = conn.execute(
            """
            SELECT
                COALESCE(brand, 'Unknown') AS brand,
                COUNT(*) AS events,
                COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchases,
                ROUND(SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END), 2) AS revenue
            FROM events
            WHERE job_id = ?
            GROUP BY brand
            ORDER BY revenue DESC, events DESC
            LIMIT 15
            """,
            [job_id],
        ).fetchdf().to_dict(orient="records")
        top_categories = conn.execute(
            """
            SELECT
                COALESCE(category_code, 'Unknown') AS category_code,
                COUNT(*) AS events,
                COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchases,
                ROUND(SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END), 2) AS revenue
            FROM events
            WHERE job_id = ?
            GROUP BY category_code
            ORDER BY revenue DESC, events DESC
            LIMIT 15
            """,
            [job_id],
        ).fetchdf().to_dict(orient="records")
        average_revenue_by_day = conn.execute(
            """
            WITH monthly_revenue AS (
                SELECT
                    DATE_TRUNC('month', event_time) AS month_start,
                    STRFTIME(event_time, '%Y-%m') AS event_month,
                    ROUND(SUM(price), 2) AS revenue,
                    COUNT(*) AS purchases
                FROM events
                WHERE job_id = ?
                  AND event_time IS NOT NULL
                  AND event_type = 'purchase'
                GROUP BY month_start, event_month
            )
            SELECT
                event_month,
                revenue,
                CAST(DATE_DIFF('day', month_start, month_start + INTERVAL 1 MONTH) AS INTEGER) AS days_in_month,
                ROUND(revenue / NULLIF(DATE_DIFF('day', month_start, month_start + INTERVAL 1 MONTH), 0), 2) AS average_daily_revenue,
                purchases
            FROM monthly_revenue
            ORDER BY event_month
            """,
            [job_id],
        ).fetchdf().to_dict(orient="records")
        conversion_by_brand = conn.execute(
            """
            SELECT
                COALESCE(brand, 'Unknown') AS brand,
                COUNT(*) FILTER (WHERE event_type = 'view') AS views,
                COUNT(*) FILTER (WHERE event_type = 'cart') AS carts,
                COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchases,
                ROUND(
                    100.0 * COUNT(*) FILTER (WHERE event_type = 'purchase')
                    / NULLIF(COUNT(*) FILTER (WHERE event_type = 'cart'), 0),
                    2
                ) AS cart_to_purchase_rate
            FROM events
            WHERE job_id = ?
            GROUP BY brand
            HAVING COUNT(*) FILTER (WHERE event_type = 'cart') > 0
            ORDER BY cart_to_purchase_rate DESC NULLS LAST, purchases DESC
            LIMIT 15
            """,
            [job_id],
        ).fetchdf().to_dict(orient="records")
    return {
        "totals": totals,
        "date_range": date_range,
        "by_type": by_type,
        "top_brands": top_brands,
        "top_categories": top_categories,
        "average_revenue_by_day": average_revenue_by_day,
        "conversion_by_brand": conversion_by_brand,
    }


@app.get("/api/jobs/{job_id}/events")
async def events(
    job_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
    event_type: str | None = None,
    brand: str | None = None,
    category: str | None = None,
    min_price: float | None = Query(None, ge=0),
    max_price: float | None = Query(None, ge=0),
    start_time: str | None = None,
    end_time: str | None = None,
    _: dict = Depends(get_current_user),
) -> dict:
    require_completed_or_processing(job_id)
    offset = (page - 1) * page_size
    filters = ["job_id = ?"]
    params: list[str | int | float] = [job_id]
    if event_type:
        filters.append("event_type = ?")
        params.append(event_type)
    if brand:
        filters.append("brand = ?")
        params.append(brand)
    if category:
        filters.append("category_code = ?")
        params.append(category)
    if min_price is not None:
        filters.append("price >= ?")
        params.append(min_price)
    if max_price is not None:
        filters.append("price <= ?")
        params.append(max_price)
    if start_time:
        filters.append("event_time >= CAST(? AS DATE)")
        params.append(start_time)
    if end_time:
        filters.append("event_time < CAST(? AS DATE) + INTERVAL 1 DAY")
        params.append(end_time)
    where = " AND ".join(filters)

    with duckdb_connection() as conn:
        date_range = conn.execute(
            """
            SELECT
                CAST(MIN(event_time) AS VARCHAR) AS min_event_time,
                CAST(MAX(event_time) AS VARCHAR) AS max_event_time
            FROM events
            WHERE job_id = ? AND event_time IS NOT NULL
            """,
            [job_id],
        ).fetchdf().to_dict(orient="records")[0]
        total = conn.execute(f"SELECT COUNT(*) FROM events WHERE {where}", params).fetchone()[0]
        rows = conn.execute(
            f"""
            SELECT event_time, event_type, product_id, category_code, brand, price, user_id
            FROM events
            WHERE {where}
            ORDER BY event_time DESC NULLS LAST
            LIMIT ? OFFSET ?
            """,
            [*params, page_size, offset],
        ).fetchdf().to_dict(orient="records")
        filter_options = conn.execute(
            """
            SELECT
                (
                    SELECT LIST(brand ORDER BY brand)
                    FROM (
                        SELECT COALESCE(brand, 'Unknown') AS brand
                        FROM events
                        WHERE job_id = ?
                        GROUP BY brand
                        ORDER BY COUNT(*) DESC
                        LIMIT 5000
                    )
                ) AS brands,
                (
                    SELECT LIST(category_code ORDER BY category_code)
                    FROM (
                        SELECT COALESCE(category_code, 'Unknown') AS category_code
                        FROM events
                        WHERE job_id = ?
                        GROUP BY category_code
                        ORDER BY COUNT(*) DESC
                    )
                ) AS categories
            """,
            [job_id, job_id],
        ).fetchdf().to_dict(orient="records")[0]
        brands = filter_options.get("brands")
        categories = filter_options.get("categories")
    total_pages = max(1, (total + page_size - 1) // page_size)
    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": total_pages,
        "date_range": date_range,
        "filter_options": {
            "brands": brands.tolist() if hasattr(brands, "tolist") else brands or [],
            "categories": categories.tolist() if hasattr(categories, "tolist") else categories or [],
        },
        "rows": rows,
    }


@app.get("/api/jobs/{job_id}/anomalies")
async def anomalies(
    job_id: str,
    _: dict = Depends(get_current_user),
) -> dict:
    state = get_anomaly_state(job_id)
    if not state:
        return {"job_id": job_id, "method": "IQR_1.5", "total_anomalies": 0, "columns": [], "report": "Anomaly results are not ready yet."}
    return state


@app.post("/api/jobs/{job_id}/ask")
async def ask(
    job_id: str,
    payload: PromptRequest,
    _: dict = Depends(get_current_user),
) -> dict:
    require_completed_or_processing(job_id)
    if not payload.prompt.strip():
        raise HTTPException(status_code=400, detail="Prompt cannot be empty.")
    try:
        with duckdb_connection() as conn:
            return answer_prompt(conn, job_id, payload.prompt)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except DuckDBException as exc:
        raise HTTPException(status_code=400, detail=f"Generated query could not run: {exc}") from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"AI assistant failed: {exc}") from exc


def require_completed_or_processing(job_id: str) -> None:
    state = get_job_state(job_id)
    if not state:
        raise HTTPException(status_code=404, detail="Job not found.")
    if state.get("status") == "failed":
        raise HTTPException(status_code=409, detail=state.get("error", "Job failed."))
    if state.get("status") not in {"processing", "analyzing", "completed"}:
        raise HTTPException(status_code=409, detail="Job is not queryable yet.")

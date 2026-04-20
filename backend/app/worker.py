"""
Celery background worker for CSV ingestion and anomaly detection.

The single task (process_upload) streams progress into Redis so the WebSocket
endpoint can relay it to the frontend in near-real-time. The uploaded file is
deleted after processing regardless of success or failure to keep disk usage low.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from celery import Celery

from app.config import get_settings
from app.db import EVENT_COLUMNS, duckdb_connection
from app.services.anomaly import detect_iqr_anomalies
from app.state import set_anomaly_state, set_job_state

settings = get_settings()
celery_app = Celery("codeace_worker", broker=settings.celery_broker_url, backend=settings.celery_result_backend)


@celery_app.task(name="app.worker.process_upload")
def process_upload(job_id: str, file_path: str) -> dict[str, Any]:
    """Ingest a CSV file into DuckDB and run IQR anomaly detection.

    Progress is written to Redis at each major stage so the WebSocket stream
    reflects real-time advancement from 5 % (counting) to 100 % (done).
    Any existing rows for the job_id are deleted before insertion so re-uploads
    of the same logical dataset replace rather than duplicate data.
    """
    path = Path(file_path)
    try:
        set_job_state(job_id, status="counting", stage="Counting rows", progress=5)
        total_rows = max(count_csv_rows(path), 1)
        inserted_rows = 0

        set_job_state(job_id, status="processing", stage="Loading CSV into DuckDB", progress=10, total_rows=total_rows)
        with duckdb_connection() as conn:
            conn.execute("DELETE FROM events WHERE job_id = ?", [job_id])
            for df in pd.read_csv(path, chunksize=settings.chunk_size):
                cleaned = normalize_chunk(df, job_id)
                conn.register("chunk_df", cleaned)
                conn.execute("INSERT INTO events SELECT * FROM chunk_df")
                conn.unregister("chunk_df")

                inserted_rows += len(cleaned)
                progress = min(85, 10 + int((inserted_rows / total_rows) * 70))
                set_job_state(
                    job_id,
                    status="processing",
                    stage="Loading CSV into DuckDB",
                    progress=progress,
                    rows_processed=inserted_rows,
                    total_rows=total_rows,
                )

            set_job_state(job_id, status="analyzing", stage="Running IQR anomaly detection", progress=88, rows_processed=inserted_rows)
            anomalies = detect_iqr_anomalies(conn, job_id)
            set_anomaly_state(job_id, anomalies)

        set_job_state(
            job_id,
            status="completed",
            stage="Ready",
            progress=100,
            rows_processed=inserted_rows,
            total_rows=total_rows,
            anomaly_count=anomalies["total_anomalies"],
        )
        return {"job_id": job_id, "rows_processed": inserted_rows}
    except Exception as exc:
        set_job_state(job_id, status="failed", stage="Failed", progress=100, error=str(exc))
        raise
    finally:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass


def count_csv_rows(path: Path) -> int:
    """Count CSV data rows by counting newlines minus the header line.

    Binary read avoids loading the full file into memory for large CSVs.
    """
    with path.open("rb") as handle:
        line_count = sum(1 for _ in handle)
    return max(line_count - 1, 0)


def normalize_chunk(df: pd.DataFrame, job_id: str) -> pd.DataFrame:
    """Validate and coerce a raw CSV chunk into the events table schema.

    Timestamps are parsed as UTC then stripped of timezone info because DuckDB
    stores TIMESTAMP without tz. Missing numeric values in price default to 0
    rather than NULL to keep aggregations simple.
    """
    missing = [column for column in EVENT_COLUMNS if column not in df.columns]
    if missing:
        raise ValueError(f"CSV is missing required columns: {', '.join(missing)}")

    cleaned = df[EVENT_COLUMNS].copy()
    cleaned["event_time"] = pd.to_datetime(cleaned["event_time"], errors="coerce", utc=True).dt.tz_localize(None)
    cleaned["product_id"] = pd.to_numeric(cleaned["product_id"], errors="coerce").astype("Int64")
    cleaned["price"] = pd.to_numeric(cleaned["price"], errors="coerce").fillna(0)
    cleaned["user_id"] = pd.to_numeric(cleaned["user_id"], errors="coerce").astype("Int64")
    cleaned["category_code"] = clean_text_column(cleaned["category_code"])
    cleaned["brand"] = clean_text_column(cleaned["brand"])
    # Prepend job_id so DuckDB can filter by job without a separate lookup table.
    cleaned.insert(0, "job_id", job_id)
    return cleaned


def clean_text_column(series: pd.Series) -> pd.Series:
    """Strip whitespace and replace empty/null strings with 'Unknown'."""
    cleaned = series.astype("string").str.strip()
    return cleaned.mask(cleaned.isna() | (cleaned == ""), "Unknown")

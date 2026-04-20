"""
DuckDB connection management and schema definition.

All events from every upload job are stored in a single 'events' table,
partitioned logically by job_id. Two indexes keep per-job filters fast
without requiring separate tables per upload.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

import duckdb

from app.config import get_settings


EVENT_COLUMNS = [
    "event_time",
    "event_type",
    "product_id",
    "category_code",
    "brand",
    "price",
    "user_id",
    "user_session",
]

# Only price is treated as a numeric signal for anomaly detection;
# product_id and user_id are identifiers and are excluded intentionally.
NUMERIC_COLUMNS = ["price"]


@contextmanager
def duckdb_connection() -> Iterator[duckdb.DuckDBPyConnection]:
    """Open a DuckDB connection, ensure the schema exists, and close on exit.

    Each call creates a fresh connection; DuckDB file-mode connections are
    not thread-safe for concurrent writes, so the Celery worker and FastAPI
    workers each open their own short-lived connections.
    """
    settings = get_settings()
    conn = duckdb.connect(str(settings.database_path))
    try:
        # Parallelise scans across 4 threads while staying single-writer.
        conn.execute("PRAGMA threads=4")
        ensure_schema(conn)
        yield conn
    finally:
        conn.close()


def ensure_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the events table and indexes if they do not already exist."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            job_id VARCHAR NOT NULL,
            event_time TIMESTAMP,
            event_type VARCHAR,
            product_id BIGINT,
            category_code VARCHAR,
            brand VARCHAR,
            price DOUBLE,
            user_id BIGINT,
            user_session VARCHAR
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_job_id ON events(job_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_job_type ON events(job_id, event_type)")

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

NUMERIC_COLUMNS = ["price"]


@contextmanager
def duckdb_connection() -> Iterator[duckdb.DuckDBPyConnection]:
    settings = get_settings()
    conn = duckdb.connect(str(settings.database_path))
    try:
        conn.execute("PRAGMA threads=4")
        ensure_schema(conn)
        yield conn
    finally:
        conn.close()


def ensure_schema(conn: duckdb.DuckDBPyConnection) -> None:
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

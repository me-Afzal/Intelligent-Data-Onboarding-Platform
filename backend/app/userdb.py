from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from typing import Iterator

from app.config import get_settings


@contextmanager
def user_db() -> Iterator[sqlite3.Connection]:
    db_path = get_settings().database_path.parent / "users.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def ensure_user_schema() -> None:
    with user_db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                username        TEXT    UNIQUE NOT NULL COLLATE NOCASE,
                email           TEXT    UNIQUE NOT NULL COLLATE NOCASE,
                hashed_password TEXT    NOT NULL,
                created_at      TEXT    DEFAULT (datetime('now'))
            )
            """
        )
        conn.commit()

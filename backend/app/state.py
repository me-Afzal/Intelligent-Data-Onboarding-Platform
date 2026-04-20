from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from redis import Redis

from app.config import get_settings


def redis_client() -> Redis:
    return Redis.from_url(get_settings().redis_url, decode_responses=True)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def job_key(job_id: str) -> str:
    return f"job:{job_id}"


def anomaly_key(job_id: str) -> str:
    return f"job:{job_id}:anomalies"


def set_job_state(job_id: str, **fields: Any) -> dict[str, Any]:
    client = redis_client()
    current = get_job_state(job_id) or {}
    current.update(fields)
    current["updated_at"] = utc_now()
    client.set(job_key(job_id), json.dumps(current, default=str), ex=60 * 60 * 24)
    return current


def get_job_state(job_id: str) -> dict[str, Any] | None:
    raw = redis_client().get(job_key(job_id))
    return json.loads(raw) if raw else None


def set_anomaly_state(job_id: str, payload: dict[str, Any]) -> None:
    redis_client().set(anomaly_key(job_id), json.dumps(payload, default=str), ex=60 * 60 * 24)


def get_anomaly_state(job_id: str) -> dict[str, Any] | None:
    raw = redis_client().get(anomaly_key(job_id))
    return json.loads(raw) if raw else None

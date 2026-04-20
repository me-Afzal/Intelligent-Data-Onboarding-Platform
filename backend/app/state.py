"""
Redis-backed job and anomaly state management.

Job state is a JSON blob stored under 'job:<id>' with a 24-hour TTL.
Anomaly results use a separate key 'job:<id>:anomalies' so they can be
written once by the worker and read independently by the API without
touching the main job state blob.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from redis import Redis

from app.config import get_settings


def redis_client() -> Redis:
    """Create a new Redis connection from the configured URL with string decoding enabled."""
    return Redis.from_url(get_settings().redis_url, decode_responses=True)


def utc_now() -> str:
    """Return the current UTC time as an ISO-8601 string for timestamping state updates."""
    return datetime.now(timezone.utc).isoformat()


def job_key(job_id: str) -> str:
    """Return the Redis key used to store the main job state blob."""
    return f"job:{job_id}"


def anomaly_key(job_id: str) -> str:
    """Return the Redis key used to store anomaly detection results for a job."""
    return f"job:{job_id}:anomalies"


def set_job_state(job_id: str, **fields: Any) -> dict[str, Any]:
    """Merge the given fields into the existing job state and persist it.

    Reads the current blob first so partial updates don't overwrite unrelated
    fields written by other parts of the pipeline.
    """
    client = redis_client()
    current = get_job_state(job_id) or {}
    current.update(fields)
    current["updated_at"] = utc_now()
    # TTL of 24 hours matches the JWT expiry so sessions stay consistent.
    client.set(job_key(job_id), json.dumps(current, default=str), ex=60 * 60 * 24)
    return current


def get_job_state(job_id: str) -> dict[str, Any] | None:
    """Fetch and deserialise the current job state from Redis, or None if not found."""
    raw = redis_client().get(job_key(job_id))
    return json.loads(raw) if raw else None


def set_anomaly_state(job_id: str, payload: dict[str, Any]) -> None:
    """Persist the anomaly detection result payload to Redis with a 24-hour TTL."""
    redis_client().set(anomaly_key(job_id), json.dumps(payload, default=str), ex=60 * 60 * 24)


def get_anomaly_state(job_id: str) -> dict[str, Any] | None:
    """Fetch and deserialise anomaly results from Redis, or None if not yet written."""
    raw = redis_client().get(anomaly_key(job_id))
    return json.loads(raw) if raw else None

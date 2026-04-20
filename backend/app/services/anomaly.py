"""
IQR-based anomaly detection for numeric event columns.

Uses the standard 1.5 × IQR fence: values below Q1 − 1.5·IQR or above
Q3 + 1.5·IQR are flagged. Only 'price' is checked; identifier columns
(product_id, user_id) are excluded because large ID values aren't anomalies.

After detection, an AI narrative report is generated via Ollama. If Ollama is
unavailable the numeric results are still returned with a fallback message.
"""

from __future__ import annotations

import json
from typing import Any

import duckdb

from app.db import NUMERIC_COLUMNS
from app.services.ollama import OllamaError, generate_text


def detect_iqr_anomalies(conn: duckdb.DuckDBPyConnection, job_id: str) -> dict[str, Any]:
    """Run IQR anomaly detection on all NUMERIC_COLUMNS for the given job.

    Returns a dict with total_anomaly count, per-column stats (bounds, sample rows),
    and an AI-generated report string.
    """
    columns: list[dict[str, Any]] = []
    total_anomalies = 0

    for column in NUMERIC_COLUMNS:
        stats = conn.execute(
            f"""
            SELECT
                quantile_cont({column}, 0.25) AS q1,
                quantile_cont({column}, 0.75) AS q3,
                COUNT({column}) AS non_null_count
            FROM events
            WHERE job_id = ? AND {column} IS NOT NULL
            """,
            [job_id],
        ).fetchone()

        if not stats or stats[0] is None or stats[1] is None:
            continue

        q1, q3, non_null_count = stats
        iqr = q3 - q1
        lower = q1 - (1.5 * iqr)
        upper = q3 + (1.5 * iqr)

        count = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM events
            WHERE job_id = ?
              AND {column} IS NOT NULL
              AND ({column} < ? OR {column} > ?)
            """,
            [job_id, lower, upper],
        ).fetchone()[0]

        # Order sample rows by distance from the median so the most extreme
        # outliers appear first in the AI prompt and the anomaly panel.
        sample = conn.execute(
            f"""
            SELECT event_time, event_type, category_code, brand, price, {column} AS anomaly_value
            FROM events
            WHERE job_id = ?
              AND {column} IS NOT NULL
              AND ({column} < ? OR {column} > ?)
            ORDER BY ABS({column} - ?) DESC
            LIMIT 50
            """,
            [job_id, lower, upper, (q1 + q3) / 2],
        ).fetchdf().to_dict(orient="records")

        total_anomalies += int(count)
        columns.append(
            {
                "column": column,
                "q1": q1,
                "q3": q3,
                "iqr": iqr,
                "lower_bound": lower,
                "upper_bound": upper,
                "non_null_count": non_null_count,
                "anomaly_count": int(count),
                "sample_rows": sample,
            }
        )

    report = build_anomaly_report(job_id, columns, total_anomalies)
    return {"job_id": job_id, "method": "IQR_1.5", "total_anomalies": total_anomalies, "columns": columns, "report": report}


def build_anomaly_report(job_id: str, columns: list[dict[str, Any]], total_anomalies: int) -> str:
    """Generate a plain-text business narrative for the anomaly findings via Ollama.

    Passes a trimmed JSON summary (bounds, counts, up to 50 sample rows per column)
    to the LLM so it can produce concrete observations without inventing data.
    Returns a fallback string if Ollama is unreachable so the API response is
    always complete even without the AI layer.
    """
    summary = [
        {
            "column": item["column"],
            "bounds": [item["lower_bound"], item["upper_bound"]],
            "anomaly_count": item["anomaly_count"],
            "sample_rows": [trim_anomaly_row(row) for row in item["sample_rows"][:50]],
        }
        for item in columns
    ]
    summary_json = json.dumps(summary, default=str)
    prompt = f"""
You are analyzing e-commerce behavioral event data for upload job {job_id}.
IQR anomaly detection was run on numeric columns using 1.5 * IQR bounds.
Only price is checked. Identifier fields such as product_id and user_id are excluded because large ID values are not business anomalies.
You have up to 50 sampled anomaly rows with only crucial fields: event_time, event_type, category_code, brand, price, and anomaly_value.

Return a concise business-facing report with:
1. What looks unusual.
2. Why it may matter.
3. Practical next checks.
Use short section headings and plain text. Do not use markdown syntax.
Mention concrete patterns from the sampled rows when useful, such as event type, brand, category, price magnitude, or timing.

Do not invent facts beyond this JSON summary and sampled anomaly rows:
{summary_json}

Total anomaly rows across column checks: {total_anomalies}
"""
    try:
        return generate_text(prompt, system="You are a careful data analyst. Be concise and specific.", temperature=0.2)
    except OllamaError as exc:
        return (
            "AI report unavailable because Ollama could not be reached. "
            f"IQR detection still completed and found {total_anomalies} column-level anomaly matches. Error: {exc}"
        )


def trim_anomaly_row(row: dict[str, Any]) -> dict[str, Any]:
    """Strip a raw anomaly row down to the fields relevant for the AI prompt.

    Excludes product_id, user_id, and user_session because they add noise
    without helping the LLM form business-meaningful observations.
    """
    return {
        "event_time": row.get("event_time"),
        "event_type": row.get("event_type"),
        "category_code": row.get("category_code"),
        "brand": row.get("brand"),
        "price": row.get("price"),
        "anomaly_value": row.get("anomaly_value"),
    }

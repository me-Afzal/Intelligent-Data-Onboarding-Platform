from __future__ import annotations

import re
from typing import Any

import duckdb
import pandas as pd

from app.config import get_settings
from app.services.ollama import OllamaError, generate_json


BLOCKED_SQL = re.compile(r"\b(insert|update|delete|drop|alter|create|copy|attach|detach|pragma|call)\b", re.IGNORECASE)


def answer_prompt(conn: duckdb.DuckDBPyConnection, job_id: str, prompt: str) -> dict[str, Any]:
    plan = create_query_plan(job_id, prompt)
    try:
        if should_use_fallback(prompt, plan.get("sql", "")):
            plan = fallback_plan(job_id, prompt)
        sql = validate_sql(plan["sql"], job_id)
    except ValueError:
        plan = fallback_plan(job_id, prompt)
        sql = validate_sql(plan["sql"], job_id)
    df = conn.execute(sql).fetchdf()
    df = df.where(pd.notnull(df), None)
    rows = [{key: make_json_safe(value) for key, value in row.items()} for row in df.to_dict(orient="records")]
    return {
        "prompt": prompt,
        "sql": sql,
        "chart_type": plan.get("chart_type", "table"),
        "title": plan.get("title", "Query result"),
        "x": plan.get("x"),
        "y": plan.get("y"),
        "columns": list(df.columns),
        "rows": rows,
    }


def create_query_plan(job_id: str, user_prompt: str) -> dict[str, Any]:
    settings = get_settings()
    schema = """
Table: events
Required isolation filter: job_id = '<job_id>'
Columns:
- job_id VARCHAR
- event_time TIMESTAMP
- event_type VARCHAR values usually view/cart/purchase
- product_id BIGINT
- category_code VARCHAR
- brand VARCHAR nullable
- price DOUBLE
- user_id BIGINT
- user_session VARCHAR
"""
    instruction = f"""
Convert the user question into a DuckDB SELECT query over the schema.
Always include WHERE job_id = '{job_id}' or combine it with other filters.
Never use write statements. Keep result rows under {settings.max_query_rows}.
Return only JSON with keys: sql, chart_type, title, x, y.
chart_type must be one of: bar, line, pie, scatter, table, metric.
Use simple column aliases that are valid JSON keys.
Revenue means purchase revenue only: SUM(price) where event_type = 'purchase'.
For relative periods like this week, last week, this month, or last month, anchor the date math to the latest event_time in this job, not CURRENT_DATE.
Examples:
- "this week" means event_time >= DATE_TRUNC('week', (SELECT MAX(event_time) FROM events WHERE job_id = '{job_id}')) and event_time < DATE_TRUNC('week', (SELECT MAX(event_time) FROM events WHERE job_id = '{job_id}')) + INTERVAL 1 WEEK.
- "this month" means DATE_TRUNC('month', latest job event_time), not week and not CURRENT_DATE.
- "last week" means the full week before DATE_TRUNC('week', latest job event_time).

Schema:
{schema}

User question: {user_prompt}
"""
    try:
        plan = generate_json(instruction, system="You generate safe DuckDB SQL for an analytics dashboard.")
    except OllamaError:
        plan = fallback_plan(job_id, user_prompt)

    if not isinstance(plan, dict) or "sql" not in plan:
        plan = fallback_plan(job_id, user_prompt)
    return plan


def fallback_plan(job_id: str, user_prompt: str) -> dict[str, str | None]:
    normalized = user_prompt.lower()
    asks_revenue = any(word in normalized for word in ["revenue", "earned", "sales", "income"])
    asks_week = "week" in normalized
    if asks_revenue and asks_week:
        return {
            "sql": f"""
                WITH latest AS (
                    SELECT DATE_TRUNC('week', MAX(event_time)) AS week_start
                    FROM events
                    WHERE job_id = '{job_id}'
                )
                SELECT
                    ROUND(COALESCE(SUM(price), 0), 2) AS total_revenue,
                    COUNT(*) AS purchases,
                    MIN(event_time) AS first_purchase_at,
                    MAX(event_time) AS last_purchase_at
                FROM events, latest
                WHERE job_id = '{job_id}'
                  AND event_type = 'purchase'
                  AND event_time >= latest.week_start
                  AND event_time < latest.week_start + INTERVAL 1 WEEK
                LIMIT 1
            """,
            "chart_type": "metric",
            "title": "Revenue this week",
            "x": None,
            "y": "total_revenue",
        }
    if asks_revenue and ("today" in normalized or "day" in normalized):
        return {
            "sql": f"""
                WITH latest AS (
                    SELECT DATE_TRUNC('day', MAX(event_time)) AS day_start
                    FROM events
                    WHERE job_id = '{job_id}'
                )
                SELECT ROUND(COALESCE(SUM(price), 0), 2) AS total_revenue, COUNT(*) AS purchases
                FROM events, latest
                WHERE job_id = '{job_id}'
                  AND event_type = 'purchase'
                  AND event_time >= latest.day_start
                  AND event_time < latest.day_start + INTERVAL 1 DAY
                LIMIT 1
            """,
            "chart_type": "metric",
            "title": "Revenue today",
            "x": None,
            "y": "total_revenue",
        }
    if "brand" in normalized and ("purchase" in normalized or "revenue" in normalized):
        return {
            "sql": f"""
                SELECT COALESCE(brand, 'Unknown') AS brand,
                       COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchases,
                       ROUND(SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END), 2) AS revenue
                FROM events
                WHERE job_id = '{job_id}'
                GROUP BY brand
                ORDER BY revenue DESC
                LIMIT 25
            """,
            "chart_type": "bar",
            "title": "Purchases and revenue by brand",
            "x": "brand",
            "y": "revenue",
        }
    if "category" in normalized:
        return {
            "sql": f"""
                SELECT COALESCE(category_code, 'Unknown') AS category_code, COUNT(*) AS events
                FROM events
                WHERE job_id = '{job_id}'
                GROUP BY category_code
                ORDER BY events DESC
                LIMIT 25
            """,
            "chart_type": "bar",
            "title": "Top categories by events",
            "x": "category_code",
            "y": "events",
        }
    return {
        "sql": f"""
            SELECT event_type, COUNT(*) AS events
            FROM events
            WHERE job_id = '{job_id}'
            GROUP BY event_type
            ORDER BY events DESC
            LIMIT 25
        """,
        "chart_type": "bar",
        "title": "Events by type",
        "x": "event_type",
        "y": "events",
    }


def should_use_fallback(user_prompt: str, sql: str) -> bool:
    normalized = user_prompt.lower()
    lowered_sql = sql.lower()
    if "week" in normalized and "date_trunc('month'" in lowered_sql:
        return True
    if any(word in normalized for word in ["this week", "last week", "this month", "last month", "today"]) and "current_date" in lowered_sql:
        return True
    if any(word in normalized for word in ["revenue", "earned", "sales", "income"]) and "event_type = 'purchase'" not in lowered_sql and 'event_type = "purchase"' not in lowered_sql:
        return True
    return False


def validate_sql(sql: str, job_id: str) -> str:
    compact = sql.strip().rstrip(";")
    if not (compact.lower().startswith("select") or compact.lower().startswith("with")):
        raise ValueError("Only SELECT queries are allowed.")
    if BLOCKED_SQL.search(compact):
        raise ValueError("Generated SQL contains a blocked statement.")
    if "events" not in compact.lower():
        raise ValueError("Generated SQL must query the events table.")
    if job_id not in compact:
        raise ValueError("Generated SQL must include the current job_id isolation filter.")
    if "limit" not in compact.lower():
        compact = f"{compact} LIMIT {get_settings().max_query_rows}"
    return compact


def make_json_safe(value: Any) -> Any:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value

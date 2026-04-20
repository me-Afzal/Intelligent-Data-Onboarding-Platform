from __future__ import annotations

import json
from typing import Any

import requests

from app.config import get_settings


class OllamaError(RuntimeError):
    pass


def generate_text(prompt: str, system: str | None = None, temperature: float = 0.1) -> str:
    settings = get_settings()
    payload: dict[str, Any] = {
        "model": settings.ollama_model,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": temperature},
    }
    if system:
        payload["system"] = system

    try:
        response = requests.post(f"{settings.ollama_base_url}/api/generate", json=payload, timeout=120)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise OllamaError(str(exc)) from exc

    data = response.json()
    return str(data.get("response", "")).strip()


def generate_json(prompt: str, system: str | None = None) -> dict[str, Any]:
    text = generate_text(prompt, system=system)
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`")
        cleaned = cleaned.replace("json\n", "", 1).replace("JSON\n", "", 1)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError as exc:
        raise OllamaError(f"Ollama returned non-JSON output: {text[:400]}") from exc

"""
JWT authentication helpers and FastAPI dependency injection for route protection.

Tokens are HS256-signed JWTs with a 24-hour expiry. Each token embeds the
user's numeric ID and username so routes can read them without a DB round-trip,
but get_current_user still verifies the user still exists in SQLite.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from passlib.context import CryptContext

from app.config import get_settings
from app.userdb import user_db

ALGORITHM = "HS256"
TOKEN_EXPIRE_HOURS = 24

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
_bearer = HTTPBearer(auto_error=False)


def hash_password(password: str) -> str:
    """Return a bcrypt hash of the given plaintext password."""
    return pwd_context.hash(password)


def verify_password(plain: str, hashed: str) -> bool:
    """Return True if plain matches the stored bcrypt hash."""
    return pwd_context.verify(plain, hashed)


def create_token(user_id: int, username: str) -> str:
    """Mint a signed JWT for the given user, valid for TOKEN_EXPIRE_HOURS."""
    payload = {
        "sub": str(user_id),
        "username": username,
        "exp": datetime.now(timezone.utc) + timedelta(hours=TOKEN_EXPIRE_HOURS),
    }
    return jwt.encode(payload, get_settings().secret_key, algorithm=ALGORITHM)


def _decode(token: str) -> dict[str, Any]:
    """Decode and verify a JWT, raising 401 on any failure."""
    try:
        return jwt.decode(token, get_settings().secret_key, algorithms=[ALGORITHM])
    except JWTError as exc:
        raise HTTPException(status_code=401, detail="Invalid or expired token.") from exc


def get_current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer),
) -> dict[str, Any]:
    """FastAPI dependency that resolves a Bearer token to the current user dict.

    Re-checks SQLite so deleted accounts are rejected even with a valid token.
    """
    if not credentials:
        raise HTTPException(status_code=401, detail="Authentication required.")
    payload = _decode(credentials.credentials)
    user_id = payload.get("sub")
    with user_db() as conn:
        row = conn.execute(
            "SELECT id, username, email, created_at FROM users WHERE id = ?", [user_id]
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=401, detail="User not found.")
    return dict(row)


def authenticate_ws_token(token: str) -> bool:
    """Validates a JWT token for WebSocket endpoints (no raise, returns bool)."""
    if not token:
        return False
    try:
        payload = jwt.decode(token, get_settings().secret_key, algorithms=[ALGORITHM])
        user_id = payload.get("sub")
        if not user_id:
            return False
        with user_db() as conn:
            row = conn.execute("SELECT id FROM users WHERE id = ?", [user_id]).fetchone()
        return row is not None
    except JWTError:
        return False

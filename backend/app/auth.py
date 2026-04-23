"""
JWT authentication helpers and global middleware for route protection.

JWTAuthMiddleware enforces authentication on all routes except the explicit
public list. HTTP requests are authenticated via the Authorization: Bearer
header; WebSocket upgrades use a `token` query parameter because browsers
cannot send custom headers during the WebSocket handshake.

The resolved user dict is attached to request.state.user so any route handler
can access it without a separate database call.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import parse_qs

from jose import JWTError, jwt
from passlib.context import CryptContext
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

from app.config import get_settings
from app.userdb import user_db

ALGORITHM = "HS256"
TOKEN_EXPIRE_HOURS = 24

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# (method, path) pairs that do not require a JWT token.
_PUBLIC_ROUTES: set[tuple[str, str]] = {
    ("GET", "/api/health"),
    ("POST", "/api/auth/login"),
    ("POST", "/api/auth/register"),
}


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)


def create_token(user_id: int, username: str) -> str:
    payload = {
        "sub": str(user_id),
        "username": username,
        "exp": datetime.now(timezone.utc) + timedelta(hours=TOKEN_EXPIRE_HOURS),
    }
    return jwt.encode(payload, get_settings().secret_key, algorithm=ALGORITHM)


def _resolve_token(token: str) -> dict[str, Any] | None:
    """Validate a JWT and return the user dict, or None on any failure.

    Re-checks SQLite so deleted accounts are rejected even with a valid token.
    """
    if not token:
        return None
    try:
        payload = jwt.decode(token, get_settings().secret_key, algorithms=[ALGORITHM])
        user_id = payload.get("sub")
        if not user_id:
            return None
        with user_db() as conn:
            row = conn.execute(
                "SELECT id, username, email, created_at FROM users WHERE id = ?", [user_id]
            ).fetchone()
        return dict(row) if row else None
    except JWTError:
        return None


class JWTAuthMiddleware:
    """Raw ASGI middleware that enforces JWT authentication globally.

    Covers both HTTP and WebSocket scopes so all endpoints are protected in
    one place without per-route Depends() declarations.

    Public routes (health, login, register) and CORS OPTIONS preflights bypass
    authentication. Every other request must carry a valid, non-expired JWT.
    The resolved user is stored in request.state.user for downstream handlers.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        # Pass through lifespan events unchanged.
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return

        # Skip CORS OPTIONS preflights — the CORS middleware handles those.
        if scope["type"] == "http" and scope.get("method") == "OPTIONS":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")

        if scope["type"] == "http":
            method = scope.get("method", "")
            if (method, path) in _PUBLIC_ROUTES:
                await self.app(scope, receive, send)
                return
            # Extract Bearer token from Authorization header.
            headers = {k.lower(): v for k, v in scope.get("headers", [])}
            auth = headers.get(b"authorization", b"").decode()
            token = auth[7:] if auth.startswith("Bearer ") else ""
        else:
            # WebSocket: token arrives as a query parameter.
            qs = scope.get("query_string", b"").decode()
            token = parse_qs(qs).get("token", [""])[0]

        user = _resolve_token(token)

        if user is None:
            if scope["type"] == "websocket":
                # Consume the WebSocket connect event then close with auth error code.
                await receive()
                await send({"type": "websocket.close", "code": 4001, "reason": "Authentication required."})
            else:
                response = JSONResponse({"detail": "Authentication required."}, status_code=401)
                await response(scope, receive, send)
            return

        # Store user directly in scope so route handlers can read it via
        # request.scope["_auth_user"] without depending on Starlette's State object.
        scope["_auth_user"] = user

        await self.app(scope, receive, send)

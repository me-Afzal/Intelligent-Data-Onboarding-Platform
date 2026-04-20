"""
Authentication API routes: registration, login, profile, password change, account deletion.

All mutation endpoints require the current password to prevent account takeover
if a token is stolen but the attacker doesn't know the password.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, field_validator

from app.auth import create_token, get_current_user, hash_password, verify_password
from app.userdb import user_db

router = APIRouter(prefix="/api/auth", tags=["auth"])


class RegisterRequest(BaseModel):
    """Payload for POST /api/auth/register.

    username is stripped of whitespace before the length check so leading/trailing
    spaces don't count toward the 3-character minimum.
    """

    username: str
    email: str
    password: str

    @field_validator("username")
    @classmethod
    def _check_username(cls, v: str) -> str:
        """Strip and enforce a minimum username length of 3 characters."""
        v = v.strip()
        if len(v) < 3:
            raise ValueError("Username must be at least 3 characters.")
        return v

    @field_validator("password")
    @classmethod
    def _check_password(cls, v: str) -> str:
        """Enforce a minimum password length of 6 characters at the model level."""
        if len(v) < 6:
            raise ValueError("Password must be at least 6 characters.")
        return v


class LoginRequest(BaseModel):
    """Payload for POST /api/auth/login."""

    username: str
    password: str


class UpdatePasswordRequest(BaseModel):
    """Payload for PUT /api/auth/password.

    Requires both the current password (for verification) and the new password.
    """

    current_password: str
    new_password: str

    @field_validator("new_password")
    @classmethod
    def _check_new(cls, v: str) -> str:
        """Enforce minimum length on the new password before it reaches the handler."""
        if len(v) < 6:
            raise ValueError("New password must be at least 6 characters.")
        return v


class DeleteAccountRequest(BaseModel):
    """Payload for DELETE /api/auth/account — requires password confirmation."""

    password: str


@router.post("/register")
def register(payload: RegisterRequest) -> dict[str, Any]:
    """Create a new user account and return a JWT access token on success.

    Returns 409 if the username or email is already registered (SQLite UNIQUE
    constraint). The token is returned immediately so the user is logged in
    without a separate login round-trip.
    """
    hashed = hash_password(payload.password)
    with user_db() as conn:
        try:
            cursor = conn.execute(
                "INSERT INTO users (username, email, hashed_password) VALUES (?, ?, ?)",
                [payload.username, payload.email, hashed],
            )
            conn.commit()
            user_id = cursor.lastrowid
        except Exception:
            # SQLite UNIQUE constraint violations surface as a generic Exception here;
            # treat any insert failure as a duplicate username/email conflict.
            raise HTTPException(status_code=409, detail="Username or email already registered.")
    return {"access_token": create_token(user_id, payload.username), "token_type": "bearer"}


@router.post("/login")
def login(payload: LoginRequest) -> dict[str, Any]:
    """Verify credentials and return a JWT access token.

    Returns the same 401 message for both 'user not found' and 'wrong password'
    to avoid leaking which usernames exist in the system.
    """
    with user_db() as conn:
        row = conn.execute(
            "SELECT id, username, hashed_password FROM users WHERE username = ?",
            [payload.username],
        ).fetchone()
    if row is None or not verify_password(payload.password, row["hashed_password"]):
        raise HTTPException(status_code=401, detail="Invalid username or password.")
    return {"access_token": create_token(row["id"], row["username"]), "token_type": "bearer"}


@router.get("/me")
def me(current_user: dict = Depends(get_current_user)) -> dict[str, Any]:
    """Return the profile of the currently authenticated user.

    Used by the frontend on startup to validate a stored token and hydrate
    the user display name without prompting for login again.
    """
    return current_user


@router.put("/password")
def update_password(
    payload: UpdatePasswordRequest,
    current_user: dict = Depends(get_current_user),
) -> dict[str, str]:
    """Change the authenticated user's password after verifying the current one.

    Re-fetches the stored hash rather than trusting the token payload because
    the token doesn't carry the hashed password.
    """
    with user_db() as conn:
        row = conn.execute(
            "SELECT hashed_password FROM users WHERE id = ?", [current_user["id"]]
        ).fetchone()
        if row is None or not verify_password(payload.current_password, row["hashed_password"]):
            raise HTTPException(status_code=401, detail="Current password is incorrect.")
        conn.execute(
            "UPDATE users SET hashed_password = ? WHERE id = ?",
            [hash_password(payload.new_password), current_user["id"]],
        )
        conn.commit()
    return {"message": "Password updated successfully."}


@router.delete("/account")
def delete_account(
    payload: DeleteAccountRequest,
    current_user: dict = Depends(get_current_user),
) -> dict[str, str]:
    """Permanently delete the authenticated user's account.

    Password re-confirmation is required as a second factor to protect against
    accidental or malicious deletion when a session is left open.
    Note: associated job data in DuckDB and Redis is not purged here; it
    expires naturally via the Redis TTL and is isolated by job_id.
    """
    with user_db() as conn:
        row = conn.execute(
            "SELECT hashed_password FROM users WHERE id = ?", [current_user["id"]]
        ).fetchone()
        if row is None or not verify_password(payload.password, row["hashed_password"]):
            raise HTTPException(status_code=401, detail="Password is incorrect.")
        conn.execute("DELETE FROM users WHERE id = ?", [current_user["id"]])
        conn.commit()
    return {"message": "Account deleted."}

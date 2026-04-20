from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, field_validator

from app.auth import create_token, get_current_user, hash_password, verify_password
from app.userdb import user_db

router = APIRouter(prefix="/api/auth", tags=["auth"])


class RegisterRequest(BaseModel):
    username: str
    email: str
    password: str

    @field_validator("username")
    @classmethod
    def _check_username(cls, v: str) -> str:
        v = v.strip()
        if len(v) < 3:
            raise ValueError("Username must be at least 3 characters.")
        return v

    @field_validator("password")
    @classmethod
    def _check_password(cls, v: str) -> str:
        if len(v) < 6:
            raise ValueError("Password must be at least 6 characters.")
        return v


class LoginRequest(BaseModel):
    username: str
    password: str


class UpdatePasswordRequest(BaseModel):
    current_password: str
    new_password: str

    @field_validator("new_password")
    @classmethod
    def _check_new(cls, v: str) -> str:
        if len(v) < 6:
            raise ValueError("New password must be at least 6 characters.")
        return v


class DeleteAccountRequest(BaseModel):
    password: str


@router.post("/register")
def register(payload: RegisterRequest) -> dict[str, Any]:
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
            raise HTTPException(status_code=409, detail="Username or email already registered.")
    return {"access_token": create_token(user_id, payload.username), "token_type": "bearer"}


@router.post("/login")
def login(payload: LoginRequest) -> dict[str, Any]:
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
    return current_user


@router.put("/password")
def update_password(
    payload: UpdatePasswordRequest,
    current_user: dict = Depends(get_current_user),
) -> dict[str, str]:
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
    with user_db() as conn:
        row = conn.execute(
            "SELECT hashed_password FROM users WHERE id = ?", [current_user["id"]]
        ).fetchone()
        if row is None or not verify_password(payload.password, row["hashed_password"]):
            raise HTTPException(status_code=401, detail="Password is incorrect.")
        conn.execute("DELETE FROM users WHERE id = ?", [current_user["id"]])
        conn.commit()
    return {"message": "Account deleted."}

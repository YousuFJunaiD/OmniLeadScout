import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from dotenv import load_dotenv
from jose import JWTError, jwt
from passlib.context import CryptContext
from env_utils import require_env

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

JWT_SECRET = require_env("JWT_SECRET")
JWT_ALGORITHM = "HS256"
ACCESS_TOKEN_MINUTES = int(os.getenv("ACCESS_TOKEN_MINUTES", "60"))
REFRESH_WINDOW_MINUTES = int(os.getenv("REFRESH_WINDOW_MINUTES", "15"))

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def _bcrypt_safe_password(password: str) -> str:
    raw = (password or "").encode("utf-8")
    return raw[:72].decode("utf-8", errors="ignore")


def hash_password_bcrypt(password: str) -> str:
    return pwd_context.hash(_bcrypt_safe_password(password))


def verify_password(password: str, hashed_password: str) -> bool:
    return pwd_context.verify(_bcrypt_safe_password(password), hashed_password)


def create_access_token(payload: Dict[str, Any], expires_minutes: int = ACCESS_TOKEN_MINUTES) -> str:
    to_encode = dict(payload)
    to_encode["exp"] = datetime.now(timezone.utc) + timedelta(minutes=expires_minutes)
    return jwt.encode(to_encode, JWT_SECRET, algorithm=JWT_ALGORITHM)


def decode_access_token(token: str) -> Optional[Dict[str, Any]]:
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except JWTError:
        return None


def should_refresh_token(token_payload: Optional[Dict[str, Any]]) -> bool:
    if not token_payload or "exp" not in token_payload:
        return False
    try:
        expires_at = datetime.fromtimestamp(float(token_payload["exp"]), tz=timezone.utc)
    except Exception:
        return False
    return expires_at - datetime.now(timezone.utc) <= timedelta(minutes=REFRESH_WINDOW_MINUTES)

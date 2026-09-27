"""Минимальная безопасность лабораторного стенда."""
import hashlib
import hmac
import os
import secrets

from fastapi import Depends, HTTPException
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

from coordinator.db import Role, Session, User, UserRole


ROLE_ORDER = {"viewer": 1, "operator": 2, "administrator": 3}
SESSION_MAX_AGE = 3600
_basic = HTTPBasic()


def _required_secret(name: str) -> str:
    value = os.environ.get(name, "")
    if len(value) < 16:
        raise RuntimeError(f"переменная {name} должна содержать не менее 16 символов")
    return value


def _serializer():
    return URLSafeTimedSerializer(_required_secret("SECRET_KEY"), salt="web-session")


def hash_password(password: str) -> str:
    salt = os.urandom(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 200_000)
    return f"{salt.hex()}${digest.hex()}"


def verify_password(password: str, stored: str) -> bool:
    try:
        salt_hex, digest_hex = stored.split("$", 1)
        salt = bytes.fromhex(salt_hex)
    except (ValueError, TypeError):
        return False
    actual = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 200_000)
    return hmac.compare_digest(actual.hex(), digest_hex)


def _create_user_if_configured(username: str, password_env: str, role_name: str):
    password = os.environ.get(password_env)
    if not password:
        return
    session = Session()
    try:
        user = session.query(User).filter(User.username == username).first()
        role = session.query(Role).filter(Role.name == role_name).first()
        if user is None:
            user = User(username=username, password_hash=hash_password(password))
            session.add(user)
            session.flush()
        relation = (
            session.query(UserRole)
            .filter(UserRole.user_id == user.id, UserRole.role_id == role.id)
            .first()
        )
        if relation is None:
            session.add(UserRole(user_id=user.id, role_id=role.id))
        session.commit()
    finally:
        session.close()


def ensure_bootstrap_users():
    """Создаёт роли и пользователей, пароли которых переданы окружением."""
    session = Session()
    try:
        for name in ROLE_ORDER:
            if session.query(Role).filter(Role.name == name).first() is None:
                session.add(Role(name=name))
        session.commit()
    finally:
        session.close()
    admin_name = os.environ.get("BOOTSTRAP_ADMIN_USERNAME", "admin")
    _create_user_if_configured(admin_name, "BOOTSTRAP_ADMIN_PASSWORD", "administrator")
    _create_user_if_configured("operator", "BOOTSTRAP_OPERATOR_PASSWORD", "operator")
    _create_user_if_configured("viewer", "BOOTSTRAP_VIEWER_PASSWORD", "viewer")


def authenticate(username: str, password: str) -> bool:
    session = Session()
    try:
        user = session.query(User).filter(User.username == username).first()
        return bool(user and verify_password(password, user.password_hash))
    finally:
        session.close()


def user_role(username: str) -> str:
    session = Session()
    try:
        user = session.query(User).filter(User.username == username).first()
        if user is None:
            return ""
        role_ids = [
            row.role_id
            for row in session.query(UserRole).filter(UserRole.user_id == user.id).all()
        ]
        roles = session.query(Role).filter(Role.id.in_(role_ids)).all() if role_ids else []
        return max((role.name for role in roles), key=lambda x: ROLE_ORDER[x], default="")
    finally:
        session.close()


def has_role(username: str, minimum: str) -> bool:
    return ROLE_ORDER.get(user_role(username), 0) >= ROLE_ORDER.get(minimum, 999)


def make_session(username: str) -> str:
    return _serializer().dumps({"username": username, "csrf": secrets.token_urlsafe(24)})


def read_session(token: str | None):
    if not token:
        return None
    try:
        return _serializer().loads(token, max_age=SESSION_MAX_AGE)
    except (BadSignature, SignatureExpired):
        return None


def verify_csrf(session_data: dict | None, form_token: str) -> bool:
    expected = (session_data or {}).get("csrf", "")
    return bool(expected and hmac.compare_digest(expected, form_token or ""))


def check_agent_token(token: str) -> bool:
    expected = _required_secret("AGENT_TOKEN")
    return hmac.compare_digest(token or "", expected)


def api_role(minimum: str):
    """FastAPI-зависимость для защищённых пользовательских методов API."""
    def dependency(credentials: HTTPBasicCredentials = Depends(_basic)):
        if not authenticate(credentials.username, credentials.password):
            raise HTTPException(401, "неверный логин или пароль")
        if not has_role(credentials.username, minimum):
            raise HTTPException(403, "недостаточно прав")
        return credentials.username
    return dependency

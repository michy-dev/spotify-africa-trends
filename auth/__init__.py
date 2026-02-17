"""
Authentication module for Spotify Africa Trends Dashboard.

Simple password-based authentication with session management.
"""

from .middleware import (
    AuthMiddleware,
    verify_password,
    create_session,
    get_session_from_cookie,
    SESSION_COOKIE_NAME,
)

__all__ = [
    "AuthMiddleware",
    "verify_password",
    "create_session",
    "get_session_from_cookie",
    "SESSION_COOKIE_NAME",
]

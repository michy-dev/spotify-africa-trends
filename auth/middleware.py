"""
Authentication middleware for password-protected dashboard access.
"""

import os
import secrets
import hashlib
from datetime import datetime, timedelta
from typing import Optional, Callable
import structlog

from fastapi import Request, Response
from fastapi.responses import RedirectResponse, HTMLResponse
from starlette.middleware.base import BaseHTTPMiddleware

logger = structlog.get_logger()

SESSION_COOKIE_NAME = "spotify_trends_session"
SESSION_DURATION_HOURS = 24

# In-memory session cache (backed by database)
_session_cache: dict = {}


def get_auth_password() -> Optional[str]:
    """Get the AUTH_PASSWORD from environment."""
    return os.environ.get("AUTH_PASSWORD")


def verify_password(password: str) -> bool:
    """Verify password against AUTH_PASSWORD env var."""
    expected = get_auth_password()
    if not expected:
        # No password set = auth disabled
        return True
    return secrets.compare_digest(password, expected)


def create_session() -> str:
    """Create a new session ID."""
    return secrets.token_urlsafe(32)


def get_session_from_cookie(request: Request) -> Optional[str]:
    """Extract session ID from cookie."""
    return request.cookies.get(SESSION_COOKIE_NAME)


def is_auth_enabled() -> bool:
    """Check if authentication is enabled."""
    return bool(get_auth_password())


# Public paths that don't require authentication
PUBLIC_PATHS = {
    "/health",
    "/api/health",
    "/auth/login",
    "/auth/logout",
    "/static",
}


def is_public_path(path: str) -> bool:
    """Check if path is public (no auth required)."""
    for public in PUBLIC_PATHS:
        if path.startswith(public):
            return True
    return False


class AuthMiddleware(BaseHTTPMiddleware):
    """
    Middleware to protect dashboard with password authentication.

    - All /api/* and / endpoints require authentication
    - /health and /auth/* are public
    - Session stored in cookie after successful login
    """

    def __init__(self, app, storage_getter: Callable = None):
        super().__init__(app)
        self.storage_getter = storage_getter

    async def dispatch(self, request: Request, call_next) -> Response:
        # Skip auth if not enabled
        if not is_auth_enabled():
            return await call_next(request)

        path = request.url.path

        # Allow public paths
        if is_public_path(path):
            return await call_next(request)

        # Check session
        session_id = get_session_from_cookie(request)
        if session_id:
            # Validate session
            is_valid = await self._validate_session(request, session_id)
            if is_valid:
                return await call_next(request)

        # Not authenticated - redirect to login or return 401 for API
        if path.startswith("/api/"):
            return Response(
                content='{"error": "Authentication required"}',
                status_code=401,
                media_type="application/json"
            )

        # Redirect to login page
        return RedirectResponse(
            url=f"/auth/login?next={path}",
            status_code=302
        )

    async def _validate_session(self, request: Request, session_id: str) -> bool:
        """Validate session from cache or database."""
        # Check cache first
        if session_id in _session_cache:
            cached = _session_cache[session_id]
            if cached["expires_at"] > datetime.utcnow():
                return True
            else:
                del _session_cache[session_id]
                return False

        # Check database if storage available
        if self.storage_getter:
            try:
                storage = self.storage_getter(request)
                if storage:
                    from storage.base import UserSession
                    session = await storage.get_user_session(session_id)
                    if session and session.is_valid:
                        if session.expires_at and session.expires_at > datetime.utcnow():
                            # Cache it
                            _session_cache[session_id] = {
                                "expires_at": session.expires_at
                            }
                            return True
            except Exception as e:
                logger.warning("session_validation_error", error=str(e))

        return False


def get_login_page_html(error: str = "", next_url: str = "/") -> str:
    """Generate login page HTML."""
    error_html = f'<p class="text-red-600 text-sm mb-4">{error}</p>' if error else ""

    return f'''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Login - Spotify Africa Trends</title>
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-gray-900 min-h-screen flex items-center justify-center">
    <div class="bg-gray-800 p-8 rounded-lg shadow-xl w-full max-w-md">
        <div class="flex items-center justify-center mb-6">
            <div class="w-12 h-12 bg-green-500 rounded-full flex items-center justify-center">
                <svg class="w-8 h-8 text-white" fill="currentColor" viewBox="0 0 24 24">
                    <path d="M12 0C5.4 0 0 5.4 0 12s5.4 12 12 12 12-5.4 12-12S18.66 0 12 0zm5.521 17.34c-.24.359-.66.48-1.021.24-2.82-1.74-6.36-2.101-10.561-1.141-.418.122-.779-.179-.899-.539-.12-.421.18-.78.54-.9 4.56-1.021 8.52-.6 11.64 1.32.42.18.479.659.301 1.02zm1.44-3.3c-.301.42-.841.6-1.262.3-3.239-1.98-8.159-2.58-11.939-1.38-.479.12-1.02-.12-1.14-.6-.12-.48.12-1.021.6-1.141C9.6 9.9 15 10.561 18.72 12.84c.361.181.54.78.241 1.2zm.12-3.36C15.24 8.4 8.82 8.16 5.16 9.301c-.6.179-1.2-.181-1.38-.721-.18-.601.18-1.2.72-1.381 4.26-1.26 11.28-1.02 15.721 1.621.539.3.719 1.02.419 1.56-.299.421-1.02.599-1.559.3z"/>
                </svg>
            </div>
        </div>
        <h1 class="text-2xl font-bold text-white text-center mb-2">Africa Comms Trends</h1>
        <p class="text-gray-400 text-center mb-6">Enter password to access dashboard</p>

        {error_html}

        <form method="POST" action="/auth/login">
            <input type="hidden" name="next" value="{next_url}">
            <div class="mb-4">
                <label class="block text-gray-300 text-sm font-medium mb-2" for="password">
                    Password
                </label>
                <input
                    type="password"
                    id="password"
                    name="password"
                    class="w-full px-4 py-3 bg-gray-700 text-white rounded-lg focus:outline-none focus:ring-2 focus:ring-green-500"
                    placeholder="Enter password"
                    required
                    autofocus
                >
            </div>
            <button
                type="submit"
                class="w-full bg-green-500 hover:bg-green-600 text-white font-bold py-3 px-4 rounded-lg transition-colors"
            >
                Sign In
            </button>
        </form>
    </div>
</body>
</html>
'''

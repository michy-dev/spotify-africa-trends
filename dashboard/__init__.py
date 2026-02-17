"""
Dashboard module for Spotify Africa Trends Dashboard.

Provides a FastAPI-based web interface for:
- Viewing trends with filters
- Trend detail pages
- Risk/sensitivity panels
- Action recommendations
"""

from .app import create_app, app

__all__ = ["create_app", "app"]

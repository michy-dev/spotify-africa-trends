"""
Storage layer for Spotify Africa Trends Dashboard.

Supports SQLite (default) and PostgreSQL backends.
"""

from .base import BaseStorage, TrendRecord
from .sqlite import SQLiteStorage
from .postgres import PostgresStorage

__all__ = [
    "BaseStorage",
    "TrendRecord",
    "SQLiteStorage",
    "PostgresStorage",
    "get_storage",
]


def get_storage(config: dict) -> BaseStorage:
    """
    Get configured storage backend.

    Args:
        config: Application configuration

    Returns:
        Storage instance
    """
    storage_config = config.get("storage", {})
    storage_type = storage_config.get("type", "sqlite")

    if storage_type == "postgres":
        return PostgresStorage(storage_config)
    else:
        return SQLiteStorage(storage_config)

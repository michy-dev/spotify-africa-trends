"""
Base connector interface and data models.

All data source connectors must inherit from BaseConnector
and implement the required methods.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional
import hashlib
import structlog

logger = structlog.get_logger()


class SourceStatus(Enum):
    """Status of a data source connector."""
    ACTIVE = "active"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"
    REQUIRES_AUTH = "requires_auth"
    DISABLED = "disabled"


@dataclass
class TrendItem:
    """A single trend item collected from a source."""

    # Core identifiers
    id: str
    source: str
    source_url: Optional[str] = None

    # Content
    title: str = ""
    description: str = ""
    raw_text: str = ""

    # Classification (populated by pipeline)
    topic: Optional[str] = None
    subtopic: Optional[str] = None

    # Location & language
    market: Optional[str] = None
    language: Optional[str] = None

    # Metrics
    volume: int = 0
    engagement: int = 0
    velocity: float = 0.0

    # Entities (populated by pipeline)
    entities: dict = field(default_factory=dict)  # {type: [names]}

    # Timestamps
    published_at: Optional[datetime] = None
    collected_at: datetime = field(default_factory=datetime.utcnow)

    # Source-specific metadata
    metadata: dict = field(default_factory=dict)

    def __post_init__(self):
        """Generate ID if not provided."""
        if not self.id:
            content = f"{self.source}:{self.title}:{self.source_url}"
            self.id = hashlib.sha256(content.encode()).hexdigest()[:16]

    def to_dict(self) -> dict:
        """Convert to dictionary for storage."""
        return {
            "id": self.id,
            "source": self.source,
            "source_url": self.source_url,
            "title": self.title,
            "description": self.description,
            "raw_text": self.raw_text,
            "topic": self.topic,
            "subtopic": self.subtopic,
            "market": self.market,
            "language": self.language,
            "volume": self.volume,
            "engagement": self.engagement,
            "velocity": self.velocity,
            "entities": self.entities,
            "published_at": self.published_at.isoformat() if self.published_at else None,
            "collected_at": self.collected_at.isoformat(),
            "metadata": self.metadata,
        }


@dataclass
class ConnectorResult:
    """Result from a connector fetch operation."""

    source: str
    status: SourceStatus
    items: list[TrendItem] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    # Timing
    started_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None

    # Rate limiting
    requests_made: int = 0
    requests_remaining: Optional[int] = None

    def __post_init__(self):
        if self.completed_at is None:
            self.completed_at = datetime.utcnow()

    @property
    def success(self) -> bool:
        return self.status in (SourceStatus.ACTIVE, SourceStatus.DEGRADED)

    @property
    def item_count(self) -> int:
        return len(self.items)

    @property
    def duration_seconds(self) -> float:
        if self.completed_at and self.started_at:
            return (self.completed_at - self.started_at).total_seconds()
        return 0.0


class BaseConnector(ABC):
    """
    Abstract base class for all data source connectors.

    Each connector is responsible for:
    1. Connecting to its data source
    2. Fetching relevant trend data
    3. Normalizing data into TrendItem format
    4. Handling rate limits and errors gracefully
    """

    # Connector metadata
    name: str = "base"
    display_name: str = "Base Connector"
    requires_auth: bool = False

    def __init__(self, config: dict):
        """
        Initialize the connector with configuration.

        Args:
            config: Source-specific configuration from settings.yaml
        """
        self.config = config
        self.enabled = config.get("enabled", True)
        self.priority = config.get("priority", 5)
        self.reliability = config.get("reliability", 0.5)
        self.rate_limit = config.get("rate_limit", 100)
        self._status = SourceStatus.ACTIVE
        self._last_fetch: Optional[datetime] = None

        self.logger = structlog.get_logger().bind(connector=self.name)

    @property
    def status(self) -> SourceStatus:
        """Get current connector status."""
        if not self.enabled:
            return SourceStatus.DISABLED
        if self.requires_auth and not self._has_credentials():
            return SourceStatus.REQUIRES_AUTH
        return self._status

    def _has_credentials(self) -> bool:
        """Check if required credentials are configured."""
        return True  # Override in subclasses

    @abstractmethod
    async def fetch(
        self,
        markets: list[str],
        keywords: list[str],
        **kwargs
    ) -> ConnectorResult:
        """
        Fetch trend data from the source.

        Args:
            markets: List of market codes to query (e.g., ["ZA", "NG"])
            keywords: List of keywords/topics to search
            **kwargs: Source-specific parameters

        Returns:
            ConnectorResult with collected items
        """
        pass

    @abstractmethod
    async def health_check(self) -> bool:
        """
        Check if the connector is operational.

        Returns:
            True if the connector can fetch data
        """
        pass

    async def test_connection(self) -> dict:
        """
        Test the connection and return diagnostic info.

        Returns:
            Dictionary with connection status and details
        """
        try:
            healthy = await self.health_check()
            return {
                "connector": self.name,
                "healthy": healthy,
                "status": self.status.value,
                "requires_auth": self.requires_auth,
                "has_credentials": self._has_credentials(),
            }
        except Exception as e:
            return {
                "connector": self.name,
                "healthy": False,
                "status": "error",
                "error": str(e),
            }

    def _create_result(
        self,
        items: list[TrendItem],
        status: SourceStatus = SourceStatus.ACTIVE,
        errors: list[str] = None,
        warnings: list[str] = None,
    ) -> ConnectorResult:
        """Helper to create a ConnectorResult."""
        return ConnectorResult(
            source=self.name,
            status=status,
            items=items,
            errors=errors or [],
            warnings=warnings or [],
        )

    def _normalize_market(self, market: str) -> str:
        """Normalize market code to standard format."""
        return market.upper().strip()

    def _extract_language(self, text: str) -> Optional[str]:
        """
        Detect language of text.
        Simple heuristic - can be enhanced with proper detection.
        """
        # This is a placeholder - real implementation would use
        # langdetect or similar library
        return None


class StubConnector(BaseConnector):
    """
    Stub connector for sources that are not yet implemented
    or require special access.
    """

    name = "stub"
    display_name = "Stub Connector"

    def __init__(self, config: dict, warning: str = ""):
        super().__init__(config)
        self.warning = warning or config.get("warning", "This connector is a stub.")
        self._status = SourceStatus.UNAVAILABLE

    async def fetch(
        self,
        markets: list[str],
        keywords: list[str],
        **kwargs
    ) -> ConnectorResult:
        """Return empty result with warning."""
        self.logger.warning("stub_connector_called", warning=self.warning)
        return self._create_result(
            items=[],
            status=SourceStatus.UNAVAILABLE,
            warnings=[self.warning],
        )

    async def health_check(self) -> bool:
        """Stub is never healthy."""
        return False

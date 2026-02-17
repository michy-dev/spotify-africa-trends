"""
Base storage interface and data models.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List
from enum import Enum
import json


class SensitivityTag(str, Enum):
    """Sensitivity tags for culture searches."""
    MUSIC = "music"
    FASHION = "fashion"
    SPORT = "sport"
    FILM_TV = "film_tv"
    MEME = "meme"
    CELEBRITY = "celebrity"
    POLITICS = "politics"


class ModuleStatus(str, Enum):
    """Status for data health monitoring."""
    OK = "ok"
    DEGRADED = "degraded"
    DOWN = "down"


@dataclass
class ArtistSpike:
    """Artist search spike data."""
    id: str
    artist_name: str
    market: str
    spike_score: float  # 0-100
    time_window: str  # "24h" or "7d"
    sparkline_data: List[float] = field(default_factory=list)  # 7 data points
    why_spiking: List[str] = field(default_factory=list)  # Bullet points
    confidence: str = "medium"  # high/medium/low
    is_ambiguous: bool = False  # Common word names
    related_queries: List[str] = field(default_factory=list)
    related_topics: List[str] = field(default_factory=list)
    current_interest: float = 0.0
    baseline_interest: float = 0.0
    collected_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "artist_name": self.artist_name,
            "market": self.market,
            "spike_score": self.spike_score,
            "time_window": self.time_window,
            "sparkline_data": self.sparkline_data,
            "why_spiking": self.why_spiking,
            "confidence": self.confidence,
            "is_ambiguous": self.is_ambiguous,
            "related_queries": self.related_queries,
            "related_topics": self.related_topics,
            "current_interest": self.current_interest,
            "baseline_interest": self.baseline_interest,
            "collected_at": self.collected_at.isoformat(),
        }


@dataclass
class CultureSearch:
    """Rising culture search term."""
    id: str
    term: str
    market: str
    sensitivity_tag: str  # SensitivityTag value
    rise_percentage: float
    volume: int = 0
    is_cross_market: bool = False
    markets_present: List[str] = field(default_factory=list)
    risk_level: str = "low"  # auto high for politics
    collected_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "term": self.term,
            "market": self.market,
            "sensitivity_tag": self.sensitivity_tag,
            "rise_percentage": self.rise_percentage,
            "volume": self.volume,
            "is_cross_market": self.is_cross_market,
            "markets_present": self.markets_present,
            "risk_level": self.risk_level,
            "collected_at": self.collected_at.isoformat(),
        }


@dataclass
class StyleSignal:
    """Streetwear/fashion signal from RSS."""
    id: str
    headline: str
    source: str
    source_url: str
    summary: str
    publish_date: datetime
    country_relevance: List[str] = field(default_factory=list)  # NG/KE/GH/ZA
    spotify_tags: List[str] = field(default_factory=list)  # artist_collab, tour_merch, youth_culture
    risk_level: str = "low"
    collected_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "headline": self.headline,
            "source": self.source,
            "source_url": self.source_url,
            "summary": self.summary,
            "publish_date": self.publish_date.isoformat(),
            "country_relevance": self.country_relevance,
            "spotify_tags": self.spotify_tags,
            "risk_level": self.risk_level,
            "collected_at": self.collected_at.isoformat(),
        }


@dataclass
class PitchCard:
    """Generated trend-jack opportunity card."""
    id: str
    market: str
    hook: str  # Headline-style opener
    why_now: List[str]  # Supporting signals
    spotify_angle: str  # FTR/playlist/podcast/editorial/creator angle
    next_steps: List[str]  # Who to message, what to prep
    risks: List[str]  # 1-2 bullet sensitivities
    confidence: str  # high/medium/low
    source_signals: List[str] = field(default_factory=list)  # IDs of source data
    generated_at: datetime = field(default_factory=datetime.utcnow)
    expires_at: Optional[datetime] = None

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "market": self.market,
            "hook": self.hook,
            "why_now": self.why_now,
            "spotify_angle": self.spotify_angle,
            "next_steps": self.next_steps,
            "risks": self.risks,
            "confidence": self.confidence,
            "source_signals": self.source_signals,
            "generated_at": self.generated_at.isoformat(),
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
        }


@dataclass
class DataHealth:
    """Module health status."""
    module_name: str
    status: str  # ok/degraded/down
    last_success: Optional[datetime] = None
    last_error: Optional[str] = None
    item_count: int = 0
    freshness_hours: float = 0.0
    checked_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict:
        return {
            "module_name": self.module_name,
            "status": self.status,
            "last_success": self.last_success.isoformat() if self.last_success else None,
            "last_error": self.last_error,
            "item_count": self.item_count,
            "freshness_hours": self.freshness_hours,
            "checked_at": self.checked_at.isoformat(),
        }


@dataclass
class UserSession:
    """User authentication session."""
    session_id: str
    created_at: datetime = field(default_factory=datetime.utcnow)
    expires_at: Optional[datetime] = None
    is_valid: bool = True


@dataclass
class TrendRecord:
    """A trend record for storage."""

    # Core fields
    id: str
    title: str
    source: str

    # Classification
    topic: Optional[str] = None
    subtopic: Optional[str] = None
    market: Optional[str] = None
    language: Optional[str] = None

    # Scores
    total_score: float = 0.0
    velocity_score: float = 0.0
    reach_score: float = 0.0
    market_impact_score: float = 0.0
    spotify_adjacency_score: float = 0.0
    risk_score: float = 0.0

    # Status
    risk_level: str = "low"
    suggested_action: str = "monitor"
    confidence: str = "medium"
    priority_level: str = "low"

    # Content
    description: str = ""
    source_url: Optional[str] = None
    entities: dict = field(default_factory=dict)

    # Summary fields
    whats_happening: str = ""
    why_it_matters: List[str] = field(default_factory=list)
    if_goes_wrong: str = ""

    # Metrics
    volume: int = 0
    engagement: int = 0
    velocity: float = 0.0

    # Timestamps
    first_seen: Optional[datetime] = None
    last_updated: datetime = field(default_factory=datetime.utcnow)
    collected_at: datetime = field(default_factory=datetime.utcnow)

    # Raw data
    raw_data: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "title": self.title,
            "source": self.source,
            "topic": self.topic,
            "subtopic": self.subtopic,
            "market": self.market,
            "language": self.language,
            "total_score": self.total_score,
            "velocity_score": self.velocity_score,
            "reach_score": self.reach_score,
            "market_impact_score": self.market_impact_score,
            "spotify_adjacency_score": self.spotify_adjacency_score,
            "risk_score": self.risk_score,
            "risk_level": self.risk_level,
            "suggested_action": self.suggested_action,
            "confidence": self.confidence,
            "priority_level": self.priority_level,
            "description": self.description,
            "source_url": self.source_url,
            "entities": self.entities,
            "whats_happening": self.whats_happening,
            "why_it_matters": self.why_it_matters,
            "if_goes_wrong": self.if_goes_wrong,
            "volume": self.volume,
            "engagement": self.engagement,
            "velocity": self.velocity,
            "first_seen": self.first_seen.isoformat() if self.first_seen else None,
            "last_updated": self.last_updated.isoformat(),
            "collected_at": self.collected_at.isoformat(),
        }

    @classmethod
    def from_summary(cls, summary) -> "TrendRecord":
        """Create from TrendSummary object."""
        return cls(
            id=summary.trend_id,
            title=summary.title,
            source=summary.sources[0] if summary.sources else "unknown",
            topic=summary.topic,
            subtopic=summary.subtopic,
            market=summary.market,
            language=summary.language,
            total_score=summary.total_score,
            velocity_score=summary.score_breakdown.get("components", {}).get("velocity", {}).get("score", 0),
            reach_score=summary.score_breakdown.get("components", {}).get("reach", {}).get("score", 0),
            market_impact_score=summary.score_breakdown.get("components", {}).get("market_impact", {}).get("score", 0),
            spotify_adjacency_score=summary.score_breakdown.get("components", {}).get("spotify_adjacency", {}).get("score", 0),
            risk_score=summary.score_breakdown.get("components", {}).get("risk", {}).get("score", 0),
            risk_level=summary.risk_level,
            suggested_action=summary.suggested_action,
            confidence=summary.confidence,
            priority_level=summary.priority_level,
            source_url=summary.source_url,
            entities=summary.key_entities,
            whats_happening=summary.whats_happening,
            why_it_matters=summary.why_it_matters,
            if_goes_wrong=summary.if_goes_wrong,
            first_seen=summary.first_seen,
            last_updated=summary.last_updated,
        )


class BaseStorage(ABC):
    """
    Abstract base class for storage backends.

    Implementations must handle:
    - Trend record persistence
    - Historical data tracking
    - Baseline calculation
    - Query and filtering
    """

    def __init__(self, config: dict):
        """
        Initialize storage with configuration.

        Args:
            config: Storage configuration
        """
        self.config = config

    @abstractmethod
    async def initialize(self):
        """Initialize storage (create tables, etc.)."""
        pass

    @abstractmethod
    async def close(self):
        """Close storage connections."""
        pass

    @abstractmethod
    async def save_trends(self, trends: List[TrendRecord]) -> int:
        """
        Save trend records.

        Args:
            trends: List of TrendRecord objects

        Returns:
            Number of records saved
        """
        pass

    @abstractmethod
    async def get_trends(
        self,
        limit: int = 50,
        offset: int = 0,
        market: Optional[str] = None,
        topic: Optional[str] = None,
        risk_level: Optional[str] = None,
        min_score: Optional[float] = None,
        since: Optional[datetime] = None,
    ) -> List[TrendRecord]:
        """
        Get trend records with filters.

        Args:
            limit: Maximum records to return
            offset: Pagination offset
            market: Filter by market
            topic: Filter by topic
            risk_level: Filter by risk level
            min_score: Minimum score threshold
            since: Only records since this time

        Returns:
            List of matching TrendRecord objects
        """
        pass

    @abstractmethod
    async def get_trend_by_id(self, trend_id: str) -> Optional[TrendRecord]:
        """Get a single trend by ID."""
        pass

    @abstractmethod
    async def get_trend_history(
        self,
        trend_id: str,
        days: int = 7
    ) -> List[dict]:
        """
        Get historical data for a trend.

        Returns daily snapshots for charting.
        """
        pass

    @abstractmethod
    async def get_baselines(
        self,
        market: Optional[str] = None,
        topic: Optional[str] = None
    ) -> dict:
        """
        Get baseline metrics for velocity calculation.

        Returns average metrics over the baseline period.
        """
        pass

    @abstractmethod
    async def save_pipeline_run(self, metrics: dict) -> str:
        """
        Save pipeline run metadata.

        Args:
            metrics: Pipeline run metrics

        Returns:
            Run ID
        """
        pass

    @abstractmethod
    async def get_last_run(self) -> Optional[dict]:
        """Get metadata for the last pipeline run."""
        pass

    @abstractmethod
    async def cleanup_old_data(self, days: int = 90) -> int:
        """
        Remove data older than specified days.

        Args:
            days: Data retention period

        Returns:
            Number of records removed
        """
        pass

    # ==================== Trend-Jack Intelligence Methods ====================

    @abstractmethod
    async def save_artist_spikes(self, spikes: List["ArtistSpike"]) -> int:
        """Save artist spike records."""
        pass

    @abstractmethod
    async def get_artist_spikes(
        self,
        market: Optional[str] = None,
        time_window: str = "24h",
        limit: int = 20,
    ) -> List["ArtistSpike"]:
        """Get artist spikes for a market."""
        pass

    @abstractmethod
    async def save_culture_searches(self, searches: List["CultureSearch"]) -> int:
        """Save culture search records."""
        pass

    @abstractmethod
    async def get_culture_searches(
        self,
        market: Optional[str] = None,
        sensitivity_tag: Optional[str] = None,
        limit: int = 10,
    ) -> List["CultureSearch"]:
        """Get culture searches with filters."""
        pass

    @abstractmethod
    async def get_culture_overlaps(self) -> List[dict]:
        """Get cross-market culture search overlaps."""
        pass

    @abstractmethod
    async def save_style_signals(self, signals: List["StyleSignal"]) -> int:
        """Save style signal records."""
        pass

    @abstractmethod
    async def get_style_signals(
        self,
        country_relevance: Optional[str] = None,
        max_risk: str = "high",
        limit: int = 15,
    ) -> List["StyleSignal"]:
        """Get style signals with filters."""
        pass

    @abstractmethod
    async def save_pitch_cards(self, cards: List["PitchCard"]) -> int:
        """Save generated pitch cards."""
        pass

    @abstractmethod
    async def get_pitch_cards(
        self,
        market: Optional[str] = None,
        limit: int = 6,
    ) -> List["PitchCard"]:
        """Get pitch cards for a market."""
        pass

    @abstractmethod
    async def save_data_health(self, health: "DataHealth") -> None:
        """Save module health status."""
        pass

    @abstractmethod
    async def get_data_health(self) -> List["DataHealth"]:
        """Get health status for all modules."""
        pass

    @abstractmethod
    async def save_user_session(self, session: "UserSession") -> None:
        """Save user session."""
        pass

    @abstractmethod
    async def get_user_session(self, session_id: str) -> Optional["UserSession"]:
        """Get user session by ID."""
        pass

    @abstractmethod
    async def delete_user_session(self, session_id: str) -> None:
        """Delete user session."""
        pass

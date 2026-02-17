"""
Comms relevance scorer - calculates actionable scores for trend items.

The scoring system is designed to be EXPLAINABLE, showing comms teams
exactly why each trend is ranked the way it is.
"""

import re
from dataclasses import dataclass, field
from typing import Optional
import structlog

from connectors.base import TrendItem

logger = structlog.get_logger()


@dataclass
class ScoreBreakdown:
    """Detailed breakdown of a trend's score."""

    # Component scores (0-100 each)
    velocity_score: float = 0.0
    reach_score: float = 0.0
    market_impact_score: float = 0.0
    spotify_adjacency_score: float = 0.0
    risk_score: float = 0.0

    # Final weighted score
    total_score: float = 0.0

    # Explanations
    velocity_reason: str = ""
    reach_reason: str = ""
    market_reason: str = ""
    adjacency_reason: str = ""
    risk_reason: str = ""

    # Risk flags
    risk_level: str = "low"  # low, medium, high
    risk_keywords_found: list = field(default_factory=list)

    # Suggested action
    suggested_action: str = "monitor"  # monitor, engage, partner, avoid, escalate
    confidence: str = "medium"  # low, medium, high

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "total_score": round(self.total_score, 1),
            "components": {
                "velocity": {
                    "score": round(self.velocity_score, 1),
                    "reason": self.velocity_reason,
                },
                "reach": {
                    "score": round(self.reach_score, 1),
                    "reason": self.reach_reason,
                },
                "market_impact": {
                    "score": round(self.market_impact_score, 1),
                    "reason": self.market_reason,
                },
                "spotify_adjacency": {
                    "score": round(self.spotify_adjacency_score, 1),
                    "reason": self.adjacency_reason,
                },
                "risk": {
                    "score": round(self.risk_score, 1),
                    "reason": self.risk_reason,
                    "level": self.risk_level,
                    "keywords": self.risk_keywords_found,
                },
            },
            "suggested_action": self.suggested_action,
            "confidence": self.confidence,
        }


class CommsScorer:
    """
    Calculates Comms Relevance Score for trend items.

    Score Components:
    - Velocity (25%): Growth rate vs baseline
    - Reach (20%): Volume across platforms
    - Market Impact (20%): Weighted by priority markets
    - Spotify Adjacency (20%): Connection to audio/music culture
    - Risk Factor (15%): Safety/politics/conflict signals

    All scoring logic is transparent and explainable.
    """

    def __init__(self, config: dict):
        """
        Initialize scorer with configuration.

        Args:
            config: Application configuration with scoring weights
        """
        self.config = config
        self.scoring_config = config.get("scoring", {})
        self.weights = self.scoring_config.get("weights", {})

        # Default weights if not configured
        self.weight_velocity = self.weights.get("velocity", 0.25)
        self.weight_reach = self.weights.get("reach", 0.20)
        self.weight_market = self.weights.get("market_impact", 0.20)
        self.weight_adjacency = self.weights.get("spotify_adjacency", 0.20)
        self.weight_risk = self.weights.get("risk_factor", 0.15)

        # Load market weights
        self.market_weights = {
            m["code"]: m.get("weight", 1.0)
            for m in config.get("markets", {}).get("priority", [])
        }

        # Load risk keywords
        self.risk_keywords = self.scoring_config.get("risk_keywords", {})

        # Thresholds
        self.thresholds = self.scoring_config.get("thresholds", {})

    def score_batch(self, items: list[TrendItem]) -> list[tuple[TrendItem, ScoreBreakdown]]:
        """
        Score a batch of items.

        Args:
            items: List of TrendItem objects

        Returns:
            List of (item, breakdown) tuples sorted by score descending
        """
        scored = []

        for item in items:
            breakdown = self.score_item(item)
            scored.append((item, breakdown))

        # Sort by total score descending
        scored.sort(key=lambda x: x[1].total_score, reverse=True)

        # Log score distribution
        scores = [b.total_score for _, b in scored]
        if scores:
            logger.info(
                "scoring_complete",
                count=len(scored),
                avg_score=sum(scores) / len(scores),
                max_score=max(scores),
                min_score=min(scores)
            )

        return scored

    def score_item(self, item: TrendItem) -> ScoreBreakdown:
        """
        Score a single item with full breakdown.

        Args:
            item: TrendItem to score

        Returns:
            ScoreBreakdown with component scores and explanations
        """
        breakdown = ScoreBreakdown()

        # Calculate each component
        breakdown.velocity_score, breakdown.velocity_reason = self._score_velocity(item)
        breakdown.reach_score, breakdown.reach_reason = self._score_reach(item)
        breakdown.market_impact_score, breakdown.market_reason = self._score_market_impact(item)
        breakdown.spotify_adjacency_score, breakdown.adjacency_reason = self._score_spotify_adjacency(item)
        breakdown.risk_score, breakdown.risk_reason, breakdown.risk_level, breakdown.risk_keywords_found = self._score_risk(item)

        # Calculate weighted total
        breakdown.total_score = (
            breakdown.velocity_score * self.weight_velocity +
            breakdown.reach_score * self.weight_reach +
            breakdown.market_impact_score * self.weight_market +
            breakdown.spotify_adjacency_score * self.weight_adjacency +
            breakdown.risk_score * self.weight_risk
        )

        # Determine suggested action and confidence
        breakdown.suggested_action = self._determine_action(breakdown)
        breakdown.confidence = self._determine_confidence(item, breakdown)

        return breakdown

    def _score_velocity(self, item: TrendItem) -> tuple[float, str]:
        """Score based on growth velocity."""
        velocity = item.velocity

        if velocity >= 2.0:
            return 100, f"Explosive growth ({velocity:.1f}x above baseline)"
        elif velocity >= 1.0:
            return 80, f"Strong growth ({velocity:.1f}x above baseline)"
        elif velocity >= 0.5:
            return 60, f"Moderate growth ({velocity:.1f}x above baseline)"
        elif velocity >= 0.2:
            return 40, f"Slight uptick ({velocity:.1f}x above baseline)"
        elif velocity > 0:
            return 20, "Minimal change"
        else:
            return 10, "Stable or declining"

    def _score_reach(self, item: TrendItem) -> tuple[float, str]:
        """Score based on reach/volume metrics."""
        volume = item.volume
        engagement = item.engagement

        # Combined reach metric
        combined = volume + (engagement * 2)  # Weight engagement higher

        if combined >= 1000000:
            return 100, f"Massive reach ({volume:,} volume, {engagement:,} engagement)"
        elif combined >= 100000:
            return 80, f"High reach ({volume:,} volume, {engagement:,} engagement)"
        elif combined >= 10000:
            return 60, f"Moderate reach ({volume:,} volume)"
        elif combined >= 1000:
            return 40, f"Growing reach ({volume:,} volume)"
        elif combined >= 100:
            return 20, f"Limited reach ({volume:,} volume)"
        else:
            return 10, "Minimal reach"

    def _score_market_impact(self, item: TrendItem) -> tuple[float, str]:
        """Score based on market relevance."""
        market = item.market

        if not market:
            return 30, "Market not identified"

        weight = self.market_weights.get(market, 1.0)

        # Base score from market weight
        base_score = min(weight * 50, 100)

        if weight >= 1.5:
            return base_score, f"Priority market ({market}, weight {weight}x)"
        elif weight >= 1.2:
            return base_score, f"Important market ({market})"
        elif weight >= 1.0:
            return base_score, f"Standard market ({market})"
        else:
            return base_score, f"Secondary market ({market})"

    def _score_spotify_adjacency(self, item: TrendItem) -> tuple[float, str]:
        """Score based on connection to Spotify/music culture."""
        text = f"{item.title} {item.description}".lower()

        # Direct Spotify mentions
        if "spotify" in text:
            return 100, "Direct Spotify mention"

        # Competitor mentions
        competitors = ["apple music", "boomplay", "audiomack", "youtube music", "deezer"]
        for comp in competitors:
            if comp in text:
                return 90, f"Competitor mention ({comp})"

        # Artist/music entities
        if "artists" in item.entities:
            artist_count = len(item.entities["artists"])
            score = min(70 + artist_count * 10, 95)
            return score, f"Artist-related ({artist_count} artists detected)"

        # Music topic
        if item.topic == "music_audio":
            return 70, "Music/audio topic"

        # Culture topics with music connection
        music_keywords = ["song", "music", "album", "track", "playlist", "stream", "listen"]
        for kw in music_keywords:
            if kw in text:
                return 60, f"Music-related ({kw} mentioned)"

        # General culture relevance
        if item.topic == "culture":
            return 40, "Culture/entertainment topic"

        return 20, "Limited audio/music connection"

    def _score_risk(self, item: TrendItem) -> tuple[float, str, str, list]:
        """
        Score risk factor.

        Higher score = higher risk = needs attention.
        """
        text = f"{item.title} {item.description} {item.raw_text}".lower()

        high_risk = self.risk_keywords.get("high", [])
        medium_risk = self.risk_keywords.get("medium", [])
        low_risk = self.risk_keywords.get("low", [])

        found_keywords = []
        risk_level = "low"

        # Check high risk
        for kw in high_risk:
            if kw.lower() in text:
                found_keywords.append(kw)
                risk_level = "high"

        # Check medium risk
        for kw in medium_risk:
            if kw.lower() in text:
                found_keywords.append(kw)
                if risk_level != "high":
                    risk_level = "medium"

        # Check low risk
        for kw in low_risk:
            if kw.lower() in text:
                found_keywords.append(kw)

        # Check topic risk weight
        topic_risk = 1.0
        if item.topic in ["current_affairs", "brand_comms"]:
            topic_risk = 1.5

        # Calculate score
        if risk_level == "high":
            score = 100 * topic_risk
            reason = f"High risk detected: {', '.join(found_keywords[:3])}"
        elif risk_level == "medium":
            score = 60 * topic_risk
            reason = f"Medium risk: {', '.join(found_keywords[:3])}"
        elif found_keywords:
            score = 30
            reason = f"Low risk signals: {', '.join(found_keywords[:3])}"
        else:
            score = 10
            reason = "No significant risk signals"

        return min(score, 100), reason, risk_level, found_keywords

    def _determine_action(self, breakdown: ScoreBreakdown) -> str:
        """
        Determine suggested comms action based on scores.

        Actions:
        - MONITOR: Watch but no action needed
        - ENGAGE: Opportunity to participate
        - PARTNER: Consider partnership/collaboration
        - AVOID: Stay away, don't engage
        - ESCALATE: Needs immediate attention/decision
        """
        if breakdown.risk_level == "high":
            if breakdown.spotify_adjacency_score >= 80:
                return "escalate"  # High risk + direct relevance
            return "avoid"

        if breakdown.total_score >= 80:
            if breakdown.spotify_adjacency_score >= 70:
                return "engage"
            return "monitor"

        if breakdown.total_score >= 60:
            if breakdown.spotify_adjacency_score >= 80:
                return "partner"
            if breakdown.risk_level == "medium":
                return "monitor"
            return "engage"

        if breakdown.risk_level == "medium":
            return "monitor"

        return "monitor"

    def _determine_confidence(
        self,
        item: TrendItem,
        breakdown: ScoreBreakdown
    ) -> str:
        """
        Determine confidence level in the scoring.

        Based on data quality and source coverage.
        """
        # Check for multiple sources
        sources = item.metadata.get("merged_sources", [item.source])
        source_count = len(sources)

        # Check for rich entity data
        entity_count = sum(len(v) for v in item.entities.values())

        # Check for velocity data
        has_velocity = item.velocity > 0

        confidence_score = 0

        if source_count >= 3:
            confidence_score += 3
        elif source_count >= 2:
            confidence_score += 2
        else:
            confidence_score += 1

        if entity_count >= 3:
            confidence_score += 2
        elif entity_count >= 1:
            confidence_score += 1

        if has_velocity:
            confidence_score += 2

        if item.market:
            confidence_score += 1

        if confidence_score >= 6:
            return "high"
        elif confidence_score >= 3:
            return "medium"
        else:
            return "low"

    def get_priority_level(self, score: float) -> str:
        """Get priority level from score."""
        high = self.thresholds.get("high_priority", 75)
        medium = self.thresholds.get("medium_priority", 50)

        if score >= high:
            return "high"
        elif score >= medium:
            return "medium"
        else:
            return "low"

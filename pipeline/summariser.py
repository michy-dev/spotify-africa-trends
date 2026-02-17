"""
Trend summariser - generates comms-ready summaries for each trend.

Outputs structured summaries optimized for comms decision-making.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List
import structlog

from connectors.base import TrendItem
from .scorer import ScoreBreakdown

logger = structlog.get_logger()


@dataclass
class TrendSummary:
    """
    Comms-ready summary for a trend.

    Designed for quick scanning and decision-making.
    """

    # Identification
    trend_id: str
    title: str

    # Core summary
    whats_happening: str  # 1-2 lines
    why_it_matters: List[str]  # 2 bullets
    suggested_action: str  # Monitor/Engage/Partner/Avoid/Escalate
    if_goes_wrong: str  # 1 bullet

    # Classification
    topic: str
    topic_display: str
    subtopic: Optional[str]
    market: Optional[str]
    language: Optional[str]

    # Scores
    total_score: float
    priority_level: str  # high/medium/low
    risk_level: str  # high/medium/low
    confidence: str  # high/medium/low

    # Score breakdown (for transparency)
    score_breakdown: dict

    # Sources
    sources: List[str]
    source_url: Optional[str]

    # Entities
    key_entities: dict

    # Timestamps
    first_seen: Optional[datetime]
    last_updated: datetime

    def to_dict(self) -> dict:
        """Convert to dictionary for API/storage."""
        return {
            "id": self.trend_id,
            "title": self.title,
            "whats_happening": self.whats_happening,
            "why_it_matters": self.why_it_matters,
            "suggested_action": self.suggested_action,
            "if_goes_wrong": self.if_goes_wrong,
            "topic": self.topic,
            "topic_display": self.topic_display,
            "subtopic": self.subtopic,
            "market": self.market,
            "language": self.language,
            "total_score": self.total_score,
            "priority_level": self.priority_level,
            "risk_level": self.risk_level,
            "confidence": self.confidence,
            "score_breakdown": self.score_breakdown,
            "sources": self.sources,
            "source_url": self.source_url,
            "key_entities": self.key_entities,
            "first_seen": self.first_seen.isoformat() if self.first_seen else None,
            "last_updated": self.last_updated.isoformat(),
        }


class TrendSummariser:
    """
    Generates comms-optimized summaries for trends.

    Each summary includes:
    - What's happening (1-2 lines)
    - Why it matters for Spotify Africa comms (2 bullets)
    - Suggested action
    - "If this goes wrong" scenario
    - Confidence level
    """

    def __init__(self, config: dict):
        """
        Initialize summariser.

        Args:
            config: Application configuration
        """
        self.config = config
        self.topics = config.get("topics", {})

    def summarise_batch(
        self,
        scored_items: List[tuple[TrendItem, ScoreBreakdown]]
    ) -> List[TrendSummary]:
        """
        Generate summaries for a batch of scored items.

        Args:
            scored_items: List of (TrendItem, ScoreBreakdown) tuples

        Returns:
            List of TrendSummary objects
        """
        summaries = []

        for item, breakdown in scored_items:
            summary = self.summarise_item(item, breakdown)
            summaries.append(summary)

        logger.info("summarisation_complete", count=len(summaries))
        return summaries

    def summarise_item(
        self,
        item: TrendItem,
        breakdown: ScoreBreakdown
    ) -> TrendSummary:
        """
        Generate a summary for a single item.

        Args:
            item: TrendItem with enriched data
            breakdown: Score breakdown

        Returns:
            TrendSummary ready for display
        """
        # Generate "What's happening"
        whats_happening = self._generate_whats_happening(item, breakdown)

        # Generate "Why it matters"
        why_it_matters = self._generate_why_it_matters(item, breakdown)

        # Generate "If goes wrong"
        if_goes_wrong = self._generate_risk_scenario(item, breakdown)

        # Get topic display name
        topic_data = self.topics.get(item.topic, {})
        topic_display = topic_data.get("name", item.topic or "Unknown")

        # Determine priority level
        priority_level = self._get_priority_level(breakdown.total_score)

        # Collect sources
        sources = [item.source]
        if "merged_sources" in item.metadata:
            sources = item.metadata["merged_sources"]

        return TrendSummary(
            trend_id=item.id,
            title=item.title,
            whats_happening=whats_happening,
            why_it_matters=why_it_matters,
            suggested_action=breakdown.suggested_action.upper(),
            if_goes_wrong=if_goes_wrong,
            topic=item.topic or "unknown",
            topic_display=topic_display,
            subtopic=item.subtopic,
            market=item.market,
            language=item.language,
            total_score=round(breakdown.total_score, 1),
            priority_level=priority_level,
            risk_level=breakdown.risk_level,
            confidence=breakdown.confidence,
            score_breakdown=breakdown.to_dict(),
            sources=sources,
            source_url=item.source_url,
            key_entities=item.entities,
            first_seen=item.published_at,
            last_updated=datetime.utcnow(),
        )

    def _generate_whats_happening(
        self,
        item: TrendItem,
        breakdown: ScoreBreakdown
    ) -> str:
        """Generate a concise description of what's happening."""
        parts = []

        # Start with the title or a summary
        title = item.title[:100] if item.title else "Unidentified trend"
        parts.append(title)

        # Add context
        context_parts = []

        if item.market:
            context_parts.append(f"trending in {item.market}")

        if breakdown.velocity_score >= 60:
            context_parts.append("gaining momentum")

        if item.entities.get("artists"):
            artists = item.entities["artists"][:2]
            context_parts.append(f"featuring {', '.join(artists)}")

        if context_parts:
            parts.append(f"({', '.join(context_parts)})")

        return " ".join(parts)

    def _generate_why_it_matters(
        self,
        item: TrendItem,
        breakdown: ScoreBreakdown
    ) -> List[str]:
        """Generate 2 bullets on why this matters for Spotify Africa comms."""
        bullets = []

        # First bullet: Relevance to Spotify
        if breakdown.spotify_adjacency_score >= 80:
            if "spotify" in item.title.lower():
                bullets.append(
                    "Direct Spotify mention - requires monitoring for brand impact and response opportunity."
                )
            else:
                bullets.append(
                    "Highly relevant to music/audio culture - strong opportunity for brand alignment."
                )
        elif breakdown.spotify_adjacency_score >= 50:
            artists = item.entities.get("artists", [])
            if artists:
                bullets.append(
                    f"Involves key artists ({', '.join(artists[:2])}) - potential partnership or content opportunity."
                )
            else:
                bullets.append(
                    "Connected to youth culture and entertainment - contextual relevance for campaigns."
                )
        else:
            bullets.append(
                "Tangential to core music audience - monitor for cultural context only."
            )

        # Second bullet: Impact/Risk assessment
        if breakdown.risk_level == "high":
            bullets.append(
                f"HIGH SENSITIVITY: {breakdown.risk_reason}. Avoid association, prepare holding statement."
            )
        elif breakdown.risk_level == "medium":
            bullets.append(
                f"Moderate sensitivity detected. Monitor closely before any engagement."
            )
        else:
            if breakdown.total_score >= 70:
                bullets.append(
                    f"High visibility trend in {item.market or 'multiple markets'}. Good timing for relevant content or creator collaboration."
                )
            elif breakdown.velocity_score >= 60:
                bullets.append(
                    "Rapidly growing - early engagement could establish brand presence."
                )
            else:
                bullets.append(
                    "Standard trend velocity. No urgent action required."
                )

        return bullets[:2]  # Ensure max 2 bullets

    def _generate_risk_scenario(
        self,
        item: TrendItem,
        breakdown: ScoreBreakdown
    ) -> str:
        """Generate the 'if this goes wrong' scenario."""
        # Base risk scenarios by topic
        topic_risks = {
            "current_affairs": "If Spotify appears to take sides or misreads public sentiment, risk of boycott calls or backlash in market.",
            "brand_comms": "Misstep could amplify negative conversation and associate Spotify with controversy.",
            "music_audio": "Artist-related controversy could impact playlist partnerships and creator relations.",
            "culture": "Tone-deaf engagement could damage youth/cultural credibility.",
            "fashion_beauty": "Brand association with wrong influencer could affect perception.",
            "spotify_specific": "Direct brand issue requires immediate comms response; delay increases reputational risk.",
        }

        if breakdown.risk_level == "high":
            keywords = breakdown.risk_keywords_found[:2]
            return f"Serious reputational risk due to {', '.join(keywords)}. Any association could trigger backlash and media scrutiny."

        if breakdown.risk_level == "medium":
            return topic_risks.get(
                item.topic,
                "Engagement without proper context could appear opportunistic or insensitive."
            )

        return topic_risks.get(
            item.topic,
            "Minimal downside if approached authentically. Main risk is appearing irrelevant or late."
        )

    def _get_priority_level(self, score: float) -> str:
        """Determine priority level from score."""
        thresholds = self.config.get("scoring", {}).get("thresholds", {})
        high = thresholds.get("high_priority", 75)
        medium = thresholds.get("medium_priority", 50)

        if score >= high:
            return "high"
        elif score >= medium:
            return "medium"
        else:
            return "low"

    def generate_digest_summary(
        self,
        summaries: List[TrendSummary],
        top_n: int = 10
    ) -> dict:
        """
        Generate a digest-ready summary of trends.

        Returns structured data for the daily digest.
        """
        # Sort by score
        sorted_summaries = sorted(summaries, key=lambda x: x.total_score, reverse=True)

        # Categorize
        top_trends = sorted_summaries[:top_n]
        risks = [s for s in summaries if s.risk_level in ("high", "medium")]
        opportunities = [
            s for s in summaries
            if s.suggested_action in ("ENGAGE", "PARTNER") and s.risk_level == "low"
        ]

        # Market breakdown
        market_counts = {}
        for s in summaries:
            market = s.market or "Unknown"
            market_counts[market] = market_counts.get(market, 0) + 1

        return {
            "generated_at": datetime.utcnow().isoformat(),
            "total_trends": len(summaries),
            "top_trends": [s.to_dict() for s in top_trends],
            "risks": [s.to_dict() for s in risks[:5]],
            "opportunities": [s.to_dict() for s in opportunities[:5]],
            "watchlist": [
                s.to_dict() for s in sorted_summaries
                if s.confidence == "low" and s.total_score >= 50
            ][:10],
            "market_breakdown": market_counts,
            "topic_breakdown": self._get_topic_breakdown(summaries),
        }

    def _get_topic_breakdown(self, summaries: List[TrendSummary]) -> dict:
        """Get trend counts by topic."""
        counts = {}
        for s in summaries:
            topic = s.topic_display
            counts[topic] = counts.get(topic, 0) + 1
        return counts

"""
Topic classifier - assigns topics and subtopics to trend items.
"""

import re
from typing import Optional, Tuple
import structlog

from connectors.base import TrendItem

logger = structlog.get_logger()


class TopicClassifier:
    """
    Classifies trend items into the configured topic taxonomy.

    Uses keyword matching with configurable rules.
    Can be extended with ML-based classification.
    """

    def __init__(self, config: dict):
        """
        Initialize classifier with topic taxonomy.

        Args:
            config: Application configuration with topics
        """
        self.config = config
        self.topics = config.get("topics", {})
        self._compile_patterns()

    def _compile_patterns(self):
        """Compile keyword patterns for each topic."""
        self.topic_patterns = {}

        for topic_key, topic_data in self.topics.items():
            keywords = topic_data.get("keywords", [])
            subtopics = topic_data.get("subtopics", [])

            # Create pattern for topic keywords
            if keywords:
                pattern = re.compile(
                    '|'.join(r'\b' + re.escape(kw) + r'\b' for kw in keywords),
                    re.IGNORECASE
                )
            else:
                pattern = None

            # Store topic data with pattern
            self.topic_patterns[topic_key] = {
                "name": topic_data.get("name", topic_key),
                "pattern": pattern,
                "keywords": keywords,
                "subtopics": subtopics,
                "risk_weight": topic_data.get("risk_weight", 1.0),
                "spotify_adjacency": topic_data.get("spotify_adjacency", 1.0),
            }

    def classify_batch(self, items: list[TrendItem]) -> list[TrendItem]:
        """
        Classify a batch of items.

        Args:
            items: List of TrendItem objects

        Returns:
            Items with topic and subtopic assigned
        """
        classified = []

        for item in items:
            classified.append(self.classify_item(item))

        # Log classification distribution
        topic_counts = {}
        for item in classified:
            topic = item.topic or "unknown"
            topic_counts[topic] = topic_counts.get(topic, 0) + 1

        logger.info(
            "classification_complete",
            count=len(classified),
            distribution=topic_counts
        )

        return classified

    def classify_item(self, item: TrendItem) -> TrendItem:
        """
        Classify a single item.

        Assigns topic and subtopic based on content analysis.
        """
        text = f"{item.title} {item.description} {item.raw_text}".lower()

        # Score each topic
        topic_scores = {}
        for topic_key, topic_data in self.topic_patterns.items():
            score = self._score_topic(text, topic_data, item)
            if score > 0:
                topic_scores[topic_key] = score

        if not topic_scores:
            # Default classification based on source
            item.topic, item.subtopic = self._default_classification(item)
        else:
            # Assign highest scoring topic
            best_topic = max(topic_scores, key=topic_scores.get)
            item.topic = best_topic
            item.subtopic = self._determine_subtopic(text, best_topic)

        # Add classification metadata
        item.metadata["topic_scores"] = topic_scores
        item.metadata["topic_name"] = self.topic_patterns.get(
            item.topic, {}
        ).get("name", item.topic)

        return item

    def _score_topic(
        self,
        text: str,
        topic_data: dict,
        item: TrendItem
    ) -> float:
        """
        Score how well an item matches a topic.

        Returns a score from 0 to 100.
        """
        score = 0

        # Keyword matching
        pattern = topic_data.get("pattern")
        if pattern:
            matches = pattern.findall(text)
            score += len(matches) * 10

        # Entity matching (if item has artist entities, boost music topic)
        if "artists" in item.entities:
            if topic_data.get("name") == "Music & Audio":
                score += len(item.entities["artists"]) * 15

        # Source-based boosting
        source_topic_affinity = {
            "youtube": "music_audio",
            "wikipedia": "music_audio",
            "news_rss": "current_affairs",
            "reddit": "culture",
        }
        if source_topic_affinity.get(item.source) == topic_data.get("name", "").lower().replace(" & ", "_"):
            score += 5

        return min(score, 100)

    def _determine_subtopic(self, text: str, topic_key: str) -> Optional[str]:
        """Determine the most relevant subtopic within a topic."""
        topic_data = self.topic_patterns.get(topic_key, {})
        subtopics = topic_data.get("subtopics", [])

        if not subtopics:
            return None

        # Simple keyword matching for subtopics
        subtopic_keywords = {
            # Music & Audio subtopics
            "artists": ["artist", "singer", "rapper", "musician", "dj"],
            "genres": ["afrobeats", "amapiano", "gqom", "hip hop", "rap", "r&b", "genre"],
            "songs": ["song", "track", "single", "hit"],
            "playlists": ["playlist", "mix", "compilation"],
            "live_events": ["concert", "festival", "tour", "show", "performance", "live"],
            "streaming_moments": ["stream", "views", "plays", "viral"],
            "industry_issues": ["label", "contract", "royalties", "industry"],

            # Culture subtopics
            "memes": ["meme", "viral", "trending"],
            "youth_culture": ["gen z", "youth", "young"],
            "identity": ["identity", "pride", "community"],
            "tv_film": ["nollywood", "movie", "film", "series", "show"],
            "sport": ["football", "soccer", "afcon", "sport", "player"],
            "internet_slang": ["slang", "lingo"],

            # Fashion/beauty subtopics
            "drops": ["drop", "release", "launch", "new"],
            "designers": ["designer", "design", "fashion"],
            "runway": ["runway", "fashion week", "model"],
            "streetwear": ["streetwear", "street style"],
            "beauty_trends": ["makeup", "beauty", "skincare"],

            # Current affairs subtopics
            "elections": ["election", "vote", "ballot", "campaign"],
            "protests": ["protest", "demonstration", "march"],
            "conflict": ["conflict", "war", "violence", "attack"],
            "public_safety": ["safety", "security", "crime"],
            "policy": ["policy", "law", "government", "minister"],

            # Brand/comms subtopics
            "trust_safety": ["trust", "safety", "policy"],
            "misinformation": ["fake", "misinformation", "disinformation"],
            "creator_economy": ["creator", "influencer", "monetization"],
            "ai_debates": ["ai", "artificial intelligence", "deepfake"],
            "sponsorship": ["sponsor", "partnership", "brand deal"],

            # Spotify subtopics
            "mentions": ["spotify"],
            "competitors": ["apple music", "boomplay", "audiomack", "youtube music"],
            "features": ["wrapped", "blend", "discover weekly"],
            "app_issues": ["bug", "crash", "issue", "not working"],
            "pricing": ["price", "premium", "subscription", "free"],
            "partnerships": ["partner", "collab", "deal"],
            "artist_relations": ["artist", "label", "release"],
        }

        # Find matching subtopic
        for subtopic in subtopics:
            keywords = subtopic_keywords.get(subtopic, [subtopic.replace("_", " ")])
            for keyword in keywords:
                if keyword in text:
                    return subtopic

        return subtopics[0] if subtopics else None

    def _default_classification(self, item: TrendItem) -> Tuple[str, Optional[str]]:
        """
        Provide default classification based on source.
        """
        source_defaults = {
            "google_trends": ("culture", "memes"),
            "youtube": ("music_audio", "streaming_moments"),
            "news_rss": ("current_affairs", None),
            "reddit": ("culture", None),
            "twitter": ("culture", "memes"),
            "wikipedia": ("music_audio", "artists"),
        }

        return source_defaults.get(item.source, ("culture", None))

    def get_topic_info(self, topic_key: str) -> dict:
        """Get information about a topic."""
        return self.topic_patterns.get(topic_key, {})

    def get_risk_weight(self, topic_key: str) -> float:
        """Get the risk weight for a topic."""
        return self.topic_patterns.get(topic_key, {}).get("risk_weight", 1.0)

    def get_spotify_adjacency(self, topic_key: str) -> float:
        """Get the Spotify adjacency score for a topic."""
        return self.topic_patterns.get(topic_key, {}).get("spotify_adjacency", 1.0)

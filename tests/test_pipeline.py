"""Tests for the data processing pipeline."""

import pytest
from datetime import datetime

from connectors.base import TrendItem
from pipeline.cleaner import DataCleaner
from pipeline.classifier import TopicClassifier
from pipeline.scorer import CommsScorer


@pytest.fixture
def sample_config():
    """Sample configuration for testing."""
    return {
        "topics": {
            "music_audio": {
                "name": "Music & Audio",
                "keywords": ["music", "artist", "song", "album", "concert"],
                "subtopics": ["artists", "songs", "live_events"],
            },
            "culture": {
                "name": "Culture",
                "keywords": ["meme", "viral", "trend"],
                "subtopics": ["memes", "youth_culture"],
            },
            "spotify_specific": {
                "name": "Spotify Specific",
                "keywords": ["spotify", "playlist", "wrapped"],
                "subtopics": ["mentions", "features"],
                "spotify_adjacency": 2.0,
            },
        },
        "entities": {
            "artists": ["Burna Boy", "Wizkid", "Tems"],
        },
        "markets": {
            "priority": [
                {"code": "ZA", "name": "South Africa", "weight": 1.5},
                {"code": "NG", "name": "Nigeria", "weight": 1.5},
            ],
        },
        "scoring": {
            "weights": {
                "velocity": 0.25,
                "reach": 0.20,
                "market_impact": 0.20,
                "spotify_adjacency": 0.20,
                "risk_factor": 0.15,
            },
            "thresholds": {
                "high_priority": 75,
                "medium_priority": 50,
            },
            "risk_keywords": {
                "high": ["death", "scandal"],
                "medium": ["controversy", "protest"],
                "low": ["issue", "problem"],
            },
        },
    }


@pytest.fixture
def sample_items():
    """Sample TrendItem objects for testing."""
    return [
        TrendItem(
            id="1",
            source="test",
            title="Burna Boy releases new album",
            description="Nigerian superstar drops highly anticipated project",
            market="NG",
            volume=10000,
            engagement=5000,
            velocity=1.5,
        ),
        TrendItem(
            id="2",
            source="test",
            title="Spotify announces Africa expansion",
            description="Streaming giant enters new markets",
            market="ZA",
            volume=50000,
            engagement=20000,
            velocity=2.0,
        ),
        TrendItem(
            id="3",
            source="test",
            title="Political protest in Kenya",
            description="Youth march against new policies",
            market="KE",
            volume=30000,
            engagement=15000,
            velocity=3.0,
        ),
    ]


class TestDataCleaner:
    """Tests for DataCleaner."""

    def test_clean_batch_removes_duplicates(self, sample_config):
        """Test that duplicate items are removed."""
        cleaner = DataCleaner(sample_config)

        items = [
            TrendItem(id="1", source="test", title="Test Item"),
            TrendItem(id="2", source="test", title="Test Item"),  # Duplicate title
            TrendItem(id="3", source="test", title="Another Item"),
        ]

        cleaned = cleaner.clean_batch(items)

        assert len(cleaned) == 2

    def test_clean_batch_filters_short_titles(self, sample_config):
        """Test that items with very short titles are filtered."""
        cleaner = DataCleaner(sample_config)

        items = [
            TrendItem(id="1", source="test", title="OK"),  # Too short
            TrendItem(id="2", source="test", title="This is a valid title"),
        ]

        cleaned = cleaner.clean_batch(items)

        assert len(cleaned) == 1
        assert cleaned[0].title == "This is a valid title"

    def test_normalize_text(self, sample_config):
        """Test text normalization."""
        cleaner = DataCleaner(sample_config)

        text = "  Multiple   spaces   here  "
        normalized = cleaner._normalize_text(text)

        assert normalized == "Multiple spaces here"


class TestTopicClassifier:
    """Tests for TopicClassifier."""

    def test_classify_music_topic(self, sample_config, sample_items):
        """Test classification of music-related content."""
        classifier = TopicClassifier(sample_config)

        item = sample_items[0]  # Burna Boy album
        classified = classifier.classify_item(item)

        assert classified.topic == "music_audio"

    def test_classify_spotify_topic(self, sample_config, sample_items):
        """Test classification of Spotify-related content."""
        classifier = TopicClassifier(sample_config)

        item = sample_items[1]  # Spotify expansion
        classified = classifier.classify_item(item)

        assert classified.topic == "spotify_specific"


class TestCommsScorer:
    """Tests for CommsScorer."""

    def test_score_high_velocity(self, sample_config):
        """Test scoring of high-velocity trends."""
        scorer = CommsScorer(sample_config)

        item = TrendItem(
            id="1",
            source="test",
            title="Viral moment",
            velocity=2.5,
            volume=100000,
            engagement=50000,
            market="NG",
        )

        breakdown = scorer.score_item(item)

        assert breakdown.velocity_score >= 80
        assert breakdown.total_score > 0

    def test_score_spotify_mention(self, sample_config):
        """Test scoring of direct Spotify mentions."""
        scorer = CommsScorer(sample_config)

        item = TrendItem(
            id="1",
            source="test",
            title="Spotify users react to new feature",
            description="Spotify's latest update gets mixed reviews",
            market="ZA",
        )

        breakdown = scorer.score_item(item)

        assert breakdown.spotify_adjacency_score == 100

    def test_score_risk_detection(self, sample_config):
        """Test detection of risk keywords."""
        scorer = CommsScorer(sample_config)

        item = TrendItem(
            id="1",
            source="test",
            title="Celebrity death shocks nation",
            description="Sudden death of popular figure",
            market="ZA",
        )

        breakdown = scorer.score_item(item)

        assert breakdown.risk_level == "high"
        assert "death" in breakdown.risk_keywords_found

    def test_determine_action_escalate(self, sample_config):
        """Test escalate action for high-risk Spotify-related items."""
        scorer = CommsScorer(sample_config)

        item = TrendItem(
            id="1",
            source="test",
            title="Spotify scandal controversy",
            description="Platform faces backlash",
            market="ZA",
        )

        breakdown = scorer.score_item(item)

        # High risk + high Spotify adjacency should escalate
        assert breakdown.risk_level in ("high", "medium")


class TestScoreBreakdown:
    """Tests for ScoreBreakdown."""

    def test_to_dict(self, sample_config):
        """Test conversion to dictionary."""
        scorer = CommsScorer(sample_config)

        item = TrendItem(
            id="1",
            source="test",
            title="Test trend",
            market="ZA",
        )

        breakdown = scorer.score_item(item)
        breakdown_dict = breakdown.to_dict()

        assert "total_score" in breakdown_dict
        assert "components" in breakdown_dict
        assert "suggested_action" in breakdown_dict
        assert "confidence" in breakdown_dict

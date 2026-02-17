"""
Unit tests for trend-jack intelligence modules.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

from storage.base import (
    ArtistSpike, CultureSearch, StyleSignal, PitchCard,
    DataHealth, UserSession, SensitivityTag, ModuleStatus
)
from monitoring.health import DataHealthMonitor, FRESHNESS_THRESHOLDS
from monitoring.risk_validator import RiskFactorValidator, RISK_SCORE_RANGES
from pipeline.pitch_generator import (
    PitchCardGenerator, calculate_confidence, generate_card_id
)


class TestArtistSpike:
    """Tests for ArtistSpike data model."""

    def test_create_artist_spike(self):
        spike = ArtistSpike(
            id="test123",
            artist_name="Burna Boy",
            market="NG",
            spike_score=75.5,
            time_window="24h",
            sparkline_data=[10, 20, 30, 50, 70, 80, 90],
            why_spiking=["New album release", "Award nomination"],
            confidence="high",
            is_ambiguous=False,
        )

        assert spike.id == "test123"
        assert spike.artist_name == "Burna Boy"
        assert spike.market == "NG"
        assert spike.spike_score == 75.5
        assert len(spike.sparkline_data) == 7
        assert spike.confidence == "high"
        assert not spike.is_ambiguous

    def test_artist_spike_to_dict(self):
        spike = ArtistSpike(
            id="test456",
            artist_name="Wizkid",
            market="NG",
            spike_score=60.0,
            time_window="7d",
        )

        data = spike.to_dict()

        assert data["id"] == "test456"
        assert data["artist_name"] == "Wizkid"
        assert data["spike_score"] == 60.0
        assert "collected_at" in data


class TestCultureSearch:
    """Tests for CultureSearch data model."""

    def test_create_culture_search(self):
        search = CultureSearch(
            id="culture123",
            term="afrobeats award",
            market="NG",
            sensitivity_tag=SensitivityTag.MUSIC.value,
            rise_percentage=250.0,
            volume=5000,
            is_cross_market=True,
            markets_present=["NG", "GH", "KE"],
        )

        assert search.term == "afrobeats award"
        assert search.sensitivity_tag == "music"
        assert search.rise_percentage == 250.0
        assert search.is_cross_market
        assert "GH" in search.markets_present

    def test_politics_high_risk(self):
        search = CultureSearch(
            id="politics123",
            term="election protest",
            market="NG",
            sensitivity_tag=SensitivityTag.POLITICS.value,
            rise_percentage=500.0,
            risk_level="high",
        )

        assert search.sensitivity_tag == "politics"
        assert search.risk_level == "high"


class TestStyleSignal:
    """Tests for StyleSignal data model."""

    def test_create_style_signal(self):
        signal = StyleSignal(
            id="style123",
            headline="Burna Boy launches fashion collaboration with major brand",
            source="Highsnobiety",
            source_url="https://example.com/article",
            summary="Nigerian superstar announces streetwear collection",
            publish_date=datetime.utcnow(),
            country_relevance=["NG", "GH"],
            spotify_tags=["artist_collab", "youth_culture"],
            risk_level="low",
        )

        assert signal.source == "Highsnobiety"
        assert "NG" in signal.country_relevance
        assert "artist_collab" in signal.spotify_tags

    def test_style_signal_to_dict(self):
        signal = StyleSignal(
            id="style456",
            headline="Test headline",
            source="Test Source",
            source_url="https://test.com",
            summary="Test summary",
            publish_date=datetime.utcnow(),
        )

        data = signal.to_dict()

        assert data["headline"] == "Test headline"
        assert "publish_date" in data


class TestPitchCard:
    """Tests for PitchCard data model."""

    def test_create_pitch_card(self):
        card = PitchCard(
            id="pitch123",
            market="NG",
            hook="Burna Boy trending in Nigeria searches",
            why_now=["New album buzz", "Award season"],
            spotify_angle="Update Fresh Finds playlist",
            next_steps=["Contact artist team", "Check streaming data"],
            risks=["Verify no recent controversies"],
            confidence="high",
            source_signals=["spike123", "culture456"],
        )

        assert card.market == "NG"
        assert card.confidence == "high"
        assert len(card.why_now) == 2
        assert len(card.source_signals) == 2

    def test_pitch_card_expiry(self):
        now = datetime.utcnow()
        card = PitchCard(
            id="pitch456",
            market="KE",
            hook="Test hook",
            why_now=[],
            spotify_angle="Test angle",
            next_steps=[],
            risks=[],
            confidence="medium",
            expires_at=now + timedelta(hours=24),
        )

        assert card.expires_at > now


class TestRiskValidator:
    """Tests for RiskFactorValidator."""

    def test_validate_risk_level_valid(self):
        validator = RiskFactorValidator()

        result = validator.validate_risk_level("high")
        assert result.is_valid

        result = validator.validate_risk_level("medium")
        assert result.is_valid

        result = validator.validate_risk_level("low")
        assert result.is_valid

    def test_validate_risk_level_invalid(self):
        validator = RiskFactorValidator()

        result = validator.validate_risk_level("extreme")
        assert not result.is_valid
        assert result.severity == "error"

    def test_validate_risk_score_valid(self):
        validator = RiskFactorValidator()

        result = validator.validate_risk_score(50)
        assert result.is_valid

        result = validator.validate_risk_score(0)
        assert result.is_valid

        result = validator.validate_risk_score(100)
        assert result.is_valid

    def test_validate_risk_score_invalid(self):
        validator = RiskFactorValidator()

        result = validator.validate_risk_score(-10)
        assert not result.is_valid

        result = validator.validate_risk_score(150)
        assert not result.is_valid

    def test_validate_risk_consistency(self):
        validator = RiskFactorValidator()

        # Valid: low risk with score in low range
        result = validator.validate_risk_consistency("low", 20)
        assert result.is_valid

        # Valid: high risk with score in high range
        result = validator.validate_risk_consistency("high", 80)
        assert result.is_valid

        # Invalid: low risk with high score
        result = validator.validate_risk_consistency("low", 90)
        assert not result.is_valid
        assert result.severity == "warning"

    def test_get_risk_level_for_score(self):
        validator = RiskFactorValidator()

        assert validator.get_risk_level_for_score(10) == "low"
        assert validator.get_risk_level_for_score(50) == "medium"
        assert validator.get_risk_level_for_score(80) == "high"

    def test_validate_freshness(self):
        validator = RiskFactorValidator()

        # Fresh data
        recent = datetime.utcnow() - timedelta(hours=2)
        result = validator.validate_freshness(recent, threshold_hours=24)
        assert result.is_valid

        # Stale data
        old = datetime.utcnow() - timedelta(hours=48)
        result = validator.validate_freshness(old, threshold_hours=24)
        assert not result.is_valid
        assert result.severity == "warning"

    def test_validate_batch(self):
        validator = RiskFactorValidator()

        trends = [
            {
                "id": "1",
                "risk_level": "high",
                "risk_score": 85,
                "last_updated": datetime.utcnow().isoformat(),
            },
            {
                "id": "2",
                "risk_level": "low",
                "risk_score": 90,  # Inconsistent
                "last_updated": datetime.utcnow().isoformat(),
            },
        ]

        result = validator.validate_batch(trends)

        assert result["total_trends"] == 2
        assert result["warnings"] > 0  # Inconsistency warning

    def test_get_risk_badge_class(self):
        validator = RiskFactorValidator()

        assert "red" in validator.get_risk_badge_class("high")
        assert "amber" in validator.get_risk_badge_class("medium")
        assert "green" in validator.get_risk_badge_class("low")


class TestPitchCardGenerator:
    """Tests for PitchCardGenerator."""

    def test_calculate_confidence(self):
        # High confidence: multiple signals, high spikes, multiple sources
        confidence = calculate_confidence(
            signal_count=3,
            spike_scores=[70, 80],
            has_multiple_sources=True
        )
        assert confidence == "high"

        # Medium confidence: decent signals
        confidence = calculate_confidence(
            signal_count=2,
            spike_scores=[50],
            has_multiple_sources=False
        )
        assert confidence == "medium"

        # Low confidence: few signals
        confidence = calculate_confidence(
            signal_count=1,
            spike_scores=[20],
            has_multiple_sources=False
        )
        assert confidence == "low"

    def test_generate_card_id(self):
        id1 = generate_card_id("NG", ["signal1", "signal2"])
        id2 = generate_card_id("NG", ["signal1", "signal2"])
        id3 = generate_card_id("KE", ["signal1", "signal2"])

        # Same inputs should generate same ID (for same day)
        assert id1 == id2
        # Different market should generate different ID
        assert id1 != id3

    @pytest.mark.asyncio
    async def test_generate_cards_empty(self):
        generator = PitchCardGenerator()

        cards = await generator.generate_cards(
            artist_spikes=[],
            culture_searches=[],
            style_signals=[],
            markets=["NG"]
        )

        assert cards == []

    @pytest.mark.asyncio
    async def test_generate_cards_with_spike(self):
        generator = PitchCardGenerator()

        spike = ArtistSpike(
            id="spike123",
            artist_name="Test Artist",
            market="NG",
            spike_score=75,
            time_window="24h",
            why_spiking=["Album release"],
            confidence="high",
        )

        cards = await generator.generate_cards(
            artist_spikes=[spike],
            culture_searches=[],
            style_signals=[],
            markets=["NG"]
        )

        assert len(cards) >= 1
        assert cards[0].market == "NG"
        assert "Test Artist" in cards[0].hook

    @pytest.mark.asyncio
    async def test_cards_per_market_limit(self):
        generator = PitchCardGenerator({"cards_per_market": 3})

        spikes = [
            ArtistSpike(
                id=f"spike{i}",
                artist_name=f"Artist {i}",
                market="NG",
                spike_score=50 + i * 5,
                time_window="24h",
                why_spiking=["Trending"],
                confidence="medium",
            )
            for i in range(10)
        ]

        cards = await generator.generate_cards(
            artist_spikes=spikes,
            culture_searches=[],
            style_signals=[],
            markets=["NG"]
        )

        assert len(cards) <= 3


class TestDataHealthMonitor:
    """Tests for DataHealthMonitor."""

    @pytest.mark.asyncio
    async def test_check_module_ok(self):
        # Mock storage
        storage = MagicMock()
        storage.get_artist_spikes = AsyncMock(return_value=[
            ArtistSpike(
                id="spike1",
                artist_name="Test",
                market="NG",
                spike_score=50,
                time_window="24h",
                collected_at=datetime.utcnow(),
            )
        ])

        monitor = DataHealthMonitor(storage)
        health = await monitor.check_module("artist_spikes")

        assert health.status == ModuleStatus.OK.value
        assert health.item_count == 1

    @pytest.mark.asyncio
    async def test_check_module_degraded(self):
        # Mock storage with old data
        storage = MagicMock()
        old_time = datetime.utcnow() - timedelta(hours=10)
        storage.get_artist_spikes = AsyncMock(return_value=[
            ArtistSpike(
                id="spike1",
                artist_name="Test",
                market="NG",
                spike_score=50,
                time_window="24h",
                collected_at=old_time,
            )
        ])

        monitor = DataHealthMonitor(storage)
        health = await monitor.check_module("artist_spikes")

        assert health.status == ModuleStatus.DEGRADED.value

    @pytest.mark.asyncio
    async def test_check_module_down(self):
        # Mock storage with exception
        storage = MagicMock()
        storage.get_artist_spikes = AsyncMock(side_effect=Exception("DB error"))

        monitor = DataHealthMonitor(storage)
        health = await monitor.check_module("artist_spikes")

        assert health.status == ModuleStatus.DOWN.value
        assert health.last_error is not None


class TestDataModels:
    """Tests for data model edge cases."""

    def test_data_health_to_dict(self):
        health = DataHealth(
            module_name="test_module",
            status="ok",
            last_success=datetime.utcnow(),
            item_count=100,
            freshness_hours=2.5,
        )

        data = health.to_dict()

        assert data["module_name"] == "test_module"
        assert data["status"] == "ok"
        assert data["item_count"] == 100

    def test_user_session(self):
        session = UserSession(
            session_id="sess123",
            created_at=datetime.utcnow(),
            expires_at=datetime.utcnow() + timedelta(hours=24),
            is_valid=True,
        )

        assert session.is_valid
        assert session.expires_at > session.created_at

    def test_sensitivity_tag_enum(self):
        assert SensitivityTag.MUSIC.value == "music"
        assert SensitivityTag.POLITICS.value == "politics"
        assert SensitivityTag.FASHION.value == "fashion"

    def test_module_status_enum(self):
        assert ModuleStatus.OK.value == "ok"
        assert ModuleStatus.DEGRADED.value == "degraded"
        assert ModuleStatus.DOWN.value == "down"


class TestSpikeScoreCalculation:
    """Tests for spike score calculation logic."""

    def test_spike_score_ranges(self):
        from connectors.artist_spikes import calculate_spike_score

        # Zero baseline
        score = calculate_spike_score(100, 0)
        assert 0 <= score <= 100

        # Normal growth
        score = calculate_spike_score(150, 100)
        assert score == 50  # 50% increase

        # Large growth (capped at 100)
        score = calculate_spike_score(300, 100)
        assert score == 100

        # No change
        score = calculate_spike_score(100, 100)
        assert score == 0


class TestCultureSearchClassification:
    """Tests for culture search classification."""

    def test_sensitivity_classification(self):
        from connectors.culture_search import classify_sensitivity

        assert classify_sensitivity("new album release") == "music"
        assert classify_sensitivity("premier league match") == "sport"
        assert classify_sensitivity("fashion week lagos") == "fashion"
        assert classify_sensitivity("president speech") == "politics"
        assert classify_sensitivity("viral challenge tiktok") == "meme"

    def test_risk_level_determination(self):
        from connectors.culture_search import determine_risk_level

        assert determine_risk_level("protest march", "politics") == "high"
        assert determine_risk_level("new song", "music") == "low"
        assert determine_risk_level("celebrity scandal", "celebrity") == "high"  # scandal is high risk
        assert determine_risk_level("celebrity drama", "celebrity") == "medium"  # drama is medium risk
        assert determine_risk_level("artist beef", "music") == "medium"  # beef is medium risk


class TestStyleSignalDetection:
    """Tests for style signal detection."""

    def test_country_relevance_detection(self):
        from connectors.style_signals import detect_country_relevance

        text = "Nigerian designer showcases at Lagos Fashion Week"
        relevance = detect_country_relevance(text)
        assert "NG" in relevance

        text = "South African amapiano artists collaborate"
        relevance = detect_country_relevance(text)
        assert "ZA" in relevance

        text = "African fashion rising globally"
        relevance = detect_country_relevance(text)
        assert len(relevance) == 4  # All markets

    def test_spotify_tag_detection(self):
        from connectors.style_signals import detect_spotify_tags

        text = "Artist collaboration with fashion brand launches collection"
        tags = detect_spotify_tags(text)
        assert "artist_collab" in tags

        text = "Gen Z viral streetwear trend"
        tags = detect_spotify_tags(text)
        assert "youth_culture" in tags or "streetwear" in tags

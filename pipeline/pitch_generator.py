"""
Pitch Card Generator for trend-jack opportunities.

Combines signals from artist spikes, culture searches, and style signals
to generate actionable pitch cards for comms teams.
"""

import hashlib
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import structlog

from storage.base import PitchCard, ArtistSpike, CultureSearch, StyleSignal

logger = structlog.get_logger()

# Spotify angles templates
SPOTIFY_ANGLES = {
    "playlist": [
        "Create a themed playlist featuring {artist} and similar artists",
        "Update existing market playlists to highlight this trending moment",
        "Pitch for Fresh Finds or New Music Friday placement",
    ],
    "editorial": [
        "Draft editorial piece on the cultural moment",
        "Coordinate with editorial team for homepage feature",
        "Prepare artist spotlight content",
    ],
    "podcast": [
        "Pitch to relevant Spotify podcast partners",
        "Coordinate with podcast team for episode tie-in",
        "Explore creator podcast collaboration opportunity",
    ],
    "ftr": [
        "Prepare For The Record pitch on this moment",
        "Draft social content for Spotify Africa channels",
        "Coordinate with social team for timely post",
    ],
    "creator": [
        "Identify relevant creators for collaboration",
        "Coordinate with creator partnerships team",
        "Explore UGC campaign opportunity",
    ],
    "partnership": [
        "Explore brand partnership opportunity",
        "Coordinate with partnerships team",
        "Draft partnership pitch deck talking points",
    ],
}

# Next steps templates
NEXT_STEPS_TEMPLATES = {
    "artist_spike": [
        "Reach out to {artist}'s team/label for comment",
        "Check internal data for streaming metrics",
        "Coordinate with artist relations if applicable",
    ],
    "culture_moment": [
        "Monitor conversation for 24-48 hours",
        "Prepare reactive social content if appropriate",
        "Brief relevant market leads",
    ],
    "style_opportunity": [
        "Share with brand partnerships team",
        "Identify relevant creator partnerships",
        "Prepare brief for potential merch collaboration",
    ],
}

# Risk templates
RISK_TEMPLATES = {
    "high_visibility": "High visibility moment - ensure all content is reviewed before posting",
    "political_adjacent": "Political sensitivity - route through legal/policy review",
    "artist_controversy": "Artist may have recent controversy - verify before engagement",
    "competitor_mention": "Competitor context - avoid direct comparisons",
    "ambiguous_name": "Ambiguous search term - verify this is actually the artist",
    "stale_data": "Data may be outdated - verify current relevance before acting",
}


def generate_card_id(market: str, signals: List[str]) -> str:
    """Generate unique ID for pitch card."""
    key = f"{market}:{':'.join(sorted(signals))}:{datetime.utcnow().date()}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def calculate_confidence(
    signal_count: int,
    spike_scores: List[float],
    has_multiple_sources: bool
) -> str:
    """Calculate confidence level for pitch card."""
    if signal_count >= 3 and has_multiple_sources:
        avg_spike = sum(spike_scores) / len(spike_scores) if spike_scores else 0
        if avg_spike >= 50:
            return "high"
        return "medium"
    elif signal_count >= 2 or (spike_scores and max(spike_scores) >= 70):
        return "medium"
    return "low"


class PitchCardGenerator:
    """
    Generates trend-jack pitch cards by combining signals.

    Rules:
    - No invented partners or talent (use placeholders)
    - No claimed Spotify internal stats unless in existing data
    - 3-6 cards per country per day
    """

    def __init__(self, config: dict = None):
        self.config = config or {}
        self.cards_per_market = self.config.get("cards_per_market", 6)

    async def generate_cards(
        self,
        artist_spikes: List[ArtistSpike],
        culture_searches: List[CultureSearch],
        style_signals: List[StyleSignal],
        markets: List[str] = None,
    ) -> List[PitchCard]:
        """
        Generate pitch cards from combined signals.

        Args:
            artist_spikes: Recent artist spike data
            culture_searches: Recent culture search data
            style_signals: Recent style signal data
            markets: Markets to generate cards for

        Returns:
            List of generated PitchCard objects
        """
        if markets is None:
            markets = ["NG", "KE", "GH", "ZA"]

        all_cards = []

        for market in markets:
            cards = await self._generate_market_cards(
                market,
                artist_spikes,
                culture_searches,
                style_signals,
            )
            all_cards.extend(cards)

        return all_cards

    async def _generate_market_cards(
        self,
        market: str,
        artist_spikes: List[ArtistSpike],
        culture_searches: List[CultureSearch],
        style_signals: List[StyleSignal],
    ) -> List[PitchCard]:
        """Generate pitch cards for a single market."""
        cards = []

        # Filter data for this market
        market_spikes = [s for s in artist_spikes if s.market == market and s.spike_score >= 30]
        market_culture = [c for c in culture_searches if c.market == market]
        market_style = [s for s in style_signals if market in s.country_relevance]

        # Generate cards from different signal combinations

        # 1. Artist spike + culture context
        for spike in market_spikes[:3]:
            # Find related culture searches
            related_culture = self._find_related_culture(spike, market_culture)

            card = self._create_artist_spike_card(spike, related_culture, market)
            if card:
                cards.append(card)

        # 2. Culture moment cards
        for culture in market_culture[:2]:
            if culture.sensitivity_tag != "politics" and culture.risk_level != "high":
                card = self._create_culture_moment_card(culture, market_style, market)
                if card:
                    cards.append(card)

        # 3. Style opportunity cards
        for style in market_style[:2]:
            if style.spotify_tags and style.risk_level != "high":
                card = self._create_style_opportunity_card(style, market_spikes, market)
                if card:
                    cards.append(card)

        # Deduplicate and limit
        seen_hooks = set()
        unique_cards = []
        for card in cards:
            hook_key = card.hook[:50].lower()
            if hook_key not in seen_hooks:
                seen_hooks.add(hook_key)
                unique_cards.append(card)

        return unique_cards[:self.cards_per_market]

    def _find_related_culture(
        self,
        spike: ArtistSpike,
        culture_searches: List[CultureSearch]
    ) -> List[CultureSearch]:
        """Find culture searches related to an artist spike."""
        related = []
        artist_lower = spike.artist_name.lower()

        for culture in culture_searches:
            term_lower = culture.term.lower()
            # Check for artist name in culture term or related topics
            if artist_lower in term_lower:
                related.append(culture)
                continue
            # Check for music/entertainment tags that might be related
            if culture.sensitivity_tag in ["music", "celebrity"]:
                for topic in spike.related_topics:
                    if topic.lower() in term_lower:
                        related.append(culture)
                        break

        return related[:3]

    def _create_artist_spike_card(
        self,
        spike: ArtistSpike,
        related_culture: List[CultureSearch],
        market: str
    ) -> Optional[PitchCard]:
        """Create pitch card from artist spike."""

        # Generate hook
        if spike.spike_score >= 70:
            hook = f"{spike.artist_name} search interest surging in {market}"
        elif spike.spike_score >= 50:
            hook = f"{spike.artist_name} trending in {market} searches"
        else:
            hook = f"Rising interest in {spike.artist_name} in {market}"

        # Why now bullets
        why_now = []
        for reason in spike.why_spiking[:2]:
            why_now.append(reason)
        if related_culture:
            why_now.append(f"Coincides with '{related_culture[0].term}' trending")
        if not why_now:
            why_now.append(f"Search spike score: {spike.spike_score:.0f}/100")

        # Spotify angle
        if "album" in " ".join(spike.related_topics).lower():
            angle = "New music moment - " + SPOTIFY_ANGLES["playlist"][0].format(artist=spike.artist_name)
        elif "tour" in " ".join(spike.related_topics).lower():
            angle = "Live moment - " + SPOTIFY_ANGLES["editorial"][0]
        else:
            angle = SPOTIFY_ANGLES["ftr"][0]

        # Next steps
        next_steps = [
            NEXT_STEPS_TEMPLATES["artist_spike"][0].format(artist=spike.artist_name),
            NEXT_STEPS_TEMPLATES["artist_spike"][1],
        ]

        # Risks
        risks = []
        if spike.is_ambiguous:
            risks.append(RISK_TEMPLATES["ambiguous_name"])
        if spike.confidence == "low":
            risks.append(RISK_TEMPLATES["stale_data"])
        if not risks:
            risks.append(RISK_TEMPLATES["high_visibility"])

        # Confidence
        confidence = calculate_confidence(
            len(why_now),
            [spike.spike_score],
            len(related_culture) > 0
        )

        return PitchCard(
            id=generate_card_id(market, [spike.id]),
            market=market,
            hook=hook,
            why_now=why_now,
            spotify_angle=angle,
            next_steps=next_steps,
            risks=risks[:2],
            confidence=confidence,
            source_signals=[spike.id],
            generated_at=datetime.utcnow(),
            expires_at=datetime.utcnow() + timedelta(hours=24),
        )

    def _create_culture_moment_card(
        self,
        culture: CultureSearch,
        style_signals: List[StyleSignal],
        market: str
    ) -> Optional[PitchCard]:
        """Create pitch card from culture moment."""

        # Generate hook
        tag_display = culture.sensitivity_tag.replace("_", "/").title()
        hook = f"'{culture.term}' trending in {market} ({tag_display})"

        # Why now
        why_now = [
            f"Rise: +{culture.rise_percentage:.0f}% in search interest",
        ]
        if culture.is_cross_market:
            why_now.append(f"Also trending in: {', '.join(culture.markets_present)}")

        # Find related style content
        related_style = [s for s in style_signals if culture.term.lower() in s.headline.lower()]
        if related_style:
            why_now.append(f"Fashion coverage: {related_style[0].source}")

        # Spotify angle based on tag
        if culture.sensitivity_tag == "music":
            angle = SPOTIFY_ANGLES["playlist"][1]
        elif culture.sensitivity_tag == "celebrity":
            angle = SPOTIFY_ANGLES["editorial"][1]
        elif culture.sensitivity_tag == "meme":
            angle = SPOTIFY_ANGLES["creator"][0]
        else:
            angle = SPOTIFY_ANGLES["ftr"][1]

        # Next steps
        next_steps = [
            NEXT_STEPS_TEMPLATES["culture_moment"][0],
            NEXT_STEPS_TEMPLATES["culture_moment"][2],
        ]

        # Risks
        risks = []
        if culture.risk_level == "medium":
            risks.append(RISK_TEMPLATES["high_visibility"])
        if culture.sensitivity_tag == "celebrity":
            risks.append(RISK_TEMPLATES["artist_controversy"])
        if not risks:
            risks.append("Monitor for 24h before major engagement")

        # Confidence
        confidence = calculate_confidence(
            len(why_now),
            [],
            culture.is_cross_market
        )

        return PitchCard(
            id=generate_card_id(market, [culture.id]),
            market=market,
            hook=hook,
            why_now=why_now,
            spotify_angle=angle,
            next_steps=next_steps,
            risks=risks[:2],
            confidence=confidence,
            source_signals=[culture.id],
            generated_at=datetime.utcnow(),
            expires_at=datetime.utcnow() + timedelta(hours=24),
        )

    def _create_style_opportunity_card(
        self,
        style: StyleSignal,
        artist_spikes: List[ArtistSpike],
        market: str
    ) -> Optional[PitchCard]:
        """Create pitch card from style opportunity."""

        # Generate hook
        hook = f"Style moment: {style.headline[:60]}..."

        # Why now
        why_now = [
            f"Source: {style.source}",
        ]

        # Add Spotify tags context
        tag_descriptions = {
            "artist_collab": "Artist collaboration angle",
            "tour_merch": "Tour/merch opportunity",
            "youth_culture": "Youth culture relevance",
            "music_fashion": "Music-fashion crossover",
            "streetwear": "Streetwear/sneaker culture",
            "african_designer": "African designer spotlight",
        }
        for tag in style.spotify_tags[:2]:
            if tag in tag_descriptions:
                why_now.append(tag_descriptions[tag])

        # Find related artists
        related_artists = []
        for spike in artist_spikes:
            if spike.artist_name.lower() in style.headline.lower():
                related_artists.append(spike.artist_name)
        if related_artists:
            why_now.append(f"Related artist trending: {related_artists[0]}")

        # Spotify angle
        if "artist_collab" in style.spotify_tags:
            angle = SPOTIFY_ANGLES["partnership"][0]
        elif "tour_merch" in style.spotify_tags:
            angle = SPOTIFY_ANGLES["editorial"][0]
        elif "youth_culture" in style.spotify_tags:
            angle = SPOTIFY_ANGLES["creator"][0]
        else:
            angle = SPOTIFY_ANGLES["editorial"][2]

        # Next steps
        next_steps = [
            NEXT_STEPS_TEMPLATES["style_opportunity"][0],
            NEXT_STEPS_TEMPLATES["style_opportunity"][1],
        ]

        # Risks
        risks = []
        if style.risk_level == "medium":
            risks.append(RISK_TEMPLATES["high_visibility"])
        if "competitor" in style.headline.lower():
            risks.append(RISK_TEMPLATES["competitor_mention"])
        if not risks:
            risks.append("Review source for brand safety before sharing")

        # Confidence
        confidence = calculate_confidence(
            len(style.spotify_tags),
            [],
            len(style.country_relevance) > 1
        )

        return PitchCard(
            id=generate_card_id(market, [style.id]),
            market=market,
            hook=hook,
            why_now=why_now,
            spotify_angle=angle,
            next_steps=next_steps,
            risks=risks[:2],
            confidence=confidence,
            source_signals=[style.id],
            generated_at=datetime.utcnow(),
            expires_at=datetime.utcnow() + timedelta(hours=48),
        )

    async def generate_and_save(
        self,
        storage,
        artist_spikes: List[ArtistSpike],
        culture_searches: List[CultureSearch],
        style_signals: List[StyleSignal],
        markets: List[str] = None,
    ) -> int:
        """Generate cards and save to storage."""
        cards = await self.generate_cards(
            artist_spikes,
            culture_searches,
            style_signals,
            markets,
        )

        if cards:
            saved = await storage.save_pitch_cards(cards)
            logger.info("pitch_cards_generated", count=saved, markets=markets)
            return saved

        return 0

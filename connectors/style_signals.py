"""
Style Signals connector for streetwear/fashion RSS aggregation.

Aggregates from stable fashion/streetwear RSS feeds with Africa relevance filtering.
"""

import asyncio
import hashlib
import re
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Set
import aiohttp
import feedparser
import structlog

from .base import BaseConnector, ConnectorResult, TrendItem, SourceStatus
from storage.base import StyleSignal

logger = structlog.get_logger()

# RSS feed sources
RSS_FEEDS = {
    # Global streetwear/fashion
    "highsnobiety": {
        "url": "https://www.highsnobiety.com/feed/",
        "name": "Highsnobiety",
        "region": "global",
    },
    "hypebeast": {
        "url": "https://hypebeast.com/feed",
        "name": "Hypebeast",
        "region": "global",
    },
    "complex_style": {
        "url": "https://www.complex.com/style/rss",
        "name": "Complex Style",
        "region": "global",
    },
    "dazed": {
        "url": "https://www.dazeddigital.com/rss",
        "name": "Dazed",
        "region": "global",
    },
    "i_d": {
        "url": "https://i-d.vice.com/en_uk/rss",
        "name": "i-D",
        "region": "global",
    },
    # Africa-focused
    "nataal": {
        "url": "https://nataal.com/feed",
        "name": "Nataal",
        "region": "africa",
    },
    "between_10_and_5": {
        "url": "https://10and5.com/feed/",
        "name": "Between 10 and 5",
        "region": "africa",
    },
    "okayafrica_style": {
        "url": "https://www.okayafrica.com/rss/",
        "name": "OkayAfrica",
        "region": "africa",
    },
}

# Country relevance keywords
COUNTRY_KEYWORDS = {
    "NG": [
        "nigeria", "nigerian", "lagos", "naija", "nollywood",
        "afrobeats", "wizkid", "burna boy", "davido", "asake",
    ],
    "KE": [
        "kenya", "kenyan", "nairobi", "swahili", "east africa",
        "sauti sol", "gengetone",
    ],
    "GH": [
        "ghana", "ghanaian", "accra", "black sherif", "sarkodie",
        "west africa",
    ],
    "ZA": [
        "south africa", "south african", "johannesburg", "cape town",
        "amapiano", "gqom", "soweto", "zulu", "nasty c",
    ],
}

# Spotify relevance tags
SPOTIFY_TAG_PATTERNS = {
    "artist_collab": [
        r"\b(collab|collaboration|featuring|ft\.|feat\.)\b",
        r"\b(artist|musician|rapper|singer|dj)\b.*\b(fashion|style|brand)\b",
    ],
    "tour_merch": [
        r"\b(tour|concert|merch|merchandise|drop)\b",
        r"\b(capsule|collection|limited)\b",
    ],
    "youth_culture": [
        r"\b(gen.?z|youth|young|teen|student)\b",
        r"\b(viral|trend|tiktok|challenge)\b",
    ],
    "music_fashion": [
        r"\b(album|music|video|mv)\b.*\b(fashion|outfit|style)\b",
        r"\b(fashion week|runway)\b.*\b(music|artist)\b",
    ],
    "streetwear": [
        r"\b(streetwear|street style|sneaker|hypebeast)\b",
        r"\b(supreme|off.?white|nike|adidas|jordan)\b",
    ],
    "african_designer": [
        r"\b(african designer|lagos fashion|africa fashion)\b",
        r"\b(maxhosa|thebe magugu|kenneth ize|orange culture)\b",
    ],
}

# Risk keywords
RISK_KEYWORDS = {
    "high": [
        "controversy", "scandal", "appropriation", "racist", "offensive",
        "boycott", "cancelled", "backlash",
    ],
    "medium": [
        "criticism", "debate", "controversial", "divisive", "provocative",
    ],
}


def generate_signal_id(url: str) -> str:
    """Generate unique ID for style signal."""
    return hashlib.sha256(url.encode()).hexdigest()[:16]


def detect_country_relevance(text: str) -> List[str]:
    """Detect which African markets the content is relevant to."""
    text_lower = text.lower()
    relevant = []

    for country, keywords in COUNTRY_KEYWORDS.items():
        for keyword in keywords:
            if keyword in text_lower:
                if country not in relevant:
                    relevant.append(country)
                break

    # If African source but no specific country, tag all
    if not relevant:
        for region_marker in ["africa", "african"]:
            if region_marker in text_lower:
                return ["NG", "KE", "GH", "ZA"]

    return relevant


def detect_spotify_tags(text: str) -> List[str]:
    """Detect Spotify relevance tags."""
    text_lower = text.lower()
    tags = []

    for tag, patterns in SPOTIFY_TAG_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, text_lower, re.IGNORECASE):
                if tag not in tags:
                    tags.append(tag)
                break

    return tags


def determine_risk_level(text: str) -> str:
    """Determine risk level based on content."""
    text_lower = text.lower()

    for keyword in RISK_KEYWORDS["high"]:
        if keyword in text_lower:
            return "high"

    for keyword in RISK_KEYWORDS["medium"]:
        if keyword in text_lower:
            return "medium"

    return "low"


def extract_summary(entry: dict, max_length: int = 200) -> str:
    """Extract a non-infringing summary from feed entry."""
    # Try to get summary/description
    summary = entry.get("summary", "") or entry.get("description", "")

    # Strip HTML tags
    summary = re.sub(r"<[^>]+>", "", summary)
    # Strip extra whitespace
    summary = " ".join(summary.split())

    # Truncate to max length
    if len(summary) > max_length:
        summary = summary[:max_length].rsplit(" ", 1)[0] + "..."

    return summary


class StyleSignalsConnector(BaseConnector):
    """
    Connector for streetwear/fashion style signals via RSS.

    Features:
    - 10-15 items/day prioritized for NG/KE/GH/ZA youth culture
    - Each item shows: headline, source, date, summary
    - Spotify relevance tags: Artist collab / Tour merch / Youth culture
    - Risk rating with filtering support
    """

    name = "style_signals"
    display_name = "Streetwear & Style Signals"
    requires_auth = False

    def __init__(self, config: dict):
        super().__init__(config)
        self.feeds = {**RSS_FEEDS}
        # Allow config to add/override feeds
        config_feeds = config.get("style_signals", {}).get("feeds", [])
        for feed in config_feeds:
            if "url" in feed and "name" in feed:
                key = feed["name"].lower().replace(" ", "_")
                self.feeds[key] = feed

    async def fetch(
        self,
        markets: List[str] = None,
        keywords: List[str] = None,
        **kwargs
    ) -> ConnectorResult:
        """Fetch style signals from RSS feeds."""
        items = []
        errors = []

        signals = await self.fetch_signals(markets)

        for signal in signals:
            items.append(TrendItem(
                id=signal.id,
                source=self.name,
                title=signal.headline,
                description=signal.summary,
                market=signal.country_relevance[0] if signal.country_relevance else None,
                source_url=signal.source_url,
                metadata={
                    "type": "style_signal",
                    "source_name": signal.source,
                    "publish_date": signal.publish_date.isoformat(),
                    "country_relevance": signal.country_relevance,
                    "spotify_tags": signal.spotify_tags,
                    "risk_level": signal.risk_level,
                }
            ))

        status = SourceStatus.ACTIVE if not errors else SourceStatus.DEGRADED
        return self._create_result(items, status, errors)

    async def fetch_signals(
        self,
        markets: List[str] = None,
        max_risk: str = "high",
        limit: int = 15,
    ) -> List[StyleSignal]:
        """
        Fetch style signals as StyleSignal objects.

        Args:
            markets: Filter to signals relevant to these markets
            max_risk: Maximum risk level to include ("low", "medium", "high")
            limit: Maximum number of signals to return
        """
        all_signals: List[StyleSignal] = []
        cutoff_date = datetime.utcnow() - timedelta(days=7)

        async with aiohttp.ClientSession() as session:
            tasks = []
            for feed_key, feed_info in self.feeds.items():
                tasks.append(self._fetch_feed(session, feed_key, feed_info, cutoff_date))

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, Exception):
                    self.logger.warning("feed_fetch_error", error=str(result))
                    continue
                all_signals.extend(result)

        # Filter by market relevance if specified
        if markets:
            all_signals = [
                s for s in all_signals
                if any(m in s.country_relevance for m in markets) or not s.country_relevance
            ]

        # Filter by risk level
        risk_order = {"low": 1, "medium": 2, "high": 3}
        max_risk_value = risk_order.get(max_risk, 3)
        all_signals = [
            s for s in all_signals
            if risk_order.get(s.risk_level, 1) <= max_risk_value
        ]

        # Sort by publish date (newest first) and limit
        all_signals.sort(key=lambda x: x.publish_date, reverse=True)

        # Prioritize Africa-relevant signals
        africa_signals = [s for s in all_signals if s.country_relevance]
        global_signals = [s for s in all_signals if not s.country_relevance]

        # Return mix: prioritize Africa, fill with global
        result = africa_signals[:limit]
        remaining = limit - len(result)
        if remaining > 0:
            result.extend(global_signals[:remaining])

        return result[:limit]

    async def _fetch_feed(
        self,
        session: aiohttp.ClientSession,
        feed_key: str,
        feed_info: dict,
        cutoff_date: datetime
    ) -> List[StyleSignal]:
        """Fetch and parse a single RSS feed."""
        signals = []
        url = feed_info["url"]
        source_name = feed_info["name"]

        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status != 200:
                    self.logger.warning("feed_http_error", feed=feed_key, status=response.status)
                    return []

                content = await response.text()

            # Parse feed
            feed = feedparser.parse(content)

            for entry in feed.entries[:20]:  # Limit entries per feed
                # Parse publish date
                published = entry.get("published_parsed") or entry.get("updated_parsed")
                if published:
                    publish_date = datetime(*published[:6])
                else:
                    publish_date = datetime.utcnow()

                # Skip old entries
                if publish_date < cutoff_date:
                    continue

                title = entry.get("title", "")
                link = entry.get("link", "")
                summary = extract_summary(entry)

                # Combine title and summary for analysis
                full_text = f"{title} {summary}"

                # Detect country relevance
                country_relevance = detect_country_relevance(full_text)

                # For Africa-focused sources, assume relevance if none detected
                if not country_relevance and feed_info.get("region") == "africa":
                    country_relevance = ["NG", "KE", "GH", "ZA"]

                # Detect Spotify tags
                spotify_tags = detect_spotify_tags(full_text)

                # Determine risk level
                risk_level = determine_risk_level(full_text)

                signal = StyleSignal(
                    id=generate_signal_id(link),
                    headline=title,
                    source=source_name,
                    source_url=link,
                    summary=summary,
                    publish_date=publish_date,
                    country_relevance=country_relevance,
                    spotify_tags=spotify_tags,
                    risk_level=risk_level,
                    collected_at=datetime.utcnow(),
                )
                signals.append(signal)

        except asyncio.TimeoutError:
            self.logger.warning("feed_timeout", feed=feed_key)
        except Exception as e:
            self.logger.warning("feed_parse_error", feed=feed_key, error=str(e))

        return signals

    async def health_check(self) -> bool:
        """Check if at least one feed is accessible."""
        try:
            async with aiohttp.ClientSession() as session:
                # Try to fetch one feed
                feed_info = list(self.feeds.values())[0]
                async with session.get(
                    feed_info["url"],
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    return response.status == 200
        except Exception as e:
            self.logger.error("health_check_failed", error=str(e))
            return False

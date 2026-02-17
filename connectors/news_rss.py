"""
News RSS feed connector for tracking African news and media.
"""

import asyncio
from datetime import datetime, timezone
from typing import Optional
import hashlib
import structlog

from .base import BaseConnector, ConnectorResult, TrendItem, SourceStatus

logger = structlog.get_logger()


class NewsRSSConnector(BaseConnector):
    """
    Connector for news RSS feeds from African publishers and
    global outlets with Africa coverage.

    Supports:
    - Multiple RSS feeds with configurable weights
    - Keyword filtering
    - Market/region tagging based on content
    """

    name = "news_rss"
    display_name = "News RSS Feeds"
    requires_auth = False

    def __init__(self, config: dict):
        super().__init__(config)
        self.feeds = config.get("feeds", [])

    async def fetch(
        self,
        markets: list[str],
        keywords: list[str],
        **kwargs
    ) -> ConnectorResult:
        """
        Fetch and parse news from configured RSS feeds.
        """
        items = []
        errors = []
        warnings = []

        try:
            import feedparser
            import aiohttp
        except ImportError as e:
            return self._create_result(
                items=[],
                status=SourceStatus.UNAVAILABLE,
                errors=[f"Required library not installed: {e}"],
            )

        # Create keyword patterns for filtering
        keyword_set = set(k.lower() for k in keywords) if keywords else set()

        # Fetch all feeds concurrently
        async with aiohttp.ClientSession() as session:
            tasks = [
                self._fetch_feed(session, feed, keyword_set, markets)
                for feed in self.feeds
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                errors.append(str(result))
            elif isinstance(result, list):
                items.extend(result)

        status = SourceStatus.ACTIVE
        if errors and not items:
            status = SourceStatus.UNAVAILABLE
        elif errors:
            status = SourceStatus.DEGRADED

        return self._create_result(items, status, errors, warnings)

    async def _fetch_feed(
        self,
        session,
        feed_config: dict,
        keywords: set,
        markets: list[str]
    ) -> list[TrendItem]:
        """Fetch and parse a single RSS feed."""
        import feedparser

        feed_name = feed_config.get("name", "Unknown")
        feed_url = feed_config.get("url")

        if not feed_url:
            self.logger.warning("missing_feed_url", feed=feed_name)
            return []

        items = []

        try:
            async with session.get(feed_url, timeout=30) as response:
                if response.status != 200:
                    self.logger.warning(
                        "feed_fetch_failed",
                        feed=feed_name,
                        status=response.status
                    )
                    return []

                content = await response.text()

            # Parse feed
            feed = feedparser.parse(content)

            for entry in feed.entries[:50]:  # Limit to most recent 50
                title = entry.get("title", "")
                summary = entry.get("summary", entry.get("description", ""))
                link = entry.get("link", "")

                # Combine text for analysis
                full_text = f"{title} {summary}".lower()

                # Filter by keywords if provided
                if keywords:
                    matches = [k for k in keywords if k in full_text]
                    if not matches:
                        continue

                # Parse published date
                published = None
                if hasattr(entry, "published_parsed") and entry.published_parsed:
                    try:
                        published = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
                    except (TypeError, ValueError):
                        pass

                # Detect market from content
                detected_market = self._detect_market(full_text, markets)

                # Generate unique ID
                content_hash = hashlib.sha256(
                    f"{feed_name}:{link}".encode()
                ).hexdigest()[:16]

                items.append(TrendItem(
                    id=content_hash,
                    source=self.name,
                    source_url=link,
                    title=title,
                    description=summary[:500] if summary else "",
                    raw_text=full_text[:2000],
                    market=detected_market,
                    published_at=published,
                    metadata={
                        "feed_name": feed_name,
                        "feed_url": feed_url,
                        "type": "news_article",
                    }
                ))

        except asyncio.TimeoutError:
            self.logger.warning("feed_timeout", feed=feed_name)
        except Exception as e:
            self.logger.error("feed_parse_error", feed=feed_name, error=str(e))

        return items

    def _detect_market(self, text: str, markets: list[str]) -> Optional[str]:
        """
        Detect which market an article is about based on content.
        Returns the first matching market or None.
        """
        market_keywords = {
            "ZA": ["south africa", "johannesburg", "cape town", "pretoria", "durban", "soweto"],
            "NG": ["nigeria", "lagos", "abuja", "naija"],
            "KE": ["kenya", "nairobi", "mombasa"],
            "GH": ["ghana", "accra", "kumasi"],
            "TZ": ["tanzania", "dar es salaam", "dodoma"],
            "UG": ["uganda", "kampala"],
            "AO": ["angola", "luanda"],
            "CI": ["ivory coast", "côte d'ivoire", "cote d'ivoire", "abidjan"],
            "SN": ["senegal", "dakar"],
            "EG": ["egypt", "cairo", "alexandria"],
            "MA": ["morocco", "rabat", "casablanca", "marrakech"],
        }

        text_lower = text.lower()

        for market in markets:
            if market in market_keywords:
                if any(kw in text_lower for kw in market_keywords[market]):
                    return market

        return None

    async def health_check(self) -> bool:
        """Check if at least one feed is accessible."""
        if not self.feeds:
            return False

        try:
            import aiohttp

            # Try to fetch the first feed
            feed_url = self.feeds[0].get("url")
            if not feed_url:
                return False

            async with aiohttp.ClientSession() as session:
                async with session.get(feed_url, timeout=10) as response:
                    return response.status == 200

        except Exception as e:
            self.logger.error("health_check_failed", error=str(e))
            return False

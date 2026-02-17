"""
Wikipedia pageviews connector for tracking interest spikes in artists and events.
"""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional
import structlog

from .base import BaseConnector, ConnectorResult, TrendItem, SourceStatus

logger = structlog.get_logger()


class WikipediaConnector(BaseConnector):
    """
    Connector for Wikipedia pageview statistics.

    Tracks pageview spikes for:
    - African artists
    - Music events/festivals
    - Cultural figures
    - Current events

    Uses the Wikimedia REST API for pageview data.
    """

    name = "wikipedia"
    display_name = "Wikipedia Pageviews"
    requires_auth = False

    WIKIMEDIA_API = "https://wikimedia.org/api/rest_v1"

    def __init__(self, config: dict):
        super().__init__(config)
        self.pageview_threshold = config.get("pageview_threshold", 10000)

    async def fetch(
        self,
        markets: list[str],
        keywords: list[str],
        entities: dict = None,
        **kwargs
    ) -> ConnectorResult:
        """
        Fetch pageview data for tracked entities.

        Args:
            markets: Not used directly (Wikipedia is global)
            keywords: Keywords to search
            entities: Dict of entity types to names (e.g., {"artists": ["Burna Boy", ...]})
        """
        items = []
        errors = []
        warnings = []

        # Build list of pages to check
        pages_to_check = list(keywords) if keywords else []

        if entities:
            for entity_type, names in entities.items():
                pages_to_check.extend(names)

        if not pages_to_check:
            return self._create_result(
                items=[],
                status=SourceStatus.DEGRADED,
                warnings=["No pages to track"],
            )

        # Fetch pageviews for each page
        try:
            import aiohttp
        except ImportError:
            return self._create_result(
                items=[],
                status=SourceStatus.UNAVAILABLE,
                errors=["aiohttp not installed"],
            )

        # Calculate date range (last 7 days)
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=7)

        async with aiohttp.ClientSession() as session:
            tasks = [
                self._fetch_pageviews(session, page, start_date, end_date)
                for page in pages_to_check[:100]  # Limit to avoid overwhelming API
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                self.logger.warning("pageview_fetch_error", error=str(result))
            elif result:
                items.append(result)

        status = SourceStatus.ACTIVE if items else SourceStatus.DEGRADED
        return self._create_result(items, status, errors, warnings)

    async def _fetch_pageviews(
        self,
        session,
        page_title: str,
        start_date: datetime,
        end_date: datetime
    ) -> Optional[TrendItem]:
        """Fetch pageview data for a single Wikipedia page."""

        # Format dates for API (YYYYMMDD)
        start = start_date.strftime("%Y%m%d")
        end = end_date.strftime("%Y%m%d")

        # Normalize page title for URL
        page_url = page_title.replace(" ", "_")

        url = (
            f"{self.WIKIMEDIA_API}/metrics/pageviews/per-article/"
            f"en.wikipedia/all-access/all-agents/{page_url}/daily/{start}/{end}"
        )

        try:
            async with session.get(url, timeout=15) as response:
                if response.status == 404:
                    # Page doesn't exist
                    return None
                if response.status != 200:
                    return None

                data = await response.json()

            items = data.get("items", [])
            if not items:
                return None

            # Calculate metrics
            views = [item.get("views", 0) for item in items]
            total_views = sum(views)
            avg_views = total_views / len(views) if views else 0
            latest_views = views[-1] if views else 0

            # Calculate velocity (spike detection)
            baseline = sum(views[:-1]) / len(views[:-1]) if len(views) > 1 else avg_views
            velocity = (latest_views - baseline) / baseline if baseline > 0 else 0

            # Only include if above threshold or spiking
            if total_views < self.pageview_threshold and velocity < 0.5:
                return None

            return TrendItem(
                id=f"wiki_{page_url}",
                source=self.name,
                source_url=f"https://en.wikipedia.org/wiki/{page_url}",
                title=page_title,
                description=f"Wikipedia page with {total_views:,} views in 7 days",
                volume=total_views,
                velocity=velocity,
                metadata={
                    "type": "wikipedia_pageviews",
                    "daily_views": views,
                    "average_daily": round(avg_views),
                    "latest_daily": latest_views,
                    "spike_detected": velocity > 0.5,
                }
            )

        except asyncio.TimeoutError:
            self.logger.warning("pageview_timeout", page=page_title)
            return None
        except Exception as e:
            self.logger.warning("pageview_error", page=page_title, error=str(e))
            return None

    async def get_trending_pages(
        self,
        date: datetime = None,
        limit: int = 50
    ) -> list[TrendItem]:
        """
        Get top viewed pages for a specific date.

        This can be used to discover new trending topics.
        """
        import aiohttp

        if date is None:
            date = datetime.now(timezone.utc) - timedelta(days=1)

        year = date.strftime("%Y")
        month = date.strftime("%m")
        day = date.strftime("%d")

        url = (
            f"{self.WIKIMEDIA_API}/metrics/pageviews/top/"
            f"en.wikipedia/all-access/{year}/{month}/{day}"
        )

        items = []

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=15) as response:
                    if response.status != 200:
                        return items

                    data = await response.json()

            articles = data.get("items", [{}])[0].get("articles", [])

            for article in articles[:limit]:
                page_title = article.get("article", "").replace("_", " ")
                views = article.get("views", 0)

                # Filter out main page and special pages
                if page_title in ("Main_Page", "Special:Search") or ":" in page_title:
                    continue

                items.append(TrendItem(
                    id=f"wiki_top_{article.get('article', '')}",
                    source=self.name,
                    source_url=f"https://en.wikipedia.org/wiki/{article.get('article', '')}",
                    title=page_title,
                    description=f"Top Wikipedia page with {views:,} views",
                    volume=views,
                    metadata={
                        "type": "wikipedia_trending",
                        "rank": article.get("rank", 0),
                        "date": date.isoformat(),
                    }
                ))

        except Exception as e:
            self.logger.error("trending_pages_error", error=str(e))

        return items

    async def health_check(self) -> bool:
        """Check if Wikipedia API is accessible."""
        try:
            import aiohttp

            # Check API status
            url = f"{self.WIKIMEDIA_API}/metrics/pageviews/top/en.wikipedia/all-access/2024/01/01"

            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as response:
                    return response.status == 200

        except Exception as e:
            self.logger.error("health_check_failed", error=str(e))
            return False

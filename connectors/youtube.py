"""
YouTube connector for tracking trending videos and music content in Africa.
"""

import asyncio
import os
from datetime import datetime, timezone
from typing import Optional
import structlog

from .base import BaseConnector, ConnectorResult, TrendItem, SourceStatus

logger = structlog.get_logger()

# Market code to YouTube region code mapping
MARKET_REGION_MAP = {
    "ZA": "ZA",
    "NG": "NG",
    "KE": "KE",
    "GH": "GH",
    "TZ": "TZ",
    "UG": "UG",
    "AO": "AO",
    "CI": "CI",
    "SN": "SN",
    "EG": "EG",
    "MA": "MA",
}


class YouTubeConnector(BaseConnector):
    """
    Connector for YouTube trending and search data.

    Uses YouTube Data API v3 to fetch:
    - Trending videos by region
    - Music category trends
    - Search results for keywords
    """

    name = "youtube"
    display_name = "YouTube"
    requires_auth = True

    def __init__(self, config: dict):
        super().__init__(config)
        self._youtube = None

    def _has_credentials(self) -> bool:
        """Check if YouTube API key is configured."""
        return bool(os.getenv("YOUTUBE_API_KEY"))

    def _get_youtube(self):
        """Get YouTube API client."""
        if self._youtube is None and self._has_credentials():
            try:
                from googleapiclient.discovery import build
                self._youtube = build(
                    "youtube", "v3",
                    developerKey=os.getenv("YOUTUBE_API_KEY")
                )
            except ImportError:
                self.logger.error("google-api-python-client not installed")
        return self._youtube

    async def fetch(
        self,
        markets: list[str],
        keywords: list[str],
        **kwargs
    ) -> ConnectorResult:
        """
        Fetch trending videos and search results.
        """
        items = []
        errors = []
        warnings = []

        if not self._has_credentials():
            return self._create_result(
                items=[],
                status=SourceStatus.REQUIRES_AUTH,
                warnings=["YOUTUBE_API_KEY not configured"],
            )

        youtube = self._get_youtube()
        if not youtube:
            return self._create_result(
                items=[],
                status=SourceStatus.UNAVAILABLE,
                errors=["Failed to initialize YouTube client"],
            )

        # Fetch trending videos per market
        for market in markets:
            region = MARKET_REGION_MAP.get(market)
            if not region:
                continue

            try:
                trending_items = await self._fetch_trending(youtube, region, market)
                items.extend(trending_items)
            except Exception as e:
                errors.append(f"Error fetching trends for {market}: {e}")
                self.logger.error("youtube_trending_error", market=market, error=str(e))

        # Search for keywords
        if keywords:
            for keyword in keywords[:20]:  # Limit to avoid quota exhaustion
                try:
                    search_items = await self._search_videos(youtube, keyword)
                    items.extend(search_items)
                except Exception as e:
                    self.logger.warning("youtube_search_error", keyword=keyword, error=str(e))

        status = SourceStatus.ACTIVE if items else SourceStatus.DEGRADED
        return self._create_result(items, status, errors, warnings)

    async def _fetch_trending(
        self,
        youtube,
        region: str,
        market: str
    ) -> list[TrendItem]:
        """Fetch trending videos for a region."""
        items = []

        loop = asyncio.get_event_loop()

        try:
            # Get trending videos (category 10 = Music)
            request = youtube.videos().list(
                part="snippet,statistics",
                chart="mostPopular",
                regionCode=region,
                videoCategoryId="10",  # Music
                maxResults=25
            )

            response = await loop.run_in_executor(None, request.execute)

            for video in response.get("items", []):
                snippet = video.get("snippet", {})
                stats = video.get("statistics", {})

                items.append(TrendItem(
                    id=video.get("id", ""),
                    source=self.name,
                    source_url=f"https://youtube.com/watch?v={video.get('id', '')}",
                    title=snippet.get("title", ""),
                    description=snippet.get("description", "")[:500],
                    market=market,
                    volume=int(stats.get("viewCount", 0)),
                    engagement=int(stats.get("likeCount", 0)) + int(stats.get("commentCount", 0)),
                    published_at=datetime.fromisoformat(
                        snippet.get("publishedAt", "").replace("Z", "+00:00")
                    ) if snippet.get("publishedAt") else None,
                    metadata={
                        "type": "youtube_trending",
                        "channel_title": snippet.get("channelTitle", ""),
                        "channel_id": snippet.get("channelId", ""),
                        "view_count": int(stats.get("viewCount", 0)),
                        "like_count": int(stats.get("likeCount", 0)),
                        "comment_count": int(stats.get("commentCount", 0)),
                        "category": "Music",
                        "region": region,
                    }
                ))

        except Exception as e:
            self.logger.error("trending_fetch_error", region=region, error=str(e))

        return items

    async def _search_videos(
        self,
        youtube,
        keyword: str,
        max_results: int = 10
    ) -> list[TrendItem]:
        """Search for videos matching a keyword."""
        items = []

        loop = asyncio.get_event_loop()

        try:
            request = youtube.search().list(
                part="snippet",
                q=keyword,
                type="video",
                order="viewCount",
                publishedAfter=(datetime.now(timezone.utc).replace(
                    hour=0, minute=0, second=0, microsecond=0
                ) - __import__('datetime').timedelta(days=7)).isoformat(),
                maxResults=max_results
            )

            response = await loop.run_in_executor(None, request.execute)

            for item in response.get("items", []):
                snippet = item.get("snippet", {})
                video_id = item.get("id", {}).get("videoId", "")

                items.append(TrendItem(
                    id=video_id,
                    source=self.name,
                    source_url=f"https://youtube.com/watch?v={video_id}",
                    title=snippet.get("title", ""),
                    description=snippet.get("description", "")[:500],
                    published_at=datetime.fromisoformat(
                        snippet.get("publishedAt", "").replace("Z", "+00:00")
                    ) if snippet.get("publishedAt") else None,
                    metadata={
                        "type": "youtube_search",
                        "search_query": keyword,
                        "channel_title": snippet.get("channelTitle", ""),
                        "channel_id": snippet.get("channelId", ""),
                    }
                ))

        except Exception as e:
            self.logger.warning("search_error", keyword=keyword, error=str(e))

        return items

    async def health_check(self) -> bool:
        """Check if YouTube API is accessible."""
        if not self._has_credentials():
            return False

        youtube = self._get_youtube()
        if not youtube:
            return False

        try:
            loop = asyncio.get_event_loop()
            request = youtube.videos().list(
                part="snippet",
                chart="mostPopular",
                regionCode="ZA",
                maxResults=1
            )
            response = await loop.run_in_executor(None, request.execute)
            return "items" in response

        except Exception as e:
            self.logger.error("health_check_failed", error=str(e))
            return False

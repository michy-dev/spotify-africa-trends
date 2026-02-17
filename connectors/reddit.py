"""
Reddit connector for tracking discussions in Africa-related subreddits.
"""

import asyncio
import os
from datetime import datetime, timezone
from typing import Optional
import structlog

from .base import BaseConnector, ConnectorResult, TrendItem, SourceStatus

logger = structlog.get_logger()


class RedditConnector(BaseConnector):
    """
    Connector for Reddit discussions via PRAW or public JSON API.

    Tracks:
    - Hot posts in configured subreddits
    - Keyword mentions
    - Rising discussions
    """

    name = "reddit"
    display_name = "Reddit"
    requires_auth = False  # Can work without auth via JSON API

    def __init__(self, config: dict):
        super().__init__(config)
        self.subreddits = config.get("subreddits", [])
        self._praw_reddit = None

    def _has_credentials(self) -> bool:
        """Check if Reddit API credentials are configured."""
        return all([
            os.getenv("REDDIT_CLIENT_ID"),
            os.getenv("REDDIT_CLIENT_SECRET"),
        ])

    def _get_praw(self):
        """Get authenticated PRAW instance if credentials available."""
        if self._praw_reddit is None and self._has_credentials():
            try:
                import praw
                self._praw_reddit = praw.Reddit(
                    client_id=os.getenv("REDDIT_CLIENT_ID"),
                    client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
                    user_agent="SpotifyAfricaTrends/1.0"
                )
            except ImportError:
                pass
        return self._praw_reddit

    async def fetch(
        self,
        markets: list[str],
        keywords: list[str],
        **kwargs
    ) -> ConnectorResult:
        """
        Fetch trending posts from configured subreddits.
        """
        items = []
        errors = []
        warnings = []

        if not self.subreddits:
            return self._create_result(
                items=[],
                status=SourceStatus.DEGRADED,
                warnings=["No subreddits configured"],
            )

        # Try PRAW first, fall back to JSON API
        if self._has_credentials():
            try:
                items = await self._fetch_with_praw(keywords)
            except Exception as e:
                errors.append(f"PRAW fetch failed: {e}")
                items = await self._fetch_with_json_api(keywords)
        else:
            warnings.append("Using unauthenticated API - rate limits apply")
            items = await self._fetch_with_json_api(keywords)

        status = SourceStatus.ACTIVE if items else SourceStatus.DEGRADED
        return self._create_result(items, status, errors, warnings)

    async def _fetch_with_praw(self, keywords: list[str]) -> list[TrendItem]:
        """Fetch using authenticated PRAW library."""
        import praw

        items = []
        reddit = self._get_praw()

        if not reddit:
            return items

        keyword_set = set(k.lower() for k in keywords) if keywords else set()

        for sub_name in self.subreddits:
            try:
                loop = asyncio.get_event_loop()
                subreddit = await loop.run_in_executor(
                    None,
                    lambda s=sub_name: reddit.subreddit(s)
                )

                # Get hot posts
                hot_posts = await loop.run_in_executor(
                    None,
                    lambda sr=subreddit: list(sr.hot(limit=25))
                )

                for post in hot_posts:
                    # Filter by keywords if provided
                    full_text = f"{post.title} {post.selftext}".lower()
                    if keyword_set and not any(k in full_text for k in keyword_set):
                        continue

                    items.append(TrendItem(
                        id=post.id,
                        source=self.name,
                        source_url=f"https://reddit.com{post.permalink}",
                        title=post.title,
                        description=post.selftext[:500] if post.selftext else "",
                        raw_text=full_text[:2000],
                        volume=post.score,
                        engagement=post.num_comments,
                        published_at=datetime.fromtimestamp(post.created_utc, tz=timezone.utc),
                        metadata={
                            "subreddit": sub_name,
                            "upvote_ratio": post.upvote_ratio,
                            "is_self": post.is_self,
                            "type": "reddit_post",
                        }
                    ))

            except Exception as e:
                self.logger.warning("praw_subreddit_error", subreddit=sub_name, error=str(e))

        return items

    async def _fetch_with_json_api(self, keywords: list[str]) -> list[TrendItem]:
        """Fetch using Reddit's public JSON API (no auth required)."""
        import aiohttp

        items = []
        keyword_set = set(k.lower() for k in keywords) if keywords else set()

        headers = {
            "User-Agent": "SpotifyAfricaTrends/1.0"
        }

        async with aiohttp.ClientSession(headers=headers) as session:
            for sub_name in self.subreddits:
                try:
                    url = f"https://www.reddit.com/r/{sub_name}/hot.json?limit=25"

                    async with session.get(url, timeout=15) as response:
                        if response.status != 200:
                            self.logger.warning(
                                "reddit_api_error",
                                subreddit=sub_name,
                                status=response.status
                            )
                            continue

                        data = await response.json()

                    posts = data.get("data", {}).get("children", [])

                    for post_wrapper in posts:
                        post = post_wrapper.get("data", {})

                        title = post.get("title", "")
                        selftext = post.get("selftext", "")
                        full_text = f"{title} {selftext}".lower()

                        # Filter by keywords
                        if keyword_set and not any(k in full_text for k in keyword_set):
                            continue

                        items.append(TrendItem(
                            id=post.get("id", ""),
                            source=self.name,
                            source_url=f"https://reddit.com{post.get('permalink', '')}",
                            title=title,
                            description=selftext[:500],
                            raw_text=full_text[:2000],
                            volume=post.get("score", 0),
                            engagement=post.get("num_comments", 0),
                            published_at=datetime.fromtimestamp(
                                post.get("created_utc", 0),
                                tz=timezone.utc
                            ),
                            metadata={
                                "subreddit": sub_name,
                                "upvote_ratio": post.get("upvote_ratio", 0),
                                "type": "reddit_post",
                            }
                        ))

                    # Rate limiting
                    await asyncio.sleep(1)

                except Exception as e:
                    self.logger.warning(
                        "reddit_json_error",
                        subreddit=sub_name,
                        error=str(e)
                    )

        return items

    async def health_check(self) -> bool:
        """Check if Reddit API is accessible."""
        try:
            import aiohttp

            headers = {"User-Agent": "SpotifyAfricaTrends/1.0"}
            async with aiohttp.ClientSession(headers=headers) as session:
                async with session.get(
                    "https://www.reddit.com/r/Africa/hot.json?limit=1",
                    timeout=10
                ) as response:
                    return response.status == 200

        except Exception as e:
            self.logger.error("health_check_failed", error=str(e))
            return False

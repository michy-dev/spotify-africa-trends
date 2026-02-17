"""
Twitter/X connector for tracking social conversations.

NOTE: Twitter API access requires paid plans. This connector implements
the interface but requires valid API credentials to function.
"""

import asyncio
import os
from datetime import datetime, timezone
from typing import Optional
import structlog

from .base import BaseConnector, ConnectorResult, TrendItem, SourceStatus

logger = structlog.get_logger()


class TwitterConnector(BaseConnector):
    """
    Connector for Twitter/X API.

    Tracks:
    - Trending topics per market
    - Keyword/hashtag search
    - Mentions of tracked entities

    IMPORTANT: Requires Twitter API v2 credentials (Basic tier or higher).
    """

    name = "twitter"
    display_name = "Twitter/X"
    requires_auth = True

    def __init__(self, config: dict):
        super().__init__(config)
        self._client = None

    def _has_credentials(self) -> bool:
        """Check if Twitter API credentials are configured."""
        return all([
            os.getenv("TWITTER_BEARER_TOKEN"),
        ])

    def _get_client(self):
        """Get authenticated Twitter client."""
        if self._client is None and self._has_credentials():
            try:
                import tweepy
                self._client = tweepy.Client(
                    bearer_token=os.getenv("TWITTER_BEARER_TOKEN"),
                    wait_on_rate_limit=True
                )
            except ImportError:
                self.logger.error("tweepy not installed")
        return self._client

    async def fetch(
        self,
        markets: list[str],
        keywords: list[str],
        **kwargs
    ) -> ConnectorResult:
        """
        Fetch tweets and trends from Twitter.
        """
        items = []
        errors = []
        warnings = []

        if not self._has_credentials():
            return self._create_result(
                items=[],
                status=SourceStatus.REQUIRES_AUTH,
                warnings=[
                    "Twitter API credentials not configured.",
                    "Set TWITTER_BEARER_TOKEN environment variable.",
                    "Note: Twitter API requires paid access (Basic tier or higher)."
                ],
            )

        client = self._get_client()
        if not client:
            return self._create_result(
                items=[],
                status=SourceStatus.UNAVAILABLE,
                errors=["Failed to initialize Twitter client. Is tweepy installed?"],
            )

        loop = asyncio.get_event_loop()

        # Search for keywords
        for keyword in keywords[:10]:  # Limit to preserve rate limits
            try:
                # Run search in executor since tweepy is synchronous
                tweets = await loop.run_in_executor(
                    None,
                    lambda k=keyword: client.search_recent_tweets(
                        query=f"{k} -is:retweet lang:en",
                        max_results=50,
                        tweet_fields=["created_at", "public_metrics", "lang", "geo"],
                        expansions=["author_id"],
                        user_fields=["name", "username", "public_metrics"]
                    )
                )

                if tweets.data:
                    for tweet in tweets.data:
                        metrics = tweet.public_metrics or {}
                        items.append(TrendItem(
                            id=str(tweet.id),
                            source=self.name,
                            source_url=f"https://twitter.com/i/web/status/{tweet.id}",
                            title=tweet.text[:100],
                            description=tweet.text,
                            raw_text=tweet.text,
                            language=tweet.lang,
                            volume=metrics.get("impression_count", 0),
                            engagement=(
                                metrics.get("like_count", 0) +
                                metrics.get("retweet_count", 0) +
                                metrics.get("reply_count", 0)
                            ),
                            published_at=tweet.created_at,
                            metadata={
                                "type": "tweet",
                                "search_query": keyword,
                                "like_count": metrics.get("like_count", 0),
                                "retweet_count": metrics.get("retweet_count", 0),
                                "reply_count": metrics.get("reply_count", 0),
                            }
                        ))

            except Exception as e:
                errors.append(f"Search error for '{keyword}': {e}")
                self.logger.error("twitter_search_error", keyword=keyword, error=str(e))

            # Rate limiting
            await asyncio.sleep(1)

        status = SourceStatus.ACTIVE if items else SourceStatus.DEGRADED
        if errors and not items:
            status = SourceStatus.UNAVAILABLE

        return self._create_result(items, status, errors, warnings)

    async def health_check(self) -> bool:
        """Check if Twitter API is accessible."""
        if not self._has_credentials():
            return False

        client = self._get_client()
        if not client:
            return False

        try:
            loop = asyncio.get_event_loop()
            # Simple test query
            result = await loop.run_in_executor(
                None,
                lambda: client.search_recent_tweets(
                    query="test",
                    max_results=10
                )
            )
            return result is not None

        except Exception as e:
            self.logger.error("health_check_failed", error=str(e))
            return False

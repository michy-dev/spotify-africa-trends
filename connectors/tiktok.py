"""
TikTok connector stub.

NOTE: TikTok API access is highly restricted and requires approval.
This is a placeholder connector that documents the expected interface.
"""

import structlog
from .base import StubConnector, ConnectorResult, SourceStatus

logger = structlog.get_logger()


class TikTokConnector(StubConnector):
    """
    Stub connector for TikTok trends.

    TikTok's API (Research API) requires:
    1. Application approval for research purposes
    2. Compliance with data usage policies
    3. Regional availability restrictions

    When access is available, this connector would track:
    - Trending sounds/music
    - Viral videos and challenges
    - Hashtag trends
    - Creator mentions

    Alternative approaches (if API unavailable):
    - Use TikTok Creative Center (publicly available trend data)
    - Manual monitoring and data entry
    - Third-party social listening tools with TikTok access
    """

    name = "tiktok"
    display_name = "TikTok"
    requires_auth = True

    def __init__(self, config: dict):
        super().__init__(
            config,
            warning=(
                "TikTok connector requires Research API access. "
                "Apply at https://developers.tiktok.com/. "
                "Alternative: Use TikTok Creative Center for public trend data."
            )
        )

    def _has_credentials(self) -> bool:
        """TikTok Research API credentials check."""
        import os
        return all([
            os.getenv("TIKTOK_CLIENT_KEY"),
            os.getenv("TIKTOK_CLIENT_SECRET"),
        ])

    async def fetch_creative_center_trends(self) -> list:
        """
        Placeholder for fetching from TikTok Creative Center.

        The Creative Center (https://ads.tiktok.com/business/creativecenter/trends)
        provides some public trend data that could be scraped, though this
        may violate ToS. Implement with caution.
        """
        self.logger.warning(
            "creative_center_not_implemented",
            message="TikTok Creative Center scraping not implemented. "
                    "Consider manual data entry or authorized API access."
        )
        return []

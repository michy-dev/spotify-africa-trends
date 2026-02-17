"""
Instagram connector stub.

NOTE: Instagram data access requires Meta Business API approval.
This is a placeholder connector that documents the expected interface.
"""

import structlog
from .base import StubConnector

logger = structlog.get_logger()


class InstagramConnector(StubConnector):
    """
    Stub connector for Instagram trends.

    Instagram API access requires:
    1. Meta Business account
    2. App review for Instagram Basic Display or Graph API
    3. Compliance with Meta Platform Terms

    When access is available, this connector would track:
    - Hashtag trends (Instagram Graph API)
    - Reels trends
    - Creator mentions
    - Engagement on music-related content

    API documentation:
    - https://developers.facebook.com/docs/instagram-api/

    Alternative approaches:
    - CrowdTangle (Meta's research tool, restricted access)
    - Third-party social listening platforms
    - Manual monitoring of key accounts
    """

    name = "instagram"
    display_name = "Instagram"
    requires_auth = True

    def __init__(self, config: dict):
        super().__init__(
            config,
            warning=(
                "Instagram connector requires Meta Graph API access. "
                "Apply through Meta Business Suite. "
                "Alternative: Use CrowdTangle or third-party listening tools."
            )
        )

    def _has_credentials(self) -> bool:
        """Instagram API credentials check."""
        import os
        return bool(os.getenv("INSTAGRAM_ACCESS_TOKEN"))

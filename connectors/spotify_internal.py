"""
Spotify internal signals connector stub.

This is a PLACEHOLDER for internal Spotify data integration.
No actual implementation - requires internal Spotify systems access.
"""

import structlog
from .base import StubConnector, TrendItem

logger = structlog.get_logger()


class SpotifyInternalConnector(StubConnector):
    """
    Stub connector for internal Spotify signals.

    This connector is a PLACEHOLDER ONLY and does not generate fake data.

    When integrated with internal systems, this could provide:

    1. Streaming Signals:
       - Daily/weekly streaming spikes
       - Viral track detection
       - Playlist add velocity
       - Geographic hotspots

    2. Artist Signals:
       - New releases in focus markets
       - Follower growth anomalies
       - Social link activity
       - Collaboration networks

    3. Platform Signals:
       - Feature usage (Blend, Wrapped, etc.)
       - App reviews and ratings trends
       - Support ticket spikes
       - Technical issues

    4. Editorial Signals:
       - Playlist placements
       - Campaign activations
       - Partnership announcements

    Integration Notes:
    - Would require internal API access
    - May need data warehouse queries (BigQuery, etc.)
    - Should respect internal data governance policies
    - Consider PII and confidentiality requirements
    """

    name = "spotify_internal"
    display_name = "Spotify Internal (Stub)"
    requires_auth = True

    def __init__(self, config: dict):
        super().__init__(
            config,
            warning=(
                "Spotify Internal connector is a placeholder. "
                "Requires integration with internal Spotify systems. "
                "No data is fetched or generated."
            )
        )

    def _has_credentials(self) -> bool:
        """Internal system credentials check."""
        # Always False for stub
        return False

    # Interface documentation for future implementation

    async def get_streaming_spikes(
        self,
        markets: list[str],
        threshold: float = 2.0
    ) -> list[TrendItem]:
        """
        Placeholder: Get tracks with streaming above threshold vs baseline.

        Args:
            markets: List of market codes
            threshold: Multiplier above baseline to flag as spike

        Returns:
            List of TrendItem for tracks with streaming spikes
        """
        self.logger.info("streaming_spikes_stub_called")
        return []

    async def get_viral_tracks(
        self,
        markets: list[str],
        limit: int = 50
    ) -> list[TrendItem]:
        """
        Placeholder: Get currently viral tracks.

        Args:
            markets: List of market codes
            limit: Maximum tracks to return

        Returns:
            List of TrendItem for viral tracks
        """
        self.logger.info("viral_tracks_stub_called")
        return []

    async def get_artist_signals(
        self,
        artist_ids: list[str]
    ) -> list[TrendItem]:
        """
        Placeholder: Get signals for specific artists.

        Args:
            artist_ids: List of Spotify artist IDs

        Returns:
            List of TrendItem with artist-specific signals
        """
        self.logger.info("artist_signals_stub_called")
        return []

    async def get_platform_issues(self) -> list[TrendItem]:
        """
        Placeholder: Get current platform issues/incidents.

        Returns:
            List of TrendItem for active platform issues
        """
        self.logger.info("platform_issues_stub_called")
        return []

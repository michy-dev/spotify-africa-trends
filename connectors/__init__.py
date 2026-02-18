"""
Data source connectors for Spotify Africa Trends Dashboard.

Each connector implements the BaseConnector interface and handles
data collection from a specific source.
"""

from .base import BaseConnector, ConnectorResult, TrendItem
from .google_trends import GoogleTrendsConnector
from .reddit import RedditConnector
from .news_rss import NewsRSSConnector
from .wikipedia import WikipediaConnector
from .youtube import YouTubeConnector
from .twitter import TwitterConnector
from .tiktok import TikTokConnector
from .instagram import InstagramConnector
from .spotify_internal import SpotifyInternalConnector
from .artist_spikes import ArtistSpikesConnector
from .style_signals import StyleSignalsConnector
# CultureSearchConnector removed - Google Trends rate limiting prevents population

__all__ = [
    "BaseConnector",
    "ConnectorResult",
    "TrendItem",
    "GoogleTrendsConnector",
    "RedditConnector",
    "NewsRSSConnector",
    "WikipediaConnector",
    "YouTubeConnector",
    "TwitterConnector",
    "TikTokConnector",
    "InstagramConnector",
    "SpotifyInternalConnector",
    "ArtistSpikesConnector",
    "StyleSignalsConnector",
]

# Registry of available connectors
CONNECTOR_REGISTRY = {
    "google_trends": GoogleTrendsConnector,
    "reddit": RedditConnector,
    "news_rss": NewsRSSConnector,
    "wikipedia": WikipediaConnector,
    "youtube": YouTubeConnector,
    "twitter": TwitterConnector,
    "tiktok": TikTokConnector,
    "instagram": InstagramConnector,
    "spotify_internal": SpotifyInternalConnector,
    "artist_spikes": ArtistSpikesConnector,
    "style_signals": StyleSignalsConnector,
}


def get_connector(name: str) -> type:
    """Get connector class by name."""
    if name not in CONNECTOR_REGISTRY:
        raise ValueError(f"Unknown connector: {name}. Available: {list(CONNECTOR_REGISTRY.keys())}")
    return CONNECTOR_REGISTRY[name]

"""
Artist Search Spikes connector for detecting artist search interest spikes.

Extends Google Trends for artist-specific queries per market.
"""

import asyncio
import hashlib
import statistics
from datetime import datetime, timedelta
from typing import List, Optional, Dict
import structlog

from .base import BaseConnector, ConnectorResult, TrendItem, SourceStatus
from storage.base import ArtistSpike

logger = structlog.get_logger()

# Market code to Google Trends geo mapping
MARKET_GEO_MAP = {
    "ZA": "ZA",
    "NG": "NG",
    "KE": "KE",
    "GH": "GH",
}

# Seed artists per market (can be extended from config)
MARKET_ARTISTS = {
    "NG": [
        "Burna Boy", "Wizkid", "Davido", "Tems", "Rema", "Asake",
        "Ayra Starr", "Fireboy DML", "Olamide", "Tiwa Savage",
        "Kizz Daniel", "Omah Lay", "Ckay", "Pheelz", "BNXN",
        "Adekunle Gold", "Joeboy", "Ruger", "Seyi Vibez", "Magixx"
    ],
    "KE": [
        "Sauti Sol", "Nyashinski", "Otile Brown", "Nviiri the Storyteller",
        "Bensoul", "Bien", "Nadia Mukami", "Fena Gitu", "Khaligraph Jones",
        "Octopizzo", "King Kaka", "Mejja", "Ssaru", "Trio Mio", "Breeder LW"
    ],
    "GH": [
        "Black Sherif", "Sarkodie", "Stonebwoy", "Shatta Wale",
        "King Promise", "Camidoh", "Gyakie", "Kwesi Arthur",
        "Kuami Eugene", "KiDi", "Medikal", "R2Bees", "Darkovibes", "Amaarae"
    ],
    "ZA": [
        "Nasty C", "Cassper Nyovest", "Focalistic", "Uncle Waffles",
        "DBN Gogo", "Kabza De Small", "DJ Maphorisa", "A-Reece",
        "Costa Titch", "Blxckie", "Young Stunna", "Lady Du",
        "Musa Keys", "Tyla", "Makhadzi"
    ],
}

# Common word artists that need disambiguation
AMBIGUOUS_ARTISTS = {
    "Tems", "Rema", "Bien", "KiDi", "Tyla", "Ice",
}


def generate_spike_id(artist: str, market: str, time_window: str) -> str:
    """Generate unique ID for artist spike."""
    key = f"{artist.lower()}:{market}:{time_window}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def calculate_spike_score(
    current: float,
    baseline: float,
    method: str = "wow"
) -> float:
    """
    Calculate spike score (0-100).

    Methods:
    - wow: Week-over-Week percentage change
    - zscore: Z-score vs baseline, normalized to 0-100
    """
    if method == "wow":
        if baseline <= 0:
            return min(100, current * 2) if current > 0 else 0
        change = ((current - baseline) / baseline) * 100
        return min(100, max(0, change))

    elif method == "zscore":
        # Assume baseline is mean, calculate approximate z-score
        if baseline <= 0:
            return min(100, current * 2) if current > 0 else 0
        z = (current - baseline) / (baseline * 0.3)  # Approx std dev
        # Normalize z-score to 0-100 (z=3 -> 100)
        return min(100, max(0, (z / 3) * 100))

    return 0


def determine_confidence(
    spike_score: float,
    data_points: int,
    has_related: bool
) -> str:
    """Determine confidence level based on signal strength."""
    if spike_score >= 50 and data_points >= 5 and has_related:
        return "high"
    elif spike_score >= 25 or (data_points >= 3 and has_related):
        return "medium"
    return "low"


class ArtistSpikesConnector(BaseConnector):
    """
    Connector for detecting artist search spikes.

    Features:
    - Per-country (NG/KE/GH/ZA) artist detection
    - Two time windows: "24h" and "7d"
    - Spike score calculation (WoW % change or z-score)
    - Sparkline data (7 data points)
    - "Why spiking" bullets from related queries/topics
    - Ambiguous flag for common-word names
    """

    name = "artist_spikes"
    display_name = "Artist Search Spikes"
    requires_auth = False

    def __init__(self, config: dict):
        super().__init__(config)
        self._pytrends = None
        # Allow config to extend artist lists
        self.artists_by_market = {**MARKET_ARTISTS}
        config_artists = config.get("entities", {}).get("artists", [])
        if config_artists:
            # Add config artists to all markets
            for market in self.artists_by_market:
                for artist in config_artists:
                    if artist not in self.artists_by_market[market]:
                        self.artists_by_market[market].append(artist)

    def _get_pytrends(self):
        """Lazy load pytrends."""
        if self._pytrends is None:
            try:
                from pytrends.request import TrendReq
                self._pytrends = TrendReq(hl="en-US", tz=120)
            except ImportError:
                self.logger.error("pytrends not installed")
                raise
        return self._pytrends

    async def fetch(
        self,
        markets: List[str] = None,
        keywords: List[str] = None,
        **kwargs
    ) -> ConnectorResult:
        """Fetch artist spikes for all markets."""
        items = []
        errors = []

        if markets is None:
            markets = list(MARKET_GEO_MAP.keys())

        try:
            pytrends = self._get_pytrends()
        except ImportError:
            return self._create_result(
                items=[],
                status=SourceStatus.UNAVAILABLE,
                errors=["pytrends library not installed"],
            )

        for market in markets:
            if market not in MARKET_GEO_MAP:
                continue

            try:
                # Fetch for both time windows
                for time_window in ["24h", "7d"]:
                    spikes = await self._fetch_market_spikes(
                        pytrends, market, time_window
                    )
                    # Convert ArtistSpike to TrendItem for connector interface
                    for spike in spikes:
                        items.append(TrendItem(
                            id=spike.id,
                            source=self.name,
                            title=f"{spike.artist_name} search spike",
                            description="; ".join(spike.why_spiking) if spike.why_spiking else f"Rising search interest in {market}",
                            market=market,
                            volume=int(spike.current_interest),
                            velocity=spike.spike_score / 100,
                            metadata={
                                "type": "artist_spike",
                                "artist_name": spike.artist_name,
                                "spike_score": spike.spike_score,
                                "time_window": spike.time_window,
                                "sparkline_data": spike.sparkline_data,
                                "why_spiking": spike.why_spiking,
                                "confidence": spike.confidence,
                                "is_ambiguous": spike.is_ambiguous,
                                "related_queries": spike.related_queries,
                                "related_topics": spike.related_topics,
                            }
                        ))

            except Exception as e:
                errors.append(f"Error fetching spikes for {market}: {str(e)}")
                self.logger.error("artist_spikes_error", market=market, error=str(e))

            await asyncio.sleep(1)

        status = SourceStatus.ACTIVE if not errors else SourceStatus.DEGRADED
        return self._create_result(items, status, errors)

    async def fetch_spikes(
        self,
        markets: List[str] = None,
        time_window: str = "24h",
    ) -> List[ArtistSpike]:
        """
        Fetch artist spikes as ArtistSpike objects.

        This is the main method for direct spike data retrieval.
        """
        if markets is None:
            markets = list(MARKET_GEO_MAP.keys())

        all_spikes = []

        try:
            pytrends = self._get_pytrends()
        except ImportError:
            return []

        for market in markets:
            if market not in MARKET_GEO_MAP:
                continue

            try:
                spikes = await self._fetch_market_spikes(pytrends, market, time_window)
                all_spikes.extend(spikes)
            except Exception as e:
                self.logger.error("fetch_spikes_error", market=market, error=str(e))

            await asyncio.sleep(1)

        # Sort by spike score and return top 20 per market
        market_spikes: Dict[str, List[ArtistSpike]] = {}
        for spike in all_spikes:
            if spike.market not in market_spikes:
                market_spikes[spike.market] = []
            market_spikes[spike.market].append(spike)

        result = []
        for market, spikes in market_spikes.items():
            sorted_spikes = sorted(spikes, key=lambda x: x.spike_score, reverse=True)
            result.extend(sorted_spikes[:20])

        return result

    async def _fetch_market_spikes(
        self,
        pytrends,
        market: str,
        time_window: str
    ) -> List[ArtistSpike]:
        """Fetch artist spikes for a single market."""
        spikes = []
        geo = MARKET_GEO_MAP[market]
        artists = self.artists_by_market.get(market, [])

        # Determine timeframe based on window
        if time_window == "24h":
            timeframe = "now 1-d"
        else:
            timeframe = "now 7-d"

        # Process artists in batches of 5 (Google Trends limit)
        for i in range(0, len(artists), 5):
            batch = artists[i:i+5]

            try:
                loop = asyncio.get_event_loop()

                # Build payload
                await loop.run_in_executor(
                    None,
                    lambda: pytrends.build_payload(batch, timeframe=timeframe, geo=geo)
                )

                # Get interest over time
                interest = await loop.run_in_executor(
                    None,
                    pytrends.interest_over_time
                )

                if interest is not None and not interest.empty:
                    for artist in batch:
                        if artist not in interest.columns:
                            continue

                        series = interest[artist]
                        current = float(series.iloc[-1])

                        # Calculate baseline (mean excluding last point)
                        if len(series) > 1:
                            baseline = float(series.iloc[:-1].mean())
                        else:
                            baseline = 0

                        # Calculate spike score
                        spike_score = calculate_spike_score(current, baseline)

                        # Skip low-spike artists
                        if spike_score < 10:
                            continue

                        # Generate sparkline data (7 points)
                        if len(series) >= 7:
                            sparkline = [float(x) for x in series.iloc[-7:].tolist()]
                        else:
                            sparkline = [float(x) for x in series.tolist()]

                        # Get related queries for "why spiking"
                        why_spiking = []
                        related_queries = []
                        related_topics = []

                        try:
                            # Build payload for single artist to get related
                            await loop.run_in_executor(
                                None,
                                lambda a=artist: pytrends.build_payload([a], timeframe=timeframe, geo=geo)
                            )

                            related = await loop.run_in_executor(
                                None,
                                pytrends.related_queries
                            )

                            if related and artist in related:
                                rising = related[artist].get("rising")
                                if rising is not None and not rising.empty:
                                    queries = rising["query"].head(5).tolist()
                                    related_queries = queries
                                    # Extract context for why_spiking
                                    for q in queries[:3]:
                                        if artist.lower() not in q.lower():
                                            why_spiking.append(f"Related to: {q}")

                            topics = await loop.run_in_executor(
                                None,
                                pytrends.related_topics
                            )

                            if topics and artist in topics:
                                rising = topics[artist].get("rising")
                                if rising is not None and not rising.empty:
                                    topic_titles = rising["topic_title"].head(5).tolist()
                                    related_topics = topic_titles
                                    for t in topic_titles[:2]:
                                        if artist.lower() not in t.lower() and len(why_spiking) < 3:
                                            why_spiking.append(f"Trending with: {t}")

                        except Exception as e:
                            self.logger.debug("related_queries_error", artist=artist, error=str(e))

                        # Add default why if none found
                        if not why_spiking:
                            if spike_score >= 50:
                                why_spiking.append(f"Significant search interest increase in {market}")
                            else:
                                why_spiking.append(f"Rising search interest in {market}")

                        # Determine confidence
                        confidence = determine_confidence(
                            spike_score,
                            len(sparkline),
                            len(related_queries) > 0
                        )

                        # Check if ambiguous name
                        is_ambiguous = artist in AMBIGUOUS_ARTISTS

                        spike = ArtistSpike(
                            id=generate_spike_id(artist, market, time_window),
                            artist_name=artist,
                            market=market,
                            spike_score=spike_score,
                            time_window=time_window,
                            sparkline_data=sparkline,
                            why_spiking=why_spiking,
                            confidence=confidence,
                            is_ambiguous=is_ambiguous,
                            related_queries=related_queries,
                            related_topics=related_topics,
                            current_interest=current,
                            baseline_interest=baseline,
                            collected_at=datetime.utcnow(),
                        )
                        spikes.append(spike)

            except Exception as e:
                self.logger.warning("batch_error", batch=batch, market=market, error=str(e))

            await asyncio.sleep(0.5)

        return spikes

    async def health_check(self) -> bool:
        """Check if Google Trends is accessible."""
        try:
            pytrends = self._get_pytrends()
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda: pytrends.trending_searches(pn="south_africa")
            )
            return result is not None
        except Exception as e:
            self.logger.error("health_check_failed", error=str(e))
            return False

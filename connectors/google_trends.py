"""
Google Trends connector for tracking search interest across African markets.
"""

import asyncio
from datetime import datetime, timedelta
from typing import Optional
import structlog

from .base import BaseConnector, ConnectorResult, TrendItem, SourceStatus

logger = structlog.get_logger()

# Market code to Google Trends geo mapping
MARKET_GEO_MAP = {
    "ZA": "ZA",  # South Africa
    "NG": "NG",  # Nigeria
    "KE": "KE",  # Kenya
    "GH": "GH",  # Ghana
    "TZ": "TZ",  # Tanzania
    "UG": "UG",  # Uganda
    "AO": "AO",  # Angola
    "CI": "CI",  # Côte d'Ivoire
    "SN": "SN",  # Senegal
    "EG": "EG",  # Egypt
    "MA": "MA",  # Morocco
}


class GoogleTrendsConnector(BaseConnector):
    """
    Connector for Google Trends data.

    Uses pytrends library to fetch:
    - Daily trending searches
    - Interest over time for keywords
    - Related queries and topics
    """

    name = "google_trends"
    display_name = "Google Trends"
    requires_auth = False

    def __init__(self, config: dict):
        super().__init__(config)
        self._pytrends = None

    def _get_pytrends(self):
        """Lazy load pytrends to avoid import errors if not installed."""
        if self._pytrends is None:
            try:
                from pytrends.request import TrendReq
                self._pytrends = TrendReq(hl="en-US", tz=120)  # GMT+2 for Africa
            except ImportError:
                self.logger.error("pytrends not installed")
                raise
        return self._pytrends

    async def fetch(
        self,
        markets: list[str],
        keywords: list[str],
        **kwargs
    ) -> ConnectorResult:
        """
        Fetch trending data from Google Trends.

        Args:
            markets: List of market codes
            keywords: List of keywords to track interest
        """
        items = []
        errors = []
        warnings = []

        try:
            pytrends = self._get_pytrends()
        except ImportError:
            return self._create_result(
                items=[],
                status=SourceStatus.UNAVAILABLE,
                errors=["pytrends library not installed"],
            )

        # Fetch daily trending searches per market
        for market in markets:
            geo = MARKET_GEO_MAP.get(market)
            if not geo:
                warnings.append(f"Unknown market: {market}")
                continue

            try:
                trending_items = await self._fetch_daily_trends(pytrends, geo, market)
                items.extend(trending_items)
            except Exception as e:
                errors.append(f"Error fetching trends for {market}: {str(e)}")
                self.logger.error("google_trends_fetch_error", market=market, error=str(e))

            # Rate limiting between markets
            await asyncio.sleep(1)

        # Fetch interest over time for keywords
        if keywords:
            try:
                keyword_items = await self._fetch_keyword_interest(pytrends, keywords, markets)
                items.extend(keyword_items)
            except Exception as e:
                errors.append(f"Error fetching keyword interest: {str(e)}")

        status = SourceStatus.ACTIVE if not errors else SourceStatus.DEGRADED
        return self._create_result(items, status, errors, warnings)

    async def _fetch_daily_trends(
        self,
        pytrends,
        geo: str,
        market: str
    ) -> list[TrendItem]:
        """Fetch daily trending searches for a market."""
        items = []

        try:
            # Run in executor since pytrends is synchronous
            loop = asyncio.get_event_loop()
            trending = await loop.run_in_executor(
                None,
                lambda: pytrends.trending_searches(pn=geo.lower())
            )

            if trending is not None and not trending.empty:
                for idx, row in trending.iterrows():
                    query = row[0] if isinstance(row, (list, tuple)) else str(row.iloc[0]) if hasattr(row, 'iloc') else str(row)
                    items.append(TrendItem(
                        id="",  # Will be auto-generated
                        source=self.name,
                        title=query,
                        description=f"Trending search in {market}",
                        market=market,
                        volume=100 - idx * 5,  # Rough ranking score
                        metadata={
                            "rank": idx + 1,
                            "geo": geo,
                            "type": "daily_trend",
                        }
                    ))

        except Exception as e:
            self.logger.warning("daily_trends_error", geo=geo, error=str(e))

        return items

    async def _fetch_keyword_interest(
        self,
        pytrends,
        keywords: list[str],
        markets: list[str]
    ) -> list[TrendItem]:
        """Fetch interest over time for specific keywords."""
        items = []

        # Process keywords in batches of 5 (Google Trends limit)
        for i in range(0, len(keywords), 5):
            batch = keywords[i:i+5]

            for market in markets:
                geo = MARKET_GEO_MAP.get(market, "")

                try:
                    loop = asyncio.get_event_loop()

                    # Build payload
                    await loop.run_in_executor(
                        None,
                        lambda b=batch, g=geo: pytrends.build_payload(
                            b,
                            cat=0,
                            timeframe="now 7-d",
                            geo=g
                        )
                    )

                    # Get interest over time
                    interest = await loop.run_in_executor(
                        None,
                        pytrends.interest_over_time
                    )

                    if interest is not None and not interest.empty:
                        for keyword in batch:
                            if keyword in interest.columns:
                                current = interest[keyword].iloc[-1]
                                avg = interest[keyword].mean()
                                velocity = (current - avg) / avg if avg > 0 else 0

                                if velocity > 0.2:  # Only include if above baseline
                                    items.append(TrendItem(
                                        id="",
                                        source=self.name,
                                        title=keyword,
                                        description=f"Rising search interest in {market}",
                                        market=market,
                                        volume=int(current),
                                        velocity=velocity,
                                        metadata={
                                            "type": "keyword_interest",
                                            "current_interest": int(current),
                                            "average_interest": float(avg),
                                            "geo": geo,
                                        }
                                    ))

                except Exception as e:
                    self.logger.warning(
                        "keyword_interest_error",
                        keywords=batch,
                        market=market,
                        error=str(e)
                    )

                await asyncio.sleep(0.5)  # Rate limiting

        return items

    async def health_check(self) -> bool:
        """Check if Google Trends is accessible."""
        try:
            pytrends = self._get_pytrends()
            # Simple check - try to get trending for South Africa
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda: pytrends.trending_searches(pn="south_africa")
            )
            return result is not None
        except Exception as e:
            self.logger.error("health_check_failed", error=str(e))
            return False

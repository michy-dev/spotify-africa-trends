"""
Culture Search connector for rising culture terms with sensitivity tags.

Fetches Google Trends rising queries by category for cultural moments.
"""

import asyncio
import hashlib
import re
from datetime import datetime
from typing import List, Optional, Dict, Set
import structlog

from .base import BaseConnector, ConnectorResult, TrendItem, SourceStatus
from storage.base import CultureSearch, SensitivityTag

logger = structlog.get_logger()

# Market code to Google Trends geo mapping
# For trending_searches: use country names (lowercase with underscores)
# For interest_over_time: use country codes
MARKET_GEO_MAP = {
    "ZA": "ZA",
    "NG": "NG",
    "KE": "KE",
    "GH": "GH",
}

# Pytrends trending_searches expects country names
MARKET_TRENDING_MAP = {
    "ZA": "south_africa",
    "NG": "nigeria",
    "KE": "kenya",
    "GH": "ghana",
}

# Google Trends category IDs
CATEGORY_IDS = {
    "entertainment": 3,
    "music": 35,
    "sports": 20,
    "movies": 34,
    "tv": 36,
    "fashion": 185,
    "celebrities": 184,
}

# Keywords for sensitivity tag classification
SENSITIVITY_PATTERNS = {
    SensitivityTag.MUSIC.value: [
        r"\b(album|song|concert|tour|music|artist|rapper|singer|dj|amapiano|afrobeats)\b",
        r"\b(spotify|apple music|boomplay|playlist|stream)\b",
    ],
    SensitivityTag.FASHION.value: [
        r"\b(fashion|style|outfit|wear|clothing|brand|designer|runway)\b",
        r"\b(sneaker|drip|fit|lookbook|streetwear)\b",
    ],
    SensitivityTag.SPORT.value: [
        r"\b(football|soccer|match|game|player|team|league|afcon|psl)\b",
        r"\b(basketball|athletics|olympics|world cup)\b",
    ],
    SensitivityTag.FILM_TV.value: [
        r"\b(movie|film|series|show|episode|nollywood|netflix|tv)\b",
        r"\b(big brother|idol|reality|drama)\b",
    ],
    SensitivityTag.MEME.value: [
        r"\b(meme|viral|trend|challenge|tiktok)\b",
        r"\b(funny|lol|reaction|slang)\b",
    ],
    SensitivityTag.CELEBRITY.value: [
        r"\b(celebrity|star|famous|influencer|personality)\b",
        r"\b(wedding|divorce|dating|relationship|scandal)\b",
    ],
    SensitivityTag.POLITICS.value: [
        r"\b(president|minister|government|parliament|election)\b",
        r"\b(protest|strike|policy|vote|political|party)\b",
        r"\b(anc|apc|pdp|jubilee|eac|ecowas)\b",
    ],
}

# High risk political terms
POLITICAL_RISK_TERMS = [
    "protest", "strike", "riot", "violence", "arrest", "corruption",
    "scandal", "controversy", "death", "killed", "war", "conflict",
]


def generate_search_id(term: str, market: str) -> str:
    """Generate unique ID for culture search."""
    key = f"{term.lower()}:{market}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def classify_sensitivity(term: str) -> str:
    """Classify a term into a sensitivity tag."""
    term_lower = term.lower()

    # Check each category
    for tag, patterns in SENSITIVITY_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, term_lower, re.IGNORECASE):
                return tag

    # Default to celebrity for unclassified
    return SensitivityTag.CELEBRITY.value


def determine_risk_level(term: str, sensitivity_tag: str) -> str:
    """Determine risk level based on content."""
    term_lower = term.lower()

    # Politics is always high sensitivity
    if sensitivity_tag == SensitivityTag.POLITICS.value:
        return "high"

    # Check for high-risk terms
    for risk_term in POLITICAL_RISK_TERMS:
        if risk_term in term_lower:
            return "high"

    # Medium risk for celebrity scandals
    scandal_terms = ["scandal", "controversy", "drama", "fight", "beef"]
    for scandal in scandal_terms:
        if scandal in term_lower:
            return "medium"

    return "low"


class CultureSearchConnector(BaseConnector):
    """
    Connector for rising culture search terms.

    Features:
    - Top 10 rising culture items per country
    - Sensitivity tags: Music / Fashion / Sport / Film-TV / Meme / Celebrity / Politics
    - Politics auto-tagged as High sensitivity
    - Regional view showing cross-country overlaps
    """

    name = "culture_search"
    display_name = "Culture Searches"
    requires_auth = False

    def __init__(self, config: dict):
        super().__init__(config)
        self._pytrends = None

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
        """Fetch culture searches for all markets."""
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
                searches = await self._fetch_market_culture(pytrends, market)
                for search in searches:
                    items.append(TrendItem(
                        id=search.id,
                        source=self.name,
                        title=search.term,
                        description=f"Rising {search.sensitivity_tag} search in {market}",
                        market=market,
                        volume=search.volume,
                        velocity=search.rise_percentage / 100,
                        metadata={
                            "type": "culture_search",
                            "sensitivity_tag": search.sensitivity_tag,
                            "rise_percentage": search.rise_percentage,
                            "risk_level": search.risk_level,
                            "is_cross_market": search.is_cross_market,
                            "markets_present": search.markets_present,
                        }
                    ))

            except Exception as e:
                errors.append(f"Error fetching culture for {market}: {str(e)}")
                self.logger.error("culture_search_error", market=market, error=str(e))

            await asyncio.sleep(1)

        status = SourceStatus.ACTIVE if not errors else SourceStatus.DEGRADED
        return self._create_result(items, status, errors)

    async def fetch_searches(
        self,
        markets: List[str] = None,
    ) -> List[CultureSearch]:
        """
        Fetch culture searches as CultureSearch objects.

        This is the main method for direct culture search retrieval.
        """
        if markets is None:
            markets = list(MARKET_GEO_MAP.keys())

        all_searches: List[CultureSearch] = []
        terms_by_market: Dict[str, Set[str]] = {}

        try:
            pytrends = self._get_pytrends()
        except ImportError:
            return []

        for market in markets:
            if market not in MARKET_GEO_MAP:
                continue

            try:
                searches = await self._fetch_market_culture(pytrends, market)
                all_searches.extend(searches)

                # Track terms by market for cross-market detection
                terms_by_market[market] = {s.term.lower() for s in searches}

            except Exception as e:
                self.logger.error("fetch_searches_error", market=market, error=str(e))

            await asyncio.sleep(1)

        # Detect cross-market terms
        all_searches = self._detect_cross_market(all_searches, terms_by_market)

        return all_searches

    async def _fetch_market_culture(
        self,
        pytrends,
        market: str
    ) -> List[CultureSearch]:
        """Fetch culture searches for a single market."""
        searches = []
        geo = MARKET_GEO_MAP[market]
        trending_pn = MARKET_TRENDING_MAP.get(market)
        seen_terms: Set[str] = set()

        # Skip if market not supported for trending searches
        if not trending_pn:
            self.logger.warning("market_not_supported_for_trending", market=market)
            return searches

        # Fetch trending searches
        try:
            loop = asyncio.get_event_loop()
            trending = await loop.run_in_executor(
                None,
                lambda: pytrends.trending_searches(pn=trending_pn)
            )

            if trending is not None and not trending.empty:
                for idx, row in trending.head(20).iterrows():
                    term = str(row.iloc[0]) if hasattr(row, 'iloc') else str(row[0])
                    term_lower = term.lower()

                    if term_lower in seen_terms:
                        continue
                    seen_terms.add(term_lower)

                    # Classify sensitivity
                    sensitivity_tag = classify_sensitivity(term)
                    risk_level = determine_risk_level(term, sensitivity_tag)

                    # Estimate rise percentage (based on rank)
                    rise_percentage = max(100, 500 - idx * 20)

                    search = CultureSearch(
                        id=generate_search_id(term, market),
                        term=term,
                        market=market,
                        sensitivity_tag=sensitivity_tag,
                        rise_percentage=rise_percentage,
                        volume=100 - idx * 4,  # Rough volume estimate
                        is_cross_market=False,
                        markets_present=[market],
                        risk_level=risk_level,
                        collected_at=datetime.utcnow(),
                    )
                    searches.append(search)

        except Exception as e:
            self.logger.warning("trending_searches_error", market=market, error=str(e))

        # Fetch category-specific rising queries
        for category_name, category_id in CATEGORY_IDS.items():
            try:
                # Use a common seed term to get category-filtered results
                seed_terms = ["music", "trending", "viral"]

                await loop.run_in_executor(
                    None,
                    lambda: pytrends.build_payload(
                        seed_terms[:1],
                        cat=category_id,
                        timeframe="now 7-d",
                        geo=geo
                    )
                )

                related = await loop.run_in_executor(
                    None,
                    pytrends.related_queries
                )

                if related:
                    for seed in seed_terms[:1]:
                        if seed not in related:
                            continue

                        rising = related[seed].get("rising")
                        if rising is None or rising.empty:
                            continue

                        for _, qrow in rising.head(5).iterrows():
                            term = qrow.get("query", "")
                            if not term or term.lower() in seen_terms:
                                continue
                            seen_terms.add(term.lower())

                            # Map category to sensitivity tag
                            tag_map = {
                                "music": SensitivityTag.MUSIC.value,
                                "entertainment": SensitivityTag.CELEBRITY.value,
                                "sports": SensitivityTag.SPORT.value,
                                "movies": SensitivityTag.FILM_TV.value,
                                "tv": SensitivityTag.FILM_TV.value,
                                "fashion": SensitivityTag.FASHION.value,
                                "celebrities": SensitivityTag.CELEBRITY.value,
                            }
                            sensitivity_tag = tag_map.get(category_name, classify_sensitivity(term))
                            risk_level = determine_risk_level(term, sensitivity_tag)

                            rise_value = qrow.get("value", 100)
                            if isinstance(rise_value, str):
                                rise_value = int(rise_value.replace(",", "").replace("+", "").replace("%", "") or 100)

                            search = CultureSearch(
                                id=generate_search_id(term, market),
                                term=term,
                                market=market,
                                sensitivity_tag=sensitivity_tag,
                                rise_percentage=float(rise_value),
                                volume=50,
                                is_cross_market=False,
                                markets_present=[market],
                                risk_level=risk_level,
                                collected_at=datetime.utcnow(),
                            )
                            searches.append(search)

                await asyncio.sleep(0.3)

            except Exception as e:
                self.logger.debug("category_error", category=category_name, error=str(e))

        # Sort by rise percentage and limit to top 10
        searches.sort(key=lambda x: x.rise_percentage, reverse=True)
        return searches[:10]

    def _detect_cross_market(
        self,
        searches: List[CultureSearch],
        terms_by_market: Dict[str, Set[str]]
    ) -> List[CultureSearch]:
        """Detect and mark cross-market terms."""
        # Find terms present in multiple markets
        all_terms: Dict[str, List[str]] = {}
        for market, terms in terms_by_market.items():
            for term in terms:
                if term not in all_terms:
                    all_terms[term] = []
                all_terms[term].append(market)

        cross_market_terms = {
            term: markets
            for term, markets in all_terms.items()
            if len(markets) > 1
        }

        # Update searches with cross-market info
        for search in searches:
            term_lower = search.term.lower()
            if term_lower in cross_market_terms:
                search.is_cross_market = True
                search.markets_present = cross_market_terms[term_lower]

        return searches

    async def get_overlaps(self, searches: List[CultureSearch] = None) -> List[dict]:
        """Get cross-market overlaps."""
        if searches is None:
            searches = await self.fetch_searches()

        overlaps = []
        seen_terms: Set[str] = set()

        for search in searches:
            if search.is_cross_market and search.term.lower() not in seen_terms:
                seen_terms.add(search.term.lower())
                overlaps.append({
                    "term": search.term,
                    "markets": search.markets_present,
                    "sensitivity_tag": search.sensitivity_tag,
                    "max_rise_percentage": search.rise_percentage,
                    "risk_level": search.risk_level,
                })

        return sorted(overlaps, key=lambda x: x["max_rise_percentage"], reverse=True)

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

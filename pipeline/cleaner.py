"""
Data cleaner - deduplication, normalization, and quality filtering.
"""

from datetime import datetime, timedelta
from typing import Optional
import hashlib
import re
import structlog

from connectors.base import TrendItem

logger = structlog.get_logger()


class DataCleaner:
    """
    Cleans and deduplicates trend data.

    Features:
    - Content-based deduplication
    - Text normalization
    - Quality scoring
    - Spam/bot detection
    """

    def __init__(self, config: dict):
        """
        Initialize cleaner.

        Args:
            config: Application configuration
        """
        self.config = config
        self.seen_hashes = set()
        self.seen_urls = set()

    def clean_batch(self, items: list[TrendItem]) -> list[TrendItem]:
        """
        Clean and dedupe a batch of items.

        Args:
            items: List of raw TrendItem objects

        Returns:
            Cleaned and deduplicated list
        """
        cleaned = []
        duplicates = 0
        filtered = 0

        for item in items:
            # Normalize text
            item = self._normalize_item(item)

            # Check for duplicates
            if self._is_duplicate(item):
                duplicates += 1
                continue

            # Quality check
            if not self._passes_quality_check(item):
                filtered += 1
                continue

            cleaned.append(item)

        logger.info(
            "cleaning_complete",
            input_count=len(items),
            output_count=len(cleaned),
            duplicates=duplicates,
            filtered=filtered
        )

        return cleaned

    def _normalize_item(self, item: TrendItem) -> TrendItem:
        """Normalize text fields in an item."""
        # Clean title
        if item.title:
            item.title = self._normalize_text(item.title)

        # Clean description
        if item.description:
            item.description = self._normalize_text(item.description)

        # Clean raw text
        if item.raw_text:
            item.raw_text = self._normalize_text(item.raw_text)

        # Normalize market code
        if item.market:
            item.market = item.market.upper().strip()

        return item

    def _normalize_text(self, text: str) -> str:
        """Normalize a text string."""
        if not text:
            return ""

        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)

        # Remove special unicode characters that might cause issues
        text = text.encode('ascii', 'ignore').decode('ascii')

        # Strip leading/trailing whitespace
        text = text.strip()

        return text

    def _is_duplicate(self, item: TrendItem) -> bool:
        """
        Check if item is a duplicate.

        Uses both URL-based and content-based deduplication.
        """
        # URL-based dedup
        if item.source_url:
            url_normalized = item.source_url.lower().strip()
            if url_normalized in self.seen_urls:
                return True
            self.seen_urls.add(url_normalized)

        # Content-based dedup
        content_hash = self._compute_content_hash(item)
        if content_hash in self.seen_hashes:
            return True
        self.seen_hashes.add(content_hash)

        return False

    def _compute_content_hash(self, item: TrendItem) -> str:
        """
        Compute a hash for content-based deduplication.

        Uses title + source for matching.
        """
        # Normalize content for hashing
        title_normalized = (item.title or "").lower().strip()
        source = item.source or ""

        content = f"{source}:{title_normalized}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    def _passes_quality_check(self, item: TrendItem) -> bool:
        """
        Check if item meets quality standards.

        Filters:
        - Empty content
        - Too short titles
        - Suspected spam/bot content
        """
        # Must have a title
        if not item.title or len(item.title) < 3:
            return False

        # Filter obvious spam patterns
        spam_patterns = [
            r'click here',
            r'buy now',
            r'limited offer',
            r'act fast',
            r'[0-9]{4,}\s*followers',
        ]

        text_to_check = f"{item.title} {item.description}".lower()
        for pattern in spam_patterns:
            if re.search(pattern, text_to_check, re.IGNORECASE):
                logger.debug("spam_filtered", title=item.title[:50])
                return False

        return True

    def reset(self):
        """Reset deduplication state (for new batch runs)."""
        self.seen_hashes.clear()
        self.seen_urls.clear()

    def dedupe_across_sources(
        self,
        items: list[TrendItem],
        similarity_threshold: float = 0.8
    ) -> list[TrendItem]:
        """
        Advanced cross-source deduplication using similarity matching.

        Groups similar items and keeps the highest quality version.
        """
        if len(items) <= 1:
            return items

        # Group by similar titles
        groups = []
        used = set()

        for i, item in enumerate(items):
            if i in used:
                continue

            group = [item]
            used.add(i)

            for j, other in enumerate(items[i+1:], start=i+1):
                if j in used:
                    continue

                if self._are_similar(item.title, other.title, similarity_threshold):
                    group.append(other)
                    used.add(j)

            groups.append(group)

        # Select best item from each group
        result = []
        for group in groups:
            if len(group) == 1:
                result.append(group[0])
            else:
                # Pick the one with highest engagement or most content
                best = max(
                    group,
                    key=lambda x: (x.engagement, len(x.description or ""))
                )
                # Merge sources in metadata
                best.metadata["merged_sources"] = [
                    item.source for item in group
                ]
                result.append(best)

        logger.info(
            "cross_source_dedupe",
            input_count=len(items),
            groups=len(groups),
            output_count=len(result)
        )

        return result

    def _are_similar(
        self,
        text1: str,
        text2: str,
        threshold: float
    ) -> bool:
        """
        Check if two texts are similar using Jaccard similarity.
        """
        if not text1 or not text2:
            return False

        # Tokenize
        words1 = set(text1.lower().split())
        words2 = set(text2.lower().split())

        # Jaccard similarity
        intersection = len(words1 & words2)
        union = len(words1 | words2)

        if union == 0:
            return False

        similarity = intersection / union
        return similarity >= threshold

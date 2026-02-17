"""
Data enricher - entity extraction, language detection, and metadata enhancement.
"""

import re
from typing import Optional
import structlog

from connectors.base import TrendItem

logger = structlog.get_logger()


class DataEnricher:
    """
    Enriches trend items with extracted entities and metadata.

    Features:
    - Named entity recognition (people, orgs, places, brands)
    - Language detection
    - Market inference from content
    - Artist/song detection using seed lists
    """

    def __init__(self, config: dict):
        """
        Initialize enricher with configuration.

        Args:
            config: Application configuration with entity seed lists
        """
        self.config = config
        self._nlp = None

        # Load seed entities for matching
        self.seed_entities = config.get("entities", {})
        self._compile_entity_patterns()

        # Market keywords for inference
        self.market_keywords = self._build_market_keywords()

    def _compile_entity_patterns(self):
        """Compile regex patterns for seed entity matching."""
        self.entity_patterns = {}

        for entity_type, entities in self.seed_entities.items():
            patterns = []
            for entity in entities:
                # Create case-insensitive pattern with word boundaries
                pattern = re.compile(
                    r'\b' + re.escape(entity) + r'\b',
                    re.IGNORECASE
                )
                patterns.append((entity, pattern))
            self.entity_patterns[entity_type] = patterns

    def _build_market_keywords(self) -> dict:
        """Build market detection keywords from config."""
        return {
            "ZA": ["south africa", "johannesburg", "cape town", "pretoria", "durban", "soweto", "za", "mzansi"],
            "NG": ["nigeria", "lagos", "abuja", "naija", "nigerians"],
            "KE": ["kenya", "nairobi", "mombasa", "kenyan"],
            "GH": ["ghana", "accra", "kumasi", "ghanaian"],
            "TZ": ["tanzania", "dar es salaam", "dodoma", "tanzanian", "bongo"],
            "UG": ["uganda", "kampala", "ugandan"],
            "AO": ["angola", "luanda", "angolan", "kuduro"],
            "CI": ["ivory coast", "côte d'ivoire", "cote d'ivoire", "abidjan", "ivorian"],
            "SN": ["senegal", "dakar", "senegalese"],
            "EG": ["egypt", "cairo", "alexandria", "egyptian"],
            "MA": ["morocco", "rabat", "casablanca", "marrakech", "moroccan"],
        }

    def _get_nlp(self):
        """Lazy load spaCy model for NER."""
        if self._nlp is None:
            try:
                import spacy
                # Try to load the medium English model
                try:
                    self._nlp = spacy.load("en_core_web_md")
                except OSError:
                    # Fall back to small model
                    try:
                        self._nlp = spacy.load("en_core_web_sm")
                    except OSError:
                        logger.warning("spacy_model_not_found", message="Run: python -m spacy download en_core_web_sm")
                        return None
            except ImportError:
                logger.warning("spacy_not_installed")
                return None
        return self._nlp

    def enrich_batch(self, items: list[TrendItem]) -> list[TrendItem]:
        """
        Enrich a batch of items.

        Args:
            items: List of TrendItem objects

        Returns:
            Enriched items
        """
        enriched = []

        for item in items:
            enriched.append(self.enrich_item(item))

        logger.info("enrichment_complete", count=len(enriched))
        return enriched

    def enrich_item(self, item: TrendItem) -> TrendItem:
        """
        Enrich a single item.

        Adds:
        - Extracted entities
        - Detected language
        - Inferred market (if not set)
        """
        text = f"{item.title} {item.description} {item.raw_text}"

        # Extract entities from seed lists
        item.entities = self._extract_seed_entities(text)

        # Extract entities using NLP
        nlp_entities = self._extract_nlp_entities(text)
        for entity_type, names in nlp_entities.items():
            if entity_type in item.entities:
                item.entities[entity_type].extend(names)
                item.entities[entity_type] = list(set(item.entities[entity_type]))
            else:
                item.entities[entity_type] = names

        # Detect language
        if not item.language:
            item.language = self._detect_language(text)

        # Infer market from content if not set
        if not item.market:
            item.market = self._infer_market(text)

        return item

    def _extract_seed_entities(self, text: str) -> dict:
        """Extract entities using configured seed lists."""
        extracted = {}

        for entity_type, patterns in self.entity_patterns.items():
            matches = []
            for entity_name, pattern in patterns:
                if pattern.search(text):
                    matches.append(entity_name)
            if matches:
                extracted[entity_type] = matches

        return extracted

    def _extract_nlp_entities(self, text: str) -> dict:
        """Extract entities using spaCy NER."""
        nlp = self._get_nlp()
        if not nlp:
            return {}

        # Limit text length for performance
        text = text[:5000]

        try:
            doc = nlp(text)
            entities = {}

            entity_type_map = {
                "PERSON": "people",
                "ORG": "organizations",
                "GPE": "places",
                "LOC": "places",
                "PRODUCT": "products",
                "EVENT": "events",
                "WORK_OF_ART": "works",
            }

            for ent in doc.ents:
                mapped_type = entity_type_map.get(ent.label_)
                if mapped_type:
                    if mapped_type not in entities:
                        entities[mapped_type] = []
                    if ent.text not in entities[mapped_type]:
                        entities[mapped_type].append(ent.text)

            return entities

        except Exception as e:
            logger.warning("nlp_extraction_error", error=str(e))
            return {}

    def _detect_language(self, text: str) -> Optional[str]:
        """
        Detect the language of text.

        Returns ISO 639-1 language code.
        """
        if not text or len(text) < 20:
            return None

        try:
            from langdetect import detect
            return detect(text[:1000])
        except ImportError:
            # Fall back to simple heuristics
            return self._detect_language_simple(text)
        except Exception:
            return None

    def _detect_language_simple(self, text: str) -> Optional[str]:
        """Simple language detection using keyword heuristics."""
        text_lower = text.lower()

        # French indicators
        french_words = ["le", "la", "les", "de", "du", "des", "est", "sont", "avec", "pour"]
        french_count = sum(1 for w in french_words if f" {w} " in f" {text_lower} ")

        # Portuguese indicators
        portuguese_words = ["não", "que", "para", "com", "uma", "são", "está"]
        portuguese_count = sum(1 for w in portuguese_words if w in text_lower)

        # Arabic indicators (basic check for Arabic script)
        arabic_pattern = re.compile(r'[\u0600-\u06FF]')
        has_arabic = bool(arabic_pattern.search(text))

        # Swahili indicators
        swahili_words = ["na", "kwa", "wa", "ya", "ni", "kutoka", "kwamba"]
        swahili_count = sum(1 for w in swahili_words if f" {w} " in f" {text_lower} ")

        if has_arabic:
            return "ar"
        elif french_count >= 3:
            return "fr"
        elif portuguese_count >= 2:
            return "pt"
        elif swahili_count >= 2:
            return "sw"
        else:
            return "en"  # Default to English

    def _infer_market(self, text: str) -> Optional[str]:
        """Infer market from text content."""
        text_lower = text.lower()

        # Count keyword matches per market
        market_scores = {}
        for market, keywords in self.market_keywords.items():
            score = sum(1 for kw in keywords if kw in text_lower)
            if score > 0:
                market_scores[market] = score

        if not market_scores:
            return None

        # Return market with highest score
        return max(market_scores, key=market_scores.get)

    def extract_hashtags(self, text: str) -> list[str]:
        """Extract hashtags from text."""
        pattern = re.compile(r'#(\w+)')
        return pattern.findall(text)

    def extract_mentions(self, text: str) -> list[str]:
        """Extract @mentions from text."""
        pattern = re.compile(r'@(\w+)')
        return pattern.findall(text)

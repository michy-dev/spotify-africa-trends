"""
Data processing pipeline for Spotify Africa Trends Dashboard.

Pipeline stages:
1. Collect - Fetch data from all connectors
2. Clean - Dedupe and normalize
3. Enrich - Entity extraction, language detection
4. Classify - Topic and subtopic classification
5. Score - Calculate comms relevance scores
6. Summarise - Generate comms-ready summaries
"""

from .collector import DataCollector
from .cleaner import DataCleaner
from .enricher import DataEnricher
from .classifier import TopicClassifier
from .scorer import CommsScorer
from .summariser import TrendSummariser
from .orchestrator import PipelineOrchestrator

__all__ = [
    "DataCollector",
    "DataCleaner",
    "DataEnricher",
    "TopicClassifier",
    "CommsScorer",
    "TrendSummariser",
    "PipelineOrchestrator",
]

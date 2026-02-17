"""
Pipeline orchestrator - coordinates the full data processing pipeline.

Stages:
1. Collect → 2. Clean → 3. Enrich → 4. Classify → 5. Score → 6. Summarise
"""

import asyncio
from datetime import datetime
from typing import Optional, List
import structlog

from connectors.base import TrendItem, ConnectorResult
from .collector import DataCollector
from .cleaner import DataCleaner
from .enricher import DataEnricher
from .classifier import TopicClassifier
from .scorer import CommsScorer, ScoreBreakdown
from .summariser import TrendSummariser, TrendSummary

logger = structlog.get_logger()


class PipelineOrchestrator:
    """
    Orchestrates the full trend processing pipeline.

    Features:
    - Configurable pipeline stages
    - Progress tracking
    - Error handling and recovery
    - Metrics collection
    """

    def __init__(self, config: dict):
        """
        Initialize orchestrator with configuration.

        Args:
            config: Full application configuration
        """
        self.config = config

        # Initialize pipeline stages
        self.collector = DataCollector(config)
        self.cleaner = DataCleaner(config)
        self.enricher = DataEnricher(config)
        self.classifier = TopicClassifier(config)
        self.scorer = CommsScorer(config)
        self.summariser = TrendSummariser(config)

        # Metrics
        self.last_run: Optional[datetime] = None
        self.last_run_metrics: dict = {}

    async def run_full_pipeline(
        self,
        markets: List[str] = None,
        keywords: List[str] = None
    ) -> dict:
        """
        Run the complete pipeline from collection to summarisation.

        Args:
            markets: Optional market filter
            keywords: Optional keyword filter

        Returns:
            Dictionary with pipeline results
        """
        started_at = datetime.utcnow()
        metrics = {
            "started_at": started_at.isoformat(),
            "stages": {},
        }

        logger.info("pipeline_starting")

        try:
            # Stage 1: Collect
            stage_start = datetime.utcnow()
            collection_results = await self.collector.collect_all(
                markets=markets,
                keywords=keywords
            )
            metrics["stages"]["collect"] = {
                "duration_seconds": (datetime.utcnow() - stage_start).total_seconds(),
                "items_collected": collection_results["summary"]["total_items"],
                "sources_successful": collection_results["summary"]["sources_successful"],
            }

            # Extract items from all sources
            all_items = []
            for source_name, result in collection_results["results"].items():
                if isinstance(result, ConnectorResult):
                    all_items.extend(result.items)

            logger.info("collection_complete", items=len(all_items))

            if not all_items:
                logger.warning("no_items_collected")
                return {
                    "success": False,
                    "message": "No items collected from any source",
                    "metrics": metrics,
                    "summaries": [],
                }

            # Stage 2: Clean
            stage_start = datetime.utcnow()
            self.cleaner.reset()  # Reset dedup state
            cleaned_items = self.cleaner.clean_batch(all_items)
            cleaned_items = self.cleaner.dedupe_across_sources(cleaned_items)
            metrics["stages"]["clean"] = {
                "duration_seconds": (datetime.utcnow() - stage_start).total_seconds(),
                "input_items": len(all_items),
                "output_items": len(cleaned_items),
                "duplicates_removed": len(all_items) - len(cleaned_items),
            }

            logger.info("cleaning_complete", items=len(cleaned_items))

            # Stage 3: Enrich
            stage_start = datetime.utcnow()
            enriched_items = self.enricher.enrich_batch(cleaned_items)
            metrics["stages"]["enrich"] = {
                "duration_seconds": (datetime.utcnow() - stage_start).total_seconds(),
                "items_processed": len(enriched_items),
            }

            logger.info("enrichment_complete", items=len(enriched_items))

            # Stage 4: Classify
            stage_start = datetime.utcnow()
            classified_items = self.classifier.classify_batch(enriched_items)
            metrics["stages"]["classify"] = {
                "duration_seconds": (datetime.utcnow() - stage_start).total_seconds(),
                "items_processed": len(classified_items),
            }

            logger.info("classification_complete", items=len(classified_items))

            # Stage 5: Score
            stage_start = datetime.utcnow()
            scored_items = self.scorer.score_batch(classified_items)
            metrics["stages"]["score"] = {
                "duration_seconds": (datetime.utcnow() - stage_start).total_seconds(),
                "items_scored": len(scored_items),
            }

            logger.info("scoring_complete", items=len(scored_items))

            # Stage 6: Summarise
            stage_start = datetime.utcnow()
            summaries = self.summariser.summarise_batch(scored_items)
            metrics["stages"]["summarise"] = {
                "duration_seconds": (datetime.utcnow() - stage_start).total_seconds(),
                "summaries_generated": len(summaries),
            }

            logger.info("summarisation_complete", summaries=len(summaries))

            # Finalize metrics
            completed_at = datetime.utcnow()
            metrics["completed_at"] = completed_at.isoformat()
            metrics["total_duration_seconds"] = (completed_at - started_at).total_seconds()
            metrics["success"] = True

            # Update instance state
            self.last_run = completed_at
            self.last_run_metrics = metrics

            # Generate summary statistics
            summary_stats = self._compute_summary_stats(summaries)

            return {
                "success": True,
                "summaries": summaries,
                "stats": summary_stats,
                "metrics": metrics,
            }

        except Exception as e:
            logger.error("pipeline_error", error=str(e))
            metrics["error"] = str(e)
            metrics["success"] = False
            return {
                "success": False,
                "error": str(e),
                "metrics": metrics,
                "summaries": [],
            }

    def _compute_summary_stats(self, summaries: List[TrendSummary]) -> dict:
        """Compute summary statistics from results."""
        if not summaries:
            return {}

        scores = [s.total_score for s in summaries]

        # Count by priority
        priority_counts = {"high": 0, "medium": 0, "low": 0}
        for s in summaries:
            priority_counts[s.priority_level] = priority_counts.get(s.priority_level, 0) + 1

        # Count by risk
        risk_counts = {"high": 0, "medium": 0, "low": 0}
        for s in summaries:
            risk_counts[s.risk_level] = risk_counts.get(s.risk_level, 0) + 1

        # Count by action
        action_counts = {}
        for s in summaries:
            action_counts[s.suggested_action] = action_counts.get(s.suggested_action, 0) + 1

        # Count by market
        market_counts = {}
        for s in summaries:
            market = s.market or "Unknown"
            market_counts[market] = market_counts.get(market, 0) + 1

        # Count by topic
        topic_counts = {}
        for s in summaries:
            topic_counts[s.topic_display] = topic_counts.get(s.topic_display, 0) + 1

        return {
            "total_trends": len(summaries),
            "score_stats": {
                "average": round(sum(scores) / len(scores), 1),
                "max": round(max(scores), 1),
                "min": round(min(scores), 1),
            },
            "by_priority": priority_counts,
            "by_risk": risk_counts,
            "by_action": action_counts,
            "by_market": market_counts,
            "by_topic": topic_counts,
        }

    async def run_collection_only(
        self,
        markets: List[str] = None,
        keywords: List[str] = None
    ) -> dict:
        """Run only the collection stage (for testing)."""
        return await self.collector.collect_all(markets, keywords)

    async def health_check(self) -> dict:
        """Check health of all pipeline components."""
        connector_health = await self.collector.health_check_all()

        return {
            "pipeline_status": "healthy",
            "last_run": self.last_run.isoformat() if self.last_run else None,
            "connectors": connector_health,
        }

    def get_status(self) -> dict:
        """Get current pipeline status."""
        return {
            "last_run": self.last_run.isoformat() if self.last_run else None,
            "last_run_metrics": self.last_run_metrics,
            "connectors": self.collector.get_connector_status(),
        }


async def run_pipeline(config: dict) -> dict:
    """
    Convenience function to run the full pipeline.

    Args:
        config: Application configuration

    Returns:
        Pipeline results
    """
    orchestrator = PipelineOrchestrator(config)
    return await orchestrator.run_full_pipeline()

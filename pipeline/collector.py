"""
Data collector - fetches data from all enabled connectors.
"""

import asyncio
from datetime import datetime
from typing import Optional
import structlog

from connectors import CONNECTOR_REGISTRY, get_connector
from connectors.base import ConnectorResult, TrendItem, SourceStatus

logger = structlog.get_logger()


class DataCollector:
    """
    Orchestrates data collection from multiple sources.

    Features:
    - Concurrent connector execution
    - Error handling and fallbacks
    - Source health monitoring
    - Rate limit awareness
    """

    def __init__(self, config: dict):
        """
        Initialize collector with configuration.

        Args:
            config: Full application configuration
        """
        self.config = config
        self.sources_config = config.get("sources", {})
        self.markets = [m["code"] for m in config.get("markets", {}).get("priority", [])]
        self.connectors = {}
        self._init_connectors()

    def _init_connectors(self):
        """Initialize all enabled connectors."""
        for source_name, source_config in self.sources_config.items():
            if not source_config.get("enabled", False):
                logger.info("connector_disabled", connector=source_name)
                continue

            try:
                connector_cls = get_connector(source_name)
                self.connectors[source_name] = connector_cls(source_config)
                logger.info("connector_initialized", connector=source_name)
            except ValueError as e:
                logger.warning("connector_not_found", connector=source_name, error=str(e))
            except Exception as e:
                logger.error("connector_init_error", connector=source_name, error=str(e))

    def _get_all_keywords(self) -> list[str]:
        """Extract all keywords from configuration."""
        keywords = set()

        # From topics
        topics = self.config.get("topics", {})
        for topic_key, topic_data in topics.items():
            keywords.update(topic_data.get("keywords", []))

        # From entities
        entities = self.config.get("entities", {})
        for entity_type, entity_list in entities.items():
            keywords.update(entity_list)

        return list(keywords)

    def _get_entities(self) -> dict:
        """Get configured entities by type."""
        return self.config.get("entities", {})

    async def collect_all(
        self,
        markets: list[str] = None,
        keywords: list[str] = None
    ) -> dict:
        """
        Collect data from all enabled connectors.

        Args:
            markets: Optional override for markets to query
            keywords: Optional override for keywords to search

        Returns:
            Dictionary with collection results per source
        """
        target_markets = markets or self.markets
        target_keywords = keywords or self._get_all_keywords()
        entities = self._get_entities()

        logger.info(
            "collection_starting",
            connectors=list(self.connectors.keys()),
            markets=target_markets,
            keyword_count=len(target_keywords)
        )

        started_at = datetime.utcnow()

        # Run all connectors concurrently
        tasks = {
            name: self._collect_from_source(
                connector,
                target_markets,
                target_keywords,
                entities
            )
            for name, connector in self.connectors.items()
        }

        results = {}
        task_results = await asyncio.gather(
            *tasks.values(),
            return_exceptions=True
        )

        for name, result in zip(tasks.keys(), task_results):
            if isinstance(result, Exception):
                logger.error("collector_exception", connector=name, error=str(result))
                results[name] = ConnectorResult(
                    source=name,
                    status=SourceStatus.UNAVAILABLE,
                    errors=[str(result)]
                )
            else:
                results[name] = result

        completed_at = datetime.utcnow()
        duration = (completed_at - started_at).total_seconds()

        # Log summary
        total_items = sum(r.item_count for r in results.values())
        successful = sum(1 for r in results.values() if r.success)

        logger.info(
            "collection_complete",
            total_items=total_items,
            successful_sources=successful,
            total_sources=len(results),
            duration_seconds=duration
        )

        return {
            "results": results,
            "summary": {
                "started_at": started_at.isoformat(),
                "completed_at": completed_at.isoformat(),
                "duration_seconds": duration,
                "total_items": total_items,
                "sources_successful": successful,
                "sources_total": len(results),
            }
        }

    async def _collect_from_source(
        self,
        connector,
        markets: list[str],
        keywords: list[str],
        entities: dict
    ) -> ConnectorResult:
        """Collect data from a single source with error handling."""
        try:
            # Pass entities for connectors that support it (e.g., Wikipedia)
            if hasattr(connector, 'fetch'):
                return await connector.fetch(
                    markets=markets,
                    keywords=keywords,
                    entities=entities
                )
            else:
                logger.warning("connector_no_fetch", connector=connector.name)
                return ConnectorResult(
                    source=connector.name,
                    status=SourceStatus.UNAVAILABLE,
                    errors=["Connector does not implement fetch()"]
                )
        except Exception as e:
            logger.error(
                "connector_fetch_error",
                connector=connector.name,
                error=str(e)
            )
            return ConnectorResult(
                source=connector.name,
                status=SourceStatus.UNAVAILABLE,
                errors=[str(e)]
            )

    async def health_check_all(self) -> dict:
        """
        Run health checks on all connectors.

        Returns:
            Dictionary with health status per connector
        """
        tasks = {
            name: connector.test_connection()
            for name, connector in self.connectors.items()
        }

        results = {}
        task_results = await asyncio.gather(
            *tasks.values(),
            return_exceptions=True
        )

        for name, result in zip(tasks.keys(), task_results):
            if isinstance(result, Exception):
                results[name] = {
                    "connector": name,
                    "healthy": False,
                    "error": str(result)
                }
            else:
                results[name] = result

        return results

    def get_connector_status(self) -> list[dict]:
        """Get status summary for all connectors."""
        return [
            {
                "name": connector.name,
                "display_name": connector.display_name,
                "status": connector.status.value,
                "enabled": connector.enabled,
                "priority": connector.priority,
                "reliability": connector.reliability,
            }
            for connector in self.connectors.values()
        ]

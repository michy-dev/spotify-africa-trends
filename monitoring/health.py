"""
Data health monitoring for trend-jack intelligence modules.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional
import structlog

from storage.base import DataHealth, ModuleStatus

logger = structlog.get_logger()


# Freshness thresholds per module (in hours)
FRESHNESS_THRESHOLDS = {
    "artist_spikes": {"ok": 4, "degraded": 12},
    "culture_searches": {"ok": 6, "degraded": 24},
    "style_signals": {"ok": 12, "degraded": 48},
    "pitch_cards": {"ok": 6, "degraded": 24},
    "trends": {"ok": 12, "degraded": 24},
}


class DataHealthMonitor:
    """
    Monitor data health across all trend-jack modules.

    Tracks:
    - Freshness: How old is the data?
    - Availability: Is the module returning data?
    - Errors: Recent error messages
    """

    def __init__(self, storage):
        self.storage = storage
        self._last_check: Dict[str, datetime] = {}
        self._health_cache: Dict[str, DataHealth] = {}

    async def check_all_modules(self) -> List[DataHealth]:
        """Check health of all modules and return status."""
        modules = [
            "artist_spikes",
            "culture_searches",
            "style_signals",
            "pitch_cards",
            "trends",
        ]

        health_results = []
        for module in modules:
            health = await self.check_module(module)
            health_results.append(health)
            await self.storage.save_data_health(health)

        return health_results

    async def check_module(self, module_name: str) -> DataHealth:
        """Check health of a specific module."""
        now = datetime.utcnow()

        try:
            item_count = 0
            last_updated = None

            # Get latest data timestamp based on module
            if module_name == "artist_spikes":
                items = await self.storage.get_artist_spikes(limit=1)
                item_count = len(items)
                if items:
                    last_updated = items[0].collected_at
            elif module_name == "culture_searches":
                items = await self.storage.get_culture_searches(limit=1)
                item_count = len(items)
                if items:
                    last_updated = items[0].collected_at
            elif module_name == "style_signals":
                items = await self.storage.get_style_signals(limit=1)
                item_count = len(items)
                if items:
                    last_updated = items[0].collected_at
            elif module_name == "pitch_cards":
                items = await self.storage.get_pitch_cards(limit=1)
                item_count = len(items)
                if items:
                    last_updated = items[0].generated_at
            elif module_name == "trends":
                items = await self.storage.get_trends(limit=1)
                item_count = len(items)
                if items:
                    last_updated = items[0].last_updated

            # Calculate freshness
            if last_updated:
                freshness_hours = (now - last_updated).total_seconds() / 3600
            else:
                freshness_hours = float('inf')

            # Determine status based on freshness
            thresholds = FRESHNESS_THRESHOLDS.get(module_name, {"ok": 12, "degraded": 24})
            if freshness_hours <= thresholds["ok"]:
                status = ModuleStatus.OK.value
            elif freshness_hours <= thresholds["degraded"]:
                status = ModuleStatus.DEGRADED.value
            else:
                status = ModuleStatus.DOWN.value

            return DataHealth(
                module_name=module_name,
                status=status,
                last_success=last_updated,
                last_error=None,
                item_count=item_count,
                freshness_hours=freshness_hours if freshness_hours != float('inf') else -1,
                checked_at=now,
            )

        except Exception as e:
            logger.error("health_check_error", module=module_name, error=str(e))
            return DataHealth(
                module_name=module_name,
                status=ModuleStatus.DOWN.value,
                last_success=None,
                last_error=str(e),
                item_count=0,
                freshness_hours=-1,
                checked_at=now,
            )

    async def get_health_summary(self) -> Dict:
        """Get summary of all module health statuses."""
        health_list = await self.storage.get_data_health()

        summary = {
            "overall_status": "ok",
            "modules": {},
            "checked_at": datetime.utcnow().isoformat(),
        }

        has_degraded = False
        has_down = False

        for health in health_list:
            summary["modules"][health.module_name] = health.to_dict()
            if health.status == ModuleStatus.DOWN.value:
                has_down = True
            elif health.status == ModuleStatus.DEGRADED.value:
                has_degraded = True

        if has_down:
            summary["overall_status"] = "down"
        elif has_degraded:
            summary["overall_status"] = "degraded"

        return summary

    async def update_module_health(
        self,
        module_name: str,
        success: bool,
        item_count: int = 0,
        error_message: Optional[str] = None,
    ) -> None:
        """Update module health after a refresh operation."""
        now = datetime.utcnow()

        if success:
            thresholds = FRESHNESS_THRESHOLDS.get(module_name, {"ok": 12, "degraded": 24})
            health = DataHealth(
                module_name=module_name,
                status=ModuleStatus.OK.value,
                last_success=now,
                last_error=None,
                item_count=item_count,
                freshness_hours=0,
                checked_at=now,
            )
        else:
            # Get previous health to preserve last_success
            existing = await self.storage.get_data_health()
            prev_success = None
            prev_count = 0
            for h in existing:
                if h.module_name == module_name:
                    prev_success = h.last_success
                    prev_count = h.item_count
                    break

            health = DataHealth(
                module_name=module_name,
                status=ModuleStatus.DEGRADED.value if prev_success else ModuleStatus.DOWN.value,
                last_success=prev_success,
                last_error=error_message,
                item_count=prev_count,
                freshness_hours=(now - prev_success).total_seconds() / 3600 if prev_success else -1,
                checked_at=now,
            )

        await self.storage.save_data_health(health)
        self._health_cache[module_name] = health

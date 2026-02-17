"""
Risk factor validation for QA and consistency checks.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import structlog

logger = structlog.get_logger()


# Risk level to score range mapping
RISK_SCORE_RANGES = {
    "low": (0, 33),
    "medium": (34, 66),
    "high": (67, 100),
}


@dataclass
class ValidationResult:
    """Result of a validation check."""
    is_valid: bool
    message: str
    severity: str  # "error", "warning", "info"
    field: Optional[str] = None
    value: Optional[str] = None


class RiskFactorValidator:
    """
    Validates risk factors for schema compliance and consistency.

    Checks:
    1. Schema: risk_level is enum (low/medium/high), risk_score is 0-100
    2. Freshness: Warn if trends older than 24 hours
    3. Consistency: risk_level matches risk_score range
    """

    def __init__(self, config: dict = None):
        self.config = config or {}
        self.valid_risk_levels = {"low", "medium", "high"}

    def validate_risk_level(self, risk_level: str) -> ValidationResult:
        """Validate risk_level is a valid enum value."""
        if risk_level not in self.valid_risk_levels:
            return ValidationResult(
                is_valid=False,
                message=f"Invalid risk_level '{risk_level}'. Must be one of: {', '.join(self.valid_risk_levels)}",
                severity="error",
                field="risk_level",
                value=risk_level,
            )
        return ValidationResult(
            is_valid=True,
            message="risk_level is valid",
            severity="info",
            field="risk_level",
            value=risk_level,
        )

    def validate_risk_score(self, risk_score: float) -> ValidationResult:
        """Validate risk_score is between 0 and 100."""
        if not isinstance(risk_score, (int, float)):
            return ValidationResult(
                is_valid=False,
                message=f"risk_score must be a number, got {type(risk_score).__name__}",
                severity="error",
                field="risk_score",
                value=str(risk_score),
            )

        if risk_score < 0 or risk_score > 100:
            return ValidationResult(
                is_valid=False,
                message=f"risk_score {risk_score} out of range. Must be 0-100",
                severity="error",
                field="risk_score",
                value=str(risk_score),
            )

        return ValidationResult(
            is_valid=True,
            message="risk_score is valid",
            severity="info",
            field="risk_score",
            value=str(risk_score),
        )

    def validate_risk_consistency(
        self,
        risk_level: str,
        risk_score: float
    ) -> ValidationResult:
        """Check that risk_level matches risk_score range."""
        expected_range = RISK_SCORE_RANGES.get(risk_level)
        if not expected_range:
            return ValidationResult(
                is_valid=False,
                message=f"Unknown risk_level: {risk_level}",
                severity="error",
                field="risk_level",
                value=risk_level,
            )

        min_score, max_score = expected_range
        if min_score <= risk_score <= max_score:
            return ValidationResult(
                is_valid=True,
                message=f"risk_level '{risk_level}' consistent with score {risk_score}",
                severity="info",
            )

        # Determine expected level based on score
        expected_level = self.get_risk_level_for_score(risk_score)
        return ValidationResult(
            is_valid=False,
            message=f"Inconsistency: risk_level '{risk_level}' but score {risk_score} suggests '{expected_level}'",
            severity="warning",
            field="risk_consistency",
            value=f"{risk_level}:{risk_score}",
        )

    def get_risk_level_for_score(self, risk_score: float) -> str:
        """Determine risk level from score."""
        if risk_score >= 67:
            return "high"
        elif risk_score >= 34:
            return "medium"
        return "low"

    def validate_freshness(
        self,
        last_updated: datetime,
        threshold_hours: int = 24
    ) -> ValidationResult:
        """Check if data is fresh enough."""
        now = datetime.utcnow()
        age_hours = (now - last_updated).total_seconds() / 3600

        if age_hours <= threshold_hours:
            return ValidationResult(
                is_valid=True,
                message=f"Data is fresh ({age_hours:.1f} hours old)",
                severity="info",
                field="freshness",
                value=f"{age_hours:.1f}h",
            )

        return ValidationResult(
            is_valid=False,
            message=f"Data may be stale ({age_hours:.1f} hours old, threshold: {threshold_hours}h)",
            severity="warning",
            field="freshness",
            value=f"{age_hours:.1f}h",
        )

    def validate_trend(self, trend: dict) -> List[ValidationResult]:
        """Run all validations on a trend record."""
        results = []

        # Schema validations
        if "risk_level" in trend:
            results.append(self.validate_risk_level(trend["risk_level"]))

        if "risk_score" in trend:
            results.append(self.validate_risk_score(trend["risk_score"]))

        # Consistency check
        if "risk_level" in trend and "risk_score" in trend:
            results.append(self.validate_risk_consistency(
                trend["risk_level"],
                trend["risk_score"]
            ))

        # Freshness check
        if "last_updated" in trend:
            if isinstance(trend["last_updated"], str):
                last_updated = datetime.fromisoformat(trend["last_updated"])
            else:
                last_updated = trend["last_updated"]
            results.append(self.validate_freshness(last_updated))

        return results

    def validate_batch(self, trends: List[dict]) -> Dict:
        """Validate a batch of trends and return summary."""
        all_results = []
        errors = 0
        warnings = 0

        for i, trend in enumerate(trends):
            results = self.validate_trend(trend)
            for result in results:
                if not result.is_valid:
                    if result.severity == "error":
                        errors += 1
                    elif result.severity == "warning":
                        warnings += 1
                all_results.append({
                    "trend_index": i,
                    "trend_id": trend.get("id", "unknown"),
                    **vars(result),
                })

        return {
            "total_trends": len(trends),
            "total_checks": len(all_results),
            "errors": errors,
            "warnings": warnings,
            "is_valid": errors == 0,
            "results": all_results,
        }

    def get_risk_badge_color(self, risk_level: str) -> str:
        """Get UI badge color for risk level."""
        colors = {
            "high": "red",
            "medium": "amber",
            "low": "green",
        }
        return colors.get(risk_level, "gray")

    def get_risk_badge_class(self, risk_level: str) -> str:
        """Get Tailwind CSS class for risk badge."""
        classes = {
            "high": "bg-red-100 text-red-800 border-red-200",
            "medium": "bg-amber-100 text-amber-800 border-amber-200",
            "low": "bg-green-100 text-green-800 border-green-200",
        }
        return classes.get(risk_level, "bg-gray-100 text-gray-800 border-gray-200")

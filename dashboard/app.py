"""
FastAPI application for the Spotify Africa Trends Dashboard.
"""

import os
from datetime import datetime, timedelta
from typing import Optional, List
from pathlib import Path

from fastapi import FastAPI, Request, Query, HTTPException, BackgroundTasks, Form
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from pydantic import BaseModel
import structlog
import yaml

from auth.middleware import (
    AuthMiddleware, verify_password, create_session,
    get_session_from_cookie, get_login_page_html, is_auth_enabled,
    SESSION_COOKIE_NAME, SESSION_DURATION_HOURS, _session_cache
)
from monitoring.health import DataHealthMonitor
from monitoring.risk_validator import RiskFactorValidator

logger = structlog.get_logger()

# Refresh status tracking (in-memory for simplicity)
_refresh_status = {
    "is_running": False,
    "started_at": None,
    "completed_at": None,
    "last_result": None,
}

# Load configuration
CONFIG_PATH = Path(__file__).parent.parent / "config" / "settings.yaml"


def load_config():
    """Load application configuration."""
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


# API Models
class TrendResponse(BaseModel):
    id: str
    title: str
    whats_happening: str
    why_it_matters: List[str]
    suggested_action: str
    if_goes_wrong: str
    topic: str
    topic_display: str
    market: Optional[str]
    total_score: float
    priority_level: str
    risk_level: str
    confidence: str
    sources: List[str]
    source_url: Optional[str]


class TrendsListResponse(BaseModel):
    trends: List[TrendResponse]
    total: int
    filters: dict


class HealthResponse(BaseModel):
    status: str
    last_updated: Optional[str]
    connectors: dict


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""

    app = FastAPI(
        title="Spotify Africa Comms Trends Dashboard",
        description="Real-time trend monitoring and comms intelligence for Sub-Saharan Africa",
        version="1.0.0",
    )

    # Setup templates and static files
    templates_dir = Path(__file__).parent / "templates"
    static_dir = Path(__file__).parent / "static"

    templates_dir.mkdir(exist_ok=True)
    static_dir.mkdir(exist_ok=True)

    templates = Jinja2Templates(directory=str(templates_dir))
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    # Store config and storage in app state
    app.state.config = load_config()
    app.state.storage = None
    app.state.orchestrator = None
    app.state.health_monitor = None
    app.state.risk_validator = None

    # Add auth middleware
    def get_storage_from_request(request: Request):
        return request.app.state.storage

    app.add_middleware(AuthMiddleware, storage_getter=get_storage_from_request)

    @app.on_event("startup")
    async def startup():
        """Initialize storage on startup."""
        from storage import get_storage
        from pipeline import PipelineOrchestrator

        app.state.storage = get_storage(app.state.config)
        await app.state.storage.initialize()

        app.state.orchestrator = PipelineOrchestrator(app.state.config)
        app.state.health_monitor = DataHealthMonitor(app.state.storage)
        app.state.risk_validator = RiskFactorValidator(app.state.config)
        logger.info("dashboard_started")

    @app.on_event("shutdown")
    async def shutdown():
        """Close storage on shutdown."""
        if app.state.storage:
            await app.state.storage.close()
        logger.info("dashboard_stopped")

    # ==================== HTML Routes ====================

    @app.get("/", response_class=HTMLResponse)
    async def dashboard_home(request: Request):
        """Main dashboard page."""
        return templates.TemplateResponse("index.html", {
            "request": request,
            "config": app.state.config,
        })

    @app.get("/trend/{trend_id}", response_class=HTMLResponse)
    async def trend_detail(request: Request, trend_id: str):
        """Trend detail page."""
        trend = await app.state.storage.get_trend_by_id(trend_id)
        if not trend:
            raise HTTPException(status_code=404, detail="Trend not found")

        history = await app.state.storage.get_trend_history(trend_id)

        return templates.TemplateResponse("trend_detail.html", {
            "request": request,
            "trend": trend.to_dict(),
            "history": history,
            "config": app.state.config,
        })

    # ==================== Auth Routes ====================

    @app.get("/auth/login", response_class=HTMLResponse)
    async def login_page(request: Request, next: str = "/", error: str = ""):
        """Login page."""
        if not is_auth_enabled():
            return RedirectResponse(url=next, status_code=302)
        return HTMLResponse(get_login_page_html(error=error, next_url=next))

    @app.post("/auth/login")
    async def login(
        request: Request,
        password: str = Form(...),
        next: str = Form("/"),
    ):
        """Handle login form submission."""
        if not is_auth_enabled():
            return RedirectResponse(url=next, status_code=302)

        if verify_password(password):
            # Create session
            session_id = create_session()
            expires_at = datetime.utcnow() + timedelta(hours=SESSION_DURATION_HOURS)

            # Save to database
            from storage.base import UserSession
            session = UserSession(
                session_id=session_id,
                created_at=datetime.utcnow(),
                expires_at=expires_at,
                is_valid=True,
            )
            await app.state.storage.save_user_session(session)

            # Cache it
            _session_cache[session_id] = {"expires_at": expires_at}

            # Set cookie and redirect
            response = RedirectResponse(url=next, status_code=302)
            response.set_cookie(
                key=SESSION_COOKIE_NAME,
                value=session_id,
                httponly=True,
                max_age=SESSION_DURATION_HOURS * 3600,
                samesite="lax",
            )
            return response
        else:
            return RedirectResponse(
                url=f"/auth/login?next={next}&error=Invalid+password",
                status_code=302
            )

    @app.get("/auth/logout")
    async def logout(request: Request):
        """Handle logout."""
        session_id = get_session_from_cookie(request)
        if session_id:
            await app.state.storage.delete_user_session(session_id)
            if session_id in _session_cache:
                del _session_cache[session_id]

        response = RedirectResponse(url="/auth/login", status_code=302)
        response.delete_cookie(SESSION_COOKIE_NAME)
        return response

    # ==================== API Routes ====================

    @app.get("/api/trends", response_model=TrendsListResponse)
    async def get_trends(
        limit: int = Query(50, ge=1, le=200),
        offset: int = Query(0, ge=0),
        market: Optional[str] = None,
        topic: Optional[str] = None,
        risk_level: Optional[str] = None,
        min_score: Optional[float] = None,
        action: Optional[str] = None,
    ):
        """
        Get trends with filtering.

        Filters:
        - market: Filter by market code (ZA, NG, KE, etc.)
        - topic: Filter by topic key (music_audio, culture, etc.)
        - risk_level: Filter by risk (high, medium, low)
        - min_score: Minimum comms relevance score
        - action: Filter by suggested action
        """
        trends = await app.state.storage.get_trends(
            limit=limit,
            offset=offset,
            market=market,
            topic=topic,
            risk_level=risk_level,
            min_score=min_score,
        )

        # Additional filter for action (not in storage layer)
        if action:
            trends = [t for t in trends if t.suggested_action.lower() == action.lower()]

        return TrendsListResponse(
            trends=[
                TrendResponse(
                    id=t.id,
                    title=t.title,
                    whats_happening=t.whats_happening,
                    why_it_matters=t.why_it_matters,
                    suggested_action=t.suggested_action,
                    if_goes_wrong=t.if_goes_wrong,
                    topic=t.topic or "unknown",
                    topic_display=app.state.config.get("topics", {}).get(t.topic, {}).get("name", t.topic or "Unknown"),
                    market=t.market,
                    total_score=t.total_score,
                    priority_level=t.priority_level,
                    risk_level=t.risk_level,
                    confidence=t.confidence,
                    sources=[t.source],
                    source_url=t.source_url,
                )
                for t in trends
            ],
            total=len(trends),
            filters={
                "market": market,
                "topic": topic,
                "risk_level": risk_level,
                "min_score": min_score,
                "action": action,
            }
        )

    @app.get("/api/trends/{trend_id}")
    async def get_trend(trend_id: str):
        """Get single trend with full details."""
        trend = await app.state.storage.get_trend_by_id(trend_id)
        if not trend:
            raise HTTPException(status_code=404, detail="Trend not found")

        history = await app.state.storage.get_trend_history(trend_id)

        return {
            "trend": trend.to_dict(),
            "history": history,
        }

    @app.get("/api/risks")
    async def get_risks(limit: int = Query(10, ge=1, le=50)):
        """Get trends with high/medium risk levels."""
        high_risk = await app.state.storage.get_trends(
            limit=limit,
            risk_level="high",
        )
        medium_risk = await app.state.storage.get_trends(
            limit=limit,
            risk_level="medium",
        )

        return {
            "high_risk": [t.to_dict() for t in high_risk],
            "medium_risk": [t.to_dict() for t in medium_risk],
            "total_high": len(high_risk),
            "total_medium": len(medium_risk),
        }

    @app.get("/api/actions")
    async def get_action_summary():
        """Get trends grouped by suggested action."""
        all_trends = await app.state.storage.get_trends(limit=200)

        grouped = {
            "escalate": [],
            "engage": [],
            "partner": [],
            "monitor": [],
            "avoid": [],
        }

        for trend in all_trends:
            action = trend.suggested_action.lower()
            if action in grouped:
                grouped[action].append(trend.to_dict())

        return {
            "actions": grouped,
            "counts": {k: len(v) for k, v in grouped.items()},
        }

    @app.get("/api/markets")
    async def get_market_summary():
        """Get trend counts by market."""
        all_trends = await app.state.storage.get_trends(limit=500)

        market_counts = {}
        for trend in all_trends:
            market = trend.market or "Unknown"
            if market not in market_counts:
                market_counts[market] = {
                    "total": 0,
                    "high_risk": 0,
                    "avg_score": 0,
                    "scores": [],
                }
            market_counts[market]["total"] += 1
            market_counts[market]["scores"].append(trend.total_score)
            if trend.risk_level == "high":
                market_counts[market]["high_risk"] += 1

        # Calculate averages
        for market in market_counts:
            scores = market_counts[market]["scores"]
            market_counts[market]["avg_score"] = sum(scores) / len(scores) if scores else 0
            del market_counts[market]["scores"]

        return {"markets": market_counts}

    @app.get("/api/topics")
    async def get_topic_summary():
        """Get trend counts by topic."""
        all_trends = await app.state.storage.get_trends(limit=500)
        topics_config = app.state.config.get("topics", {})

        topic_counts = {}
        for trend in all_trends:
            topic = trend.topic or "unknown"
            if topic not in topic_counts:
                topic_counts[topic] = {
                    "name": topics_config.get(topic, {}).get("name", topic),
                    "total": 0,
                    "high_priority": 0,
                }
            topic_counts[topic]["total"] += 1
            if trend.priority_level == "high":
                topic_counts[topic]["high_priority"] += 1

        return {"topics": topic_counts}

    @app.get("/api/stats")
    async def get_stats():
        """Get dashboard statistics."""
        all_trends = await app.state.storage.get_trends(limit=1000)
        last_run = await app.state.storage.get_last_run()

        if not all_trends:
            return {
                "total_trends": 0,
                "last_updated": None,
                "by_priority": {},
                "by_risk": {},
            }

        by_priority = {"high": 0, "medium": 0, "low": 0}
        by_risk = {"high": 0, "medium": 0, "low": 0}
        scores = []

        for t in all_trends:
            by_priority[t.priority_level] = by_priority.get(t.priority_level, 0) + 1
            by_risk[t.risk_level] = by_risk.get(t.risk_level, 0) + 1
            scores.append(t.total_score)

        return {
            "total_trends": len(all_trends),
            "last_updated": last_run.get("completed_at") if last_run else None,
            "by_priority": by_priority,
            "by_risk": by_risk,
            "avg_score": sum(scores) / len(scores) if scores else 0,
            "max_score": max(scores) if scores else 0,
        }

    @app.post("/api/pipeline/run")
    async def trigger_pipeline(background_tasks: BackgroundTasks = None):
        """Manually trigger the pipeline."""
        try:
            result = await app.state.orchestrator.run_full_pipeline()

            # Save to storage
            if result["success"] and result.get("summaries"):
                from storage.base import TrendRecord
                records = [TrendRecord.from_summary(s) for s in result["summaries"]]
                await app.state.storage.save_trends(records)
                await app.state.storage.save_pipeline_run(result["metrics"])

            return {
                "success": result["success"],
                "trends_processed": len(result.get("summaries", [])),
                "metrics": result.get("metrics", {}),
            }

        except Exception as e:
            logger.error("pipeline_trigger_error", error=str(e))
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/api/pipeline/run-async")
    async def trigger_pipeline_async(secret: str = ""):
        """Trigger pipeline asynchronously (for cron jobs)."""
        import os
        expected_secret = os.environ.get("PIPELINE_SECRET", "spotify-trends-2024")

        if secret != expected_secret:
            raise HTTPException(status_code=403, detail="Invalid secret")

        # Run in background
        import asyncio
        asyncio.create_task(run_pipeline_background(app.state))

        return {"status": "started", "message": "Pipeline running in background"}

    async def run_pipeline_background(state):
        """Background pipeline runner."""
        try:
            result = await state.orchestrator.run_full_pipeline()
            if result["success"] and result.get("summaries"):
                from storage.base import TrendRecord
                records = [TrendRecord.from_summary(s) for s in result["summaries"]]
                await state.storage.save_trends(records)
                await state.storage.save_pipeline_run(result["metrics"])
            logger.info("background_pipeline_complete", trends=len(result.get("summaries", [])))
        except Exception as e:
            logger.error("background_pipeline_error", error=str(e))

    @app.get("/api/health", response_model=HealthResponse)
    async def health_check():
        """Health check endpoint."""
        last_run = await app.state.storage.get_last_run()
        connector_health = await app.state.orchestrator.health_check()

        return HealthResponse(
            status="healthy",
            last_updated=last_run.get("completed_at") if last_run else None,
            connectors=connector_health.get("connectors", {}),
        )

    @app.get("/health")
    async def simple_health():
        """Simple health check for load balancers."""
        return {"status": "ok"}

    # ==================== Trend-Jack Intelligence API ====================

    @app.get("/api/artist-spikes/{market}")
    async def get_artist_spikes(
        market: str,
        time_window: str = Query("24h", regex="^(24h|7d)$"),
        limit: int = Query(20, ge=1, le=50),
    ):
        """Get artist search spikes for a market."""
        spikes = await app.state.storage.get_artist_spikes(
            market=market.upper(),
            time_window=time_window,
            limit=limit,
        )
        # Get last updated from data health
        health = await app.state.storage.get_module_health("artist_spikes")
        last_updated = health.last_success.isoformat() if health and health.last_success else None

        return {
            "market": market.upper(),
            "time_window": time_window,
            "spikes": [s.to_dict() for s in spikes],
            "count": len(spikes),
            "last_updated": last_updated,
        }

    @app.get("/api/culture-searches/{market}")
    async def get_culture_searches(
        market: str,
        sensitivity_tag: Optional[str] = None,
        limit: int = Query(10, ge=1, le=50),
    ):
        """Get rising culture searches for a market."""
        searches = await app.state.storage.get_culture_searches(
            market=market.upper(),
            sensitivity_tag=sensitivity_tag,
            limit=limit,
        )
        health = await app.state.storage.get_module_health("culture_searches")
        last_updated = health.last_success.isoformat() if health and health.last_success else None

        return {
            "market": market.upper(),
            "sensitivity_tag": sensitivity_tag,
            "searches": [s.to_dict() for s in searches],
            "count": len(searches),
            "last_updated": last_updated,
        }

    @app.get("/api/culture-searches/regional/overlaps")
    async def get_culture_overlaps():
        """Get cross-market culture search overlaps."""
        overlaps = await app.state.storage.get_culture_overlaps()
        return {
            "overlaps": overlaps,
            "count": len(overlaps),
        }

    @app.get("/api/style-signals")
    async def get_style_signals(
        country_relevance: Optional[str] = None,
        max_risk: str = Query("high", regex="^(low|medium|high)$"),
        limit: int = Query(15, ge=1, le=50),
    ):
        """Get streetwear/fashion style signals."""
        signals = await app.state.storage.get_style_signals(
            country_relevance=country_relevance.upper() if country_relevance else None,
            max_risk=max_risk,
            limit=limit,
        )
        health = await app.state.storage.get_module_health("style_signals")
        last_updated = health.last_success.isoformat() if health and health.last_success else None

        return {
            "country_relevance": country_relevance,
            "max_risk": max_risk,
            "signals": [s.to_dict() for s in signals],
            "count": len(signals),
            "last_updated": last_updated,
        }

    @app.get("/api/pitch-cards/{market}")
    async def get_pitch_cards(
        market: str,
        limit: int = Query(6, ge=1, le=20),
    ):
        """Get pitch cards for a market."""
        cards = await app.state.storage.get_pitch_cards(
            market=market.upper(),
            limit=limit,
        )
        health = await app.state.storage.get_module_health("pitch_cards")
        last_updated = health.last_success.isoformat() if health and health.last_success else None

        return {
            "market": market.upper(),
            "cards": [c.to_dict() for c in cards],
            "count": len(cards),
            "last_updated": last_updated,
        }

    @app.get("/api/data-health")
    async def get_data_health():
        """Get data health status for all modules with risk validation."""
        health_summary = await app.state.health_monitor.get_health_summary()

        # Add risk validation summary
        try:
            trends = await app.state.storage.get_trends(limit=100)
            trend_dicts = [
                {
                    "id": t.id,
                    "risk_level": t.risk_level,
                    "risk_score": t.risk_score,
                    "last_updated": t.last_updated.isoformat() if t.last_updated else None,
                }
                for t in trends
            ]
            validation = app.state.risk_validator.validate_batch(trend_dicts)
            health_summary["risk_validation"] = {
                "total_trends": validation["total_trends"],
                "errors": validation["errors"],
                "warnings": validation["warnings"],
                "is_valid": validation["is_valid"],
            }
        except Exception as e:
            health_summary["risk_validation"] = {
                "error": str(e),
                "is_valid": False,
            }

        return health_summary

    @app.get("/api/risk-validation")
    async def validate_risks(limit: int = Query(100, ge=1, le=500)):
        """Run risk factor validation on trends."""
        trends = await app.state.storage.get_trends(limit=limit)
        trend_dicts = [
            {
                "id": t.id,
                "title": t.title,
                "risk_level": t.risk_level,
                "risk_score": t.risk_score,
                "last_updated": t.last_updated.isoformat() if t.last_updated else None,
            }
            for t in trends
        ]
        return app.state.risk_validator.validate_batch(trend_dicts)

    @app.post("/api/trendjack/refresh")
    async def refresh_trendjack(secret: str = ""):
        """Trigger trend-jack data refresh."""
        import os
        expected_secret = os.environ.get("PIPELINE_SECRET", "spotify-trends-2024")

        if secret != expected_secret:
            raise HTTPException(status_code=403, detail="Invalid secret")

        # Check if already running
        if _refresh_status["is_running"]:
            return {
                "status": "already_running",
                "started_at": _refresh_status["started_at"].isoformat() if _refresh_status["started_at"] else None,
                "message": "Trend-jack refresh already in progress"
            }

        # Mark as running
        _refresh_status["is_running"] = True
        _refresh_status["started_at"] = datetime.utcnow()
        _refresh_status["completed_at"] = None
        _refresh_status["last_result"] = None

        # Run in background
        import asyncio
        asyncio.create_task(run_trendjack_refresh(app.state))

        return {
            "status": "started",
            "started_at": _refresh_status["started_at"].isoformat(),
            "message": "Trend-jack refresh running in background. Poll GET /api/trendjack/status for progress."
        }

    @app.get("/api/trendjack/status")
    async def get_refresh_status():
        """Get the status of the last/current trend-jack refresh."""
        return {
            "is_running": _refresh_status["is_running"],
            "started_at": _refresh_status["started_at"].isoformat() if _refresh_status["started_at"] else None,
            "completed_at": _refresh_status["completed_at"].isoformat() if _refresh_status["completed_at"] else None,
            "result": _refresh_status["last_result"],
        }

    async def run_trendjack_refresh(state):
        """Background trend-jack refresh with detailed tracking."""
        from connectors import ArtistSpikesConnector, CultureSearchConnector, StyleSignalsConnector
        from pipeline.pitch_generator import PitchCardGenerator

        result = {
            "status": "success",
            "modules": {},
            "errors": [],
            "markets": ["NG", "KE", "GH", "ZA"],
        }

        try:
            config = state.config
            markets = result["markets"]

            # --- Module 1: Artist Spikes ---
            try:
                logger.info("trendjack_refresh_module_start", module="artist_spikes")
                spikes_connector = ArtistSpikesConnector(config)
                all_spikes = []
                for window in ["24h", "7d"]:
                    spikes = await spikes_connector.fetch_spikes(markets, window)
                    all_spikes.extend(spikes)
                    await state.storage.save_artist_spikes(spikes)
                    logger.info("artist_spikes_fetched", window=window, count=len(spikes))

                await state.health_monitor.update_module_health(
                    "artist_spikes", True, len(all_spikes)
                )
                result["modules"]["artist_spikes"] = {
                    "status": "ok",
                    "fetched": len(all_spikes),
                    "saved": len(all_spikes),
                    "by_window": {"24h": len([s for s in all_spikes if s.time_window == "24h"]),
                                  "7d": len([s for s in all_spikes if s.time_window == "7d"])},
                }
                logger.info("trendjack_refresh_module_complete", module="artist_spikes", count=len(all_spikes))
            except Exception as e:
                error_msg = f"artist_spikes: {str(e)}"
                result["modules"]["artist_spikes"] = {"status": "error", "error": str(e)}
                result["errors"].append(error_msg)
                logger.error("trendjack_module_error", module="artist_spikes", error=str(e))
                await state.health_monitor.update_module_health("artist_spikes", False, error_message=str(e))
                all_spikes = []

            # --- Module 2: Culture Searches ---
            try:
                logger.info("trendjack_refresh_module_start", module="culture_searches")
                culture_connector = CultureSearchConnector(config)
                searches = await culture_connector.fetch_searches(markets)
                await state.storage.save_culture_searches(searches)

                await state.health_monitor.update_module_health(
                    "culture_searches", True, len(searches)
                )
                result["modules"]["culture_searches"] = {
                    "status": "ok",
                    "fetched": len(searches),
                    "saved": len(searches),
                    "by_market": {m: len([s for s in searches if s.market == m]) for m in markets},
                }
                logger.info("trendjack_refresh_module_complete", module="culture_searches", count=len(searches))
            except Exception as e:
                error_msg = f"culture_searches: {str(e)}"
                result["modules"]["culture_searches"] = {"status": "error", "error": str(e)}
                result["errors"].append(error_msg)
                logger.error("trendjack_module_error", module="culture_searches", error=str(e))
                await state.health_monitor.update_module_health("culture_searches", False, error_message=str(e))
                searches = []

            # --- Module 3: Style Signals ---
            try:
                logger.info("trendjack_refresh_module_start", module="style_signals")
                style_connector = StyleSignalsConnector(config)
                signals = await style_connector.fetch_signals(markets)
                await state.storage.save_style_signals(signals)

                await state.health_monitor.update_module_health(
                    "style_signals", True, len(signals)
                )
                result["modules"]["style_signals"] = {
                    "status": "ok",
                    "fetched": len(signals),
                    "saved": len(signals),
                    "by_source": {},
                }
                # Count by source
                for signal in signals:
                    src = signal.source
                    result["modules"]["style_signals"]["by_source"][src] = \
                        result["modules"]["style_signals"]["by_source"].get(src, 0) + 1
                logger.info("trendjack_refresh_module_complete", module="style_signals", count=len(signals))
            except Exception as e:
                error_msg = f"style_signals: {str(e)}"
                result["modules"]["style_signals"] = {"status": "error", "error": str(e)}
                result["errors"].append(error_msg)
                logger.error("trendjack_module_error", module="style_signals", error=str(e))
                await state.health_monitor.update_module_health("style_signals", False, error_message=str(e))
                signals = []

            # --- Module 4: Pitch Cards ---
            try:
                logger.info("trendjack_refresh_module_start", module="pitch_cards")
                generator = PitchCardGenerator(config)
                cards = await generator.generate_and_save(
                    state.storage,
                    all_spikes,
                    searches,
                    signals,
                    markets,
                )
                card_count = len(cards) if cards else len(markets) * 6

                await state.health_monitor.update_module_health(
                    "pitch_cards", True, card_count
                )
                result["modules"]["pitch_cards"] = {
                    "status": "ok",
                    "generated": card_count,
                    "by_market": {m: len([c for c in (cards or []) if c.market == m]) for m in markets} if cards else {},
                }
                logger.info("trendjack_refresh_module_complete", module="pitch_cards", count=card_count)
            except Exception as e:
                error_msg = f"pitch_cards: {str(e)}"
                result["modules"]["pitch_cards"] = {"status": "error", "error": str(e)}
                result["errors"].append(error_msg)
                logger.error("trendjack_module_error", module="pitch_cards", error=str(e))
                await state.health_monitor.update_module_health("pitch_cards", False, error_message=str(e))

            # Set overall status
            if result["errors"]:
                result["status"] = "partial" if any(m.get("status") == "ok" for m in result["modules"].values()) else "failed"

            logger.info(
                "trendjack_refresh_complete",
                status=result["status"],
                artist_spikes=result["modules"].get("artist_spikes", {}).get("fetched", 0),
                culture_searches=result["modules"].get("culture_searches", {}).get("fetched", 0),
                style_signals=result["modules"].get("style_signals", {}).get("fetched", 0),
                pitch_cards=result["modules"].get("pitch_cards", {}).get("generated", 0),
                errors=len(result["errors"]),
            )

        except Exception as e:
            result["status"] = "failed"
            result["errors"].append(f"Unexpected error: {str(e)}")
            logger.error("trendjack_refresh_fatal_error", error=str(e))

        finally:
            # Update status tracking
            _refresh_status["is_running"] = False
            _refresh_status["completed_at"] = datetime.utcnow()
            _refresh_status["last_result"] = result

    # ==================== Unified Refresh Endpoint ====================

    @app.post("/api/refresh")
    async def unified_refresh(
        secret: str = "",
        modules: str = "all",
        markets: str = "NG,KE,GH,ZA",
    ):
        """
        Unified refresh endpoint for GitHub Actions scheduler.

        Args:
            secret: API secret for authentication (env: PIPELINE_SECRET)
            modules: Comma-separated modules to refresh: all, pipeline, trendjack
            markets: Comma-separated market codes (default: NG,KE,GH,ZA)

        Returns:
            Status and module counts for each refreshed module.
        """
        expected_secret = os.environ.get("PIPELINE_SECRET", "spotify-trends-2024")

        if secret != expected_secret:
            raise HTTPException(status_code=403, detail="Invalid secret")

        market_list = [m.strip().upper() for m in markets.split(",") if m.strip()]
        module_list = [m.strip().lower() for m in modules.split(",") if m.strip()]

        result = {
            "status": "success",
            "started_at": datetime.utcnow().isoformat(),
            "markets": market_list,
            "modules_requested": module_list,
            "results": {},
        }

        # Run pipeline if requested
        if "all" in module_list or "pipeline" in module_list:
            try:
                from pipeline import PipelineOrchestrator

                logger.info("unified_refresh_pipeline_start", markets=market_list)
                orchestrator = PipelineOrchestrator(app.state.config)
                orchestrator.storage = app.state.storage
                pipeline_result = await orchestrator.run()

                result["results"]["pipeline"] = {
                    "status": "ok",
                    "trends_collected": pipeline_result.get("collected", 0) if isinstance(pipeline_result, dict) else 0,
                }
                logger.info("unified_refresh_pipeline_complete", result=result["results"]["pipeline"])
            except Exception as e:
                result["results"]["pipeline"] = {"status": "error", "error": str(e)}
                logger.error("unified_refresh_pipeline_error", error=str(e))

        # Run trendjack if requested
        if "all" in module_list or "trendjack" in module_list:
            try:
                from connectors import ArtistSpikesConnector, CultureSearchConnector, StyleSignalsConnector
                from pipeline.pitch_generator import PitchCardGenerator

                logger.info("unified_refresh_trendjack_start", markets=market_list)
                config = app.state.config
                trendjack_result = {"artist_spikes": 0, "culture_searches": 0, "style_signals": 0, "pitch_cards": 0}

                # Artist spikes
                spikes_connector = ArtistSpikesConnector(config)
                all_spikes = []
                for window in ["24h", "7d"]:
                    spikes = await spikes_connector.fetch_spikes(market_list, window)
                    all_spikes.extend(spikes)
                    await app.state.storage.save_artist_spikes(spikes)
                trendjack_result["artist_spikes"] = len(all_spikes)
                await app.state.health_monitor.update_module_health("artist_spikes", True, len(all_spikes))

                # Culture searches
                culture_connector = CultureSearchConnector(config)
                searches = await culture_connector.fetch_searches(market_list)
                await app.state.storage.save_culture_searches(searches)
                trendjack_result["culture_searches"] = len(searches)
                await app.state.health_monitor.update_module_health("culture_searches", True, len(searches))

                # Style signals
                style_connector = StyleSignalsConnector(config)
                signals = await style_connector.fetch_signals(market_list)
                await app.state.storage.save_style_signals(signals)
                trendjack_result["style_signals"] = len(signals)
                await app.state.health_monitor.update_module_health("style_signals", True, len(signals))

                # Pitch cards
                generator = PitchCardGenerator(config)
                cards = await generator.generate_and_save(
                    app.state.storage, all_spikes, searches, signals, market_list
                )
                trendjack_result["pitch_cards"] = len(cards) if cards else 0
                await app.state.health_monitor.update_module_health("pitch_cards", True, trendjack_result["pitch_cards"])

                result["results"]["trendjack"] = {"status": "ok", **trendjack_result}
                logger.info("unified_refresh_trendjack_complete", result=trendjack_result)
            except Exception as e:
                result["results"]["trendjack"] = {"status": "error", "error": str(e)}
                logger.error("unified_refresh_trendjack_error", error=str(e))

        # Set overall status
        if any(r.get("status") == "error" for r in result["results"].values()):
            result["status"] = "partial" if any(r.get("status") == "ok" for r in result["results"].values()) else "failed"

        result["completed_at"] = datetime.utcnow().isoformat()
        logger.info("unified_refresh_complete", status=result["status"], results=result["results"])

        return result

    # ==================== Filter Options ====================

    @app.get("/api/filters")
    async def get_filter_options():
        """Get available filter options."""
        config = app.state.config

        markets = [
            {"code": m["code"], "name": m["name"]}
            for m in config.get("markets", {}).get("priority", [])
        ]

        topics = [
            {"key": k, "name": v.get("name", k)}
            for k, v in config.get("topics", {}).items()
        ]

        return {
            "markets": markets,
            "topics": topics,
            "risk_levels": ["high", "medium", "low"],
            "actions": ["escalate", "engage", "partner", "monitor", "avoid"],
            "priorities": ["high", "medium", "low"],
        }

    return app


# Create default app instance
app = create_app()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

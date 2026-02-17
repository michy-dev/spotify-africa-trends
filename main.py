#!/usr/bin/env python3
"""
Spotify Africa Comms Trends Dashboard - Main Entry Point

Usage:
    python main.py run-pipeline     # Run the data pipeline once
    python main.py run-server       # Start the web dashboard
    python main.py run-scheduler    # Start the scheduled job runner
    python main.py generate-digest  # Generate daily digest
    python main.py health-check     # Check connector health
"""

import asyncio
import argparse
import sys
from pathlib import Path
from datetime import datetime

import structlog
import yaml

# Configure logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.dev.ConsoleRenderer()
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()


def load_config() -> dict:
    """Load application configuration."""
    config_path = Path(__file__).parent / "config" / "settings.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f)


async def run_pipeline():
    """Run the data collection and processing pipeline."""
    logger.info("Starting pipeline run")

    config = load_config()

    from pipeline import PipelineOrchestrator
    from storage import get_storage
    from storage.base import TrendRecord

    # Initialize components
    storage = get_storage(config)
    await storage.initialize()

    orchestrator = PipelineOrchestrator(config)

    try:
        # Run pipeline
        result = await orchestrator.run_full_pipeline()

        if result["success"]:
            logger.info(
                "Pipeline completed successfully",
                trends=len(result.get("summaries", [])),
                duration=result["metrics"]["total_duration_seconds"]
            )

            # Save to storage
            if result.get("summaries"):
                records = [TrendRecord.from_summary(s) for s in result["summaries"]]
                saved = await storage.save_trends(records)
                logger.info("Saved trends to storage", count=saved)

            # Save run metadata
            await storage.save_pipeline_run(result["metrics"])

            # Print summary
            stats = result.get("stats", {})
            print("\n" + "="*60)
            print("PIPELINE RUN COMPLETE")
            print("="*60)
            print(f"Total trends processed: {stats.get('total_trends', 0)}")
            print(f"By priority: {stats.get('by_priority', {})}")
            print(f"By risk: {stats.get('by_risk', {})}")
            print(f"Duration: {result['metrics']['total_duration_seconds']:.1f}s")
            print("="*60 + "\n")

        else:
            logger.error("Pipeline failed", error=result.get("error"))
            return 1

    finally:
        await storage.close()

    return 0


def run_server():
    """Start the web dashboard server."""
    import uvicorn
    from dashboard import app

    config = load_config()
    dashboard_config = config.get("dashboard", {})

    host = dashboard_config.get("host", "0.0.0.0")
    port = dashboard_config.get("port", 8000)

    logger.info("Starting dashboard server", host=host, port=port)

    uvicorn.run(app, host=host, port=port)


async def run_scheduler():
    """Start the scheduled job runner."""
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.triggers.cron import CronTrigger

    config = load_config()

    scheduler = AsyncIOScheduler()

    # Parse pipeline schedule
    pipeline_schedule = config.get("app", {}).get("update_schedule", "0 6 * * *")
    parts = pipeline_schedule.split()
    if len(parts) == 5:
        minute, hour, day, month, dow = parts
        trigger = CronTrigger(
            minute=minute,
            hour=hour,
            day=day if day != "*" else None,
            month=month if month != "*" else None,
            day_of_week=dow if dow != "*" else None,
            timezone=config.get("app", {}).get("timezone", "Africa/Johannesburg")
        )

        async def scheduled_pipeline():
            logger.info("Running scheduled pipeline")
            await run_pipeline()

        scheduler.add_job(scheduled_pipeline, trigger, id="pipeline")
        logger.info("Scheduled pipeline job", schedule=pipeline_schedule)

    # Parse digest schedule
    digest_config = config.get("digest", {})
    if digest_config.get("enabled", True):
        digest_schedule = digest_config.get("schedule", "0 7 * * *")
        parts = digest_schedule.split()
        if len(parts) == 5:
            minute, hour, day, month, dow = parts
            trigger = CronTrigger(
                minute=minute,
                hour=hour,
                day=day if day != "*" else None,
                month=month if month != "*" else None,
                day_of_week=dow if dow != "*" else None,
                timezone=config.get("app", {}).get("timezone", "Africa/Johannesburg")
            )

            async def scheduled_digest():
                logger.info("Generating scheduled digest")
                await generate_digest()

            scheduler.add_job(scheduled_digest, trigger, id="digest")
            logger.info("Scheduled digest job", schedule=digest_schedule)

    # Add trend-jack refresh job (4 AM UTC / 6 AM SAST)
    trendjack_trigger = CronTrigger(
        minute="0",
        hour="4",
        timezone="UTC"
    )

    async def scheduled_trendjack():
        logger.info("Running scheduled trend-jack refresh")
        await run_trendjack_refresh()

    scheduler.add_job(scheduled_trendjack, trendjack_trigger, id="trendjack")
    logger.info("Scheduled trend-jack refresh job at 4 AM UTC")

    scheduler.start()
    logger.info("Scheduler started. Press Ctrl+C to exit.")

    try:
        # Keep running
        while True:
            await asyncio.sleep(60)
    except KeyboardInterrupt:
        logger.info("Shutting down scheduler")
        scheduler.shutdown()


async def generate_digest():
    """Generate the daily digest."""
    logger.info("Generating daily digest")

    config = load_config()

    from storage import get_storage
    from digest import DigestGenerator

    storage = get_storage(config)
    await storage.initialize()

    try:
        # Fetch trends
        trends = await storage.get_trends(limit=500)

        if not trends:
            logger.warning("No trends found for digest")
            return 1

        # Generate digest
        generator = DigestGenerator(config)
        result = generator.generate(trends)

        print("\n" + "="*60)
        print("DIGEST GENERATED")
        print("="*60)
        print(f"Date: {result['date']}")
        print(f"Markdown: {result['markdown_path']}")
        print(f"HTML: {result['html_path']}")
        print(f"Summary: {result['summary']}")
        print("="*60 + "\n")

    finally:
        await storage.close()

    return 0


async def run_trendjack_refresh():
    """Run the trend-jack intelligence refresh."""
    logger.info("Starting trend-jack refresh")

    config = load_config()
    markets = ["NG", "KE", "GH", "ZA"]

    from storage import get_storage
    from connectors import ArtistSpikesConnector, CultureSearchConnector, StyleSignalsConnector
    from pipeline.pitch_generator import PitchCardGenerator
    from monitoring.health import DataHealthMonitor

    storage = get_storage(config)
    await storage.initialize()
    health_monitor = DataHealthMonitor(storage)

    try:
        all_spikes = []
        all_searches = []
        all_signals = []

        # Refresh artist spikes for both time windows
        logger.info("Fetching artist spikes...")
        spikes_connector = ArtistSpikesConnector(config)
        for window in ["24h", "7d"]:
            try:
                spikes = await spikes_connector.fetch_spikes(markets, window)
                all_spikes.extend(spikes)
                await storage.save_artist_spikes(spikes)
                logger.info(f"Saved {len(spikes)} artist spikes for {window}")
            except Exception as e:
                logger.error("artist_spikes_error", window=window, error=str(e))

        await health_monitor.update_module_health("artist_spikes", len(all_spikes) > 0, len(all_spikes))

        # Refresh culture searches
        logger.info("Fetching culture searches...")
        try:
            culture_connector = CultureSearchConnector(config)
            all_searches = await culture_connector.fetch_searches(markets)
            await storage.save_culture_searches(all_searches)
            logger.info(f"Saved {len(all_searches)} culture searches")
            await health_monitor.update_module_health("culture_searches", True, len(all_searches))
        except Exception as e:
            logger.error("culture_search_error", error=str(e))
            await health_monitor.update_module_health("culture_searches", False, error_message=str(e))

        # Refresh style signals
        logger.info("Fetching style signals...")
        try:
            style_connector = StyleSignalsConnector(config)
            all_signals = await style_connector.fetch_signals(markets)
            await storage.save_style_signals(all_signals)
            logger.info(f"Saved {len(all_signals)} style signals")
            await health_monitor.update_module_health("style_signals", True, len(all_signals))
        except Exception as e:
            logger.error("style_signals_error", error=str(e))
            await health_monitor.update_module_health("style_signals", False, error_message=str(e))

        # Generate pitch cards
        logger.info("Generating pitch cards...")
        try:
            generator = PitchCardGenerator(config)
            cards_saved = await generator.generate_and_save(
                storage,
                all_spikes,
                all_searches,
                all_signals,
                markets,
            )
            logger.info(f"Generated {cards_saved} pitch cards")
            await health_monitor.update_module_health("pitch_cards", True, cards_saved)
        except Exception as e:
            logger.error("pitch_cards_error", error=str(e))
            await health_monitor.update_module_health("pitch_cards", False, error_message=str(e))

        # Print summary
        print("\n" + "="*60)
        print("TREND-JACK REFRESH COMPLETE")
        print("="*60)
        print(f"Artist spikes: {len(all_spikes)}")
        print(f"Culture searches: {len(all_searches)}")
        print(f"Style signals: {len(all_signals)}")
        print("="*60 + "\n")

    finally:
        await storage.close()

    return 0


async def health_check():
    """Check health of all connectors."""
    logger.info("Running health check")

    config = load_config()

    from pipeline import PipelineOrchestrator

    orchestrator = PipelineOrchestrator(config)
    health = await orchestrator.health_check()

    print("\n" + "="*60)
    print("CONNECTOR HEALTH CHECK")
    print("="*60)

    for name, status in health.get("connectors", {}).items():
        healthy = status.get("healthy", False)
        icon = "✓" if healthy else "✗"
        status_str = status.get("status", "unknown")
        print(f"  {icon} {name}: {status_str}")

        if not healthy and status.get("error"):
            print(f"      Error: {status['error']}")

    print("="*60 + "\n")

    return 0


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Spotify Africa Comms Trends Dashboard"
    )
    parser.add_argument(
        "command",
        choices=[
            "run-pipeline",
            "run-server",
            "run-scheduler",
            "generate-digest",
            "health-check",
            "run-trendjack",
        ],
        help="Command to run"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging"
    )

    args = parser.parse_args()

    # Run the appropriate command
    if args.command == "run-pipeline":
        exit_code = asyncio.run(run_pipeline())
    elif args.command == "run-server":
        run_server()  # Not async
        exit_code = 0
    elif args.command == "run-scheduler":
        asyncio.run(run_scheduler())
        exit_code = 0
    elif args.command == "generate-digest":
        exit_code = asyncio.run(generate_digest())
    elif args.command == "health-check":
        exit_code = asyncio.run(health_check())
    elif args.command == "run-trendjack":
        exit_code = asyncio.run(run_trendjack_refresh())
    else:
        parser.print_help()
        exit_code = 1

    sys.exit(exit_code)


if __name__ == "__main__":
    main()

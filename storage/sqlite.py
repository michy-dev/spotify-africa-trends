"""
SQLite storage backend implementation.
"""

import json
import os
from datetime import datetime, timedelta
from typing import Optional, List
import structlog

from .base import (
    BaseStorage, TrendRecord, ArtistSpike, CultureSearch,
    StyleSignal, PitchCard, DataHealth, UserSession
)

logger = structlog.get_logger()


class SQLiteStorage(BaseStorage):
    """
    SQLite storage backend.

    Good for:
    - Local development
    - Single-instance deployments
    - Low to medium data volumes
    """

    def __init__(self, config: dict):
        super().__init__(config)
        sqlite_config = config.get("sqlite", {})
        # Check environment variable first, then config
        import os
        import re

        db_path = os.environ.get("DATABASE_PATH")
        if not db_path:
            config_path = sqlite_config.get("path", "data/trends.db")
            # Expand ${VAR:-default} style env vars from YAML
            match = re.match(r'\$\{(\w+):-([^}]+)\}', config_path)
            if match:
                var_name, default = match.groups()
                db_path = os.environ.get(var_name, default)
            else:
                db_path = config_path

        self.db_path = db_path
        self._conn = None

    async def initialize(self):
        """Create database and tables."""
        import aiosqlite

        # Ensure directory exists
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

        self._conn = await aiosqlite.connect(self.db_path)

        # Create tables
        await self._conn.executescript("""
            -- Main trends table
            CREATE TABLE IF NOT EXISTS trends (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                source TEXT NOT NULL,
                topic TEXT,
                subtopic TEXT,
                market TEXT,
                language TEXT,
                total_score REAL DEFAULT 0,
                velocity_score REAL DEFAULT 0,
                reach_score REAL DEFAULT 0,
                market_impact_score REAL DEFAULT 0,
                spotify_adjacency_score REAL DEFAULT 0,
                risk_score REAL DEFAULT 0,
                risk_level TEXT DEFAULT 'low',
                suggested_action TEXT DEFAULT 'monitor',
                confidence TEXT DEFAULT 'medium',
                priority_level TEXT DEFAULT 'low',
                description TEXT,
                source_url TEXT,
                entities TEXT,  -- JSON
                whats_happening TEXT,
                why_it_matters TEXT,  -- JSON array
                if_goes_wrong TEXT,
                volume INTEGER DEFAULT 0,
                engagement INTEGER DEFAULT 0,
                velocity REAL DEFAULT 0,
                first_seen TIMESTAMP,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                raw_data TEXT  -- JSON
            );

            -- Historical snapshots for charting
            CREATE TABLE IF NOT EXISTS trend_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trend_id TEXT NOT NULL,
                snapshot_date DATE NOT NULL,
                total_score REAL,
                volume INTEGER,
                engagement INTEGER,
                velocity REAL,
                UNIQUE(trend_id, snapshot_date)
            );

            -- Pipeline run metadata
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                id TEXT PRIMARY KEY,
                started_at TIMESTAMP NOT NULL,
                completed_at TIMESTAMP,
                success INTEGER DEFAULT 0,
                total_items INTEGER DEFAULT 0,
                metrics TEXT  -- JSON
            );

            -- Indexes for common queries
            CREATE INDEX IF NOT EXISTS idx_trends_market ON trends(market);
            CREATE INDEX IF NOT EXISTS idx_trends_topic ON trends(topic);
            CREATE INDEX IF NOT EXISTS idx_trends_score ON trends(total_score DESC);
            CREATE INDEX IF NOT EXISTS idx_trends_risk ON trends(risk_level);
            CREATE INDEX IF NOT EXISTS idx_trends_updated ON trends(last_updated DESC);
            CREATE INDEX IF NOT EXISTS idx_history_trend ON trend_history(trend_id);
            CREATE INDEX IF NOT EXISTS idx_history_date ON trend_history(snapshot_date DESC);

            -- Trend-Jack Intelligence Tables

            -- Artist search spikes
            CREATE TABLE IF NOT EXISTS artist_spikes (
                id TEXT PRIMARY KEY,
                artist_name TEXT NOT NULL,
                market TEXT NOT NULL,
                spike_score REAL DEFAULT 0,
                time_window TEXT DEFAULT '24h',
                sparkline_data TEXT,  -- JSON array
                why_spiking TEXT,  -- JSON array
                confidence TEXT DEFAULT 'medium',
                is_ambiguous INTEGER DEFAULT 0,
                related_queries TEXT,  -- JSON array
                related_topics TEXT,  -- JSON array
                current_interest REAL DEFAULT 0,
                baseline_interest REAL DEFAULT 0,
                collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- Culture searches
            CREATE TABLE IF NOT EXISTS culture_searches (
                id TEXT PRIMARY KEY,
                term TEXT NOT NULL,
                market TEXT NOT NULL,
                sensitivity_tag TEXT NOT NULL,
                rise_percentage REAL DEFAULT 0,
                volume INTEGER DEFAULT 0,
                is_cross_market INTEGER DEFAULT 0,
                markets_present TEXT,  -- JSON array
                risk_level TEXT DEFAULT 'low',
                collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- Style signals (streetwear/fashion RSS)
            CREATE TABLE IF NOT EXISTS style_signals (
                id TEXT PRIMARY KEY,
                headline TEXT NOT NULL,
                source TEXT NOT NULL,
                source_url TEXT,
                summary TEXT,
                publish_date TIMESTAMP,
                country_relevance TEXT,  -- JSON array
                spotify_tags TEXT,  -- JSON array
                risk_level TEXT DEFAULT 'low',
                collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- Pitch cards
            CREATE TABLE IF NOT EXISTS pitch_cards (
                id TEXT PRIMARY KEY,
                market TEXT NOT NULL,
                hook TEXT NOT NULL,
                why_now TEXT,  -- JSON array
                spotify_angle TEXT,
                next_steps TEXT,  -- JSON array
                risks TEXT,  -- JSON array
                confidence TEXT DEFAULT 'medium',
                source_signals TEXT,  -- JSON array
                generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP
            );

            -- Data health status
            CREATE TABLE IF NOT EXISTS data_health (
                module_name TEXT PRIMARY KEY,
                status TEXT DEFAULT 'ok',
                last_success TIMESTAMP,
                last_error TEXT,
                item_count INTEGER DEFAULT 0,
                freshness_hours REAL DEFAULT 0,
                checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- User sessions
            CREATE TABLE IF NOT EXISTS user_sessions (
                session_id TEXT PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP,
                is_valid INTEGER DEFAULT 1
            );

            -- Additional indexes
            CREATE INDEX IF NOT EXISTS idx_spikes_market ON artist_spikes(market);
            CREATE INDEX IF NOT EXISTS idx_spikes_window ON artist_spikes(time_window);
            CREATE INDEX IF NOT EXISTS idx_culture_market ON culture_searches(market);
            CREATE INDEX IF NOT EXISTS idx_culture_tag ON culture_searches(sensitivity_tag);
            CREATE INDEX IF NOT EXISTS idx_style_risk ON style_signals(risk_level);
            CREATE INDEX IF NOT EXISTS idx_pitch_market ON pitch_cards(market);
            CREATE INDEX IF NOT EXISTS idx_session_valid ON user_sessions(is_valid);
        """)

        await self._conn.commit()
        logger.info("sqlite_initialized", path=self.db_path)

    async def close(self):
        """Close database connection."""
        if self._conn:
            await self._conn.close()
            self._conn = None

    async def save_trends(self, trends: List[TrendRecord]) -> int:
        """Save trend records."""
        if not self._conn:
            await self.initialize()

        saved = 0
        today = datetime.utcnow().date().isoformat()

        for trend in trends:
            try:
                # Upsert trend
                await self._conn.execute("""
                    INSERT INTO trends (
                        id, title, source, topic, subtopic, market, language,
                        total_score, velocity_score, reach_score, market_impact_score,
                        spotify_adjacency_score, risk_score, risk_level, suggested_action,
                        confidence, priority_level, description, source_url, entities,
                        whats_happening, why_it_matters, if_goes_wrong,
                        volume, engagement, velocity, first_seen, last_updated, collected_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        title = excluded.title,
                        total_score = excluded.total_score,
                        velocity_score = excluded.velocity_score,
                        reach_score = excluded.reach_score,
                        market_impact_score = excluded.market_impact_score,
                        spotify_adjacency_score = excluded.spotify_adjacency_score,
                        risk_score = excluded.risk_score,
                        risk_level = excluded.risk_level,
                        suggested_action = excluded.suggested_action,
                        confidence = excluded.confidence,
                        priority_level = excluded.priority_level,
                        whats_happening = excluded.whats_happening,
                        why_it_matters = excluded.why_it_matters,
                        if_goes_wrong = excluded.if_goes_wrong,
                        volume = excluded.volume,
                        engagement = excluded.engagement,
                        velocity = excluded.velocity,
                        last_updated = excluded.last_updated
                """, (
                    trend.id, trend.title, trend.source, trend.topic, trend.subtopic,
                    trend.market, trend.language, trend.total_score, trend.velocity_score,
                    trend.reach_score, trend.market_impact_score, trend.spotify_adjacency_score,
                    trend.risk_score, trend.risk_level, trend.suggested_action,
                    trend.confidence, trend.priority_level, trend.description,
                    trend.source_url, json.dumps(trend.entities),
                    trend.whats_happening, json.dumps(trend.why_it_matters),
                    trend.if_goes_wrong, trend.volume, trend.engagement, trend.velocity,
                    trend.first_seen.isoformat() if trend.first_seen else None,
                    trend.last_updated.isoformat(),
                    trend.collected_at.isoformat()
                ))

                # Save history snapshot
                await self._conn.execute("""
                    INSERT INTO trend_history (trend_id, snapshot_date, total_score, volume, engagement, velocity)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(trend_id, snapshot_date) DO UPDATE SET
                        total_score = excluded.total_score,
                        volume = excluded.volume,
                        engagement = excluded.engagement,
                        velocity = excluded.velocity
                """, (trend.id, today, trend.total_score, trend.volume, trend.engagement, trend.velocity))

                saved += 1

            except Exception as e:
                logger.error("save_trend_error", trend_id=trend.id, error=str(e))

        await self._conn.commit()
        logger.info("trends_saved", count=saved)
        return saved

    async def get_trends(
        self,
        limit: int = 50,
        offset: int = 0,
        market: Optional[str] = None,
        topic: Optional[str] = None,
        risk_level: Optional[str] = None,
        min_score: Optional[float] = None,
        since: Optional[datetime] = None,
    ) -> List[TrendRecord]:
        """Get trend records with filters."""
        if not self._conn:
            await self.initialize()

        query = "SELECT * FROM trends WHERE 1=1"
        params = []

        if market:
            query += " AND market = ?"
            params.append(market)

        if topic:
            query += " AND topic = ?"
            params.append(topic)

        if risk_level:
            query += " AND risk_level = ?"
            params.append(risk_level)

        if min_score is not None:
            query += " AND total_score >= ?"
            params.append(min_score)

        if since:
            query += " AND last_updated >= ?"
            params.append(since.isoformat())

        query += " ORDER BY total_score DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        cursor = await self._conn.execute(query, params)
        rows = await cursor.fetchall()

        # Get column names
        columns = [description[0] for description in cursor.description]

        trends = []
        for row in rows:
            row_dict = dict(zip(columns, row))
            trend = self._row_to_record(row_dict)
            trends.append(trend)

        return trends

    async def get_trend_by_id(self, trend_id: str) -> Optional[TrendRecord]:
        """Get a single trend by ID."""
        if not self._conn:
            await self.initialize()

        cursor = await self._conn.execute(
            "SELECT * FROM trends WHERE id = ?",
            (trend_id,)
        )
        row = await cursor.fetchone()

        if not row:
            return None

        columns = [description[0] for description in cursor.description]
        row_dict = dict(zip(columns, row))
        return self._row_to_record(row_dict)

    async def get_trend_history(
        self,
        trend_id: str,
        days: int = 7
    ) -> List[dict]:
        """Get historical data for a trend."""
        if not self._conn:
            await self.initialize()

        since = (datetime.utcnow() - timedelta(days=days)).date().isoformat()

        cursor = await self._conn.execute("""
            SELECT snapshot_date, total_score, volume, engagement, velocity
            FROM trend_history
            WHERE trend_id = ? AND snapshot_date >= ?
            ORDER BY snapshot_date ASC
        """, (trend_id, since))

        rows = await cursor.fetchall()

        return [
            {
                "date": row[0],
                "score": row[1],
                "volume": row[2],
                "engagement": row[3],
                "velocity": row[4],
            }
            for row in rows
        ]

    async def get_baselines(
        self,
        market: Optional[str] = None,
        topic: Optional[str] = None
    ) -> dict:
        """Get baseline metrics for velocity calculation."""
        if not self._conn:
            await self.initialize()

        # Calculate averages over last 7 days
        since = (datetime.utcnow() - timedelta(days=7)).isoformat()

        query = """
            SELECT
                AVG(volume) as avg_volume,
                AVG(engagement) as avg_engagement,
                AVG(total_score) as avg_score,
                COUNT(*) as count
            FROM trends
            WHERE last_updated >= ?
        """
        params = [since]

        if market:
            query += " AND market = ?"
            params.append(market)

        if topic:
            query += " AND topic = ?"
            params.append(topic)

        cursor = await self._conn.execute(query, params)
        row = await cursor.fetchone()

        return {
            "avg_volume": row[0] or 0,
            "avg_engagement": row[1] or 0,
            "avg_score": row[2] or 0,
            "sample_size": row[3] or 0,
        }

    async def save_pipeline_run(self, metrics: dict) -> str:
        """Save pipeline run metadata."""
        if not self._conn:
            await self.initialize()

        import uuid
        run_id = str(uuid.uuid4())[:8]

        await self._conn.execute("""
            INSERT INTO pipeline_runs (id, started_at, completed_at, success, total_items, metrics)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            run_id,
            metrics.get("started_at"),
            metrics.get("completed_at"),
            1 if metrics.get("success") else 0,
            metrics.get("stages", {}).get("collect", {}).get("items_collected", 0),
            json.dumps(metrics)
        ))

        await self._conn.commit()
        return run_id

    async def get_last_run(self) -> Optional[dict]:
        """Get metadata for the last pipeline run."""
        if not self._conn:
            await self.initialize()

        cursor = await self._conn.execute("""
            SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT 1
        """)
        row = await cursor.fetchone()

        if not row:
            return None

        columns = [description[0] for description in cursor.description]
        row_dict = dict(zip(columns, row))

        return {
            "id": row_dict["id"],
            "started_at": row_dict["started_at"],
            "completed_at": row_dict["completed_at"],
            "success": bool(row_dict["success"]),
            "total_items": row_dict["total_items"],
            "metrics": json.loads(row_dict["metrics"]) if row_dict["metrics"] else {},
        }

    async def cleanup_old_data(self, days: int = 90) -> int:
        """Remove data older than specified days."""
        if not self._conn:
            await self.initialize()

        cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()

        # Delete old trends
        cursor = await self._conn.execute(
            "DELETE FROM trends WHERE last_updated < ?",
            (cutoff,)
        )
        trends_deleted = cursor.rowcount

        # Delete old history
        cursor = await self._conn.execute(
            "DELETE FROM trend_history WHERE snapshot_date < ?",
            (cutoff[:10],)  # Just the date part
        )
        history_deleted = cursor.rowcount

        # Delete old runs
        cursor = await self._conn.execute(
            "DELETE FROM pipeline_runs WHERE started_at < ?",
            (cutoff,)
        )
        runs_deleted = cursor.rowcount

        await self._conn.commit()

        total_deleted = trends_deleted + history_deleted + runs_deleted
        logger.info(
            "cleanup_complete",
            trends=trends_deleted,
            history=history_deleted,
            runs=runs_deleted
        )

        return total_deleted

    def _row_to_record(self, row: dict) -> TrendRecord:
        """Convert database row to TrendRecord."""
        return TrendRecord(
            id=row["id"],
            title=row["title"],
            source=row["source"],
            topic=row.get("topic"),
            subtopic=row.get("subtopic"),
            market=row.get("market"),
            language=row.get("language"),
            total_score=row.get("total_score", 0),
            velocity_score=row.get("velocity_score", 0),
            reach_score=row.get("reach_score", 0),
            market_impact_score=row.get("market_impact_score", 0),
            spotify_adjacency_score=row.get("spotify_adjacency_score", 0),
            risk_score=row.get("risk_score", 0),
            risk_level=row.get("risk_level", "low"),
            suggested_action=row.get("suggested_action", "monitor"),
            confidence=row.get("confidence", "medium"),
            priority_level=row.get("priority_level", "low"),
            description=row.get("description", ""),
            source_url=row.get("source_url"),
            entities=json.loads(row["entities"]) if row.get("entities") else {},
            whats_happening=row.get("whats_happening", ""),
            why_it_matters=json.loads(row["why_it_matters"]) if row.get("why_it_matters") else [],
            if_goes_wrong=row.get("if_goes_wrong", ""),
            volume=row.get("volume", 0),
            engagement=row.get("engagement", 0),
            velocity=row.get("velocity", 0),
            first_seen=datetime.fromisoformat(row["first_seen"]) if row.get("first_seen") else None,
            last_updated=datetime.fromisoformat(row["last_updated"]) if row.get("last_updated") else datetime.utcnow(),
            collected_at=datetime.fromisoformat(row["collected_at"]) if row.get("collected_at") else datetime.utcnow(),
        )

    # ==================== Trend-Jack Intelligence Methods ====================

    async def save_artist_spikes(self, spikes: List[ArtistSpike]) -> int:
        """Save artist spike records."""
        if not self._conn:
            await self.initialize()

        saved = 0
        for spike in spikes:
            try:
                await self._conn.execute("""
                    INSERT INTO artist_spikes (
                        id, artist_name, market, spike_score, time_window,
                        sparkline_data, why_spiking, confidence, is_ambiguous,
                        related_queries, related_topics, current_interest,
                        baseline_interest, collected_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        spike_score = excluded.spike_score,
                        sparkline_data = excluded.sparkline_data,
                        why_spiking = excluded.why_spiking,
                        confidence = excluded.confidence,
                        current_interest = excluded.current_interest,
                        collected_at = excluded.collected_at
                """, (
                    spike.id, spike.artist_name, spike.market, spike.spike_score,
                    spike.time_window, json.dumps(spike.sparkline_data),
                    json.dumps(spike.why_spiking), spike.confidence,
                    1 if spike.is_ambiguous else 0, json.dumps(spike.related_queries),
                    json.dumps(spike.related_topics), spike.current_interest,
                    spike.baseline_interest, spike.collected_at.isoformat()
                ))
                saved += 1
            except Exception as e:
                logger.error("save_artist_spike_error", spike_id=spike.id, error=str(e))

        await self._conn.commit()
        return saved

    async def get_artist_spikes(
        self,
        market: Optional[str] = None,
        time_window: str = "24h",
        limit: int = 20,
    ) -> List[ArtistSpike]:
        """Get artist spikes for a market."""
        if not self._conn:
            await self.initialize()

        query = "SELECT * FROM artist_spikes WHERE time_window = ?"
        params = [time_window]

        if market:
            query += " AND market = ?"
            params.append(market)

        query += " ORDER BY spike_score DESC LIMIT ?"
        params.append(limit)

        cursor = await self._conn.execute(query, params)
        rows = await cursor.fetchall()
        columns = [d[0] for d in cursor.description]

        spikes = []
        for row in rows:
            row_dict = dict(zip(columns, row))
            spikes.append(ArtistSpike(
                id=row_dict["id"],
                artist_name=row_dict["artist_name"],
                market=row_dict["market"],
                spike_score=row_dict["spike_score"],
                time_window=row_dict["time_window"],
                sparkline_data=json.loads(row_dict["sparkline_data"]) if row_dict.get("sparkline_data") else [],
                why_spiking=json.loads(row_dict["why_spiking"]) if row_dict.get("why_spiking") else [],
                confidence=row_dict.get("confidence", "medium"),
                is_ambiguous=bool(row_dict.get("is_ambiguous", 0)),
                related_queries=json.loads(row_dict["related_queries"]) if row_dict.get("related_queries") else [],
                related_topics=json.loads(row_dict["related_topics"]) if row_dict.get("related_topics") else [],
                current_interest=row_dict.get("current_interest", 0),
                baseline_interest=row_dict.get("baseline_interest", 0),
                collected_at=datetime.fromisoformat(row_dict["collected_at"]) if row_dict.get("collected_at") else datetime.utcnow(),
            ))
        return spikes

    async def save_culture_searches(self, searches: List[CultureSearch]) -> int:
        """Save culture search records."""
        if not self._conn:
            await self.initialize()

        saved = 0
        for search in searches:
            try:
                await self._conn.execute("""
                    INSERT INTO culture_searches (
                        id, term, market, sensitivity_tag, rise_percentage,
                        volume, is_cross_market, markets_present, risk_level, collected_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        rise_percentage = excluded.rise_percentage,
                        volume = excluded.volume,
                        is_cross_market = excluded.is_cross_market,
                        markets_present = excluded.markets_present,
                        collected_at = excluded.collected_at
                """, (
                    search.id, search.term, search.market, search.sensitivity_tag,
                    search.rise_percentage, search.volume,
                    1 if search.is_cross_market else 0,
                    json.dumps(search.markets_present), search.risk_level,
                    search.collected_at.isoformat()
                ))
                saved += 1
            except Exception as e:
                logger.error("save_culture_search_error", search_id=search.id, error=str(e))

        await self._conn.commit()
        return saved

    async def get_culture_searches(
        self,
        market: Optional[str] = None,
        sensitivity_tag: Optional[str] = None,
        limit: int = 10,
    ) -> List[CultureSearch]:
        """Get culture searches with filters."""
        if not self._conn:
            await self.initialize()

        query = "SELECT * FROM culture_searches WHERE 1=1"
        params = []

        if market:
            query += " AND market = ?"
            params.append(market)

        if sensitivity_tag:
            query += " AND sensitivity_tag = ?"
            params.append(sensitivity_tag)

        query += " ORDER BY rise_percentage DESC LIMIT ?"
        params.append(limit)

        cursor = await self._conn.execute(query, params)
        rows = await cursor.fetchall()
        columns = [d[0] for d in cursor.description]

        searches = []
        for row in rows:
            row_dict = dict(zip(columns, row))
            searches.append(CultureSearch(
                id=row_dict["id"],
                term=row_dict["term"],
                market=row_dict["market"],
                sensitivity_tag=row_dict["sensitivity_tag"],
                rise_percentage=row_dict["rise_percentage"],
                volume=row_dict.get("volume", 0),
                is_cross_market=bool(row_dict.get("is_cross_market", 0)),
                markets_present=json.loads(row_dict["markets_present"]) if row_dict.get("markets_present") else [],
                risk_level=row_dict.get("risk_level", "low"),
                collected_at=datetime.fromisoformat(row_dict["collected_at"]) if row_dict.get("collected_at") else datetime.utcnow(),
            ))
        return searches

    async def get_culture_overlaps(self) -> List[dict]:
        """Get cross-market culture search overlaps."""
        if not self._conn:
            await self.initialize()

        cursor = await self._conn.execute("""
            SELECT term, markets_present, sensitivity_tag, MAX(rise_percentage) as max_rise
            FROM culture_searches
            WHERE is_cross_market = 1
            GROUP BY term
            ORDER BY max_rise DESC
            LIMIT 20
        """)
        rows = await cursor.fetchall()

        overlaps = []
        for row in rows:
            overlaps.append({
                "term": row[0],
                "markets": json.loads(row[1]) if row[1] else [],
                "sensitivity_tag": row[2],
                "max_rise_percentage": row[3],
            })
        return overlaps

    async def save_style_signals(self, signals: List[StyleSignal]) -> int:
        """Save style signal records."""
        if not self._conn:
            await self.initialize()

        saved = 0
        for signal in signals:
            try:
                await self._conn.execute("""
                    INSERT INTO style_signals (
                        id, headline, source, source_url, summary,
                        publish_date, country_relevance, spotify_tags,
                        risk_level, collected_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        headline = excluded.headline,
                        summary = excluded.summary,
                        spotify_tags = excluded.spotify_tags,
                        risk_level = excluded.risk_level,
                        collected_at = excluded.collected_at
                """, (
                    signal.id, signal.headline, signal.source, signal.source_url,
                    signal.summary, signal.publish_date.isoformat(),
                    json.dumps(signal.country_relevance), json.dumps(signal.spotify_tags),
                    signal.risk_level, signal.collected_at.isoformat()
                ))
                saved += 1
            except Exception as e:
                logger.error("save_style_signal_error", signal_id=signal.id, error=str(e))

        await self._conn.commit()
        return saved

    async def get_style_signals(
        self,
        country_relevance: Optional[str] = None,
        max_risk: str = "high",
        limit: int = 15,
    ) -> List[StyleSignal]:
        """Get style signals with filters."""
        if not self._conn:
            await self.initialize()

        risk_order = {"low": 1, "medium": 2, "high": 3}
        max_risk_value = risk_order.get(max_risk, 3)

        query = "SELECT * FROM style_signals WHERE 1=1"
        params = []

        if country_relevance:
            query += " AND country_relevance LIKE ?"
            params.append(f'%"{country_relevance}"%')

        # Filter by max risk
        if max_risk != "high":
            risk_filter = ["'low'"]
            if max_risk == "medium":
                risk_filter.append("'medium'")
            query += f" AND risk_level IN ({','.join(risk_filter)})"

        query += " ORDER BY publish_date DESC LIMIT ?"
        params.append(limit)

        cursor = await self._conn.execute(query, params)
        rows = await cursor.fetchall()
        columns = [d[0] for d in cursor.description]

        signals = []
        for row in rows:
            row_dict = dict(zip(columns, row))
            signals.append(StyleSignal(
                id=row_dict["id"],
                headline=row_dict["headline"],
                source=row_dict["source"],
                source_url=row_dict.get("source_url", ""),
                summary=row_dict.get("summary", ""),
                publish_date=datetime.fromisoformat(row_dict["publish_date"]) if row_dict.get("publish_date") else datetime.utcnow(),
                country_relevance=json.loads(row_dict["country_relevance"]) if row_dict.get("country_relevance") else [],
                spotify_tags=json.loads(row_dict["spotify_tags"]) if row_dict.get("spotify_tags") else [],
                risk_level=row_dict.get("risk_level", "low"),
                collected_at=datetime.fromisoformat(row_dict["collected_at"]) if row_dict.get("collected_at") else datetime.utcnow(),
            ))
        return signals

    async def save_pitch_cards(self, cards: List[PitchCard]) -> int:
        """Save generated pitch cards."""
        if not self._conn:
            await self.initialize()

        saved = 0
        for card in cards:
            try:
                await self._conn.execute("""
                    INSERT INTO pitch_cards (
                        id, market, hook, why_now, spotify_angle,
                        next_steps, risks, confidence, source_signals,
                        generated_at, expires_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        hook = excluded.hook,
                        why_now = excluded.why_now,
                        spotify_angle = excluded.spotify_angle,
                        next_steps = excluded.next_steps,
                        risks = excluded.risks,
                        confidence = excluded.confidence,
                        generated_at = excluded.generated_at,
                        expires_at = excluded.expires_at
                """, (
                    card.id, card.market, card.hook, json.dumps(card.why_now),
                    card.spotify_angle, json.dumps(card.next_steps),
                    json.dumps(card.risks), card.confidence,
                    json.dumps(card.source_signals), card.generated_at.isoformat(),
                    card.expires_at.isoformat() if card.expires_at else None
                ))
                saved += 1
            except Exception as e:
                logger.error("save_pitch_card_error", card_id=card.id, error=str(e))

        await self._conn.commit()
        return saved

    async def get_pitch_cards(
        self,
        market: Optional[str] = None,
        limit: int = 6,
    ) -> List[PitchCard]:
        """Get pitch cards for a market."""
        if not self._conn:
            await self.initialize()

        query = "SELECT * FROM pitch_cards WHERE (expires_at IS NULL OR expires_at > ?)"
        params = [datetime.utcnow().isoformat()]

        if market:
            query += " AND market = ?"
            params.append(market)

        query += " ORDER BY generated_at DESC LIMIT ?"
        params.append(limit)

        cursor = await self._conn.execute(query, params)
        rows = await cursor.fetchall()
        columns = [d[0] for d in cursor.description]

        cards = []
        for row in rows:
            row_dict = dict(zip(columns, row))
            cards.append(PitchCard(
                id=row_dict["id"],
                market=row_dict["market"],
                hook=row_dict["hook"],
                why_now=json.loads(row_dict["why_now"]) if row_dict.get("why_now") else [],
                spotify_angle=row_dict.get("spotify_angle", ""),
                next_steps=json.loads(row_dict["next_steps"]) if row_dict.get("next_steps") else [],
                risks=json.loads(row_dict["risks"]) if row_dict.get("risks") else [],
                confidence=row_dict.get("confidence", "medium"),
                source_signals=json.loads(row_dict["source_signals"]) if row_dict.get("source_signals") else [],
                generated_at=datetime.fromisoformat(row_dict["generated_at"]) if row_dict.get("generated_at") else datetime.utcnow(),
                expires_at=datetime.fromisoformat(row_dict["expires_at"]) if row_dict.get("expires_at") else None,
            ))
        return cards

    async def save_data_health(self, health: DataHealth) -> None:
        """Save module health status."""
        if not self._conn:
            await self.initialize()

        await self._conn.execute("""
            INSERT INTO data_health (
                module_name, status, last_success, last_error,
                item_count, freshness_hours, checked_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(module_name) DO UPDATE SET
                status = excluded.status,
                last_success = excluded.last_success,
                last_error = excluded.last_error,
                item_count = excluded.item_count,
                freshness_hours = excluded.freshness_hours,
                checked_at = excluded.checked_at
        """, (
            health.module_name, health.status,
            health.last_success.isoformat() if health.last_success else None,
            health.last_error, health.item_count, health.freshness_hours,
            health.checked_at.isoformat()
        ))
        await self._conn.commit()

    async def get_data_health(self) -> List[DataHealth]:
        """Get health status for all modules."""
        if not self._conn:
            await self.initialize()

        cursor = await self._conn.execute("SELECT * FROM data_health ORDER BY module_name")
        rows = await cursor.fetchall()
        columns = [d[0] for d in cursor.description]

        health_list = []
        for row in rows:
            row_dict = dict(zip(columns, row))
            health_list.append(DataHealth(
                module_name=row_dict["module_name"],
                status=row_dict["status"],
                last_success=datetime.fromisoformat(row_dict["last_success"]) if row_dict.get("last_success") else None,
                last_error=row_dict.get("last_error"),
                item_count=row_dict.get("item_count", 0),
                freshness_hours=row_dict.get("freshness_hours", 0),
                checked_at=datetime.fromisoformat(row_dict["checked_at"]) if row_dict.get("checked_at") else datetime.utcnow(),
            ))
        return health_list

    async def get_module_health(self, module_name: str) -> Optional[DataHealth]:
        """Get health status for a specific module."""
        if not self._conn:
            await self.initialize()

        cursor = await self._conn.execute(
            "SELECT * FROM data_health WHERE module_name = ?",
            (module_name,)
        )
        row = await cursor.fetchone()
        if not row:
            return None

        columns = [d[0] for d in cursor.description]
        row_dict = dict(zip(columns, row))
        return DataHealth(
            module_name=row_dict["module_name"],
            status=row_dict["status"],
            last_success=datetime.fromisoformat(row_dict["last_success"]) if row_dict.get("last_success") else None,
            last_error=row_dict.get("last_error"),
            item_count=row_dict.get("item_count", 0),
            freshness_hours=row_dict.get("freshness_hours", 0),
            checked_at=datetime.fromisoformat(row_dict["checked_at"]) if row_dict.get("checked_at") else datetime.utcnow(),
        )

    async def save_user_session(self, session: UserSession) -> None:
        """Save user session."""
        if not self._conn:
            await self.initialize()

        await self._conn.execute("""
            INSERT INTO user_sessions (session_id, created_at, expires_at, is_valid)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(session_id) DO UPDATE SET
                expires_at = excluded.expires_at,
                is_valid = excluded.is_valid
        """, (
            session.session_id, session.created_at.isoformat(),
            session.expires_at.isoformat() if session.expires_at else None,
            1 if session.is_valid else 0
        ))
        await self._conn.commit()

    async def get_user_session(self, session_id: str) -> Optional[UserSession]:
        """Get user session by ID."""
        if not self._conn:
            await self.initialize()

        cursor = await self._conn.execute(
            "SELECT * FROM user_sessions WHERE session_id = ? AND is_valid = 1",
            (session_id,)
        )
        row = await cursor.fetchone()

        if not row:
            return None

        columns = [d[0] for d in cursor.description]
        row_dict = dict(zip(columns, row))

        return UserSession(
            session_id=row_dict["session_id"],
            created_at=datetime.fromisoformat(row_dict["created_at"]) if row_dict.get("created_at") else datetime.utcnow(),
            expires_at=datetime.fromisoformat(row_dict["expires_at"]) if row_dict.get("expires_at") else None,
            is_valid=bool(row_dict.get("is_valid", 1)),
        )

    async def delete_user_session(self, session_id: str) -> None:
        """Delete user session."""
        if not self._conn:
            await self.initialize()

        await self._conn.execute(
            "UPDATE user_sessions SET is_valid = 0 WHERE session_id = ?",
            (session_id,)
        )
        await self._conn.commit()

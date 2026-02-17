"""
PostgreSQL storage backend implementation.

Placeholder - extend SQLite patterns for production PostgreSQL deployment.
"""

import structlog
from typing import Optional, List
from datetime import datetime

from .base import BaseStorage, TrendRecord

logger = structlog.get_logger()


class PostgresStorage(BaseStorage):
    """
    PostgreSQL storage backend.

    Good for:
    - Production deployments
    - High data volumes
    - Multi-instance setups
    - Advanced querying

    Requires:
    - asyncpg library
    - PostgreSQL 12+
    - Environment variables for connection
    """

    def __init__(self, config: dict):
        super().__init__(config)
        postgres_config = config.get("postgres", {})
        self.host = postgres_config.get("host", "localhost")
        self.port = postgres_config.get("port", 5432)
        self.database = postgres_config.get("database", "spotify_trends")
        self.user = postgres_config.get("user", "")
        self.password = postgres_config.get("password", "")
        self._pool = None

    async def initialize(self):
        """Create connection pool and tables."""
        try:
            import asyncpg
        except ImportError:
            logger.error("asyncpg_not_installed", message="pip install asyncpg")
            raise ImportError("asyncpg is required for PostgreSQL storage")

        try:
            self._pool = await asyncpg.create_pool(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                min_size=2,
                max_size=10,
            )

            # Create tables
            async with self._pool.acquire() as conn:
                await conn.execute("""
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
                        entities JSONB,
                        whats_happening TEXT,
                        why_it_matters JSONB,
                        if_goes_wrong TEXT,
                        volume INTEGER DEFAULT 0,
                        engagement INTEGER DEFAULT 0,
                        velocity REAL DEFAULT 0,
                        first_seen TIMESTAMP,
                        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        raw_data JSONB
                    );

                    CREATE TABLE IF NOT EXISTS trend_history (
                        id SERIAL PRIMARY KEY,
                        trend_id TEXT NOT NULL,
                        snapshot_date DATE NOT NULL,
                        total_score REAL,
                        volume INTEGER,
                        engagement INTEGER,
                        velocity REAL,
                        UNIQUE(trend_id, snapshot_date)
                    );

                    CREATE TABLE IF NOT EXISTS pipeline_runs (
                        id TEXT PRIMARY KEY,
                        started_at TIMESTAMP NOT NULL,
                        completed_at TIMESTAMP,
                        success BOOLEAN DEFAULT FALSE,
                        total_items INTEGER DEFAULT 0,
                        metrics JSONB
                    );

                    -- Indexes
                    CREATE INDEX IF NOT EXISTS idx_trends_market ON trends(market);
                    CREATE INDEX IF NOT EXISTS idx_trends_topic ON trends(topic);
                    CREATE INDEX IF NOT EXISTS idx_trends_score ON trends(total_score DESC);
                    CREATE INDEX IF NOT EXISTS idx_trends_risk ON trends(risk_level);
                    CREATE INDEX IF NOT EXISTS idx_trends_updated ON trends(last_updated DESC);
                    CREATE INDEX IF NOT EXISTS idx_history_trend ON trend_history(trend_id);
                """)

            logger.info("postgres_initialized", host=self.host, database=self.database)

        except Exception as e:
            logger.error("postgres_init_error", error=str(e))
            raise

    async def close(self):
        """Close connection pool."""
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def save_trends(self, trends: List[TrendRecord]) -> int:
        """Save trend records using UPSERT."""
        if not self._pool:
            await self.initialize()

        saved = 0
        today = datetime.utcnow().date()

        async with self._pool.acquire() as conn:
            for trend in trends:
                try:
                    import json
                    await conn.execute("""
                        INSERT INTO trends (
                            id, title, source, topic, subtopic, market, language,
                            total_score, velocity_score, reach_score, market_impact_score,
                            spotify_adjacency_score, risk_score, risk_level, suggested_action,
                            confidence, priority_level, description, source_url, entities,
                            whats_happening, why_it_matters, if_goes_wrong,
                            volume, engagement, velocity, first_seen, last_updated, collected_at
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29)
                        ON CONFLICT (id) DO UPDATE SET
                            title = EXCLUDED.title,
                            total_score = EXCLUDED.total_score,
                            velocity_score = EXCLUDED.velocity_score,
                            reach_score = EXCLUDED.reach_score,
                            market_impact_score = EXCLUDED.market_impact_score,
                            spotify_adjacency_score = EXCLUDED.spotify_adjacency_score,
                            risk_score = EXCLUDED.risk_score,
                            risk_level = EXCLUDED.risk_level,
                            suggested_action = EXCLUDED.suggested_action,
                            confidence = EXCLUDED.confidence,
                            priority_level = EXCLUDED.priority_level,
                            whats_happening = EXCLUDED.whats_happening,
                            why_it_matters = EXCLUDED.why_it_matters,
                            if_goes_wrong = EXCLUDED.if_goes_wrong,
                            volume = EXCLUDED.volume,
                            engagement = EXCLUDED.engagement,
                            velocity = EXCLUDED.velocity,
                            last_updated = EXCLUDED.last_updated
                    """,
                        trend.id, trend.title, trend.source, trend.topic, trend.subtopic,
                        trend.market, trend.language, trend.total_score, trend.velocity_score,
                        trend.reach_score, trend.market_impact_score, trend.spotify_adjacency_score,
                        trend.risk_score, trend.risk_level, trend.suggested_action,
                        trend.confidence, trend.priority_level, trend.description,
                        trend.source_url, json.dumps(trend.entities),
                        trend.whats_happening, json.dumps(trend.why_it_matters),
                        trend.if_goes_wrong, trend.volume, trend.engagement, trend.velocity,
                        trend.first_seen, trend.last_updated, trend.collected_at
                    )

                    # Save history
                    await conn.execute("""
                        INSERT INTO trend_history (trend_id, snapshot_date, total_score, volume, engagement, velocity)
                        VALUES ($1, $2, $3, $4, $5, $6)
                        ON CONFLICT (trend_id, snapshot_date) DO UPDATE SET
                            total_score = EXCLUDED.total_score,
                            volume = EXCLUDED.volume,
                            engagement = EXCLUDED.engagement,
                            velocity = EXCLUDED.velocity
                    """, trend.id, today, trend.total_score, trend.volume, trend.engagement, trend.velocity)

                    saved += 1

                except Exception as e:
                    logger.error("save_trend_error", trend_id=trend.id, error=str(e))

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
        """Get trends with filters - PostgreSQL implementation."""
        if not self._pool:
            await self.initialize()

        # Build parameterized query
        conditions = ["1=1"]
        params = []
        param_idx = 1

        if market:
            conditions.append(f"market = ${param_idx}")
            params.append(market)
            param_idx += 1

        if topic:
            conditions.append(f"topic = ${param_idx}")
            params.append(topic)
            param_idx += 1

        if risk_level:
            conditions.append(f"risk_level = ${param_idx}")
            params.append(risk_level)
            param_idx += 1

        if min_score is not None:
            conditions.append(f"total_score >= ${param_idx}")
            params.append(min_score)
            param_idx += 1

        if since:
            conditions.append(f"last_updated >= ${param_idx}")
            params.append(since)
            param_idx += 1

        query = f"""
            SELECT * FROM trends
            WHERE {' AND '.join(conditions)}
            ORDER BY total_score DESC
            LIMIT ${param_idx} OFFSET ${param_idx + 1}
        """
        params.extend([limit, offset])

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(query, *params)

        return [self._row_to_record(dict(row)) for row in rows]

    async def get_trend_by_id(self, trend_id: str) -> Optional[TrendRecord]:
        """Get single trend by ID."""
        if not self._pool:
            await self.initialize()

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM trends WHERE id = $1", trend_id)

        if not row:
            return None

        return self._row_to_record(dict(row))

    async def get_trend_history(self, trend_id: str, days: int = 7) -> List[dict]:
        """Get historical snapshots."""
        if not self._pool:
            await self.initialize()

        from datetime import timedelta
        since = datetime.utcnow().date() - timedelta(days=days)

        async with self._pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT snapshot_date, total_score, volume, engagement, velocity
                FROM trend_history
                WHERE trend_id = $1 AND snapshot_date >= $2
                ORDER BY snapshot_date ASC
            """, trend_id, since)

        return [
            {
                "date": row["snapshot_date"].isoformat(),
                "score": row["total_score"],
                "volume": row["volume"],
                "engagement": row["engagement"],
                "velocity": row["velocity"],
            }
            for row in rows
        ]

    async def get_baselines(
        self,
        market: Optional[str] = None,
        topic: Optional[str] = None
    ) -> dict:
        """Get baseline metrics."""
        if not self._pool:
            await self.initialize()

        from datetime import timedelta
        since = datetime.utcnow() - timedelta(days=7)

        conditions = ["last_updated >= $1"]
        params = [since]
        param_idx = 2

        if market:
            conditions.append(f"market = ${param_idx}")
            params.append(market)
            param_idx += 1

        if topic:
            conditions.append(f"topic = ${param_idx}")
            params.append(topic)
            param_idx += 1

        query = f"""
            SELECT
                AVG(volume) as avg_volume,
                AVG(engagement) as avg_engagement,
                AVG(total_score) as avg_score,
                COUNT(*) as count
            FROM trends
            WHERE {' AND '.join(conditions)}
        """

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(query, *params)

        return {
            "avg_volume": float(row["avg_volume"] or 0),
            "avg_engagement": float(row["avg_engagement"] or 0),
            "avg_score": float(row["avg_score"] or 0),
            "sample_size": row["count"] or 0,
        }

    async def save_pipeline_run(self, metrics: dict) -> str:
        """Save pipeline run."""
        if not self._pool:
            await self.initialize()

        import uuid
        import json
        run_id = str(uuid.uuid4())[:8]

        async with self._pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO pipeline_runs (id, started_at, completed_at, success, total_items, metrics)
                VALUES ($1, $2, $3, $4, $5, $6)
            """,
                run_id,
                metrics.get("started_at"),
                metrics.get("completed_at"),
                metrics.get("success", False),
                metrics.get("stages", {}).get("collect", {}).get("items_collected", 0),
                json.dumps(metrics)
            )

        return run_id

    async def get_last_run(self) -> Optional[dict]:
        """Get last pipeline run."""
        if not self._pool:
            await self.initialize()

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT 1
            """)

        if not row:
            return None

        return {
            "id": row["id"],
            "started_at": row["started_at"].isoformat() if row["started_at"] else None,
            "completed_at": row["completed_at"].isoformat() if row["completed_at"] else None,
            "success": row["success"],
            "total_items": row["total_items"],
            "metrics": row["metrics"] or {},
        }

    async def cleanup_old_data(self, days: int = 90) -> int:
        """Remove old data."""
        if not self._pool:
            await self.initialize()

        from datetime import timedelta
        cutoff = datetime.utcnow() - timedelta(days=days)

        total = 0
        async with self._pool.acquire() as conn:
            result = await conn.execute("DELETE FROM trends WHERE last_updated < $1", cutoff)
            total += int(result.split()[-1])

            result = await conn.execute("DELETE FROM trend_history WHERE snapshot_date < $1", cutoff.date())
            total += int(result.split()[-1])

            result = await conn.execute("DELETE FROM pipeline_runs WHERE started_at < $1", cutoff)
            total += int(result.split()[-1])

        return total

    def _row_to_record(self, row: dict) -> TrendRecord:
        """Convert database row to TrendRecord."""
        import json
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
            entities=row.get("entities") or {},
            whats_happening=row.get("whats_happening", ""),
            why_it_matters=row.get("why_it_matters") or [],
            if_goes_wrong=row.get("if_goes_wrong", ""),
            volume=row.get("volume", 0),
            engagement=row.get("engagement", 0),
            velocity=row.get("velocity", 0),
            first_seen=row.get("first_seen"),
            last_updated=row.get("last_updated") or datetime.utcnow(),
            collected_at=row.get("collected_at") or datetime.utcnow(),
        )

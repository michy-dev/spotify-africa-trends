# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run Commands

```bash
# Setup (first time)
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
python -m spacy download en_core_web_sm  # Optional: improves entity extraction

# Run the dashboard locally
python main.py run-server              # Starts at http://localhost:8000

# Run the data pipeline
python main.py run-pipeline            # Collect, process, and save trends

# Other commands
python main.py health-check            # Check connector health
python main.py generate-digest         # Generate daily digest reports
python main.py run-scheduler           # Run scheduled jobs (6 AM pipeline, 7 AM digest, 4 AM trendjack)
python main.py run-trendjack           # Run trend-jack intelligence refresh

# Testing
pytest tests/                          # Run all tests
pytest tests/test_pipeline.py -v       # Run specific test file
pytest tests/ --cov=. --cov-report=html
```

## Deployment

Deployed on Fly.io at: https://spotify-africa-trends.fly.dev/

```bash
flyctl deploy                          # Deploy to Fly.io
flyctl logs --app spotify-africa-trends  # View logs

# Trigger pipeline remotely
curl -X POST "https://spotify-africa-trends.fly.dev/api/pipeline/run-async?secret=spotify-ssa-trends-2024"
```

## Architecture

The system follows a 6-stage data pipeline:

```
Connectors → Collector → Cleaner → Enricher → Classifier → Scorer → Summariser → Storage
```

### Key Components

- **Connectors** (`connectors/`): Fetch data from external sources (Google Trends, Reddit, RSS, Wikipedia, YouTube, Twitter). Each implements `BaseConnector` interface with `fetch()` and `health_check()` methods.

- **Pipeline** (`pipeline/`): Sequential processing stages:
  - `collector.py`: Aggregates from all enabled connectors
  - `cleaner.py`: Normalizes text, deduplicates across sources
  - `enricher.py`: Extracts entities (via spaCy), detects language
  - `classifier.py`: Assigns topic/subtopic based on keyword matching
  - `scorer.py`: Computes Comms Relevance Score (velocity, reach, market impact, Spotify adjacency, risk)
  - `summariser.py`: Generates "What's Happening", "Why It Matters", action recommendations
  - `orchestrator.py`: Coordinates full pipeline execution

- **Dashboard** (`dashboard/app.py`): FastAPI web application with HTML templates and REST API

- **Storage** (`storage/`): SQLite (default) or PostgreSQL backends implementing `BaseStorage`

### Data Flow

1. `TrendItem` (from connectors) → processed through pipeline stages
2. `TrendSummary` (output of summariser) → converted to `TrendRecord`
3. `TrendRecord` saved to storage and served via API

### Configuration

All settings in `config/settings.yaml`:
- Markets with priority weights (ZA, NG, KE highest priority)
- Topic taxonomy with keywords and risk weights
- Scoring weights (velocity 25%, reach 20%, market impact 20%, Spotify adjacency 20%, risk 15%)
- Data source enable/disable and rate limits

### API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/trends` | List trends with filters (market, topic, risk_level, min_score) |
| `GET /api/trends/{id}` | Single trend with history |
| `GET /api/stats` | Dashboard statistics |
| `POST /api/pipeline/run-async` | Trigger pipeline (requires secret param) |
| `GET /health` | Health check |

## Adding a New Data Connector

1. Create `connectors/my_source.py` extending `BaseConnector`
2. Implement `async def fetch(markets, keywords)` returning `ConnectorResult`
3. Implement `async def health_check()` returning bool
4. Register in `connectors/__init__.py`: `CONNECTOR_REGISTRY["my_source"] = MySourceConnector`
5. Add config in `config/settings.yaml` under `sources:`

## Trend-Jack Intelligence

Enhanced trend-jack intelligence modules for Comms teams in Nigeria, Kenya, Ghana, and South Africa.

### New Connectors

| Connector | File | Purpose |
|-----------|------|---------|
| Artist Spikes | `connectors/artist_spikes.py` | Detects artist search interest spikes with sparkline data and "why spiking" bullets |
| Style Signals | `connectors/style_signals.py` | RSS aggregation from streetwear/fashion sources (Highsnobiety, Hypebeast, Nataal, etc.) |

Note: Culture Search connector (`connectors/culture_search.py`) was removed due to Google Trends rate limiting.

### Pitch Card Generator

`pipeline/pitch_generator.py` combines signals from Artist Spikes and Style Signals to generate actionable pitch cards with:
- **Hook**: Headline-style opener
- **Why Now**: Supporting signals
- **Spotify Angle**: FTR/playlist/podcast/editorial/creator angle
- **Next Steps**: Who to message, what to prep
- **Risks**: 1-2 bullet sensitivities
- **Confidence**: High/Medium/Low

### New API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/artist-spikes/{market}?time_window=24h\|7d` | Artist search spikes |
| `GET /api/style-signals?country_relevance=&max_risk=` | Streetwear/fashion signals |
| `GET /api/pitch-cards/{market}` | Generated pitch cards |
| `GET /api/data-health` | Module health status |
| `POST /api/trendjack/refresh?secret=` | Trigger trend-jack refresh |

### Authentication

Simple password gate via `AUTH_PASSWORD` env var:
- Login at `/auth/login`
- All `/api/*` and `/` endpoints protected (except `/health`)
- Session stored in cookie after successful login

### Data Health Monitoring

`monitoring/health.py` tracks:
- Per-module status: OK / Degraded / Down
- Freshness thresholds per module
- Last successful fetch time
- Error messages

### Risk Factor QA

`monitoring/risk_validator.py` validates:
- Schema: `risk_level` is enum (low/medium/high), `risk_score` is 0-100
- Consistency: risk_level matches risk_score range
- Freshness: Warn if trends older than 24 hours

## Environment Variables

- `DATABASE_PATH`: SQLite database path (default: `data/trends.db`)
- `PIPELINE_SECRET`: Secret for async pipeline trigger endpoint
- `AUTH_PASSWORD`: Password for dashboard authentication (optional - leave unset to disable auth)
- `YOUTUBE_API_KEY`, `TWITTER_BEARER_TOKEN`, `REDDIT_CLIENT_ID/SECRET`: Optional API keys
- `NEWS_API_KEY`: Optional News API key for "why spiking" validation

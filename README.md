# Spotify Africa Comms Trends Dashboard

A daily-updating trend monitoring and intelligence platform for Spotify Sub-Saharan Africa communications teams. This dashboard collects, analyzes, scores, and presents trends across multiple data sources, providing actionable insights for comms professionals.

## Features

- **Multi-source data collection**: Google Trends, Reddit, RSS news feeds, Wikipedia pageviews, YouTube, Twitter/X (with API access)
- **African market focus**: Explicit tracking for South Africa, Nigeria, Kenya, Ghana, Tanzania, Uganda, Angola, Côte d'Ivoire, Senegal, Egypt, and Morocco
- **Topic classification**: Music & audio, culture, fashion/beauty, current affairs, brand/comms issues, Spotify-specific
- **Explainable scoring**: Comms Relevance Score with full breakdown (velocity, reach, market impact, Spotify adjacency, risk)
- **Action recommendations**: Monitor, Engage, Partner, Avoid, or Escalate for each trend
- **Risk detection**: Automatic flagging of sensitive topics requiring legal/policy review
- **Daily digest**: Markdown and HTML reports for email distribution
- **Web dashboard**: Fast, filterable interface optimized for comms decision-making

## Quick Start

### Prerequisites

- Python 3.10+
- pip

### Installation

```bash
# Clone/navigate to the project
cd spotify-africa-trends

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Download spaCy model for entity extraction (optional but recommended)
python -m spacy download en_core_web_sm

# Copy environment file and add any API keys you have
cp .env.example .env
```

### Running the Dashboard

```bash
# Start the web dashboard
python main.py run-server

# Open http://localhost:8000 in your browser
```

### Running the Pipeline

```bash
# Run data collection and processing once
python main.py run-pipeline

# Check connector health
python main.py health-check

# Generate daily digest
python main.py generate-digest
```

### Scheduling Daily Runs

```bash
# Start the scheduler (runs pipeline at 6 AM, digest at 7 AM Africa/Johannesburg)
python main.py run-scheduler
```

Or use cron/systemd:

```bash
# crontab -e
0 6 * * * cd /path/to/spotify-africa-trends && /path/to/venv/bin/python main.py run-pipeline
0 7 * * * cd /path/to/spotify-africa-trends && /path/to/venv/bin/python main.py generate-digest
```

## Project Structure

```
spotify-africa-trends/
├── config/
│   └── settings.yaml      # Main configuration (markets, topics, scoring weights)
├── connectors/             # Data source connectors
│   ├── base.py            # Base connector interface
│   ├── google_trends.py   # Google Trends connector
│   ├── news_rss.py        # RSS feed connector
│   ├── reddit.py          # Reddit connector
│   ├── wikipedia.py       # Wikipedia pageviews
│   ├── youtube.py         # YouTube trending (requires API key)
│   ├── twitter.py         # Twitter/X (requires API access)
│   ├── tiktok.py          # TikTok (stub - restricted API)
│   ├── instagram.py       # Instagram (stub - requires Meta API)
│   └── spotify_internal.py # Placeholder for internal signals
├── pipeline/               # Data processing pipeline
│   ├── collector.py       # Multi-source data collection
│   ├── cleaner.py         # Deduplication and normalization
│   ├── enricher.py        # Entity extraction, language detection
│   ├── classifier.py      # Topic classification
│   ├── scorer.py          # Comms Relevance Score calculation
│   ├── summariser.py      # Generate comms-ready summaries
│   └── orchestrator.py    # Pipeline coordination
├── dashboard/              # Web interface
│   ├── app.py             # FastAPI application
│   ├── templates/         # HTML templates
│   └── static/            # Static assets
├── digest/                 # Daily report generator
│   └── generator.py       # Markdown/HTML digest generation
├── storage/                # Data persistence
│   ├── base.py            # Storage interface
│   ├── sqlite.py          # SQLite backend (default)
│   └── postgres.py        # PostgreSQL backend (production)
├── tests/                  # Test suite
├── main.py                # CLI entry point
├── requirements.txt
└── README.md
```

## Configuration

All configuration is in `config/settings.yaml`:

### Markets

```yaml
markets:
  priority:
    - code: "ZA"
      name: "South Africa"
      languages: ["en", "zu", "xh"]
      weight: 1.5  # Priority market weight for scoring
```

### Topics

```yaml
topics:
  music_audio:
    name: "Music & Audio"
    keywords: ["artist", "album", "concert", "afrobeats", "amapiano"]
    subtopics: ["artists", "genres", "songs", "live_events"]
```

### Scoring Weights

```yaml
scoring:
  weights:
    velocity: 0.25       # Growth rate vs baseline
    reach: 0.20          # Volume across platforms
    market_impact: 0.20  # Weighted by priority markets
    spotify_adjacency: 0.20  # Connection to audio culture
    risk_factor: 0.15    # Safety/politics/conflict signals
```

### Data Sources

```yaml
sources:
  google_trends:
    enabled: true
    priority: 1
  reddit:
    enabled: true
    subreddits: ["Africa", "Nigeria", "southafrica"]
  news_rss:
    enabled: true
    feeds:
      - name: "BBC Africa"
        url: "https://feeds.bbci.co.uk/news/world/africa/rss.xml"
```

## Adding New Data Sources

1. Create a new connector in `connectors/`:

```python
from .base import BaseConnector, ConnectorResult, TrendItem

class MyConnector(BaseConnector):
    name = "my_source"
    display_name = "My Source"
    requires_auth = False

    async def fetch(self, markets, keywords, **kwargs) -> ConnectorResult:
        items = []
        # Fetch and normalize data
        return self._create_result(items)

    async def health_check(self) -> bool:
        # Check if source is accessible
        return True
```

2. Register in `connectors/__init__.py`:

```python
from .my_source import MyConnector
CONNECTOR_REGISTRY["my_source"] = MyConnector
```

3. Add configuration in `config/settings.yaml`:

```yaml
sources:
  my_source:
    enabled: true
    priority: 2
```

## API Endpoints

The dashboard exposes a REST API:

| Endpoint | Description |
|----------|-------------|
| `GET /api/trends` | List trends with filters |
| `GET /api/trends/{id}` | Get trend details |
| `GET /api/risks` | Get high/medium risk items |
| `GET /api/actions` | Get trends by suggested action |
| `GET /api/markets` | Get trend counts by market |
| `GET /api/topics` | Get trend counts by topic |
| `GET /api/stats` | Get dashboard statistics |
| `GET /api/filters` | Get available filter options |
| `POST /api/pipeline/run` | Trigger pipeline manually |
| `GET /api/health` | Health check |

### Filtering Trends

```bash
# Get high-risk trends in Nigeria
curl "http://localhost:8000/api/trends?market=NG&risk_level=high"

# Get Spotify-specific trends with score > 70
curl "http://localhost:8000/api/trends?topic=spotify_specific&min_score=70"
```

## Scoring System

Each trend receives a **Comms Relevance Score** (0-100) based on:

| Component | Weight | Description |
|-----------|--------|-------------|
| Velocity | 25% | Growth rate compared to baseline |
| Reach | 20% | Volume and engagement metrics |
| Market Impact | 20% | Weighted by market priority |
| Spotify Adjacency | 20% | Connection to music/audio culture |
| Risk Factor | 15% | Sensitivity signals |

### Suggested Actions

Based on scores, each trend receives an action recommendation:

- **ESCALATE**: High risk + high Spotify relevance. Needs immediate attention.
- **ENGAGE**: Opportunity to participate authentically.
- **PARTNER**: Consider collaboration with artists/creators.
- **AVOID**: Stay away. Don't associate brand with this topic.
- **MONITOR**: Watch but no action needed.

## Testing

```bash
# Run all tests
pytest tests/

# Run with coverage
pytest tests/ --cov=. --cov-report=html

# Run specific test file
pytest tests/test_pipeline.py -v
```

## Deployment

### Docker (recommended for production)

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN python -m spacy download en_core_web_sm

COPY . .

CMD ["python", "main.py", "run-server"]
```

### Environment Variables

See `.env.example` for all available environment variables. Required for optional connectors:

- `YOUTUBE_API_KEY`: YouTube Data API v3
- `TWITTER_BEARER_TOKEN`: Twitter API (paid tier)
- `REDDIT_CLIENT_ID`, `REDDIT_CLIENT_SECRET`: Reddit API (improves rate limits)

### PostgreSQL (production)

1. Set up PostgreSQL database
2. Configure in `.env`:
   ```
   POSTGRES_HOST=your-host
   POSTGRES_USER=your-user
   POSTGRES_PASSWORD=your-password
   POSTGRES_DATABASE=spotify_trends
   ```
3. Update `config/settings.yaml`:
   ```yaml
   storage:
     type: "postgres"
   ```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      DATA SOURCES                           │
├──────────┬──────────┬──────────┬──────────┬────────────────┤
│  Google  │  Reddit  │   RSS    │ Wikipedia│  Twitter/YT    │
│  Trends  │          │  Feeds   │ Pageviews│  (with API)    │
└────┬─────┴────┬─────┴────┬─────┴────┬─────┴───────┬────────┘
     │          │          │          │             │
     └──────────┴──────────┴──────────┴─────────────┘
                           │
                    ┌──────▼──────┐
                    │  COLLECTOR  │
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │   CLEANER   │  Dedupe, normalize
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │  ENRICHER   │  Entities, language
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │ CLASSIFIER  │  Topics, subtopics
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │   SCORER    │  Comms Relevance Score
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │ SUMMARISER  │  Comms-ready summaries
                    └──────┬──────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
       ┌──────▼──────┐ ┌───▼────┐ ┌─────▼─────┐
       │  DASHBOARD  │ │ DIGEST │ │  STORAGE  │
       │   (FastAPI) │ │ (MD/HTML)│ │(SQLite/PG)│
       └─────────────┘ └────────┘ └───────────┘
```

## License

Internal Spotify tool. Not for external distribution.

## Support

For issues or feature requests, contact the Spotify Africa Comms team.

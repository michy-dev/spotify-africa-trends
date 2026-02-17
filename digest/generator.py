"""
Daily digest generator.

Creates markdown and HTML summaries for email distribution.
"""

from datetime import datetime
from typing import List, Optional
from pathlib import Path
import structlog

from storage.base import TrendRecord

logger = structlog.get_logger()


class DigestGenerator:
    """
    Generates daily digest reports.

    Outputs:
    - Markdown file for easy reading
    - HTML file for email distribution
    """

    def __init__(self, config: dict):
        """
        Initialize generator.

        Args:
            config: Application configuration
        """
        self.config = config
        self.digest_config = config.get("digest", {})
        self.sections = self.digest_config.get("sections", {
            "top_trends": 10,
            "top_risks": 5,
            "top_opportunities": 5,
            "watchlist": 10,
        })

    def generate(
        self,
        trends: List[TrendRecord],
        output_dir: str = "output",
        date: datetime = None
    ) -> dict:
        """
        Generate daily digest files.

        Args:
            trends: List of TrendRecord objects
            output_dir: Directory for output files
            date: Date for the digest (defaults to today)

        Returns:
            Dict with paths to generated files
        """
        if date is None:
            date = datetime.utcnow()

        date_str = date.strftime("%Y-%m-%d")

        # Sort and categorize trends
        sorted_trends = sorted(trends, key=lambda t: t.total_score, reverse=True)

        top_trends = sorted_trends[:self.sections.get("top_trends", 10)]
        risks = [t for t in sorted_trends if t.risk_level in ("high", "medium")][:self.sections.get("top_risks", 5)]
        opportunities = [
            t for t in sorted_trends
            if t.suggested_action in ("ENGAGE", "PARTNER") and t.risk_level == "low"
        ][:self.sections.get("top_opportunities", 5)]
        watchlist = [
            t for t in sorted_trends
            if t.confidence == "low" and t.total_score >= 50
        ][:self.sections.get("watchlist", 10)]

        # Generate markdown
        markdown = self._generate_markdown(
            date_str, top_trends, risks, opportunities, watchlist, trends
        )

        # Generate HTML
        html = self._generate_html(
            date_str, top_trends, risks, opportunities, watchlist, trends
        )

        # Write files
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        md_path = output_path / f"digest_{date_str}.md"
        html_path = output_path / f"digest_{date_str}.html"

        md_path.write_text(markdown)
        html_path.write_text(html)

        logger.info(
            "digest_generated",
            date=date_str,
            markdown=str(md_path),
            html=str(html_path)
        )

        return {
            "date": date_str,
            "markdown_path": str(md_path),
            "html_path": str(html_path),
            "summary": {
                "total_trends": len(trends),
                "top_trends": len(top_trends),
                "risks": len(risks),
                "opportunities": len(opportunities),
                "watchlist": len(watchlist),
            }
        }

    def _generate_markdown(
        self,
        date_str: str,
        top_trends: List[TrendRecord],
        risks: List[TrendRecord],
        opportunities: List[TrendRecord],
        watchlist: List[TrendRecord],
        all_trends: List[TrendRecord]
    ) -> str:
        """Generate markdown digest."""
        lines = [
            f"# Spotify Africa Comms Trends Digest",
            f"**{date_str}** | Generated at {datetime.utcnow().strftime('%H:%M UTC')}",
            "",
            "---",
            "",
        ]

        # Summary stats
        high_risk = sum(1 for t in all_trends if t.risk_level == "high")
        high_priority = sum(1 for t in all_trends if t.priority_level == "high")

        lines.extend([
            "## At a Glance",
            "",
            f"| Metric | Count |",
            f"|--------|-------|",
            f"| Total Trends | {len(all_trends)} |",
            f"| High Priority | {high_priority} |",
            f"| High Risk | {high_risk} |",
            f"| Engagement Opportunities | {len(opportunities)} |",
            "",
        ])

        # Top 10 trends
        lines.extend([
            "## Top 10 Trends",
            "",
        ])

        for i, trend in enumerate(top_trends, 1):
            lines.extend([
                f"### {i}. {trend.title}",
                f"**Score: {trend.total_score:.0f}** | {trend.suggested_action} | {trend.market or 'Global'} | Risk: {trend.risk_level}",
                "",
                f"> {trend.whats_happening}",
                "",
                "**Why it matters:**",
            ])
            for bullet in trend.why_it_matters:
                lines.append(f"- {bullet}")
            lines.append("")

        # Risks
        if risks:
            lines.extend([
                "## Risks & Sensitivities",
                "*Items requiring legal/policy review*",
                "",
            ])
            for trend in risks:
                emoji = "🔴" if trend.risk_level == "high" else "🟡"
                lines.append(f"- {emoji} **{trend.title}** ({trend.market or 'Global'}) - {trend.if_goes_wrong}")
            lines.append("")

        # Opportunities
        if opportunities:
            lines.extend([
                "## Engagement Opportunities",
                "*Low-risk trends with partnership/engagement potential*",
                "",
            ])
            for trend in opportunities:
                lines.append(f"- 🟢 **{trend.title}** ({trend.market or 'Global'}) - {trend.suggested_action}")
            lines.append("")

        # Watchlist
        if watchlist:
            lines.extend([
                "## Watchlist",
                "*Lower confidence items to keep an eye on*",
                "",
            ])
            for trend in watchlist:
                lines.append(f"- 👁 {trend.title} (Score: {trend.total_score:.0f})")
            lines.append("")

        # Market breakdown
        market_counts = {}
        for t in all_trends:
            market = t.market or "Unknown"
            market_counts[market] = market_counts.get(market, 0) + 1

        lines.extend([
            "## By Market",
            "",
            "| Market | Trends |",
            "|--------|--------|",
        ])
        for market, count in sorted(market_counts.items(), key=lambda x: x[1], reverse=True):
            lines.append(f"| {market} | {count} |")
        lines.append("")

        # Footer
        lines.extend([
            "---",
            "",
            "*This digest was automatically generated by the Spotify Africa Comms Trends Dashboard.*",
            "*For full details, visit the [dashboard](http://localhost:8000).*",
        ])

        return "\n".join(lines)

    def _generate_html(
        self,
        date_str: str,
        top_trends: List[TrendRecord],
        risks: List[TrendRecord],
        opportunities: List[TrendRecord],
        watchlist: List[TrendRecord],
        all_trends: List[TrendRecord]
    ) -> str:
        """Generate HTML digest for email."""
        high_risk = sum(1 for t in all_trends if t.risk_level == "high")
        high_priority = sum(1 for t in all_trends if t.priority_level == "high")

        html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Spotify Africa Trends Digest - {date_str}</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 700px;
            margin: 0 auto;
            padding: 20px;
            background: #f5f5f5;
        }}
        .container {{
            background: white;
            border-radius: 8px;
            padding: 30px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .header {{
            background: #1DB954;
            color: white;
            padding: 20px;
            border-radius: 8px 8px 0 0;
            margin: -30px -30px 30px -30px;
        }}
        h1 {{
            margin: 0;
            font-size: 24px;
        }}
        .date {{
            opacity: 0.8;
            font-size: 14px;
        }}
        .stats {{
            display: flex;
            gap: 15px;
            margin-bottom: 30px;
            flex-wrap: wrap;
        }}
        .stat {{
            background: #f9f9f9;
            padding: 15px;
            border-radius: 8px;
            text-align: center;
            flex: 1;
            min-width: 80px;
        }}
        .stat-value {{
            font-size: 28px;
            font-weight: bold;
            color: #1DB954;
        }}
        .stat-label {{
            font-size: 12px;
            color: #666;
            text-transform: uppercase;
        }}
        .stat.risk .stat-value {{
            color: #dc2626;
        }}
        h2 {{
            border-bottom: 2px solid #1DB954;
            padding-bottom: 10px;
            margin-top: 30px;
        }}
        .trend {{
            border-left: 4px solid #ddd;
            padding: 15px;
            margin: 15px 0;
            background: #fafafa;
        }}
        .trend.high {{
            border-left-color: #dc2626;
            background: #fef2f2;
        }}
        .trend.medium {{
            border-left-color: #f59e0b;
            background: #fffbeb;
        }}
        .trend-header {{
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            margin-bottom: 10px;
        }}
        .trend-title {{
            font-weight: bold;
            font-size: 16px;
        }}
        .trend-score {{
            font-size: 20px;
            font-weight: bold;
            color: #1DB954;
        }}
        .trend-meta {{
            font-size: 12px;
            color: #666;
            margin-bottom: 10px;
        }}
        .action {{
            display: inline-block;
            padding: 2px 8px;
            border-radius: 4px;
            font-size: 11px;
            font-weight: bold;
            text-transform: uppercase;
        }}
        .action-escalate {{
            background: #dc2626;
            color: white;
        }}
        .action-engage {{
            background: #059669;
            color: white;
        }}
        .action-partner {{
            background: #7c3aed;
            color: white;
        }}
        .action-monitor {{
            background: #6b7280;
            color: white;
        }}
        .action-avoid {{
            background: #f59e0b;
            color: black;
        }}
        .bullets {{
            margin: 10px 0;
            padding-left: 20px;
        }}
        .bullets li {{
            margin: 5px 0;
        }}
        .risk-item {{
            padding: 10px;
            margin: 10px 0;
            background: #fef2f2;
            border-radius: 4px;
        }}
        .opp-item {{
            padding: 10px;
            margin: 10px 0;
            background: #f0fdf4;
            border-radius: 4px;
        }}
        .footer {{
            margin-top: 30px;
            padding-top: 20px;
            border-top: 1px solid #ddd;
            font-size: 12px;
            color: #666;
            text-align: center;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Spotify Africa Comms Trends</h1>
            <div class="date">{date_str} | Generated at {datetime.utcnow().strftime('%H:%M UTC')}</div>
        </div>

        <div class="stats">
            <div class="stat">
                <div class="stat-value">{len(all_trends)}</div>
                <div class="stat-label">Total Trends</div>
            </div>
            <div class="stat">
                <div class="stat-value">{high_priority}</div>
                <div class="stat-label">High Priority</div>
            </div>
            <div class="stat risk">
                <div class="stat-value">{high_risk}</div>
                <div class="stat-label">High Risk</div>
            </div>
            <div class="stat">
                <div class="stat-value">{len(opportunities)}</div>
                <div class="stat-label">Opportunities</div>
            </div>
        </div>

        <h2>Top 10 Trends</h2>
        {"".join(self._trend_to_html(t, i) for i, t in enumerate(top_trends, 1))}

        <h2>Risks & Sensitivities</h2>
        <p style="color: #666; font-size: 14px;">Items requiring legal/policy review</p>
        {"".join(f'<div class="risk-item">🔴 <strong>{t.title}</strong> ({t.market or "Global"}) - {t.if_goes_wrong}</div>' for t in risks) if risks else '<p style="color: #666;">No high-risk items detected today.</p>'}

        <h2>Engagement Opportunities</h2>
        <p style="color: #666; font-size: 14px;">Low-risk trends with partnership/engagement potential</p>
        {"".join(f'<div class="opp-item">🟢 <strong>{t.title}</strong> ({t.market or "Global"}) - {t.suggested_action}</div>' for t in opportunities) if opportunities else '<p style="color: #666;">No immediate opportunities identified.</p>'}

        <div class="footer">
            <p>This digest was automatically generated by the Spotify Africa Comms Trends Dashboard.</p>
            <p>For full details, visit the <a href="http://localhost:8000">dashboard</a>.</p>
        </div>
    </div>
</body>
</html>"""

        return html

    def _trend_to_html(self, trend: TrendRecord, rank: int) -> str:
        """Convert a trend to HTML."""
        risk_class = trend.risk_level if trend.risk_level in ("high", "medium") else ""
        action_class = f"action-{trend.suggested_action.lower()}"

        bullets = "".join(f"<li>{b}</li>" for b in trend.why_it_matters)

        return f"""
        <div class="trend {risk_class}">
            <div class="trend-header">
                <div class="trend-title">{rank}. {trend.title}</div>
                <div class="trend-score">{trend.total_score:.0f}</div>
            </div>
            <div class="trend-meta">
                <span class="action {action_class}">{trend.suggested_action}</span>
                {trend.market or 'Global'} | Risk: {trend.risk_level}
            </div>
            <p>{trend.whats_happening}</p>
            <p><strong>Why it matters:</strong></p>
            <ul class="bullets">{bullets}</ul>
        </div>
        """


async def generate_daily_digest(config: dict, storage) -> dict:
    """
    Generate the daily digest.

    Args:
        config: Application configuration
        storage: Storage instance

    Returns:
        Digest generation result
    """
    # Fetch all recent trends
    trends = await storage.get_trends(limit=500)

    generator = DigestGenerator(config)
    return generator.generate(trends)

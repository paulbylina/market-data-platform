from __future__ import annotations

from pathlib import Path
import json
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse


SIGNALS_JSON = Path("data/signals/daily_pullback_rvol/latest_signals.json")
CANDIDATES_JSON = Path("data/signals/daily_pullback_rvol/latest_candidates.json")

APP_TITLE = "Daily Pullback RVOL Signals"

app = FastAPI(title=APP_TITLE)


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise HTTPException(
            status_code=404,
            detail=(
                f"Missing file: {path}. "
                "Run scripts/rs/signals/generate_daily_pullback_rvol_signals.py first."
            ),
        )

    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def get_latest_payload() -> dict[str, Any]:
    return read_json(SIGNALS_JSON)


def get_candidates_payload() -> dict[str, Any]:
    return read_json(CANDIDATES_JSON)


def format_number(value: Any, digits: int = 2) -> str:
    if value is None:
        return ""

    try:
        return f"{float(value):,.{digits}f}"
    except (TypeError, ValueError):
        return str(value)


def render_signal_table(signals: list[dict[str, Any]], max_rows: int | None = None) -> str:
    rows = signals[:max_rows] if max_rows is not None else signals

    if not rows:
        return "<p>No current signals.</p>"

    table_rows = []

    for row in rows:
        signal_rank = row.get("signal_rank", "")
        ticker = row.get("ticker", "")
        sector = row.get("sector", "")
        close = format_number(row.get("close"))
        pullback = format_number(row.get("close_zscore_50d"))
        threshold = format_number(row.get("stock_pullback_threshold"))
        volume_ratio = format_number(row.get("volume_ratio_20d"))
        volume = format_number(row.get("volume"), digits=0)

        table_rows.append(
            f"""
            <tr>
                <td>{signal_rank}</td>
                <td><strong>{ticker}</strong></td>
                <td>{sector}</td>
                <td>{close}</td>
                <td>{pullback}</td>
                <td>{threshold}</td>
                <td>{volume_ratio}x</td>
                <td>{volume}</td>
            </tr>
            """
        )

    return f"""
    <table>
        <thead>
            <tr>
                <th>Rank</th>
                <th>Ticker</th>
                <th>Sector</th>
                <th>Close</th>
                <th>Pullback score</th>
                <th>Signal threshold</th>
                <th>Volume vs normal</th>
                <th>Volume</th>
            </tr>
        </thead>
        <tbody>
            {''.join(table_rows)}
        </tbody>
    </table>
    """


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/signals/latest")
def latest_signals() -> dict[str, Any]:
    return get_latest_payload()


@app.get("/signals/top")
def top_signals(limit: int = 3) -> dict[str, Any]:
    payload = get_latest_payload()
    signals = payload.get("signals", [])

    return {
        "strategy_name": payload.get("strategy_name"),
        "generated_at_utc": payload.get("generated_at_utc"),
        "as_of_date": payload.get("as_of_date"),
        "limit": limit,
        "signals": signals[:limit],
    }


@app.get("/signals/candidates")
def candidates() -> dict[str, Any]:
    return get_candidates_payload()


@app.get("/", response_class=HTMLResponse)
def dashboard() -> str:
    latest = get_latest_payload()
    candidates_payload = get_candidates_payload()

    signals = latest.get("signals", [])
    all_candidates = candidates_payload.get("all_candidates", [])

    generated_at = latest.get("generated_at_utc", "")
    as_of_date = latest.get("as_of_date", "")
    signal_count = latest.get("signal_count", 0)
    total_symbols_checked = latest.get("total_symbols_checked", 0)

    top_three = signals[:3]

    return f"""
    <!doctype html>
    <html>
    <head>
        <title>{APP_TITLE}</title>
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <style>
            body {{
                font-family: Arial, sans-serif;
                margin: 32px;
                background: #f7f7f7;
                color: #222;
            }}

            .container {{
                max-width: 1200px;
                margin: 0 auto;
            }}

            .card {{
                background: white;
                border-radius: 12px;
                padding: 24px;
                margin-bottom: 24px;
                box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
            }}

            h1, h2 {{
                margin-top: 0;
            }}

            .muted {{
                color: #666;
            }}

            .metric-grid {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
                gap: 16px;
            }}

            .metric {{
                background: #f1f3f5;
                border-radius: 10px;
                padding: 16px;
            }}

            .metric-label {{
                font-size: 13px;
                color: #666;
            }}

            .metric-value {{
                font-size: 26px;
                font-weight: bold;
                margin-top: 6px;
            }}

            table {{
                width: 100%;
                border-collapse: collapse;
                font-size: 14px;
            }}

            th, td {{
                border-bottom: 1px solid #ddd;
                padding: 10px;
                text-align: left;
            }}

            th {{
                background: #f1f3f5;
            }}

            .note {{
                line-height: 1.5;
            }}

            .small {{
                font-size: 13px;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="card">
                <h1>{APP_TITLE}</h1>
                <p class="muted">
                    Last signal date: <strong>{as_of_date}</strong><br>
                    Generated at UTC: {generated_at}
                </p>
                <p class="note">
                    This scanner looks for large stocks that pulled back more than usual,
                    while the market is still healthy, and trading volume is higher than normal.
                    The tested strategy takes at most the top 3 signals.
                </p>
            </div>

            <div class="card">
                <h2>Current scan summary</h2>
                <div class="metric-grid">
                    <div class="metric">
                        <div class="metric-label">Stocks checked</div>
                        <div class="metric-value">{total_symbols_checked}</div>
                    </div>
                    <div class="metric">
                        <div class="metric-label">Signals found</div>
                        <div class="metric-value">{signal_count}</div>
                    </div>
                    <div class="metric">
                        <div class="metric-label">Model max positions</div>
                        <div class="metric-value">3</div>
                    </div>
                    <div class="metric">
                        <div class="metric-label">Hold period</div>
                        <div class="metric-value">5 days</div>
                    </div>
                </div>
            </div>

            <div class="card">
                <h2>Top 3 model picks</h2>
                {render_signal_table(top_three)}
            </div>

            <div class="card">
                <h2>All current signals</h2>
                {render_signal_table(signals)}
            </div>

            <div class="card">
                <h2>Rules in plain English</h2>
                <ul class="note">
                    <li>The stock dropped more than usual compared with its own history.</li>
                    <li>The overall market is still healthy enough.</li>
                    <li>Today’s volume is at least 1.2 times normal volume.</li>
                    <li>If more than 3 stocks qualify, choose the most oversold first.</li>
                </ul>
                <p class="small muted">
                    API endpoints:
                    <code>/health</code>,
                    <code>/signals/top</code>,
                    <code>/signals/latest</code>,
                    <code>/signals/candidates</code>
                </p>
            </div>
        </div>
    </body>
    </html>
    """


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "src.apps.daily_signal_api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )

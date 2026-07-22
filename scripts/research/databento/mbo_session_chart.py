from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.patches import Rectangle


DEFAULT_DATASET = "GLBX.MDP3"
INPUT_NAME = "mbo_quote_1m_session"


def slug(value: str) -> str:
    return (
        value.lower()
        .replace(".v.", "_v")
        .replace(".", "_")
        .replace("/", "_")
        .replace("-", "_")
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create static PNG and interactive HTML quote-candle charts from sessionized MBO 1-minute bars."
    )

    parser.add_argument("--symbol", required=True, help="Databento symbol, e.g. ES.v.0")
    parser.add_argument("--session-date", required=True, help="Session end date, e.g. 2026-07-02")
    parser.add_argument("--dataset", default=DEFAULT_DATASET)

    parser.add_argument(
        "--input-root",
        default="data/processed/databento/mbo_quote_bars_1m_sessions",
        help="Input sessionized MBO quote bar root.",
    )
    parser.add_argument(
        "--output-root",
        default="data/research/databento_mbo_charts",
        help="Chart output root.",
    )

    parser.add_argument(
        "--price-source",
        choices=["mid", "micro"],
        default="mid",
        help="Use mid-price or microprice candles. Default: mid.",
    )

    return parser.parse_args()


def load_session_bars(path: Path) -> pd.DataFrame:
    con = duckdb.connect()

    df = con.execute(
        f"""
        SELECT
            minute,
            session_date,
            session_start,
            session_end,

            mid_open,
            mid_high,
            mid_low,
            mid_close,

            micro_open,
            micro_high,
            micro_low,
            micro_close,

            spread_ticks_avg,
            spread_ticks_median,
            spread_ticks_max,

            imbalance_avg,
            imbalance_p10,
            imbalance_median,
            imbalance_p90,

            bbo_update_count,
            bid_sz_avg,
            ask_sz_avg
        FROM read_parquet('{path}')
        ORDER BY minute
        """
    ).fetchdf()

    if df.empty:
        raise ValueError(f"No rows found in {path}")

    df["minute"] = pd.to_datetime(df["minute"])
    df["minute_label"] = df["minute"].dt.strftime("%Y-%m-%d %H:%M")

    return df


def save_static_png(df: pd.DataFrame, out_path: Path, symbol: str, session_date: str, price_source: str) -> None:
    open_col = f"{price_source}_open"
    high_col = f"{price_source}_high"
    low_col = f"{price_source}_low"
    close_col = f"{price_source}_close"

    x = np.arange(len(df))

    fig, axes = plt.subplots(
        4,
        1,
        figsize=(16, 10),
        sharex=True,
        gridspec_kw={"height_ratios": [4, 1.2, 1.2, 1.2]},
    )

    candle_ax = axes[0]
    spread_ax = axes[1]
    imbalance_ax = axes[2]
    activity_ax = axes[3]

    width = 0.65

    for i, row in enumerate(df.itertuples(index=False)):
        o = getattr(row, open_col)
        h = getattr(row, high_col)
        l = getattr(row, low_col)
        c = getattr(row, close_col)

        if pd.isna(o) or pd.isna(h) or pd.isna(l) or pd.isna(c):
            continue

        up = c >= o
        color = "#2ca02c" if up else "#d62728"

        candle_ax.vlines(i, l, h, color=color, linewidth=0.8)

        body_low = min(o, c)
        body_height = abs(c - o)

        if body_height == 0:
            candle_ax.hlines(c, i - width / 2, i + width / 2, color=color, linewidth=1.0)
        else:
            candle_ax.add_patch(
                Rectangle(
                    (i - width / 2, body_low),
                    width,
                    body_height,
                    facecolor=color,
                    edgecolor=color,
                    alpha=0.8,
                )
            )

    candle_ax.set_title(f"{symbol} {session_date} MBO quote candles ({price_source})")
    candle_ax.set_ylabel(f"{price_source.title()} price")
    candle_ax.grid(True, alpha=0.25)

    spread_ax.plot(x, df["spread_ticks_avg"], linewidth=1.0, label="Avg spread")
    spread_ax.plot(x, df["spread_ticks_max"], linewidth=0.8, alpha=0.7, label="Max spread")
    spread_ax.set_ylabel("Spread ticks")
    spread_ax.legend(loc="upper right")
    spread_ax.grid(True, alpha=0.25)

    imbalance_ax.plot(x, df["imbalance_avg"], linewidth=1.0)
    imbalance_ax.axhline(0.5, linewidth=0.8, linestyle="--", alpha=0.6)
    imbalance_ax.set_ylabel("Imbalance")
    imbalance_ax.set_ylim(0, 1)
    imbalance_ax.grid(True, alpha=0.25)

    activity_ax.bar(x, df["bbo_update_count"], width=1.0)
    activity_ax.set_ylabel("BBO updates")
    activity_ax.grid(True, alpha=0.25)

    tick_count = min(12, len(df))
    tick_positions = np.linspace(0, len(df) - 1, tick_count, dtype=int)
    activity_ax.set_xticks(tick_positions)
    activity_ax.set_xticklabels(df.iloc[tick_positions]["minute"].dt.strftime("%m-%d %H:%M"), rotation=30, ha="right")

    plt.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def save_interactive_html(df: pd.DataFrame, out_path: Path, symbol: str, session_date: str, price_source: str) -> None:
    open_col = f"{price_source}_open"
    high_col = f"{price_source}_high"
    low_col = f"{price_source}_low"
    close_col = f"{price_source}_close"

    x_values = df["minute_label"].tolist()

    chart_data = {
        "x": x_values,
        "open": df[open_col].round(4).tolist(),
        "high": df[high_col].round(4).tolist(),
        "low": df[low_col].round(4).tolist(),
        "close": df[close_col].round(4).tolist(),
        "spread_avg": df["spread_ticks_avg"].round(4).tolist(),
        "spread_max": df["spread_ticks_max"].round(4).tolist(),
        "imbalance_avg": df["imbalance_avg"].round(4).tolist(),
        "bbo_update_count": df["bbo_update_count"].astype(int).tolist(),
    }

    html = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>{symbol} {session_date} MBO quote candles</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    body {{
      font-family: Arial, sans-serif;
      margin: 24px;
      background: #ffffff;
      color: #111111;
    }}
    #chart {{
      width: 100%;
      height: 950px;
    }}
    .note {{
      margin-top: 12px;
      font-size: 13px;
      color: #444444;
    }}
  </style>
</head>
<body>
  <h2>{symbol} {session_date} MBO quote candles ({price_source})</h2>
  <div id="chart"></div>
  <div class="note">
    Quote candles are built from reconstructed BBO, not trade prints.
    Lower panels show spread, imbalance, and BBO update activity.
  </div>

<script>
const d = {json.dumps(chart_data)};

const traces = [
  {{
    type: "candlestick",
    name: "{price_source} candles",
    x: d.x,
    open: d.open,
    high: d.high,
    low: d.low,
    close: d.close,
    xaxis: "x",
    yaxis: "y",
    increasing: {{line: {{color: "#2ca02c"}}}},
    decreasing: {{line: {{color: "#d62728"}}}}
  }},
  {{
    type: "scatter",
    mode: "lines",
    name: "Avg spread",
    x: d.x,
    y: d.spread_avg,
    xaxis: "x2",
    yaxis: "y2"
  }},
  {{
    type: "scatter",
    mode: "lines",
    name: "Max spread",
    x: d.x,
    y: d.spread_max,
    xaxis: "x2",
    yaxis: "y2"
  }},
  {{
    type: "scatter",
    mode: "lines",
    name: "Imbalance avg",
    x: d.x,
    y: d.imbalance_avg,
    xaxis: "x3",
    yaxis: "y3"
  }},
  {{
    type: "bar",
    name: "BBO updates",
    x: d.x,
    y: d.bbo_update_count,
    xaxis: "x4",
    yaxis: "y4"
  }}
];

const layout = {{
  height: 950,
  margin: {{l: 70, r: 30, t: 30, b: 50}},
  showlegend: true,

  xaxis: {{
    domain: [0, 1],
    anchor: "y",
    rangeslider: {{visible: false}},
    showticklabels: false
  }},
  yaxis: {{
    domain: [0.55, 1.00],
    title: "{price_source.title()} price"
  }},

  xaxis2: {{
    domain: [0, 1],
    anchor: "y2",
    matches: "x",
    showticklabels: false
  }},
  yaxis2: {{
    domain: [0.38, 0.52],
    title: "Spread ticks"
  }},

  xaxis3: {{
    domain: [0, 1],
    anchor: "y3",
    matches: "x",
    showticklabels: false
  }},
  yaxis3: {{
    domain: [0.21, 0.35],
    title: "Imbalance",
    range: [0, 1]
  }},

  xaxis4: {{
    domain: [0, 1],
    anchor: "y4",
    matches: "x",
    title: "Minute"
  }},
  yaxis4: {{
    domain: [0.00, 0.18],
    title: "BBO updates"
  }},

  hovermode: "x unified"
}};

Plotly.newPlot("chart", traces, layout, {{responsive: true}});
</script>
</body>
</html>
"""

    out_path.write_text(html, encoding="utf-8")


def main() -> None:
    args = parse_args()

    dataset_slug = slug(args.dataset)
    symbol_slug = slug(args.symbol)

    src_path = (
        Path(args.input_root)
        / dataset_slug
        / symbol_slug
        / f"session_date={args.session_date}"
        / f"{symbol_slug}_{args.session_date}_{INPUT_NAME}.parquet"
    )

    out_dir = (
        Path(args.output_root)
        / dataset_slug
        / symbol_slug
        / f"session_date={args.session_date}"
    )
    out_dir.mkdir(parents=True, exist_ok=True)

    if not src_path.exists():
        raise FileNotFoundError(src_path)

    png_path = out_dir / f"{symbol_slug}_{args.session_date}_{args.price_source}_quote_candles.png"
    html_path = out_dir / f"{symbol_slug}_{args.session_date}_{args.price_source}_quote_candles.html"

    print("Creating MBO quote candle charts:")
    print(f"  dataset:      {args.dataset}")
    print(f"  symbol:       {args.symbol}")
    print(f"  session_date: {args.session_date}")
    print(f"  price_source: {args.price_source}")
    print(f"  input:        {src_path}")
    print(f"  png:          {png_path}")
    print(f"  html:         {html_path}")

    df = load_session_bars(src_path)

    save_static_png(df, png_path, args.symbol, args.session_date, args.price_source)
    save_interactive_html(df, html_path, args.symbol, args.session_date, args.price_source)

    print()
    print("Done.")
    print(f"Bars plotted: {len(df):,}")
    print(f"Saved PNG:    {png_path}")
    print(f"Saved HTML:   {html_path}")


if __name__ == "__main__":
    main()

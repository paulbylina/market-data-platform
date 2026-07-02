from pathlib import Path

import numpy as np
import pandas as pd


INPUT_PATH = Path(
    "data/research/full_market_scanner_10y/from_scratch_volume_research/from_scratch_base_rows_with_forward_metrics.csv"
)

OUTPUT_DIR = Path(
    "data/research/full_market_scanner_10y/extended_hours_event_tasks"
)


def add_label(row):
    price = row["prev_close"]
    vr = row["volume_rvol_20d"]
    dvr = row["dollar_volume_rvol_20d"]

    labels = []

    if price < 5 and vr >= 50:
        labels.append("cheap_extreme_volume_rvol")

    if price < 5 and dvr >= 50:
        labels.append("cheap_extreme_dollar_volume_rvol")

    if price < 5 and 5 <= vr < 50:
        labels.append("cheap_moderate_volume_rvol")

    if price >= 10 and 5 <= vr < 50:
        labels.append("normal_price_moderate_volume_rvol")

    if price >= 10 and 5 <= dvr < 50:
        labels.append("normal_price_moderate_dollar_volume_rvol")

    if price >= 10 and vr >= 50:
        labels.append("normal_price_extreme_volume_rvol")

    if price >= 10 and dvr >= 50:
        labels.append("normal_price_extreme_dollar_volume_rvol")

    return "|".join(labels)


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    usecols = [
        "ticker",
        "trade_date",
        "prev_close",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "dollar_volume",
        "gap_pct",
        "volume_rvol_20d",
        "dollar_volume_rvol_20d",
        "open_to_close_pct",
        "fwd_1d_close_pct",
        "fwd_5d_close_pct",
        "next_day_open_to_close_pct",
        "runup_5d_from_close_pct",
        "drawdown_5d_from_close_pct",
    ]

    df = pd.read_csv(INPUT_PATH, usecols=lambda c: c in usecols)
    df["trade_date"] = pd.to_datetime(df["trade_date"])

    numeric_cols = [c for c in usecols if c not in ["ticker", "trade_date"]]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.sort_values(["ticker", "trade_date"]).copy()

    # Previous trading day by ticker. Needed for previous after-hours window.
    df["prev_trade_date"] = df.groupby("ticker")["trade_date"].shift(1)

    base = df[
        (df["prev_close"] > 0)
        & (df["volume_rvol_20d"].notna())
        & (df["dollar_volume_rvol_20d"].notna())
        & np.isfinite(df["volume_rvol_20d"])
        & np.isfinite(df["dollar_volume_rvol_20d"])
        & df["prev_trade_date"].notna()
    ].copy()

    base["event_labels"] = base.apply(add_label, axis=1)

    events = base[base["event_labels"] != ""].copy()

    # One row per label so later summaries/downloads are easy.
    exploded = events.assign(event_label=events["event_labels"].str.split("|")).explode("event_label")
    exploded["trade_date"] = exploded["trade_date"].dt.date.astype(str)
    exploded["prev_trade_date"] = pd.to_datetime(exploded["prev_trade_date"]).dt.date.astype(str)

    # Keep useful ordering: most extreme first.
    exploded = exploded.sort_values(
        ["event_label", "dollar_volume_rvol_20d", "volume_rvol_20d"],
        ascending=[True, False, False],
    )

    summary = (
        exploded.groupby("event_label")
        .agg(
            rows=("ticker", "size"),
            tickers=("ticker", "nunique"),
            median_prev_close=("prev_close", "median"),
            median_dollar_volume=("dollar_volume", "median"),
            median_volume_rvol=("volume_rvol_20d", "median"),
            median_dollar_volume_rvol=("dollar_volume_rvol_20d", "median"),
            median_gap_pct=("gap_pct", "median"),
            same_day_median=("open_to_close_pct", "median"),
            fwd_1d_median=("fwd_1d_close_pct", "median"),
            fwd_1d_win_rate=("fwd_1d_close_pct", lambda s: (s > 0).mean() * 100),
            fwd_5d_median=("fwd_5d_close_pct", "median"),
            drawdown_5d_median=("drawdown_5d_from_close_pct", "median"),
        )
        .reset_index()
        .sort_values("rows", ascending=False)
    )

    tasks_path = OUTPUT_DIR / "extended_hours_1m_event_tasks.csv"
    summary_path = OUTPUT_DIR / "extended_hours_event_summary.csv"

    exploded.to_csv(tasks_path, index=False)
    summary.to_csv(summary_path, index=False)

    print("saved tasks:", tasks_path)
    print("saved summary:", summary_path)

    print()
    print("=== Extended-Hours Event Summary ===")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()

from pathlib import Path

import numpy as np
import pandas as pd


INPUT_PATH = Path(
    "data/research/full_market_scanner_10y/from_scratch_volume_research/from_scratch_base_rows_with_forward_metrics.csv"
)

OUTPUT_DIR = Path(
    "data/research/full_market_scanner_10y/daily_regime_event_tasks"
)


def price_regime(price):
    if price < 5:
        return "cheap_under_5"
    if price < 50:
        return "mid_5_to_50"
    return "high_50_plus"


def volume_regime_from_pct(p):
    if p < 10:
        return "very_quiet_p0_p10"
    if p < 25:
        return "quiet_p10_p25"
    if p < 75:
        return "normal_p25_p75"
    if p < 95:
        return "elevated_p75_p95"
    if p < 99:
        return "extreme_p95_p99"
    if p < 99.9:
        return "mania_p99_p99_9"
    return "super_mania_p99_9_p100"


def summarize(group):
    return pd.Series(
        {
            "rows": len(group),
            "tickers": group["ticker"].nunique(),
            "median_prev_close": group["prev_close"].median(),
            "median_dollar_volume": group["dollar_volume"].median(),
            "median_volume_rvol": group["volume_rvol_20d"].median(),
            "median_dollar_volume_rvol": group["dollar_volume_rvol_20d"].median(),
            "same_day_median": group["open_to_close_pct"].median(),
            "same_day_green_rate": (group["open_to_close_pct"] > 0).mean() * 100,
            "fwd_1d_median": group["fwd_1d_close_pct"].median(),
            "fwd_1d_win_rate": (group["fwd_1d_close_pct"] > 0).mean() * 100,
            "next_day_oc_median": group["next_day_open_to_close_pct"].median(),
            "next_day_green_rate": (group["next_day_open_to_close_pct"] > 0).mean() * 100,
            "fwd_5d_median": group["fwd_5d_close_pct"].median(),
            "runup_5d_median": group["runup_5d_from_close_pct"].median(),
            "drawdown_5d_median": group["drawdown_5d_from_close_pct"].median(),
        }
    )


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
        "dollar_volume",
        "volume_rvol_20d",
        "dollar_volume_rvol_20d",
        "gap_pct",
        "open_to_close_pct",
        "fwd_1d_close_pct",
        "next_day_open_to_close_pct",
        "fwd_5d_close_pct",
        "runup_5d_from_close_pct",
        "drawdown_5d_from_close_pct",
    ]

    df = pd.read_csv(INPUT_PATH, usecols=usecols)
    df["trade_date"] = pd.to_datetime(df["trade_date"])

    for col in usecols:
        if col not in ["ticker", "trade_date"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df[
        (df["prev_close"] > 0)
        & df["volume_rvol_20d"].notna()
        & df["dollar_volume_rvol_20d"].notna()
        & np.isfinite(df["volume_rvol_20d"])
        & np.isfinite(df["dollar_volume_rvol_20d"])
    ].copy()

    df["price_regime"] = df["prev_close"].apply(price_regime)

    # Percentile ranks are local to each price regime.
    df["volume_rvol_pct_in_price_regime"] = (
        df.groupby("price_regime")["volume_rvol_20d"]
        .rank(pct=True, method="first")
        * 100
    )

    df["dollar_volume_rvol_pct_in_price_regime"] = (
        df.groupby("price_regime")["dollar_volume_rvol_20d"]
        .rank(pct=True, method="first")
        * 100
    )

    df["volume_regime"] = df["volume_rvol_pct_in_price_regime"].apply(volume_regime_from_pct)
    df["dollar_volume_regime"] = df["dollar_volume_rvol_pct_in_price_regime"].apply(volume_regime_from_pct)

    # Summary by price + share-volume regime.
    volume_summary = (
        df.groupby(["price_regime", "volume_regime"], observed=True)
        .apply(summarize, include_groups=False)
        .reset_index()
    )

    # Summary by price + dollar-volume regime.
    dollar_volume_summary = (
        df.groupby(["price_regime", "dollar_volume_regime"], observed=True)
        .apply(summarize, include_groups=False)
        .reset_index()
    )

    # For extended-hours download, create a focused task list.
    # We do NOT download all normal rows. We keep all extreme/mania rows and sample the rest.
    task_groups = []

    for (price, regime), sub in df.groupby(["price_regime", "dollar_volume_regime"], observed=True):
        if "super_mania" in regime or "mania" in regime or "extreme" in regime:
            sample = sub.copy()
        else:
            sample = sub.sample(
                n=min(len(sub), 500),
                random_state=42,
            )

        task_groups.append(sample)

    tasks = pd.concat(task_groups, ignore_index=True)

    # Need previous trading date for prior after-hours window.
    tasks = tasks.sort_values(["ticker", "trade_date"]).copy()
    all_dates = df[["ticker", "trade_date"]].sort_values(["ticker", "trade_date"]).copy()
    all_dates["prev_trade_date"] = all_dates.groupby("ticker")["trade_date"].shift(1)

    tasks = tasks.merge(
        all_dates,
        on=["ticker", "trade_date"],
        how="left",
    )

    tasks = tasks[tasks["prev_trade_date"].notna()].copy()

    tasks["trade_date"] = tasks["trade_date"].dt.date.astype(str)
    tasks["prev_trade_date"] = pd.to_datetime(tasks["prev_trade_date"]).dt.date.astype(str)

    keep_cols = [
        "ticker",
        "trade_date",
        "prev_trade_date",
        "price_regime",
        "volume_regime",
        "dollar_volume_regime",
        "prev_close",
        "open",
        "high",
        "low",
        "close",
        "gap_pct",
        "dollar_volume",
        "volume_rvol_20d",
        "dollar_volume_rvol_20d",
        "volume_rvol_pct_in_price_regime",
        "dollar_volume_rvol_pct_in_price_regime",
        "open_to_close_pct",
        "fwd_1d_close_pct",
        "next_day_open_to_close_pct",
        "fwd_5d_close_pct",
        "runup_5d_from_close_pct",
        "drawdown_5d_from_close_pct",
    ]

    volume_summary_path = OUTPUT_DIR / "price_x_volume_regime_summary.csv"
    dollar_summary_path = OUTPUT_DIR / "price_x_dollar_volume_regime_summary.csv"
    tasks_path = OUTPUT_DIR / "extended_hours_1m_regime_tasks.csv"

    volume_summary.to_csv(volume_summary_path, index=False)
    dollar_volume_summary.to_csv(dollar_summary_path, index=False)
    tasks[keep_cols].to_csv(tasks_path, index=False)

    print("saved:", volume_summary_path)
    print("saved:", dollar_summary_path)
    print("saved:", tasks_path)

    print()
    print("=== Price x Dollar-Volume Regime Summary ===")
    print(dollar_volume_summary.to_string(index=False))

    print()
    print("=== Extended-Hours Task Counts ===")
    counts = (
        tasks.groupby(["price_regime", "dollar_volume_regime"], observed=True)
        .size()
        .reset_index(name="rows")
        .sort_values(["price_regime", "dollar_volume_regime"])
    )
    print(counts.to_string(index=False))


if __name__ == "__main__":
    main()

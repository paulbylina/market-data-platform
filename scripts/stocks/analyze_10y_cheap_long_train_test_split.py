from pathlib import Path

import numpy as np
import pandas as pd


TRADES_PATH = Path(
    "data/research/full_market_scanner_10y/cheap_open_activation_features/cheap_long_target_stop_grid_trades.csv"
)

OUTPUT_DIR = Path(
    "data/research/full_market_scanner_10y/cheap_open_activation_features"
)


CONFIGS_TO_CHECK = [
    (10, 8),
    (10, 10),
    (12, 8),
    (15, 8),
    (15, 10),
    (18, 10),
    (20, 10),
    (25, 10),
    (25, 12),
]


def period_label(d):
    y = d.year

    if y <= 2020:
        return "train_2016_2020"
    if y <= 2022:
        return "validation_2021_2022"
    if y <= 2024:
        return "test_2023_2024"
    return "test_2025_2026"


def summarize(df):
    winners = df[df["net_return_pct"] > 0]
    losers = df[df["net_return_pct"] <= 0]

    avg_win = winners["net_return_pct"].mean() if len(winners) else np.nan
    avg_loss = losers["net_return_pct"].mean() if len(losers) else np.nan

    return {
        "trades": len(df),
        "tickers": df["ticker"].nunique(),
        "median_net_return_pct": df["net_return_pct"].median(),
        "avg_net_return_pct": df["net_return_pct"].mean(),
        "net_win_rate": (df["net_return_pct"] > 0).mean() * 100,
        "avg_win_pct": avg_win,
        "avg_loss_pct": avg_loss,
        "target_rate": (df["exit_reason"] == "target").mean() * 100,
        "stop_rate": (df["exit_reason"] == "stop").mean() * 100,
        "time_exit_rate": (df["exit_reason"].astype(str).str.startswith("time")).mean() * 100,
        "median_minutes_held": df["minutes_held"].median(),
        "worst_net_return_pct": df["net_return_pct"].min(),
        "best_net_return_pct": df["net_return_pct"].max(),
    }


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(TRADES_PATH)

    numeric_cols = [
        "target_pct",
        "stop_pct",
        "cost_bps",
        "net_return_pct",
        "gross_return_pct",
        "minutes_held",
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    df = df.dropna(subset=["trade_date"]).copy()

    df = df[df["cost_bps"] == 100].copy()

    config_df = pd.DataFrame(CONFIGS_TO_CHECK, columns=["target_pct", "stop_pct"])
    df = df.merge(config_df, on=["target_pct", "stop_pct"], how="inner")

    df["period"] = df["trade_date"].apply(period_label)
    df["year"] = df["trade_date"].dt.year

    rows = []

    for keys, sub in df.groupby(["target_pct", "stop_pct", "period"], observed=True):
        target_pct, stop_pct, period = keys
        row = {
            "target_pct": target_pct,
            "stop_pct": stop_pct,
            "period": period,
        }
        row.update(summarize(sub))
        rows.append(row)

    period_summary = pd.DataFrame(rows).sort_values(
        ["target_pct", "stop_pct", "period"]
    )

    year_rows = []

    for keys, sub in df.groupby(["target_pct", "stop_pct", "year"], observed=True):
        target_pct, stop_pct, year = keys
        row = {
            "target_pct": target_pct,
            "stop_pct": stop_pct,
            "year": int(year),
        }
        row.update(summarize(sub))
        year_rows.append(row)

    year_summary = pd.DataFrame(year_rows).sort_values(
        ["target_pct", "stop_pct", "year"]
    )

    # Stability score: prefer configs that stay positive across periods.
    score_rows = []

    for keys, sub in period_summary.groupby(["target_pct", "stop_pct"], observed=True):
        target_pct, stop_pct = keys

        score_rows.append({
            "target_pct": target_pct,
            "stop_pct": stop_pct,
            "periods": len(sub),
            "min_period_median": sub["median_net_return_pct"].min(),
            "min_period_avg": sub["avg_net_return_pct"].min(),
            "avg_of_period_avgs": sub["avg_net_return_pct"].mean(),
            "avg_of_period_medians": sub["median_net_return_pct"].mean(),
            "min_period_win_rate": sub["net_win_rate"].min(),
            "total_trades": sub["trades"].sum(),
        })

    stability = pd.DataFrame(score_rows).sort_values(
        ["min_period_avg", "avg_of_period_avgs", "min_period_median"],
        ascending=[False, False, False],
    )

    period_path = OUTPUT_DIR / "cheap_long_train_test_period_summary.csv"
    year_path = OUTPUT_DIR / "cheap_long_train_test_year_summary.csv"
    stability_path = OUTPUT_DIR / "cheap_long_train_test_stability_summary.csv"

    period_summary.to_csv(period_path, index=False)
    year_summary.to_csv(year_path, index=False)
    stability.to_csv(stability_path, index=False)

    print("saved period summary:", period_path)
    print("saved year summary:", year_path)
    print("saved stability summary:", stability_path)

    print()
    print("=== Stability Summary | 100 bps ===")
    print(stability.to_string(index=False))

    print()
    print("=== Period Summary | 100 bps ===")
    display_cols = [
        "target_pct",
        "stop_pct",
        "period",
        "trades",
        "tickers",
        "median_net_return_pct",
        "avg_net_return_pct",
        "net_win_rate",
        "target_rate",
        "stop_rate",
        "time_exit_rate",
        "worst_net_return_pct",
        "best_net_return_pct",
    ]
    print(period_summary[display_cols].to_string(index=False))

    print()
    print("=== Year Summary | 100 bps ===")
    year_display_cols = [
        "target_pct",
        "stop_pct",
        "year",
        "trades",
        "median_net_return_pct",
        "avg_net_return_pct",
        "net_win_rate",
        "target_rate",
        "stop_rate",
        "worst_net_return_pct",
        "best_net_return_pct",
    ]
    print(year_summary[year_display_cols].to_string(index=False))


if __name__ == "__main__":
    main()

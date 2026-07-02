from pathlib import Path

import numpy as np
import pandas as pd


TRADES_PATH = Path(
    "data/research/full_market_scanner_10y/cheap_open_activation_features/cheap_long_target_stop_grid_trades.csv"
)

FULL_PANEL_PATH = Path(
    "data/research/full_market_scanner_10y/historical_full_market_daily_panel.csv"
)

OUTPUT_DIR = Path(
    "data/research/full_market_scanner_10y/cheap_open_activation_features"
)


CONFIGS_TO_CHECK = [
    (10, 8),
    (15, 8),
    (25, 12),
]

COST_BPS = 100


def summarize_returns(df):
    return {
        "trades": len(df),
        "tickers": df["ticker"].nunique(),
        "median_net_return_pct": df["net_return_pct"].median(),
        "avg_net_return_pct": df["net_return_pct"].mean(),
        "net_win_rate": (df["net_return_pct"] > 0).mean() * 100,
        "target_rate": (df["exit_reason"] == "target").mean() * 100,
        "stop_rate": (df["exit_reason"] == "stop").mean() * 100,
        "time_exit_rate": (df["exit_reason"].astype(str).str.startswith("time")).mean() * 100,
        "worst_net_return_pct": df["net_return_pct"].min(),
        "best_net_return_pct": df["net_return_pct"].max(),
    }


def get_market_dates(min_date, max_date):
    if FULL_PANEL_PATH.exists():
        dates = pd.read_csv(FULL_PANEL_PATH, usecols=["trade_date"])
        dates["trade_date"] = pd.to_datetime(dates["trade_date"], errors="coerce")
        dates = dates.dropna(subset=["trade_date"])
        market_dates = (
            dates.loc[
                (dates["trade_date"] >= min_date)
                & (dates["trade_date"] <= max_date),
                "trade_date",
            ]
            .dt.normalize()
            .drop_duplicates()
            .sort_values()
        )
        return pd.Index(market_dates)

    # fallback if panel path is missing
    return pd.Index(pd.bdate_range(min_date, max_date))


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
        "prev_close",
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.normalize()
    df = df.dropna(subset=["trade_date"]).copy()

    df = df[df["cost_bps"] == COST_BPS].copy()

    config_df = pd.DataFrame(CONFIGS_TO_CHECK, columns=["target_pct", "stop_pct"])
    df = df.merge(config_df, on=["target_pct", "stop_pct"], how="inner")

    min_date = df["trade_date"].min()
    max_date = df["trade_date"].max()
    market_dates = get_market_dates(min_date, max_date)
    total_market_days = len(market_dates)

    frequency_rows = []
    month_rows = []
    year_rows = []
    ticker_rows = []
    daily_rows = []

    for keys, sub in df.groupby(["target_pct", "stop_pct"], observed=True):
        target_pct, stop_pct = keys
        config_name = f"target_{int(target_pct)}_stop_{int(stop_pct)}"

        daily_counts = (
            sub.groupby("trade_date")
            .size()
            .reindex(market_dates, fill_value=0)
            .rename("trades")
            .reset_index()
            .rename(columns={"index": "trade_date"})
        )

        active_days = int((daily_counts["trades"] > 0).sum())
        total_trades = len(sub)

        frequency_rows.append({
            "target_pct": target_pct,
            "stop_pct": stop_pct,
            "config": config_name,
            "date_start": min_date.date(),
            "date_end": max_date.date(),
            "market_days": total_market_days,
            "active_signal_days": active_days,
            "total_trades": total_trades,
            "avg_trades_per_market_day": total_trades / total_market_days,
            "avg_trades_per_active_day": total_trades / active_days if active_days else np.nan,
            "signal_day_rate_pct": active_days / total_market_days * 100,
            "median_trades_per_market_day": daily_counts["trades"].median(),
            "p75_trades_per_market_day": daily_counts["trades"].quantile(0.75),
            "p95_trades_per_market_day": daily_counts["trades"].quantile(0.95),
            "max_trades_one_day": daily_counts["trades"].max(),
        })

        daily_perf = (
            sub.groupby("trade_date")
            .agg(
                trades=("ticker", "size"),
                tickers=("ticker", "nunique"),
                avg_net_return_pct=("net_return_pct", "mean"),
                median_net_return_pct=("net_return_pct", "median"),
                win_rate=("net_return_pct", lambda x: (x > 0).mean() * 100),
                target_rate=("exit_reason", lambda x: (x == "target").mean() * 100),
                stop_rate=("exit_reason", lambda x: (x == "stop").mean() * 100),
            )
            .reset_index()
        )
        daily_perf["target_pct"] = target_pct
        daily_perf["stop_pct"] = stop_pct
        daily_perf["config"] = config_name
        daily_rows.append(daily_perf)

        tmp = sub.copy()
        tmp["month"] = tmp["trade_date"].dt.to_period("M").astype(str)
        tmp["year"] = tmp["trade_date"].dt.year

        for month, msub in tmp.groupby("month", observed=True):
            row = {
                "target_pct": target_pct,
                "stop_pct": stop_pct,
                "config": config_name,
                "month": month,
            }
            row.update(summarize_returns(msub))
            row["active_days"] = msub["trade_date"].nunique()
            row["avg_trades_per_active_day"] = len(msub) / row["active_days"]
            month_rows.append(row)

        for year, ysub in tmp.groupby("year", observed=True):
            row = {
                "target_pct": target_pct,
                "stop_pct": stop_pct,
                "config": config_name,
                "year": int(year),
            }
            row.update(summarize_returns(ysub))
            row["active_days"] = ysub["trade_date"].nunique()
            row["avg_trades_per_active_day"] = len(ysub) / row["active_days"]
            year_rows.append(row)

        ticker_counts = (
            sub.groupby("ticker")
            .agg(
                trades=("ticker", "size"),
                avg_net_return_pct=("net_return_pct", "mean"),
                median_net_return_pct=("net_return_pct", "median"),
                win_rate=("net_return_pct", lambda x: (x > 0).mean() * 100),
            )
            .reset_index()
            .sort_values("trades", ascending=False)
        )

        ticker_counts["target_pct"] = target_pct
        ticker_counts["stop_pct"] = stop_pct
        ticker_counts["config"] = config_name
        ticker_counts["trade_share_pct"] = ticker_counts["trades"] / total_trades * 100
        ticker_counts["cum_trade_share_pct"] = ticker_counts["trade_share_pct"].cumsum()

        ticker_rows.append(ticker_counts)

    frequency = pd.DataFrame(frequency_rows).sort_values(["target_pct", "stop_pct"])
    monthly = pd.DataFrame(month_rows).sort_values(["target_pct", "stop_pct", "month"])
    yearly = pd.DataFrame(year_rows).sort_values(["target_pct", "stop_pct", "year"])
    tickers = pd.concat(ticker_rows, ignore_index=True)
    daily = pd.concat(daily_rows, ignore_index=True)

    frequency_path = OUTPUT_DIR / "cheap_long_portfolio_frequency_summary.csv"
    monthly_path = OUTPUT_DIR / "cheap_long_monthly_summary.csv"
    yearly_path = OUTPUT_DIR / "cheap_long_yearly_summary.csv"
    tickers_path = OUTPUT_DIR / "cheap_long_ticker_concentration.csv"
    daily_path = OUTPUT_DIR / "cheap_long_daily_signal_summary.csv"

    frequency.to_csv(frequency_path, index=False)
    monthly.to_csv(monthly_path, index=False)
    yearly.to_csv(yearly_path, index=False)
    tickers.to_csv(tickers_path, index=False)
    daily.to_csv(daily_path, index=False)

    print("saved frequency:", frequency_path)
    print("saved monthly:", monthly_path)
    print("saved yearly:", yearly_path)
    print("saved tickers:", tickers_path)
    print("saved daily:", daily_path)

    print()
    print("=== Portfolio Frequency Summary | 100 bps ===")
    print(frequency.to_string(index=False))

    print()
    print("=== Yearly Summary | 100 bps ===")
    display_year_cols = [
        "target_pct",
        "stop_pct",
        "year",
        "trades",
        "active_days",
        "avg_trades_per_active_day",
        "median_net_return_pct",
        "avg_net_return_pct",
        "net_win_rate",
        "target_rate",
        "stop_rate",
        "worst_net_return_pct",
        "best_net_return_pct",
    ]
    print(yearly[display_year_cols].to_string(index=False))

    print()
    print("=== Worst Months By Avg Return | 100 bps ===")
    display_month_cols = [
        "target_pct",
        "stop_pct",
        "month",
        "trades",
        "active_days",
        "median_net_return_pct",
        "avg_net_return_pct",
        "net_win_rate",
        "target_rate",
        "stop_rate",
    ]
    print(
        monthly.sort_values("avg_net_return_pct")
        [display_month_cols]
        .head(30)
        .to_string(index=False)
    )

    print()
    print("=== Top Ticker Concentration | 100 bps ===")
    display_ticker_cols = [
        "target_pct",
        "stop_pct",
        "ticker",
        "trades",
        "trade_share_pct",
        "cum_trade_share_pct",
        "avg_net_return_pct",
        "median_net_return_pct",
        "win_rate",
    ]
    print(
        tickers.sort_values(["target_pct", "stop_pct", "trades"], ascending=[True, True, False])
        .groupby(["target_pct", "stop_pct"], observed=True)
        .head(15)
        [display_ticker_cols]
        .to_string(index=False)
    )


if __name__ == "__main__":
    main()

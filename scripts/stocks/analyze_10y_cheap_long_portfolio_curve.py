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


def get_market_dates(min_date, max_date):
    if FULL_PANEL_PATH.exists():
        dates = pd.read_csv(FULL_PANEL_PATH, usecols=["trade_date"])
        dates["trade_date"] = pd.to_datetime(dates["trade_date"], errors="coerce").dt.normalize()
        dates = dates.dropna(subset=["trade_date"])

        return (
            dates.loc[
                (dates["trade_date"] >= min_date)
                & (dates["trade_date"] <= max_date),
                "trade_date",
            ]
            .drop_duplicates()
            .sort_values()
            .reset_index(drop=True)
        )

    return pd.Series(pd.bdate_range(min_date, max_date))


def max_drawdown(equity):
    running_max = equity.cummax()
    dd = equity / running_max - 1.0
    return dd.min() * 100.0


def summarize_daily_curve(daily):
    active = daily[daily["trades"] > 0].copy()

    return {
        "market_days": len(daily),
        "active_days": len(active),
        "total_trades": int(daily["trades"].sum()),
        "avg_trades_per_market_day": daily["trades"].sum() / len(daily),
        "avg_trades_per_active_day": active["trades"].mean() if len(active) else np.nan,
        "signal_day_rate_pct": len(active) / len(daily) * 100,
        "avg_daily_return_all_days_pct": daily["daily_return_pct"].mean(),
        "median_daily_return_active_days_pct": active["daily_return_pct"].median() if len(active) else np.nan,
        "avg_daily_return_active_days_pct": active["daily_return_pct"].mean() if len(active) else np.nan,
        "active_day_win_rate": (active["daily_return_pct"] > 0).mean() * 100 if len(active) else np.nan,
        "worst_active_day_pct": active["daily_return_pct"].min() if len(active) else np.nan,
        "best_active_day_pct": active["daily_return_pct"].max() if len(active) else np.nan,
        "max_drawdown_pct": max_drawdown(daily["equity"]),
        "ending_equity_multiple": daily["equity"].iloc[-1],
    }


def summarize_month(msub):
    return {
        "trades": len(msub),
        "active_days": msub["trade_date"].nunique(),
        "median_trade_net_return_pct": msub["net_return_pct"].median(),
        "avg_trade_net_return_pct": msub["net_return_pct"].mean(),
        "trade_win_rate": (msub["net_return_pct"] > 0).mean() * 100,
        "target_rate": (msub["exit_reason"] == "target").mean() * 100,
        "stop_rate": (msub["exit_reason"] == "stop").mean() * 100,
        "worst_trade_pct": msub["net_return_pct"].min(),
        "best_trade_pct": msub["net_return_pct"].max(),
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

    daily_rows = []
    curve_summary_rows = []
    monthly_rows = []

    for keys, sub in df.groupby(["target_pct", "stop_pct"], observed=True):
        target_pct, stop_pct = keys
        config = f"target_{int(target_pct)}_stop_{int(stop_pct)}"

        daily = (
            sub.groupby("trade_date")
            .agg(
                trades=("ticker", "size"),
                tickers=("ticker", "nunique"),
                daily_return_pct=("net_return_pct", "mean"),
                median_trade_return_pct=("net_return_pct", "median"),
                win_rate=("net_return_pct", lambda x: (x > 0).mean() * 100),
                target_rate=("exit_reason", lambda x: (x == "target").mean() * 100),
                stop_rate=("exit_reason", lambda x: (x == "stop").mean() * 100),
            )
            .reindex(market_dates)
            .reset_index()
            .rename(columns={"index": "trade_date"})
        )

        daily["trade_date"] = pd.to_datetime(daily["trade_date"]).dt.normalize()
        daily["target_pct"] = target_pct
        daily["stop_pct"] = stop_pct
        daily["config"] = config

        daily["trades"] = daily["trades"].fillna(0).astype(int)
        daily["tickers"] = daily["tickers"].fillna(0).astype(int)
        daily["daily_return_pct"] = daily["daily_return_pct"].fillna(0.0)

        daily["equity"] = (1.0 + daily["daily_return_pct"] / 100.0).cumprod()
        daily["running_max_equity"] = daily["equity"].cummax()
        daily["drawdown_pct"] = (daily["equity"] / daily["running_max_equity"] - 1.0) * 100.0

        row = {
            "target_pct": target_pct,
            "stop_pct": stop_pct,
            "config": config,
        }
        row.update(summarize_daily_curve(daily))
        curve_summary_rows.append(row)

        daily_rows.append(daily)

        tmp = sub.copy()
        tmp["month"] = tmp["trade_date"].dt.to_period("M").astype(str)

        for month, msub in tmp.groupby("month", observed=True):
            mrow = {
                "target_pct": target_pct,
                "stop_pct": stop_pct,
                "config": config,
                "month": month,
            }
            mrow.update(summarize_month(msub))
            monthly_rows.append(mrow)

    daily_all = pd.concat(daily_rows, ignore_index=True)
    curve_summary = pd.DataFrame(curve_summary_rows).sort_values(
        ["ending_equity_multiple"], ascending=False
    )
    monthly = pd.DataFrame(monthly_rows).sort_values(
        ["target_pct", "stop_pct", "month"]
    )

    daily_path = OUTPUT_DIR / "cheap_long_portfolio_daily_curve.csv"
    curve_summary_path = OUTPUT_DIR / "cheap_long_portfolio_curve_summary.csv"
    monthly_path = OUTPUT_DIR / "cheap_long_monthly_filtered_summary.csv"

    daily_all.to_csv(daily_path, index=False)
    curve_summary.to_csv(curve_summary_path, index=False)
    monthly.to_csv(monthly_path, index=False)

    print("saved daily curve:", daily_path)
    print("saved curve summary:", curve_summary_path)
    print("saved monthly:", monthly_path)

    print()
    print("=== Portfolio Curve Summary | 100 bps ===")
    print(curve_summary.to_string(index=False))

    print()
    print("=== Worst Months With >= 5 Trades | 100 bps ===")
    cols = [
        "target_pct",
        "stop_pct",
        "month",
        "trades",
        "active_days",
        "median_trade_net_return_pct",
        "avg_trade_net_return_pct",
        "trade_win_rate",
        "target_rate",
        "stop_rate",
        "worst_trade_pct",
        "best_trade_pct",
    ]
    print(
        monthly[monthly["trades"] >= 5]
        .sort_values("avg_trade_net_return_pct")
        [cols]
        .head(30)
        .to_string(index=False)
    )

    print()
    print("=== Worst Months With >= 10 Trades | 100 bps ===")
    print(
        monthly[monthly["trades"] >= 10]
        .sort_values("avg_trade_net_return_pct")
        [cols]
        .head(30)
        .to_string(index=False)
    )


if __name__ == "__main__":
    main()

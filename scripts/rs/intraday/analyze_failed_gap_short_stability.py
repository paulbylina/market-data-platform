from pathlib import Path

import pandas as pd


TRADES_PATH = Path("data/research/intraday_gap_up/us_expanded/no_oversold_short_weak_context_trades.csv")
OUTPUT_DIR = Path("data/research/intraday_gap_up/us_expanded")

COST_BPS_LIST = [0, 5, 10, 20]

SETUP_NAME = "core_spy0_relgap4_fb075_t3_s3"


def max_streak(values: pd.Series, condition) -> int:
    best = 0
    current = 0

    for value in values:
        if condition(value):
            current += 1
            best = max(best, current)
        else:
            current = 0

    return best


def summarize_returns(df: pd.DataFrame, return_col: str) -> dict:
    equity = df[return_col].cumsum()
    running_high = equity.cummax()
    drawdown = equity - running_high

    daily = df.groupby("trade_date")[return_col].sum().reset_index()
    monthly = df.groupby("month")[return_col].sum().reset_index()

    return {
        "trades": len(df),
        "avg_trade": df[return_col].mean(),
        "median_trade": df[return_col].median(),
        "win_rate": (df[return_col] > 0).mean() * 100,
        "total": df[return_col].sum(),
        "max_drawdown": drawdown.min(),
        "max_losing_streak_trades": max_streak(df[return_col], lambda x: x <= 0),
        "max_winning_streak_trades": max_streak(df[return_col], lambda x: x > 0),
        "worst_trade": df[return_col].min(),
        "best_trade": df[return_col].max(),
        "trading_days": daily["trade_date"].nunique(),
        "avg_day": daily[return_col].mean(),
        "worst_day": daily[return_col].min(),
        "best_day": daily[return_col].max(),
        "positive_day_rate": (daily[return_col] > 0).mean() * 100,
        "months": monthly["month"].nunique(),
        "avg_month": monthly[return_col].mean(),
        "worst_month": monthly[return_col].min(),
        "best_month": monthly[return_col].max(),
        "positive_month_rate": (monthly[return_col] > 0).mean() * 100,
    }


def main() -> None:
    trades = pd.read_csv(TRADES_PATH)
    trades["trade_date"] = pd.to_datetime(trades["trade_date"])
    trades["year"] = trades["trade_date"].dt.year
    trades["month"] = trades["trade_date"].dt.to_period("M").astype(str)

    setup = trades[
        (trades["relative_gap_vs_spy_pct"] >= 4.0)
        & (trades["relative_gap_rank"] <= 5)
        & (trades["first_bar_return_pct"] <= -0.75)
        & (trades["prior_spy_zscore_200d"] >= 0)
        & (trades["target_pct"] == 3.0)
        & (trades["stop_pct"] == 3.0)
    ].copy()

    setup = setup.sort_values(["trade_date", "ticker"]).reset_index(drop=True)

    summary_rows = []
    monthly_rows = []
    yearly_rows = []
    daily_rows = []

    for cost_bps in COST_BPS_LIST:
        return_col = f"net_{cost_bps}bps"

        if cost_bps == 0:
            setup[return_col] = setup["gross_return_pct"]
        else:
            setup[return_col] = setup["gross_return_pct"] - (cost_bps / 100)

        summary_rows.append(
            {
                "setup": SETUP_NAME,
                "cost_bps": cost_bps,
                **summarize_returns(setup, return_col),
            }
        )

        monthly = (
            setup.groupby("month")
            .agg(
                trades=("ticker", "count"),
                avg=(return_col, "mean"),
                median=(return_col, "median"),
                win_rate=(return_col, lambda x: (x > 0).mean() * 100),
                total=(return_col, "sum"),
                target_rate=("exit_reason", lambda x: (x == "target").mean() * 100),
                stop_rate=("exit_reason", lambda x: (x == "stop").mean() * 100),
            )
            .reset_index()
        )
        monthly["cost_bps"] = cost_bps
        monthly_rows.append(monthly)

        yearly = (
            setup.groupby("year")
            .agg(
                trades=("ticker", "count"),
                avg=(return_col, "mean"),
                median=(return_col, "median"),
                win_rate=(return_col, lambda x: (x > 0).mean() * 100),
                total=(return_col, "sum"),
                target_rate=("exit_reason", lambda x: (x == "target").mean() * 100),
                stop_rate=("exit_reason", lambda x: (x == "stop").mean() * 100),
            )
            .reset_index()
        )
        yearly["cost_bps"] = cost_bps
        yearly_rows.append(yearly)

        daily = (
            setup.groupby("trade_date")
            .agg(
                trades=("ticker", "count"),
                total=(return_col, "sum"),
                avg=(return_col, "mean"),
                winners=(return_col, lambda x: (x > 0).sum()),
                losers=(return_col, lambda x: (x <= 0).sum()),
            )
            .reset_index()
        )
        daily["cost_bps"] = cost_bps
        daily_rows.append(daily)

    summary = pd.DataFrame(summary_rows)
    monthly = pd.concat(monthly_rows).reset_index(drop=True)
    yearly = pd.concat(yearly_rows).reset_index(drop=True)
    daily = pd.concat(daily_rows).reset_index(drop=True)

    summary_path = OUTPUT_DIR / "failed_gap_short_stability_summary.csv"
    monthly_path = OUTPUT_DIR / "failed_gap_short_monthly_summary.csv"
    yearly_path = OUTPUT_DIR / "failed_gap_short_yearly_summary.csv"
    daily_path = OUTPUT_DIR / "failed_gap_short_daily_summary.csv"

    summary.to_csv(summary_path, index=False)
    monthly.to_csv(monthly_path, index=False)
    yearly.to_csv(yearly_path, index=False)
    daily.to_csv(daily_path, index=False)

    print("=== Failed-gap short stability ===")
    print("Saved summary:", summary_path)
    print("Saved monthly:", monthly_path)
    print("Saved yearly:", yearly_path)
    print("Saved daily:", daily_path)
    print()

    print("=== Overall stability ===")
    print(summary.round(4).to_string(index=False))
    print()

    print("=== Worst months at 10 bps ===")
    worst_months_10 = monthly[monthly["cost_bps"] == 10].sort_values("total").head(15)
    print(worst_months_10.round(4).to_string(index=False))
    print()

    print("=== Best months at 10 bps ===")
    best_months_10 = monthly[monthly["cost_bps"] == 10].sort_values("total", ascending=False).head(15)
    print(best_months_10.round(4).to_string(index=False))
    print()

    print("=== Worst days at 10 bps ===")
    worst_days_10 = daily[daily["cost_bps"] == 10].sort_values("total").head(15)
    print(worst_days_10.round(4).to_string(index=False))
    print()

    print("=== Yearly at 10 bps ===")
    print(yearly[yearly["cost_bps"] == 10].round(4).to_string(index=False))


if __name__ == "__main__":
    main()

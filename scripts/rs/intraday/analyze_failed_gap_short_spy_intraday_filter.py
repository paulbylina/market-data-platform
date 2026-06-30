from pathlib import Path

import pandas as pd

from src.utils.path_builders import build_market_curated_output_path


TRADES_PATH = Path("data/research/intraday_gap_up/us_expanded/no_oversold_short_weak_context_trades.csv")
OUTPUT_DIR = Path("data/research/intraday_gap_up/us_expanded")

COST_BPS_LIST = [0, 5, 10, 20]

SETUPS = [
    {
        "name": "core_relgap4_fb075_t3_s3",
        "rel_gap": 4.0,
        "rank_limit": 5,
        "first_bar_max": -0.75,
        "prior_spy_min": 0.0,
        "target_pct": 3.0,
        "stop_pct": 3.0,
    },
    {
        "name": "core_relgap4_fb075_t25_s3",
        "rel_gap": 4.0,
        "rank_limit": 5,
        "first_bar_max": -0.75,
        "prior_spy_min": 0.0,
        "target_pct": 2.5,
        "stop_pct": 3.0,
    },
    {
        "name": "freq_relgap4_fb05_t3_s3",
        "rel_gap": 4.0,
        "rank_limit": 5,
        "first_bar_max": -0.50,
        "prior_spy_min": 0.0,
        "target_pct": 3.0,
        "stop_pct": 3.0,
    },
    {
        "name": "freq_relgap35_fb075_t3_s3",
        "rel_gap": 3.5,
        "rank_limit": 5,
        "first_bar_max": -0.75,
        "prior_spy_min": 0.0,
        "target_pct": 3.0,
        "stop_pct": 3.0,
    },
]

SPY_FIRST_BAR_FILTERS = [
    ("spy_15m_any", None),
    ("spy_15m_le_0_25", 0.25),
    ("spy_15m_le_0", 0.0),
    ("spy_15m_le_minus_0_10", -0.10),
    ("spy_15m_le_minus_0_25", -0.25),
    ("spy_15m_le_minus_0_50", -0.50),
]

DAILY_CAPS = [None, 3]


def get_rth(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["bar_start_utc"] = pd.to_datetime(out["bar_start"], utc=True)
    out["bar_start_et"] = out["bar_start_utc"].dt.tz_convert("America/New_York")
    out["time_et"] = out["bar_start_et"].dt.time

    return out[
        (out["time_et"] >= pd.Timestamp("09:30").time())
        & (out["time_et"] < pd.Timestamp("16:00").time())
    ].sort_values("bar_start_et").copy()


def load_spy_first_bar_return(trade_date: pd.Timestamp) -> float | None:
    date_str = trade_date.strftime("%Y-%m-%d")

    path = build_market_curated_output_path(
        symbol="SPY",
        start_date=date_str,
        end_date=date_str,
        timeframe="15m",
    )

    if not path.exists():
        return None

    bars = pd.read_parquet(path)
    rth = get_rth(bars)

    if rth.empty:
        return None

    first = rth.iloc[0]
    return (first["close"] / first["open"] - 1) * 100


def max_streak(values, condition) -> int:
    best = 0
    current = 0

    for value in values:
        if condition(value):
            current += 1
            best = max(best, current)
        else:
            current = 0

    return best


def summarize(df: pd.DataFrame, return_col: str) -> dict:
    equity = df[return_col].cumsum()
    drawdown = equity - equity.cummax()

    daily = df.groupby("trade_date")[return_col].sum()
    monthly = df.groupby("month")[return_col].sum()
    yearly = df.groupby("year")[return_col].sum()

    return {
        "trades": len(df),
        "trading_days": df["trade_date"].nunique(),
        "avg_trade": df[return_col].mean(),
        "median_trade": df[return_col].median(),
        "win_rate": (df[return_col] > 0).mean() * 100 if len(df) else float("nan"),
        "total": df[return_col].sum(),
        "max_drawdown": drawdown.min() if len(df) else float("nan"),
        "max_losing_streak": max_streak(df[return_col], lambda x: x <= 0),
        "avg_day": daily.mean() if len(daily) else float("nan"),
        "worst_day": daily.min() if len(daily) else float("nan"),
        "best_day": daily.max() if len(daily) else float("nan"),
        "positive_day_rate": (daily > 0).mean() * 100 if len(daily) else float("nan"),
        "avg_month": monthly.mean() if len(monthly) else float("nan"),
        "worst_month": monthly.min() if len(monthly) else float("nan"),
        "positive_month_rate": (monthly > 0).mean() * 100 if len(monthly) else float("nan"),
        "positive_years": int((yearly > 0).sum()) if len(yearly) else 0,
        "years": len(yearly),
    }


def apply_setup(trades: pd.DataFrame, setup: dict) -> pd.DataFrame:
    return trades[
        (trades["relative_gap_vs_spy_pct"] >= setup["rel_gap"])
        & (trades["relative_gap_rank"] <= setup["rank_limit"])
        & (trades["first_bar_return_pct"] <= setup["first_bar_max"])
        & (trades["prior_spy_zscore_200d"] >= setup["prior_spy_min"])
        & (trades["target_pct"] == setup["target_pct"])
        & (trades["stop_pct"] == setup["stop_pct"])
    ].copy()


def apply_daily_cap(df: pd.DataFrame, cap: int | None) -> pd.DataFrame:
    if cap is None:
        return df.copy()

    return (
        df.sort_values(
            ["trade_date", "first_bar_return_pct", "relative_gap_vs_spy_pct"],
            ascending=[True, True, False],
        )
        .groupby("trade_date")
        .head(cap)
        .copy()
    )


def main() -> None:
    trades = pd.read_csv(TRADES_PATH)
    trades["trade_date"] = pd.to_datetime(trades["trade_date"])
    trades["year"] = trades["trade_date"].dt.year
    trades["month"] = trades["trade_date"].dt.to_period("M").astype(str)

    unique_dates = trades["trade_date"].drop_duplicates().sort_values()

    spy_rows = []
    missing_spy = []

    for trade_date in unique_dates:
        ret = load_spy_first_bar_return(trade_date)

        if ret is None:
            missing_spy.append(trade_date.strftime("%Y-%m-%d"))
            continue

        spy_rows.append(
            {
                "trade_date": trade_date,
                "spy_first_bar_return_pct": ret,
            }
        )

    spy_first = pd.DataFrame(spy_rows)

    trades = trades.merge(spy_first, on="trade_date", how="left")
    trades = trades.dropna(subset=["spy_first_bar_return_pct"]).copy()

    rows = []

    for setup in SETUPS:
        setup_trades = apply_setup(trades, setup)

        for spy_filter_name, spy_first_bar_max in SPY_FIRST_BAR_FILTERS:
            if spy_first_bar_max is None:
                filtered = setup_trades.copy()
            else:
                filtered = setup_trades[
                    setup_trades["spy_first_bar_return_pct"] <= spy_first_bar_max
                ].copy()

            for cap in DAILY_CAPS:
                capped = apply_daily_cap(filtered, cap)
                capped = capped.sort_values(["trade_date", "ticker"]).reset_index(drop=True)

                cap_label = "all" if cap is None else f"max_{cap}_per_day"

                for cost_bps in COST_BPS_LIST:
                    return_col = f"net_{cost_bps}bps"

                    if cost_bps == 0:
                        capped[return_col] = capped["gross_return_pct"]
                    else:
                        capped[return_col] = capped["gross_return_pct"] - (cost_bps / 100)

                    rows.append(
                        {
                            "setup": setup["name"],
                            "spy_first_bar_filter": spy_filter_name,
                            "daily_cap": cap_label,
                            "cost_bps": cost_bps,
                            **summarize(capped, return_col),
                        }
                    )

    summary = pd.DataFrame(rows).sort_values(
        ["cost_bps", "avg_trade", "trades"],
        ascending=[True, False, False],
    )

    out = OUTPUT_DIR / "failed_gap_short_spy_intraday_filter_summary.csv"
    summary.to_csv(out, index=False)

    print("=== Failed-gap short SPY first-15m filter ===")
    print("SPY missing dates:", len(missing_spy))
    if missing_spy:
        print("First missing:", missing_spy[:10])
    print("Saved:", out)
    print()

    print("=== Best signal strength, 0 bps, minimum 300 trades ===")
    sig = summary[
        (summary["cost_bps"] == 0)
        & (summary["trades"] >= 300)
    ].copy()
    print(sig.head(40).round(4).to_string(index=False))
    print()

    print("=== Best practical, 10 bps, minimum 300 trades ===")
    practical = summary[
        (summary["cost_bps"] == 10)
        & (summary["trades"] >= 300)
    ].copy()
    print(practical.head(40).round(4).to_string(index=False))


if __name__ == "__main__":
    main()

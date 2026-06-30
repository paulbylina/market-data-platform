from pathlib import Path

import pandas as pd


TRADES_PATH = Path("data/research/intraday_gap_up/us_expanded/no_oversold_short_weak_context_trades.csv")
OUTPUT_DIR = Path("data/research/intraday_gap_up/us_expanded")

COST_BPS_LIST = [5, 10, 15, 20, 30]

SETUPS = [
    {
        "name": "core_spy0_relgap4_fb075_t3_s3",
        "rel_gap": 4.0,
        "rank_limit": 5,
        "first_bar_max": -0.75,
        "spy_min": 0.0,
        "stock_max": None,
        "target_pct": 3.0,
        "stop_pct": 3.0,
    },
    {
        "name": "core_spy0_relgap4_fb075_t25_s3",
        "rel_gap": 4.0,
        "rank_limit": 5,
        "first_bar_max": -0.75,
        "spy_min": 0.0,
        "stock_max": None,
        "target_pct": 2.5,
        "stop_pct": 3.0,
    },
    {
        "name": "freq_spy0_relgap4_fb05_t3_s3",
        "rel_gap": 4.0,
        "rank_limit": 5,
        "first_bar_max": -0.50,
        "spy_min": 0.0,
        "stock_max": None,
        "target_pct": 3.0,
        "stop_pct": 3.0,
    },
    {
        "name": "freq_spy0_relgap35_fb075_t3_s3",
        "rel_gap": 3.5,
        "rank_limit": 5,
        "first_bar_max": -0.75,
        "spy_min": 0.0,
        "stock_max": None,
        "target_pct": 3.0,
        "stop_pct": 3.0,
    },
    {
        "name": "base_no_spy_relgap4_fb075_t3_s3",
        "rel_gap": 4.0,
        "rank_limit": 5,
        "first_bar_max": -0.75,
        "spy_min": None,
        "stock_max": None,
        "target_pct": 3.0,
        "stop_pct": 3.0,
    },
]


def summarize(df: pd.DataFrame, return_col: str) -> dict:
    years = df["trade_date"].dt.year.nunique()

    return {
        "trades": len(df),
        "years": years,
        "trades_per_year": len(df) / years if years else float("nan"),
        "trades_per_day": len(df) / (years * 252) if years else float("nan"),
        "avg": df[return_col].mean(),
        "median": df[return_col].median(),
        "win_rate": (df[return_col] > 0).mean() * 100 if len(df) else float("nan"),
        "target_rate": (df["exit_reason"] == "target").mean() * 100 if len(df) else float("nan"),
        "stop_rate": (df["exit_reason"] == "stop").mean() * 100 if len(df) else float("nan"),
        "worst": df[return_col].min() if len(df) else float("nan"),
        "best": df[return_col].max() if len(df) else float("nan"),
        "total": df[return_col].sum(),
    }


def apply_setup(trades: pd.DataFrame, setup: dict) -> pd.DataFrame:
    mask = (
        (trades["relative_gap_vs_spy_pct"] >= setup["rel_gap"])
        & (trades["relative_gap_rank"] <= setup["rank_limit"])
        & (trades["first_bar_return_pct"] <= setup["first_bar_max"])
        & (trades["target_pct"] == setup["target_pct"])
        & (trades["stop_pct"] == setup["stop_pct"])
    )

    if setup["spy_min"] is not None:
        mask = mask & (trades["prior_spy_zscore_200d"] >= setup["spy_min"])

    if setup["stock_max"] is not None:
        mask = mask & (trades["prior_close_zscore_50d"] <= setup["stock_max"])

    return trades[mask].copy()


def main() -> None:
    trades = pd.read_csv(TRADES_PATH)
    trades["trade_date"] = pd.to_datetime(trades["trade_date"])
    trades["year"] = trades["trade_date"].dt.year

    all_summary_rows = []
    all_year_rows = []

    for setup in SETUPS:
        setup_trades = apply_setup(trades, setup)

        for cost_bps in COST_BPS_LIST:
            return_col = f"net_{cost_bps}bps"
            setup_trades[return_col] = setup_trades["gross_return_pct"] - (cost_bps / 100)

            summary = summarize(setup_trades, return_col)
            all_summary_rows.append(
                {
                    "setup": setup["name"],
                    "cost_bps": cost_bps,
                    **summary,
                }
            )

            for year, year_df in setup_trades.groupby("year"):
                year_summary = summarize(year_df, return_col)
                all_year_rows.append(
                    {
                        "setup": setup["name"],
                        "cost_bps": cost_bps,
                        "year": year,
                        **year_summary,
                    }
                )

    summary = pd.DataFrame(all_summary_rows).sort_values(
        ["cost_bps", "avg"],
        ascending=[True, False],
    )

    yearly = pd.DataFrame(all_year_rows).sort_values(
        ["setup", "cost_bps", "year"]
    )

    summary_path = OUTPUT_DIR / "failed_gap_short_candidate_cost_summary.csv"
    yearly_path = OUTPUT_DIR / "failed_gap_short_candidate_yearly_summary.csv"

    summary.to_csv(summary_path, index=False)
    yearly.to_csv(yearly_path, index=False)

    print("=== Failed-gap short candidate validation ===")
    print("Saved summary:", summary_path)
    print("Saved yearly:", yearly_path)
    print()

    print("=== Cost sensitivity summary ===")
    print(summary.round(4).to_string(index=False))
    print()

    print("=== Main candidate by year at 20 bps ===")
    main_20 = yearly[
        (yearly["setup"] == "core_spy0_relgap4_fb075_t3_s3")
        & (yearly["cost_bps"] == 20)
    ].copy()
    print(main_20.round(4).to_string(index=False))
    print()

    print("=== Main candidate by year at 10 bps ===")
    main_10 = yearly[
        (yearly["setup"] == "core_spy0_relgap4_fb075_t3_s3")
        & (yearly["cost_bps"] == 10)
    ].copy()
    print(main_10.round(4).to_string(index=False))


if __name__ == "__main__":
    main()

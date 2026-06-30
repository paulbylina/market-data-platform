from pathlib import Path
import argparse

import pandas as pd

from src.utils.path_builders import build_market_curated_output_path


DEFAULT_OUTPUT_DIR = Path("data/research/intraday_gap_up")

ROUND_TRIP_COST_BPS_LIST = [0, 5, 10, 20]

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory containing gap-up candidate inputs and entry-test outputs.",
    )
    return parser.parse_args()

def get_rth(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["bar_start_utc"] = pd.to_datetime(out["bar_start"], utc=True)
    out["bar_start_et"] = out["bar_start_utc"].dt.tz_convert("America/New_York")
    out["time_et"] = out["bar_start_et"].dt.time

    return out[
        (out["time_et"] >= pd.Timestamp("09:30").time())
        & (out["time_et"] < pd.Timestamp("16:00").time())
    ].sort_values("bar_start_et").copy()


def load_15m(ticker: str, trade_date: str) -> pd.DataFrame:
    path = build_market_curated_output_path(
        symbol=ticker,
        start_date=trade_date,
        end_date=trade_date,
        timeframe="15m",
    )

    if not path.exists():
        raise FileNotFoundError(path)

    return pd.read_parquet(path)


def main() -> None:
    args = parse_args()
    output_dir = args.output_dir
    candidates_path = output_dir / "gap_up_candidates_top3.csv"

    candidates = pd.read_csv(candidates_path)

    trades = []

    for _, row in candidates.iterrows():
        ticker = row["ticker"]
        trade_date = row["trade_date"]
        split = row["split"]

        df = load_15m(ticker, trade_date)
        rth = get_rth(df)

        if len(rth) == 0:
            trades.append(
                {
                    "split": split,
                    "ticker": ticker,
                    "trade_date": trade_date,
                    "status": "NO_RTH_BARS",
                }
            )
            continue

        first_bar = rth.iloc[0]
        last_bar = rth.iloc[-1]

        entry = first_bar["close"]
        exit_ = last_bar["close"]

        gross_return_pct = (exit_ / entry - 1) * 100

        first_bar_return_pct = (first_bar["close"] / first_bar["open"] - 1) * 100
        first_bar_green = first_bar["close"] > first_bar["open"]

        trades.append(
            {
                "split": split,
                "ticker": ticker,
                "signal_date": row["signal_date"],
                "trade_date": trade_date,
                "daily_signal_rank": row["daily_signal_rank"],
                "daily_gap_pct": row["next_gap_pct"],
                "daily_open_to_close_pct": row["next_open_to_close_return_pct"],
                "rth_bars": len(rth),
                "entry_time_et": str(first_bar["bar_start_et"]),
                "exit_time_et": str(last_bar["bar_start_et"]),
                "entry_price": entry,
                "exit_price": exit_,
                "gross_return_pct": gross_return_pct,
                "first_bar_return_pct": first_bar_return_pct,
                "first_bar_green": first_bar_green,
                "status": "OK",
            }
        )

    trades_df = pd.DataFrame(trades)
    ok = trades_df[trades_df["status"] == "OK"].copy()

    summary_rows = []

    for filter_name, subset in [
        ("all_gap_up_1pct", ok),
        ("first_bar_green", ok[ok["first_bar_green"]].copy()),
        ("first_bar_red_or_flat", ok[~ok["first_bar_green"]].copy()),
    ]:
        for split in ["WF1", "WF2"]:
            split_subset = subset[subset["split"] == split].copy()

            for cost_bps in ROUND_TRIP_COST_BPS_LIST:
                cost_pct = cost_bps / 100.0
                net = split_subset["gross_return_pct"] - cost_pct

                summary_rows.append(
                    {
                        "filter": filter_name,
                        "split": split,
                        "round_trip_cost_bps": cost_bps,
                        "trades": len(net),
                        "avg_net_trade": net.mean(),
                        "median_net_trade": net.median(),
                        "win_rate_net": (net > 0).mean() * 100 if len(net) else float("nan"),
                        "total_net_return_sum": net.sum(),
                        "best_net_trade": net.max() if len(net) else float("nan"),
                        "worst_net_trade": net.min() if len(net) else float("nan"),
                    }
                )

    summary = pd.DataFrame(summary_rows)

    avg = (
        summary
        .groupby(["filter", "round_trip_cost_bps"])
        .agg(
            avg_trades=("trades", "mean"),
            avg_net_trade=("avg_net_trade", "mean"),
            median_net_trade=("median_net_trade", "mean"),
            avg_win_rate_net=("win_rate_net", "mean"),
            min_split_avg_net_trade=("avg_net_trade", "min"),
            avg_total_net_return_sum=("total_net_return_sum", "mean"),
        )
        .reset_index()
        .sort_values(
            ["round_trip_cost_bps", "avg_net_trade"],
            ascending=[True, False],
        )
    )

    output_dir.mkdir(parents=True, exist_ok=True)

    trades_path = output_dir / "gap_up_15m_entry_trades.csv"
    summary_path = output_dir / "gap_up_15m_entry_summary.csv"

    trades_df.to_csv(trades_path, index=False)
    summary.to_csv(summary_path, index=False)

    print("=== Gap-up 15m entry backtest ===")
    print("Setup: daily signal + next-day gap up >= 1%")
    print("Entry: first RTH 15m close")
    print("Exit: final RTH 15m close")
    print()
    print(f"Candidates: {len(candidates)}")
    print(f"OK trades:  {len(ok)}")
    print(f"Saved trades:  {trades_path}")
    print(f"Saved summary: {summary_path}")
    print()

    problems = trades_df[trades_df["status"] != "OK"].copy()
    if not problems.empty:
        print("=== Problems ===")
        print(problems.to_string(index=False))
        print()

    for cost_bps in ROUND_TRIP_COST_BPS_LIST:
        print(f"=== Average across WF1 + WF2 | round-trip cost: {cost_bps} bps ===")
        print(
            avg[avg["round_trip_cost_bps"] == cost_bps]
            .round(4)
            .to_string(index=False)
        )
        print()

    print("=== Split detail ===")
    print(summary.round(4).to_string(index=False))


if __name__ == "__main__":
    main()

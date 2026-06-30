from pathlib import Path

import pandas as pd


TRADES_PATH = Path("data/research/intraday_gap_up/gap_up_15m_entry_trades.csv")
OUTPUT_PATH = Path("data/research/intraday_gap_up/gap_up_15m_pullback_filter_summary.csv")

ROUND_TRIP_COST_BPS_LIST = [0, 5, 10, 20]


def add_filters(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    r = out["first_bar_return_pct"]

    out["all_gap_up_1pct"] = True
    out["first_bar_green"] = r > 0
    out["first_bar_red_or_flat"] = r <= 0

    out["first_bar_pullback_le_0"] = r <= 0
    out["first_bar_pullback_le_minus_0_25"] = r <= -0.25
    out["first_bar_pullback_le_minus_0_50"] = r <= -0.50
    out["first_bar_pullback_le_minus_1_00"] = r <= -1.00

    out["first_bar_pullback_minus_0_25_to_0"] = (r > -0.25) & (r <= 0)
    out["first_bar_pullback_minus_0_50_to_0"] = (r > -0.50) & (r <= 0)
    out["first_bar_pullback_minus_1_00_to_0"] = (r > -1.00) & (r <= 0)
    out["first_bar_pullback_minus_1_00_to_minus_0_25"] = (r > -1.00) & (r <= -0.25)

    out["first_bar_green_0_to_0_50"] = (r > 0) & (r <= 0.50)
    out["first_bar_green_gt_0_50"] = r > 0.50

    return out


def summarize_filter(df: pd.DataFrame, filter_name: str) -> list[dict]:
    rows = []

    for split in ["WF1", "WF2"]:
        subset = df[(df["split"] == split) & (df[filter_name])].copy()

        for cost_bps in ROUND_TRIP_COST_BPS_LIST:
            cost_pct = cost_bps / 100.0
            net = subset["gross_return_pct"] - cost_pct

            rows.append(
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

    return rows


def main() -> None:
    trades = pd.read_csv(TRADES_PATH)

    ok = trades[trades["status"] == "OK"].copy()
    ok = add_filters(ok)

    filters = [
        "all_gap_up_1pct",
        "first_bar_green",
        "first_bar_red_or_flat",
        "first_bar_pullback_le_0",
        "first_bar_pullback_le_minus_0_25",
        "first_bar_pullback_le_minus_0_50",
        "first_bar_pullback_le_minus_1_00",
        "first_bar_pullback_minus_0_25_to_0",
        "first_bar_pullback_minus_0_50_to_0",
        "first_bar_pullback_minus_1_00_to_0",
        "first_bar_pullback_minus_1_00_to_minus_0_25",
        "first_bar_green_0_to_0_50",
        "first_bar_green_gt_0_50",
    ]

    summary_rows = []

    for filter_name in filters:
        summary_rows.extend(summarize_filter(ok, filter_name))

    summary = pd.DataFrame(summary_rows)

    avg = (
        summary
        .groupby(["filter", "round_trip_cost_bps"])
        .agg(
            avg_trades=("trades", "mean"),
            min_trades=("trades", "min"),
            avg_net_trade=("avg_net_trade", "mean"),
            median_net_trade=("median_net_trade", "mean"),
            avg_win_rate_net=("win_rate_net", "mean"),
            min_split_avg_net_trade=("avg_net_trade", "min"),
            avg_total_net_return_sum=("total_net_return_sum", "mean"),
        )
        .reset_index()
    )

    # Avoid getting fooled by tiny samples.
    avg["sample_ok"] = avg["min_trades"] >= 10

    avg_sorted = avg.sort_values(
        ["round_trip_cost_bps", "sample_ok", "avg_net_trade"],
        ascending=[True, False, False],
    )

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(OUTPUT_PATH, index=False)

    print("=== Gap-up 15m first-bar pullback filter test ===")
    print("Setup: daily signal + next-day gap up >= 1%")
    print("Entry: first RTH 15m close")
    print("Exit: final RTH 15m close")
    print()
    print(f"Trades loaded: {len(trades)}")
    print(f"OK trades:     {len(ok)}")
    print(f"Saved summary: {OUTPUT_PATH}")
    print()

    for cost_bps in ROUND_TRIP_COST_BPS_LIST:
        print(f"=== Average across WF1 + WF2 | round-trip cost: {cost_bps} bps ===")
        view = avg_sorted[avg_sorted["round_trip_cost_bps"] == cost_bps].copy()
        print(view.round(4).to_string(index=False))
        print()

    print("=== Best sample-ok filters at 10 bps ===")
    best_10 = avg[
        (avg["round_trip_cost_bps"] == 10)
        & (avg["sample_ok"])
    ].sort_values("avg_net_trade", ascending=False)

    print(best_10.round(4).to_string(index=False))

    print("\n=== Split detail ===")
    print(summary.round(4).to_string(index=False))


if __name__ == "__main__":
    main()

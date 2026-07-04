from __future__ import annotations

from pathlib import Path
import itertools
import numpy as np
import pandas as pd


INPUT = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_expanded_features/"
    "high_price_short_fade_expanded_post_first15_path_metrics.csv"
)

OUT_DIR = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_expanded_features"
)

OUT_TRADES = OUT_DIR / "hot_premarket_short_refinement_trades.csv"
OUT_SUMMARY = OUT_DIR / "hot_premarket_short_refinement_summary.csv"

COST_BPS = 10.0


def pick_col(df: pd.DataFrame, names: list[str]) -> str:
    for n in names:
        if n in df.columns:
            return n
    raise SystemExit(f"Missing required column. Tried: {names}")


def simulate_short_exit(row: pd.Series, target_pct: float, stop_pct: float) -> tuple[float, str]:
    runup = row["short_max_runup_pct"]
    drawdown = row["short_max_drawdown_pct"]
    eod = row["short_eod_pct"]

    target_hit = pd.notna(runup) and runup >= target_pct
    stop_hit = pd.notna(drawdown) and drawdown <= -stop_pct

    # Conservative if both target and stop were touched.
    if target_hit and stop_hit:
        return -stop_pct - COST_BPS / 100.0, "stop_ambiguous"
    if target_hit:
        return target_pct - COST_BPS / 100.0, "target"
    if stop_hit:
        return -stop_pct - COST_BPS / 100.0, "stop"
    return eod - COST_BPS / 100.0, "eod"


def summarize(g: pd.DataFrame) -> pd.Series:
    vals = pd.to_numeric(g["net_pct"], errors="coerce")

    return pd.Series(
        {
            "trades": len(g),
            "dates": g["trade_date"].nunique() if "trade_date" in g.columns else np.nan,
            "tickers": g["ticker"].nunique() if "ticker" in g.columns else np.nan,

            "avg_net": vals.mean(),
            "median_net": vals.median(),
            "win_rate": (vals > 0).mean() * 100,
            "target_rate": g["exit_type"].str.contains("target", na=False).mean() * 100,
            "stop_rate": g["exit_type"].str.contains("stop", na=False).mean() * 100,

            "median_short_eod_raw": g["short_eod_pct"].median(),
            "median_short_runup_raw": g["short_max_runup_pct"].median(),
            "median_short_drawdown_raw": g["short_max_drawdown_pct"].median(),

            "median_gap": g["gap_pct"].median() if "gap_pct" in g.columns else np.nan,
            "median_pm_vs_daily": g["premarket_dollar_vs_prior_daily_avg"].median(),
            "median_open_vs_pm_high": g["regular_open_vs_premarket_high_pct"].median(),
            "median_first15_ret": g["first15_ret"].median(),
            "median_first15_close_pos": g["first15_close_pos"].median(),
            "median_first15_range": g["first15_range"].median(),

            "best": vals.max(),
            "worst": vals.min(),
        }
    )


def main() -> None:
    if not INPUT.exists():
        raise SystemExit(f"Missing input: {INPUT}")

    df = pd.read_csv(INPUT)

    pm_col = pick_col(df, ["premarket_dollar_vs_prior_daily_avg"])
    open_vs_pm_high_col = pick_col(df, ["regular_open_vs_premarket_high_pct"])
    first15_ret_col = pick_col(df, ["first_15m_return_pct", "first15_close_vs_regular_open_pct"])
    first15_close_pos_col = pick_col(df, ["first15_close_position_in_range"])
    first15_range_col = pick_col(df, ["first15_range_pct"])

    required = [
        pm_col,
        open_vs_pm_high_col,
        first15_ret_col,
        first15_close_pos_col,
        first15_range_col,
        "short_eod_pct",
        "short_max_runup_pct",
        "short_max_drawdown_pct",
    ]

    if "gap_pct" in df.columns:
        required.append("gap_pct")
    if "prev_close" in df.columns:
        required.append("prev_close")

    for c in required:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["premarket_dollar_vs_prior_daily_avg"] = df[pm_col]
    df["regular_open_vs_premarket_high_pct"] = df[open_vs_pm_high_col]
    df["first15_ret"] = df[first15_ret_col]
    df["first15_close_pos"] = df[first15_close_pos_col]
    df["first15_range"] = df[first15_range_col]

    if "gap_pct" not in df.columns:
        df["gap_pct"] = np.nan

    # Keep $50+ only if column exists.
    if "prev_close" in df.columns:
        df = df[df["prev_close"] >= 50].copy()

    # Base old-style short setup.
    base = df[
        (df["premarket_dollar_vs_prior_daily_avg"] > 0.10)
        & (df["regular_open_vs_premarket_high_pct"] <= -5)
        & (df["first15_ret"] < 0)
    ].copy()

    print("input:", INPUT)
    print("rows:", len(df))
    print("base hot pre-market red first15 short trades:", len(base))
    print()

    combos = [
        (1.5, 2.0),
        (2.0, 2.5),
        (2.5, 3.0),
        (2.5, 4.0),
        (3.0, 4.0),
        (4.0, 5.0),
    ]

    # Refinement grid.
    pm_mins = [0.10, 0.25, 0.50, 1.00]
    open_vs_pm_high_maxes = [-2, -5, -8, -10]
    first15_ret_maxes = [0, -0.5, -1.0]
    close_pos_maxes = [0.25, 0.35, 0.50]
    range_mins = [0, 1, 2]
    gap_filters = [
        ("gap_any", None, None),
        ("gap_up_0_to_10", 0, 10),
        ("gap_up_0_to_5", 0, 5),
        ("gap_up_2_to_10", 2, 10),
    ]

    trade_rows = []

    for (
        pm_min,
        open_max,
        ret_max,
        close_pos_max,
        range_min,
        gap_tuple,
    ) in itertools.product(
        pm_mins,
        open_vs_pm_high_maxes,
        first15_ret_maxes,
        close_pos_maxes,
        range_mins,
        gap_filters,
    ):
        gap_label, gap_min, gap_max = gap_tuple

        mask = (
            (df["premarket_dollar_vs_prior_daily_avg"] >= pm_min)
            & (df["regular_open_vs_premarket_high_pct"] <= open_max)
            & (df["first15_ret"] <= ret_max)
            & (df["first15_close_pos"] <= close_pos_max)
            & (df["first15_range"] >= range_min)
        )

        if gap_min is not None:
            mask &= df["gap_pct"] >= gap_min
        if gap_max is not None:
            mask &= df["gap_pct"] <= gap_max

        sub = df[mask].copy()
        if len(sub) < 20:
            continue

        setup_name = (
            f"pm>={pm_min}_open<=pmhigh{open_max}_"
            f"ret<={ret_max}_closepos<={close_pos_max}_"
            f"range>={range_min}_{gap_label}"
        )

        for target, stop in combos:
            for _, row in sub.iterrows():
                net, exit_type = simulate_short_exit(row, target, stop)
                r = row.to_dict()
                r["setup_name"] = setup_name
                r["pm_min"] = pm_min
                r["open_vs_pm_high_max"] = open_max
                r["first15_ret_max"] = ret_max
                r["first15_close_pos_max"] = close_pos_max
                r["first15_range_min"] = range_min
                r["gap_filter"] = gap_label
                r["target_pct"] = target
                r["stop_pct"] = stop
                r["net_pct"] = net
                r["exit_type"] = exit_type
                r["cost_bps"] = COST_BPS
                trade_rows.append(r)

    trades = pd.DataFrame(trade_rows)

    if trades.empty:
        print("No refinement trades found.")
        return

    summary = (
        trades.groupby(
            [
                "setup_name",
                "pm_min",
                "open_vs_pm_high_max",
                "first15_ret_max",
                "first15_close_pos_max",
                "first15_range_min",
                "gap_filter",
                "target_pct",
                "stop_pct",
            ],
            observed=True,
        )
        .apply(summarize)
        .reset_index()
    )

    trades.to_csv(OUT_TRADES, index=False)
    summary.to_csv(OUT_SUMMARY, index=False)

    print("=== Best by median net | trades >= 50 ===")
    display = summary[summary["trades"] >= 50].copy()
    print(
        display.sort_values(["median_net", "avg_net"], ascending=False)
        .head(40)
        .to_string(index=False)
    )

    print()
    print("=== Best by avg net | trades >= 50 ===")
    print(
        display.sort_values(["avg_net", "median_net"], ascending=False)
        .head(40)
        .to_string(index=False)
    )

    print()
    print("=== Best balanced | avg > 0, median > 1, stop_rate < 30, trades >= 50 ===")
    balanced = display[
        (display["avg_net"] > 0)
        & (display["median_net"] > 1)
        & (display["stop_rate"] < 30)
    ].copy()

    if balanced.empty:
        print("No balanced rows found.")
    else:
        print(
            balanced.sort_values(["avg_net", "median_net"], ascending=False)
            .head(40)
            .to_string(index=False)
        )

    print()
    print("saved trades:", OUT_TRADES)
    print("saved summary:", OUT_SUMMARY)


if __name__ == "__main__":
    main()

from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd


INPUT = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_expanded_features/"
    "high_price_context_scored_daily_best_2024_2026.csv"
)


def gap_bucket(x: float) -> str:
    if pd.isna(x):
        return "missing"
    if x < -3:
        return "<-3 big gap down"
    if x < -1:
        return "-3 to -1 gap down"
    if x < 0:
        return "-1 to 0 slight down"
    if x < 1:
        return "0 to 1 flat/up"
    if x < 3:
        return "1 to 3 gap up"
    if x < 5:
        return "3 to 5 strong gap up"
    return "5+ too hot"


def simulate_exit(row: pd.Series, target_pct: float = 3.0, stop_pct: float = 4.0) -> tuple[float, str]:
    runup = row["long_max_runup_pct"]
    drawdown = row["long_max_drawdown_pct"]
    eod = row["long_eod_pct"]

    target_hit = pd.notna(runup) and runup >= target_pct
    stop_hit = pd.notna(drawdown) and drawdown <= -stop_pct

    if target_hit and stop_hit:
        return -stop_pct - 0.10, "stop_ambiguous"
    if target_hit:
        return target_pct - 0.10, "target"
    if stop_hit:
        return -stop_pct - 0.10, "stop"
    return eod - 0.10, "eod"


def summarize(g: pd.DataFrame) -> pd.Series:
    vals = pd.to_numeric(g["net_pct"], errors="coerce")

    return pd.Series(
        {
            "trades": len(g),
            "dates": g["trade_date"].nunique(),
            "tickers": g["ticker"].nunique(),
            "avg_net": vals.mean(),
            "median_net": vals.median(),
            "win_rate": (vals > 0).mean() * 100,
            "target_rate": g["exit_type"].str.contains("target", na=False).mean() * 100,
            "stop_rate": g["exit_type"].str.contains("stop", na=False).mean() * 100,
            "median_eod_raw": g["long_eod_pct"].median(),
            "median_runup_raw": g["long_max_runup_pct"].median(),
            "median_drawdown_raw": g["long_max_drawdown_pct"].median(),
        }
    )


def main() -> None:
    df = pd.read_csv(INPUT)

    numeric_cols = [
        "gap_pct",
        "prior_day_last15_dollar_rvol_20d",
        "premarket_dollar_vs_prior_daily_avg",
        "first_15m_return_pct",
        "first15_range_pct",
        "first15_close_position_in_range",
        "long_eod_pct",
        "long_max_runup_pct",
        "long_max_drawdown_pct",
    ]

    for c in numeric_cols:
        if c not in df.columns:
            raise SystemExit(f"Missing column: {c}")
        df[c] = pd.to_numeric(df[c], errors="coerce")

    pm = df["premarket_dollar_vs_prior_daily_avg"]
    prior = df["prior_day_last15_dollar_rvol_20d"]
    ret = df["first_15m_return_pct"]
    rng = df["first15_range_pct"]
    close_pos = df["first15_close_position_in_range"]

    strict_shape = (
        (pm <= 0.01)
        & (ret >= 1) & (ret < 4)
        & (rng >= 2) & (rng < 4)
        & (close_pos >= 0.90)
    )

    strong_shape = (
        (pm <= 0.03)
        & (ret >= 2) & (ret < 4)
        & (rng >= 2) & (rng < 4)
        & (close_pos >= 0.75)
    )

    extended_shape = (
        (pm <= 0.01)
        & (ret >= 1) & (ret < 4)
        & (rng >= 4) & (rng < 6)
        & (close_pos >= 0.75)
    )

    base_shape = strict_shape | strong_shape | extended_shape

    sub = df[base_shape].copy()

    sub["abc"] = np.select(
        [
            sub["prior_day_last15_dollar_rvol_20d"] >= 3,
            (sub["prior_day_last15_dollar_rvol_20d"] >= 1.5)
            & (sub["prior_day_last15_dollar_rvol_20d"] < 3),
        ],
        ["A", "B"],
        default="C",
    )

    sub["gap_bucket"] = sub["gap_pct"].map(gap_bucket)

    exits = sub.apply(simulate_exit, axis=1, result_type="expand")
    sub["net_pct"] = exits[0]
    sub["exit_type"] = exits[1]

    print("input:", INPUT)
    print("base ABC-shape trades:", len(sub))
    print()

    print("=== ABC by gap bucket | 3% target / 4% stop | stop-first ===")
    out = (
        sub.groupby(["abc", "gap_bucket"], observed=True)
        .apply(summarize)
        .reset_index()
        .sort_values(["abc", "median_net"], ascending=[True, False])
    )
    print(out.to_string(index=False))

    print()
    print("=== Simple gap filters by ABC ===")

    tests = {
        "all": sub.index == sub.index,
        "gap >= 0": sub["gap_pct"] >= 0,
        "gap >= -0.5": sub["gap_pct"] >= -0.5,
        "gap between 0 and 5": (sub["gap_pct"] >= 0) & (sub["gap_pct"] <= 5),
        "gap between -0.5 and 5": (sub["gap_pct"] >= -0.5) & (sub["gap_pct"] <= 5),
        "gap < 0": sub["gap_pct"] < 0,
        "gap < -1": sub["gap_pct"] < -1,
    }

    rows = []
    for name, mask in tests.items():
        g = sub[mask].copy()
        if g.empty:
            continue
        row = summarize(g).to_dict()
        row["filter"] = name
        rows.append(row)

    filt = pd.DataFrame(rows).sort_values(["median_net", "avg_net"], ascending=False)
    print(filt.to_string(index=False))

    out_path = Path(
        "data/research/full_market_scanner_10y/high_price_short_fade_expanded_features/"
        "abc_gap_effect_2024_2026.csv"
    )
    out.to_csv(out_path, index=False)
    print()
    print("saved:", out_path)


if __name__ == "__main__":
    main()

from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd


INPUT = Path(
    "data/research/full_market_scanner_10y/extended_hours_features_pilot/"
    "full_pilot_post_first15_path_metrics_by_price_bucket.csv"
)

OUT_TRADES = Path(
    "data/research/full_market_scanner_10y/extended_hours_features_pilot/"
    "price_10_to_50_first15_gap_setup_exit_trades.csv"
)

OUT_SUMMARY = Path(
    "data/research/full_market_scanner_10y/extended_hours_features_pilot/"
    "price_10_to_50_first15_gap_setup_exit_summary.csv"
)

COST_BPS = 10.0


def pick_col(df: pd.DataFrame, names: list[str]) -> str:
    for n in names:
        if n in df.columns:
            return n
    raise SystemExit(f"Missing all possible columns: {names}")


def simulate_exit(row: pd.Series, target_pct: float, stop_pct: float) -> tuple[float, str]:
    runup = row["long_max_runup_pct"]
    drawdown = row["long_max_drawdown_pct"]
    eod = row["long_eod_pct"]

    target_hit = pd.notna(runup) and runup >= target_pct
    stop_hit = pd.notna(drawdown) and drawdown <= -stop_pct

    # Conservative when both target and stop are touched intraday.
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
            "median_eod_raw": g["long_eod_pct"].median(),
            "median_runup_raw": g["long_max_runup_pct"].median(),
            "median_drawdown_raw": g["long_max_drawdown_pct"].median(),
            "best": vals.max(),
            "worst": vals.min(),
        }
    )


def main() -> None:
    if not INPUT.exists():
        raise SystemExit(f"Missing input: {INPUT}")

    df = pd.read_csv(INPUT)

    price_bucket_col = pick_col(df, ["price_bucket_detail", "price_bucket"])
    gap_col = pick_col(df, ["gap_pct"])
    pm_col = pick_col(df, ["premarket_dollar_vs_prior_daily_avg"])
    ret_col = pick_col(df, ["first_15m_return_pct", "first15_close_vs_regular_open_pct"])
    range_col = pick_col(df, ["first15_range_pct"])
    close_pos_col = pick_col(df, ["first15_close_position_in_range"])

    needed = [
        gap_col,
        pm_col,
        ret_col,
        range_col,
        close_pos_col,
        "long_eod_pct",
        "long_max_runup_pct",
        "long_max_drawdown_pct",
    ]

    for c in needed:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    print("input:", INPUT)
    print("rows:", len(df))
    print()
    print("=== price bucket counts ===")
    print(df[price_bucket_col].value_counts(dropna=False).to_string())
    print()

    # Adjust this if your file uses slightly different labels.
    bucket_10_20 = df[price_bucket_col].astype(str).str.contains("10") & df[price_bucket_col].astype(str).str.contains("20")
    bucket_20_50 = df[price_bucket_col].astype(str).str.contains("20") & df[price_bucket_col].astype(str).str.contains("50")
    bucket_10_50 = bucket_10_20 | bucket_20_50

    sub = df[bucket_10_50].copy()

    gap = sub[gap_col]
    pm = sub[pm_col]
    ret = sub[ret_col]
    rng = sub[range_col]
    close_pos = sub[close_pos_col]

    # Similar idea to $50+, but slightly wider because $10-50 names move more.
    strict = (
        (gap >= 0) & (gap <= 5)
        & (pm <= 0.01)
        & (ret >= 1.5) & (ret < 5)
        & (rng >= 2) & (rng < 6)
        & (close_pos >= 0.90)
    )

    valid = (
        (gap >= 0) & (gap <= 5)
        & (pm <= 0.03)
        & (ret >= 1.5) & (ret < 6)
        & (rng >= 2) & (rng < 7)
        & (close_pos >= 0.75)
    )

    fresh_gap = (
        (gap >= 1) & (gap <= 5)
        & (pm <= 0.01)
        & (ret >= 1.5) & (ret < 5)
        & (rng >= 2) & (rng < 6)
        & (close_pos >= 0.90)
    )

    sub["setup_bucket"] = np.select(
        [
            fresh_gap,
            strict,
            valid,
        ],
        [
            "fresh_gap_strict",
            "strict",
            "valid",
        ],
        default="reject",
    )

    test = sub[sub["setup_bucket"].ne("reject")].copy()

    test["price_test_bucket"] = np.select(
        [
            bucket_10_20.loc[test.index],
            bucket_20_50.loc[test.index],
        ],
        [
            "price_10_to_20",
            "price_20_to_50",
        ],
        default="price_10_to_50",
    )

    print("=== setup counts ===")
    print(test.groupby(["price_test_bucket", "setup_bucket"]).size().to_string())
    print()

    combos = [
        (2.0, 2.5),
        (2.5, 3.0),
        (3.0, 4.0),
        (4.0, 5.0),
        (5.0, 6.0),
    ]

    trade_rows = []

    for target, stop in combos:
        for _, row in test.iterrows():
            net, exit_type = simulate_exit(row, target, stop)
            r = row.to_dict()
            r["target_pct"] = target
            r["stop_pct"] = stop
            r["net_pct"] = net
            r["exit_type"] = exit_type
            r["cost_bps"] = COST_BPS
            trade_rows.append(r)

    trades = pd.DataFrame(trade_rows)

    summary = (
        trades.groupby(
            ["price_test_bucket", "setup_bucket", "target_pct", "stop_pct"],
            observed=True,
        )
        .apply(summarize)
        .reset_index()
        .sort_values(
            ["price_test_bucket", "setup_bucket", "median_net", "avg_net"],
            ascending=[True, True, False, False],
        )
    )

    trades.to_csv(OUT_TRADES, index=False)
    summary.to_csv(OUT_SUMMARY, index=False)

    print("=== Top results by median net ===")
    print(
        summary.sort_values(["median_net", "avg_net"], ascending=False)
        .head(30)
        .to_string(index=False)
    )

    print()
    print("saved trades:", OUT_TRADES)
    print("saved summary:", OUT_SUMMARY)


if __name__ == "__main__":
    main()

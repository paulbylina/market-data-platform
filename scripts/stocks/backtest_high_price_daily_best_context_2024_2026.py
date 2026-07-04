from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd


INPUT = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_expanded_features/"
    "old_research_prior_day_context_with_prior_last15_2024-01-01_to_2026-07-02.csv"
)

OUT_DIR = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_expanded_features"
)

SCORED_OUT = OUT_DIR / "high_price_context_scored_daily_best_2024_2026.csv"
SUMMARY_OUT = OUT_DIR / "high_price_context_daily_best_exit_summary_2024_2026.csv"
TRADES_OUT = OUT_DIR / "high_price_context_daily_best_exit_trades_2024_2026.csv"


def num(df: pd.DataFrame, col: str) -> pd.Series:
    return pd.to_numeric(df[col], errors="coerce")


def classify_and_score(row: pd.Series) -> tuple[str, float, str]:
    prior = row.get("prior_day_last15_dollar_rvol_20d", np.nan)
    pm = row.get("premarket_dollar_vs_prior_daily_avg", np.nan)
    f15_daily = row.get("first15_dollar_vs_prior_daily_avg", np.nan)
    f15_rvol = row.get("first15_dollar_rvol_20d", np.nan)
    ret = row.get("first_15m_return_pct", np.nan)
    rng = row.get("first15_range_pct", np.nan)
    close_pos = row.get("first15_close_position_in_range", np.nan)

    score = 0.0
    notes = []

    # Prior-day close activity.
    if pd.notna(prior):
        if prior >= 5:
            score += 4
            notes.append("5+ extreme prior close")
        elif prior >= 3:
            score += 3
            notes.append("3-5 hot prior close")
        elif prior >= 1.5:
            score += 2
            notes.append("1.5-3 active prior close")
        elif prior <= 0.75:
            score -= 2
            notes.append("quiet prior close")

    # Pre-market: quiet is best, active is dangerous.
    if pd.notna(pm):
        if pm <= 0.003:
            score += 2
            notes.append("dead pre-market")
        elif pm <= 0.01:
            score += 1.5
            notes.append("quiet pre-market")
        elif pm <= 0.03:
            score += 0.5
            notes.append("mild pre-market")
        elif pm <= 0.10:
            score -= 2
            notes.append("active pre-market")
        else:
            score -= 5
            notes.append("pre-market mania")

    # First15 return.
    if pd.notna(ret):
        if 2 <= ret < 4:
            score += 3
            notes.append("first15 return 2-4 sweet spot")
        elif 1 <= ret < 2:
            score += 1.5
            notes.append("first15 return 1-2 acceptable")
        elif 4 <= ret < 6:
            score += 0.5
            notes.append("first15 return 4-6 extended")
        elif 6 <= ret < 8:
            score -= 2
            notes.append("first15 return 6-8 caution")
        elif ret >= 8:
            score -= 5
            notes.append("first15 too hot")

    # First15 range.
    if pd.notna(rng):
        if 2 <= rng < 4:
            score += 3
            notes.append("clean 2-4 range")
        elif 4 <= rng < 6:
            score += 1
            notes.append("4-6 extended range")
        elif 6 <= rng < 8:
            score -= 2
            notes.append("6-8 wide range")
        elif rng >= 8:
            score -= 5
            notes.append("range too wide")
        elif rng < 2:
            score -= 1
            notes.append("range too tight")

    # Close position.
    if pd.notna(close_pos):
        if close_pos >= 0.90:
            score += 2
            notes.append("closed very near high")
        elif close_pos >= 0.75:
            score += 1
            notes.append("closed near high")
        elif close_pos < 0.50:
            score -= 2
            notes.append("weak first15 close")

    # First15 volume confirmation.
    if pd.notna(f15_daily):
        if 0.05 <= f15_daily < 0.50:
            score += 1
            notes.append("meaningful first15 dollar volume")
        elif f15_daily >= 0.50:
            score -= 1
            notes.append("extreme first15 dollar volume")

    if pd.notna(f15_rvol):
        if f15_rvol >= 3:
            score += 2
            notes.append("first15 RVOL >= 3")
        elif f15_rvol >= 2:
            score += 1
            notes.append("first15 RVOL >= 2")

    # Hard labels.
    if (
        pd.notna(prior) and prior >= 3
        and pd.notna(pm) and pm <= 0.01
        and pd.notna(ret) and 1 <= ret < 4
        and pd.notna(rng) and 2 <= rng < 4
        and pd.notna(close_pos) and close_pos >= 0.90
    ):
        label = "A+ strict"

    elif (
        pd.notna(prior) and prior >= 3
        and pd.notna(pm) and pm <= 0.03
        and pd.notna(ret) and 2 <= ret < 4
        and pd.notna(rng) and 2 <= rng < 4
        and pd.notna(close_pos) and close_pos >= 0.75
    ):
        label = "A strong"

    elif (
        pd.notna(prior) and prior >= 3
        and pd.notna(pm) and pm <= 0.01
        and pd.notna(ret) and 1 <= ret < 4
        and pd.notna(rng) and 4 <= rng < 6
        and pd.notna(close_pos) and close_pos >= 0.75
    ):
        label = "B+ extended clean"

    elif (
        pd.notna(prior) and prior >= 1.5
        and pd.notna(pm) and pm <= 0.03
        and pd.notna(ret) and 1 <= ret < 5
        and pd.notna(rng) and 2 <= rng < 6
        and pd.notna(close_pos) and close_pos >= 0.75
    ):
        label = "B valid"

    elif pd.notna(pm) and pm > 0.03:
        label = "DOWNRANK active pre-market"

    elif pd.notna(ret) and ret >= 6:
        label = "DOWNRANK too hot"

    elif pd.notna(rng) and rng >= 6:
        label = "DOWNRANK too wide"

    else:
        label = "watchlist only"

    return label, score, "; ".join(notes)


def simulate_exit(
    row: pd.Series,
    target_pct: float,
    stop_pct: float,
    cost_bps: float,
    both_policy: str,
) -> tuple[float, str]:
    runup = row["long_max_runup_pct"]
    drawdown = row["long_max_drawdown_pct"]
    eod = row["long_eod_pct"]

    target_hit = pd.notna(runup) and runup >= target_pct
    stop_hit = pd.notna(drawdown) and drawdown <= -stop_pct

    if target_hit and stop_hit:
        if both_policy == "target_first":
            gross = target_pct
            exit_type = "target_ambiguous"
        elif both_policy == "eod":
            gross = eod
            exit_type = "eod_ambiguous"
        else:
            gross = -stop_pct
            exit_type = "stop_ambiguous"

    elif target_hit:
        gross = target_pct
        exit_type = "target"

    elif stop_hit:
        gross = -stop_pct
        exit_type = "stop"

    else:
        gross = eod
        exit_type = "eod"

    net = gross - (cost_bps / 100.0)
    return net, exit_type


def summarize(g: pd.DataFrame) -> dict:
    vals = pd.to_numeric(g["net_pct"], errors="coerce").dropna()

    if vals.empty:
        return {}

    dates = g["trade_date"].nunique()

    return {
        "trades": len(vals),
        "dates": dates,
        "avg_trades_per_signal_day": len(vals) / dates if dates else np.nan,
        "sum_return_pct": vals.sum(),
        "avg_return_pct": vals.mean(),
        "median_return_pct": vals.median(),
        "win_rate": (vals > 0).mean() * 100,
        "target_rate": g["exit_type"].str.contains("target", na=False).mean() * 100,
        "stop_rate": g["exit_type"].str.contains("stop", na=False).mean() * 100,
        "eod_rate": g["exit_type"].str.contains("eod", na=False).mean() * 100,
        "best": vals.max(),
        "worst": vals.min(),
    }


def main() -> None:
    df = pd.read_csv(INPUT)

    needed = [
        "ticker",
        "trade_date",
        "prior_day_last15_dollar_rvol_20d",
        "premarket_dollar_vs_prior_daily_avg",
        "first_15m_return_pct",
        "first15_range_pct",
        "first15_close_position_in_range",
        "long_eod_pct",
        "long_max_runup_pct",
        "long_max_drawdown_pct",
    ]

    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise SystemExit(f"Missing columns: {missing}")

    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")

    numeric_cols = [
        "prior_day_last15_dollar_rvol_20d",
        "premarket_dollar_vs_prior_daily_avg",
        "first15_dollar_vs_prior_daily_avg",
        "first15_dollar_rvol_20d",
        "first_15m_return_pct",
        "first15_range_pct",
        "first15_close_position_in_range",
        "long_eod_pct",
        "long_max_runup_pct",
        "long_max_drawdown_pct",
        "gap_pct",
    ]

    for c in numeric_cols:
        if c in df.columns:
            df[c] = num(df, c)

    df = df.dropna(
        subset=[
            "trade_date",
            "ticker",
            "long_eod_pct",
            "long_max_runup_pct",
            "long_max_drawdown_pct",
        ]
    ).copy()

    labels = df.apply(classify_and_score, axis=1, result_type="expand")
    df["context_label"] = labels[0]
    df["context_score"] = labels[1]
    df["context_notes"] = labels[2]

    # Tie-breakers are important. This should prefer FRHC-like names over DCO-like names
    # when prior context and first15 confirmation are stronger.
    sort_cols = [
        "trade_date",
        "context_score",
        "prior_day_last15_dollar_rvol_20d",
        "first_15m_return_pct",
        "first15_close_position_in_range",
        "first15_dollar_vs_prior_daily_avg",
    ]

    df_sorted = df.sort_values(sort_cols, ascending=[True, False, False, False, False, False]).copy()

    daily_best_any = df_sorted.groupby("trade_date", observed=True).head(1).copy()
    daily_best_score_ge_8 = daily_best_any[daily_best_any["context_score"] >= 8].copy()
    daily_best_score_ge_10 = daily_best_any[daily_best_any["context_score"] >= 10].copy()

    all_strict = df[df["context_label"].eq("A+ strict")].copy()
    all_a_or_better = df[df["context_label"].isin(["A+ strict", "A strong", "B+ extended clean"])].copy()
    all_b_valid_or_better = df[df["context_label"].isin(["A+ strict", "A strong", "B+ extended clean", "B valid"])].copy()

    strategy_frames = {
        "daily_best_any": daily_best_any,
        "daily_best_score_ge_8": daily_best_score_ge_8,
        "daily_best_score_ge_10": daily_best_score_ge_10,
        "all_Aplus_strict": all_strict,
        "all_A_or_Bplus": all_a_or_better,
        "all_Bvalid_or_better": all_b_valid_or_better,
    }

    combos = [
        (1.0, 1.25),
        (1.0, 1.5),
        (1.5, 2.0),
        (2.0, 2.5),
        (2.5, 3.0),
    ]

    both_policies = ["stop_first", "target_first"]

    trade_rows = []

    for strategy_name, sdf in strategy_frames.items():
        for target, stop in combos:
            for policy in both_policies:
                tmp = sdf.copy()

                exits = tmp.apply(
                    lambda row: simulate_exit(
                        row=row,
                        target_pct=target,
                        stop_pct=stop,
                        cost_bps=10.0,
                        both_policy=policy,
                    ),
                    axis=1,
                    result_type="expand",
                )

                tmp["net_pct"] = exits[0]
                tmp["exit_type"] = exits[1]
                tmp["strategy"] = strategy_name
                tmp["target_pct"] = target
                tmp["stop_pct"] = stop
                tmp["both_policy"] = policy

                trade_rows.append(tmp)

    trades = pd.concat(trade_rows, ignore_index=True) if trade_rows else pd.DataFrame()

    summary_rows = []

    for keys, g in trades.groupby(["strategy", "target_pct", "stop_pct", "both_policy"], observed=True):
        strategy, target, stop, policy = keys
        row = {
            "strategy": strategy,
            "target_pct": target,
            "stop_pct": stop,
            "both_policy": policy,
        }
        row.update(summarize(g))
        summary_rows.append(row)

    summary = pd.DataFrame(summary_rows)

    if not summary.empty:
        summary = summary.sort_values(
            ["strategy", "sum_return_pct", "avg_return_pct", "worst"],
            ascending=[True, False, False, False],
        )

    df_sorted.to_csv(SCORED_OUT, index=False)
    trades.to_csv(TRADES_OUT, index=False)
    summary.to_csv(SUMMARY_OUT, index=False)

    print("input:", INPUT)
    print("rows:", len(df))
    print("dates with candidates:", df["trade_date"].nunique())
    print()
    print("=== Label counts ===")
    print(df["context_label"].value_counts().to_string())
    print()
    print("=== Daily best label counts ===")
    print(daily_best_any["context_label"].value_counts().to_string())
    print()
    print("=== Top daily-best examples ===")
    show_cols = [
        "trade_date",
        "ticker",
        "context_label",
        "context_score",
        "prior_day_last15_dollar_rvol_20d",
        "premarket_dollar_vs_prior_daily_avg",
        "first_15m_return_pct",
        "first15_range_pct",
        "first15_close_position_in_range",
        "long_eod_pct",
        "long_max_runup_pct",
        "long_max_drawdown_pct",
    ]
    print(daily_best_any[show_cols].head(30).to_string(index=False))

    print()
    print("=== Top exit summaries by strategy ===")
    show_summary = [
        "strategy",
        "target_pct",
        "stop_pct",
        "both_policy",
        "trades",
        "dates",
        "avg_trades_per_signal_day",
        "sum_return_pct",
        "avg_return_pct",
        "median_return_pct",
        "win_rate",
        "target_rate",
        "stop_rate",
        "best",
        "worst",
    ]

    for strategy, g in summary.groupby("strategy", observed=True):
        print()
        print(f"--- {strategy} ---")
        print(g[show_summary].head(10).to_string(index=False))

    print()
    print("saved scored:", SCORED_OUT)
    print("saved trades:", TRADES_OUT)
    print("saved summary:", SUMMARY_OUT)


if __name__ == "__main__":
    main()

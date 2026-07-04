from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd


SIGNALS_PATH = Path(
    "data/research/full_market_scanner_10y/high_price_full_universe_first15_checks/"
    "high_price_first15_final_A_Aplus_rvol_2026-06-29_to_2026-07-02.csv"
)

CONTEXT_PATH = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_expanded_features/"
    "old_research_prior_day_context_with_prior_last15_2024-01-01_to_2026-07-02.csv"
)

OUT_PATH = Path(
    "data/research/full_market_scanner_10y/high_price_full_universe_first15_checks/"
    "past_week_prior_last15_context_ranked_signals_2026-06-29_to_2026-07-02.csv"
)


def to_date(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce").dt.strftime("%Y-%m-%d")


def label_prior_last15(x: float) -> str:
    if pd.isna(x):
        return "missing"
    if x >= 5:
        return "5+ extreme"
    if x >= 3:
        return "3-5 hot"
    if x >= 1.5:
        return "1.5-3 active"
    if x >= 0.75:
        return "0.75-1.5 normal"
    return "<=0.75 quiet"


def label_premarket(x: float) -> str:
    if pd.isna(x):
        return "missing"
    if x <= 0.003:
        return "dead"
    if x <= 0.01:
        return "quiet"
    if x <= 0.03:
        return "mild"
    if x <= 0.10:
        return "active"
    return "mania"


def classify(row: pd.Series) -> tuple[str, int, str]:
    prior = row.get("prior_day_last15_dollar_rvol_20d", np.nan)
    pm = row.get("premarket_dollar_vs_prior_daily_avg", np.nan)
    f15_daily = row.get("first15_dollar_vs_prior_daily_avg", np.nan)
    f15_rvol = row.get("first15_dollar_rvol_20d", np.nan)
    ret = row.get("first_15m_return_pct", np.nan)
    rng = row.get("first15_range_pct", np.nan)
    close_pos = row.get("first15_close_position_in_range", np.nan)

    score = 0
    notes = []

    if pd.notna(prior):
        if prior >= 5:
            score += 2
            notes.append("5+ extreme prior close")
        elif prior >= 3:
            score += 1
            notes.append("3-5 hot prior close")
        elif prior <= 0.75:
            score -= 1
            notes.append("quiet prior close")

    if pd.notna(pm):
        if pm <= 0.003:
            score += 2
            notes.append("dead pre-market")
        elif pm <= 0.01:
            score += 1
            notes.append("quiet pre-market")
        elif pm <= 0.03:
            notes.append("mild pre-market")
        elif pm <= 0.10:
            score -= 1
            notes.append("active pre-market")
        else:
            score -= 3
            notes.append("pre-market mania")

    if pd.notna(ret):
        if 2 <= ret < 8:
            score += 2
            notes.append("first15 return 2-8 sweet spot")
        elif 1 <= ret < 2:
            score += 1
            notes.append("first15 return okay")
        elif ret >= 8:
            score -= 3
            notes.append("first15 too hot")

    if pd.notna(rng):
        if 2 <= rng < 4:
            score += 2
            notes.append("clean 2-4 range")
        elif 4 <= rng < 8:
            score += 1
            notes.append("wide but acceptable range")
        elif rng >= 8:
            score -= 2
            notes.append("range too wide")

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

    if pd.notna(close_pos):
        if close_pos >= 0.75:
            score += 1
            notes.append("closed near high")
        elif close_pos < 0.50:
            score -= 1
            notes.append("weak first15 close position")

    if (
        pd.notna(prior) and prior >= 3
        and pd.notna(pm) and pm <= 0.003
        and pd.notna(ret) and 2 <= ret < 8
        and pd.notna(rng) and 2 <= rng < 8
    ):
        label = "A+ clean prior-close shock"
    elif (
        pd.notna(prior) and prior >= 3
        and pd.notna(pm) and pm <= 0.01
        and pd.notna(ret) and 2 <= ret < 8
        and pd.notna(rng) and 2 <= rng < 8
    ):
        label = "A clean"
    elif (
        pd.notna(prior) and prior >= 1.5
        and pd.notna(pm) and pm <= 0.03
        and pd.notna(ret) and 2 <= ret < 8
        and pd.notna(rng) and 2 <= rng < 8
    ):
        label = "B active but valid"
    elif pd.notna(ret) and ret >= 8:
        label = "DOWNRANK first15 too hot"
    elif pd.notna(pm) and pm > 0.03:
        label = "DOWNRANK active pre-market"
    else:
        label = "watchlist only"

    return label, score, "; ".join(notes)


def main() -> None:
    if not SIGNALS_PATH.exists():
        raise SystemExit(f"Missing signal file: {SIGNALS_PATH}")

    if not CONTEXT_PATH.exists():
        raise SystemExit(f"Missing context file: {CONTEXT_PATH}")

    sig = pd.read_csv(SIGNALS_PATH)
    ctx = pd.read_csv(CONTEXT_PATH)

    sig["trade_date"] = to_date(sig["trade_date"])
    ctx["trade_date"] = to_date(ctx["trade_date"])

    prior_cols = [
        "ticker",
        "trade_date",
        "prior_trade_date_for_last15",
        "prior_last15_days_used",
        "prior_day_last15_dollar_rvol_20d",
        "prior_day_last15_volume_rvol_20d",
        "prior_day_last15_return_pct",
    ]

    prior_cols = [c for c in prior_cols if c in ctx.columns]

    ctx_small = (
        ctx[prior_cols]
        .drop_duplicates(subset=["ticker", "trade_date"])
        .copy()
    )

    df = sig.merge(ctx_small, on=["ticker", "trade_date"], how="left")

    for col in df.columns:
        if any(x in col.lower() for x in ["pct", "rvol", "volume", "dollar", "range", "position", "gap"]):
            df[col] = pd.to_numeric(df[col], errors="coerce")

    required = [
        "prior_day_last15_dollar_rvol_20d",
        "premarket_dollar_vs_prior_daily_avg",
        "first_15m_return_pct",
        "first15_range_pct",
    ]

    missing = [c for c in required if c not in df.columns]
    if missing:
        print("Missing columns:", missing)
        print("Available columns:")
        print(df.columns.tolist())
        raise SystemExit(1)

    df["prior_last15_bucket"] = df["prior_day_last15_dollar_rvol_20d"].map(label_prior_last15)
    df["premarket_bucket"] = df["premarket_dollar_vs_prior_daily_avg"].map(label_premarket)

    labels = df.apply(classify, axis=1, result_type="expand")
    df["context_signal"] = labels[0]
    df["context_score"] = labels[1]
    df["context_notes"] = labels[2]

    cols = [
        "trade_date",
        "ticker",
        "signal_quality",
        "context_signal",
        "context_score",
        "prior_last15_bucket",
        "premarket_bucket",
        "prior_day_last15_dollar_rvol_20d",
        "premarket_dollar_vs_prior_daily_avg",
        "first15_dollar_vs_prior_daily_avg",
        "first15_dollar_rvol_20d",
        "first_15m_return_pct",
        "first15_range_pct",
        "first15_close_position_in_range",
        "gap_pct",
        "context_notes",
    ]

    cols = [c for c in cols if c in df.columns]

    out = df.sort_values(["trade_date", "context_score"], ascending=[True, False])[cols]
    out.to_csv(OUT_PATH, index=False)

    print("signals:", SIGNALS_PATH)
    print("context:", CONTEXT_PATH)
    print("rows:", len(out))
    print()
    print("=== Counts ===")
    print(out["context_signal"].value_counts(dropna=False).to_string())
    print()
    print("=== Ranked past-week signals ===")
    print(out.to_string(index=False))
    print()
    print("saved:", OUT_PATH)


if __name__ == "__main__":
    main()

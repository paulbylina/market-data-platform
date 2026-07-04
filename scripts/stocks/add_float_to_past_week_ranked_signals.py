from __future__ import annotations

from pathlib import Path
import pandas as pd
import numpy as np


SIGNALS_PATH = Path(
    "data/research/full_market_scanner_10y/high_price_full_universe_first15_checks/"
    "past_week_prior_last15_direct_ranked_signals_2026-06-29_to_2026-07-02.csv"
)

REFERENCE_DIR = Path("data/reference/stocks")

OUT_PATH = Path(
    "data/research/full_market_scanner_10y/high_price_full_universe_first15_checks/"
    "past_week_prior_last15_direct_ranked_signals_with_float_2026-06-29_to_2026-07-02.csv"
)


FLOAT_COLS = [
    "ticker",
    "float",
    "free_float_percent",
    "fund_float_effective_date",
    "short_interest_pct_float",
    "market_cap",
    "basic_shares_outstanding",
    "diluted_shares_outstanding",
    "shares_outstanding",
    "float_status",
    "free_float_market_cap",
    "pre_market_volume_to_float",
    "first_15m_volume_to_float",
    "today_volume_to_float",
    "risk_low_float_under_10m",
    "risk_tiny_float_under_3m",
    "risk_micro_free_float_mcap_under_25m",
    "risk_pre_market_volume_gt_50pct_float",
    "risk_pre_market_volume_gt_float",
    "risk_pre_market_volume_gt_2x_float",
    "risk_first15_volume_gt_10pct_float",
    "risk_first15_volume_gt_25pct_float",
    "risk_short_interest_gt_10pct_float",
    "risk_short_interest_gt_20pct_float",
]


def float_bucket(x: float) -> str:
    if pd.isna(x):
        return "missing"
    if x < 3_000_000:
        return "<3M tiny"
    if x < 10_000_000:
        return "3M-10M low"
    if x < 30_000_000:
        return "10M-30M moderate-low"
    if x < 100_000_000:
        return "30M-100M medium"
    if x < 300_000_000:
        return "100M-300M large"
    return "300M+ mega"


def pick_best_reference_file(signal_tickers: set[str]) -> tuple[Path, pd.DataFrame, int]:
    candidates = []

    for path in REFERENCE_DIR.glob("*.csv"):
        try:
            df = pd.read_csv(path)
        except Exception:
            continue

        if "ticker" not in df.columns:
            continue

        useful_cols = [c for c in df.columns if c in FLOAT_COLS]

        if "float" not in df.columns and "shares_outstanding" not in df.columns:
            continue

        tmp = df.copy()
        tmp["ticker"] = tmp["ticker"].astype(str).str.upper()

        coverage = tmp["ticker"].isin(signal_tickers).sum()

        if coverage > 0:
            candidates.append((coverage, path, tmp[useful_cols].copy()))

    if not candidates:
        raise SystemExit("No reference file covered any signal tickers.")

    candidates.sort(key=lambda x: x[0], reverse=True)
    coverage, path, df = candidates[0]
    return path, df, coverage


def main() -> None:
    if not SIGNALS_PATH.exists():
        raise SystemExit(f"Missing signals file: {SIGNALS_PATH}")

    sig = pd.read_csv(SIGNALS_PATH)
    sig["ticker"] = sig["ticker"].astype(str).str.upper()

    signal_tickers = set(sig["ticker"].dropna().unique())

    ref_path, ref, coverage = pick_best_reference_file(signal_tickers)

    ref["ticker"] = ref["ticker"].astype(str).str.upper()
    ref = ref.drop_duplicates(subset=["ticker"], keep="first")

    out = sig.merge(ref, on="ticker", how="left")

    for col in [
        "float",
        "free_float_percent",
        "short_interest_pct_float",
        "market_cap",
        "shares_outstanding",
        "free_float_market_cap",
        "pre_market_volume_to_float",
        "first_15m_volume_to_float",
        "today_volume_to_float",
    ]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    if "float" in out.columns:
        out["float_bucket"] = out["float"].map(float_bucket)
    else:
        out["float_bucket"] = "missing"

    # Add simple float/risk score. This is ranking context, not a proven historical rule yet.
    out["float_context_score"] = 0

    if "float" in out.columns:
        out.loc[out["float"] < 10_000_000, "float_context_score"] -= 2
        out.loc[(out["float"] >= 10_000_000) & (out["float"] < 30_000_000), "float_context_score"] -= 1
        out.loc[(out["float"] >= 30_000_000) & (out["float"] < 300_000_000), "float_context_score"] += 1

    if "first_15m_volume_to_float" in out.columns:
        out.loc[out["first_15m_volume_to_float"] >= 0.25, "float_context_score"] -= 2
        out.loc[
            (out["first_15m_volume_to_float"] >= 0.10)
            & (out["first_15m_volume_to_float"] < 0.25),
            "float_context_score",
        ] -= 1

    if "short_interest_pct_float" in out.columns:
        out.loc[out["short_interest_pct_float"] >= 20, "float_context_score"] -= 1

    if "context_score" in out.columns:
        out["context_score_with_float"] = out["context_score"] + out["float_context_score"]

    show_cols = [
        "trade_date",
        "ticker",
        "context_signal",
        "context_score",
        "float_context_score",
        "context_score_with_float",
        "float_bucket",
        "float",
        "market_cap",
        "free_float_percent",
        "short_interest_pct_float",
        "pre_market_volume_to_float",
        "first_15m_volume_to_float",
        "prior_day_last15_dollar_rvol_20d",
        "premarket_dollar_vs_prior_daily_avg",
        "first15_dollar_rvol_20d",
        "first_15m_return_pct",
        "first15_range_pct",
        "first15_close_position_in_range",
    ]

    show_cols = [c for c in show_cols if c in out.columns]

    sort_cols = ["trade_date"]
    ascending = [True]

    if "context_score_with_float" in out.columns:
        sort_cols.append("context_score_with_float")
        ascending.append(False)
    elif "context_score" in out.columns:
        sort_cols.append("context_score")
        ascending.append(False)

    out = out.sort_values(sort_cols, ascending=ascending)
    out.to_csv(OUT_PATH, index=False)

    print("signals:", SIGNALS_PATH)
    print("best reference file:", ref_path)
    print("signal tickers:", len(signal_tickers))
    print("covered tickers:", coverage)
    print("saved:", OUT_PATH)
    print()
    print("=== Ranked signals with float context ===")
    print(out[show_cols].to_string(index=False))


if __name__ == "__main__":
    main()

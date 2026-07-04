from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


DEFAULT_INPUT = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_expanded_features/"
    "old_research_prior_day_context_with_prior_last15_2024-01-01_to_2026-07-02.csv"
)


def bucket_prior_last15(s: pd.Series) -> pd.Series:
    return pd.cut(
        pd.to_numeric(s, errors="coerce"),
        bins=[-np.inf, 0.75, 1.50, 3.00, 5.00, np.inf],
        labels=["<=0.75 quiet", "0.75-1.5 normal", "1.5-3 active", "3-5 hot", "5+ extreme"],
    )


def bucket_premarket(s: pd.Series) -> pd.Series:
    return pd.cut(
        pd.to_numeric(s, errors="coerce"),
        bins=[-np.inf, 0.003, 0.01, 0.03, 0.10, np.inf],
        labels=["<=0.003 dead", "0.003-0.01 quiet", "0.01-0.03 mild", "0.03-0.10 active", "0.10+ mania"],
    )


def bucket_first15_dollar_vs_daily(s: pd.Series) -> pd.Series:
    return pd.cut(
        pd.to_numeric(s, errors="coerce"),
        bins=[-np.inf, 0.03, 0.05, 0.10, 0.20, 0.50, np.inf],
        labels=["<=0.03 weak", "0.03-0.05 low", "0.05-0.10 good", "0.10-0.20 strong", "0.20-0.50 very strong", "0.50+ extreme"],
    )


def bucket_first15_return(s: pd.Series) -> pd.Series:
    return pd.cut(
        pd.to_numeric(s, errors="coerce"),
        bins=[-np.inf, 1, 2, 4, 8, np.inf],
        labels=["<1 weak", "1-2 good", "2-4 strong", "4-8 very strong", "8+ too hot"],
    )


def bucket_first15_range(s: pd.Series) -> pd.Series:
    return pd.cut(
        pd.to_numeric(s, errors="coerce"),
        bins=[-np.inf, 2, 4, 8, np.inf],
        labels=["<2 tight", "2-4 good", "4-8 wide", "8+ too wide"],
    )


def bucket_close_position(s: pd.Series) -> pd.Series:
    return pd.cut(
        pd.to_numeric(s, errors="coerce"),
        bins=[-np.inf, 0.50, 0.75, 0.90, np.inf],
        labels=["bottom/mid", "upper half", "near high", "at high"],
    )


def summarize(df: pd.DataFrame, group_cols: list[str], metric: str, min_rows: int) -> pd.DataFrame:
    rows = []

    for keys, g in df.groupby(group_cols, observed=True):
        if not isinstance(keys, tuple):
            keys = (keys,)

        vals = pd.to_numeric(g[metric], errors="coerce").dropna()

        if len(vals) < min_rows:
            continue

        row = {col: str(val) for col, val in zip(group_cols, keys)}
        row.update(
            {
                "rows": len(vals),
                "tickers": g["ticker"].nunique() if "ticker" in g.columns else np.nan,
                "metric": metric,
                "avg": vals.mean(),
                "median": vals.median(),
                "win_rate": (vals > 0).mean() * 100,
                "pct_ge_1": (vals >= 1).mean() * 100,
                "pct_ge_2": (vals >= 2).mean() * 100,
                "pct_le_minus_2": (vals <= -2).mean() * 100,
                "pct_le_minus_3": (vals <= -3).mean() * 100,
                "best": vals.max(),
                "worst": vals.min(),
            }
        )
        rows.append(row)

    return pd.DataFrame(rows)


def add_ratio_if_missing(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    for c in out.columns:
        if any(x in c.lower() for x in ["volume", "dollar", "pct", "rvol", "range", "position"]):
            out[c] = pd.to_numeric(out[c], errors="coerce")

    if (
        "premarket_dollar_vs_prior_daily_avg" not in out.columns
        and {"premarket_dollar_volume", "avg_dollar_volume_20d_prior"}.issubset(out.columns)
    ):
        out["premarket_dollar_vs_prior_daily_avg"] = np.where(
            out["avg_dollar_volume_20d_prior"] > 0,
            out["premarket_dollar_volume"] / out["avg_dollar_volume_20d_prior"],
            np.nan,
        )

    if (
        "first15_dollar_vs_prior_daily_avg" not in out.columns
        and {"first_15m_dollar_volume", "avg_dollar_volume_20d_prior"}.issubset(out.columns)
    ):
        out["first15_dollar_vs_prior_daily_avg"] = np.where(
            out["avg_dollar_volume_20d_prior"] > 0,
            out["first_15m_dollar_volume"] / out["avg_dollar_volume_20d_prior"],
            np.nan,
        )

    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=str(DEFAULT_INPUT))
    parser.add_argument("--metric", default="long_eod_pct")
    parser.add_argument("--min-rows", type=int, default=50)
    args = parser.parse_args()

    path = Path(args.input)
    df = pd.read_csv(path)
    df = add_ratio_if_missing(df)

    needed = ["prior_day_last15_dollar_rvol_20d", args.metric]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise SystemExit(f"Missing required columns: {missing}")

    df["prior_last15_bucket"] = bucket_prior_last15(df["prior_day_last15_dollar_rvol_20d"])

    if "premarket_dollar_vs_prior_daily_avg" in df.columns:
        df["premarket_bucket"] = bucket_premarket(df["premarket_dollar_vs_prior_daily_avg"])

    if "first15_dollar_vs_prior_daily_avg" in df.columns:
        df["first15_volume_bucket"] = bucket_first15_dollar_vs_daily(df["first15_dollar_vs_prior_daily_avg"])

    if "first_15m_return_pct" in df.columns:
        df["first15_return_bucket"] = bucket_first15_return(df["first_15m_return_pct"])

    if "first15_range_pct" in df.columns:
        df["first15_range_bucket"] = bucket_first15_range(df["first15_range_pct"])

    if "first15_close_position_in_range" in df.columns:
        df["first15_close_position_bucket"] = bucket_close_position(df["first15_close_position_in_range"])

    available_contexts = [
        "premarket_bucket",
        "first15_volume_bucket",
        "first15_return_bucket",
        "first15_range_bucket",
        "first15_close_position_bucket",
    ]

    available_contexts = [c for c in available_contexts if c in df.columns]

    print("input:", path)
    print("rows:", len(df))
    print("metric:", args.metric)
    print("available context buckets:", available_contexts)
    print()

    outputs = []

    # Prior last15 alone.
    outputs.append(
        ("prior_only", summarize(df, ["prior_last15_bucket"], args.metric, args.min_rows))
    )

    # Prior last15 crossed with each individual context.
    for ctx in available_contexts:
        outputs.append(
            (f"prior_x_{ctx}", summarize(df, ["prior_last15_bucket", ctx], args.metric, args.min_rows))
        )

    # Most important combined context: pre-market + first15 volume + prior last15.
    if {"premarket_bucket", "first15_volume_bucket"}.issubset(df.columns):
        outputs.append(
            (
                "prior_x_premarket_x_first15_volume",
                summarize(
                    df,
                    ["prior_last15_bucket", "premarket_bucket", "first15_volume_bucket"],
                    args.metric,
                    args.min_rows,
                ),
            )
        )

    # Structure context if available.
    if {"premarket_bucket", "first15_return_bucket"}.issubset(df.columns):
        outputs.append(
            (
                "prior_x_premarket_x_first15_return",
                summarize(
                    df,
                    ["prior_last15_bucket", "premarket_bucket", "first15_return_bucket"],
                    args.metric,
                    args.min_rows,
                ),
            )
        )

    out_dir = path.parent

    for name, out in outputs:
        if out.empty:
            print(f"=== {name}: no groups with min_rows={args.min_rows} ===")
            continue

        out_path = out_dir / f"{path.stem}_{name}_{args.metric}.csv"
        out.to_csv(out_path, index=False)

        print(f"=== {name} ===")
        sort_cols = ["median", "win_rate", "rows"]
        print(
            out.sort_values(sort_cols, ascending=[False, False, False])
            .head(30)
            .to_string(index=False)
        )
        print("saved:", out_path)
        print()


if __name__ == "__main__":
    main()

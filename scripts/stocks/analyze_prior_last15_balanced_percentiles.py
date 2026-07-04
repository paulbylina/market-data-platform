from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


DEFAULT_DIR = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_expanded_features"
)


def find_latest_enriched() -> Path:
    files = [
        p for p in DEFAULT_DIR.glob("old_research_prior_day_context_with_prior_last15_*.csv")
        if "_summary_" not in p.name
        and "_fixed_bucket_" not in p.name
        and "_percentile_" not in p.name
        and "_balanced_" not in p.name
    ]
    files = sorted(files, key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        raise SystemExit(
            "No prior-last15 enriched file found. Run the prior-last15 script first."
        )
    return files[0]


def bucket_prior_last15_fixed(s: pd.Series) -> pd.Series:
    return pd.cut(
        pd.to_numeric(s, errors="coerce"),
        bins=[-np.inf, 0.75, 1.50, 3.00, 5.00, np.inf],
        labels=["<=0.75 quiet", "0.75-1.5 normal", "1.5-3 active", "3-5 hot", "5+ extreme"],
    )


def metric_stats(g: pd.DataFrame, metric: str) -> dict:
    vals = pd.to_numeric(g[metric], errors="coerce").dropna()

    if vals.empty:
        return {
            "rows": 0,
            "avg": np.nan,
            "median": np.nan,
            "win_rate": np.nan,
            "pct_ge_1": np.nan,
            "pct_ge_2": np.nan,
            "pct_le_minus_2": np.nan,
            "pct_le_minus_3": np.nan,
            "best": np.nan,
            "worst": np.nan,
        }

    return {
        "rows": len(vals),
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


def summarize(df: pd.DataFrame, bucket_col: str, metric: str, label: str) -> pd.DataFrame:
    rows = []

    for bucket, g in df.groupby(bucket_col, observed=True):
        st = metric_stats(g, metric)
        rvol = pd.to_numeric(g["prior_day_last15_dollar_rvol_20d"], errors="coerce")

        rows.append(
            {
                "analysis": label,
                "bucket": str(bucket),
                "bucket_rows": len(g),
                "tickers": g["ticker"].nunique() if "ticker" in g.columns else np.nan,
                "rvol_min": rvol.min(),
                "rvol_median": rvol.median(),
                "rvol_max": rvol.max(),
                "metric": metric,
                **st,
            }
        )

    return pd.DataFrame(rows)


def balanced_bootstrap(
    df: pd.DataFrame,
    bucket_col: str,
    metric: str,
    sample_size: int | None,
    iterations: int,
    seed: int,
) -> pd.DataFrame:
    rng = np.random.default_rng(seed)

    buckets = []
    for bucket, g in df.groupby(bucket_col, observed=True):
        g = g[pd.to_numeric(g[metric], errors="coerce").notna()].copy()
        if len(g) > 0:
            buckets.append((str(bucket), g))

    if not buckets:
        return pd.DataFrame()

    min_rows = min(len(g) for _, g in buckets)
    n = sample_size if sample_size is not None else min_rows

    if n > min_rows:
        raise SystemExit(f"Requested sample size {n}, but smallest bucket has only {min_rows} rows.")

    sim_rows = []

    for i in range(iterations):
        for bucket, g in buckets:
            idx = rng.choice(g.index.to_numpy(), size=n, replace=False)
            sample = g.loc[idx]
            st = metric_stats(sample, metric)

            sim_rows.append(
                {
                    "iteration": i,
                    "bucket": bucket,
                    "sample_size": n,
                    "metric": metric,
                    "avg": st["avg"],
                    "median": st["median"],
                    "win_rate": st["win_rate"],
                    "pct_ge_1": st["pct_ge_1"],
                    "pct_ge_2": st["pct_ge_2"],
                    "pct_le_minus_2": st["pct_le_minus_2"],
                    "pct_le_minus_3": st["pct_le_minus_3"],
                }
            )

    sims = pd.DataFrame(sim_rows)

    agg_rows = []

    for bucket, g in sims.groupby("bucket", observed=True):
        row = {
            "analysis": "balanced_random_88_repeated",
            "bucket": bucket,
            "metric": metric,
            "sample_size": n,
            "iterations": iterations,
        }

        for col in [
            "avg",
            "median",
            "win_rate",
            "pct_ge_1",
            "pct_ge_2",
            "pct_le_minus_2",
            "pct_le_minus_3",
        ]:
            row[f"{col}_mean"] = g[col].mean()
            row[f"{col}_std"] = g[col].std()
            row[f"{col}_p05"] = g[col].quantile(0.05)
            row[f"{col}_p95"] = g[col].quantile(0.95)

        agg_rows.append(row)

    return pd.DataFrame(agg_rows)


def add_percentile_bucket(df: pd.DataFrame, quantiles: int) -> pd.DataFrame:
    out = df.copy()
    r = pd.to_numeric(out["prior_day_last15_dollar_rvol_20d"], errors="coerce")

    # Rank first so duplicate RVOL values do not break qcut.
    ranks = r.rank(method="first")
    q = pd.qcut(ranks, q=quantiles, duplicates="drop")

    out["prior_last15_percentile_bucket"] = q.astype(str)

    # Add cleaner labels with actual RVOL ranges.
    labels = {}
    for bucket, g in out.groupby("prior_last15_percentile_bucket", observed=True):
        rv = pd.to_numeric(g["prior_day_last15_dollar_rvol_20d"], errors="coerce")
        labels[bucket] = f"{bucket} | RVOL {rv.min():.2f} to {rv.max():.2f}"

    out["prior_last15_percentile_bucket"] = out["prior_last15_percentile_bucket"].map(labels)

    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=None)
    parser.add_argument("--metric", default="long_eod_pct")
    parser.add_argument("--iterations", type=int, default=1000)
    parser.add_argument("--sample-size", type=int, default=None)
    parser.add_argument("--quantiles", type=int, default=5)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    path = Path(args.input) if args.input else find_latest_enriched()

    df = pd.read_csv(path)

    needed = ["prior_day_last15_dollar_rvol_20d", args.metric]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise SystemExit(f"Missing columns: {missing}")

    df["prior_day_last15_dollar_rvol_20d"] = pd.to_numeric(
        df["prior_day_last15_dollar_rvol_20d"], errors="coerce"
    )
    df[args.metric] = pd.to_numeric(df[args.metric], errors="coerce")

    df = df[
        df["prior_day_last15_dollar_rvol_20d"].notna()
        & df[args.metric].notna()
    ].copy()

    df["prior_last15_fixed_bucket"] = bucket_prior_last15_fixed(
        df["prior_day_last15_dollar_rvol_20d"]
    )

    percentile_df = add_percentile_bucket(df, args.quantiles)

    fixed_summary = summarize(
        df,
        bucket_col="prior_last15_fixed_bucket",
        metric=args.metric,
        label="fixed_rvol_buckets",
    )

    percentile_summary = summarize(
        percentile_df,
        bucket_col="prior_last15_percentile_bucket",
        metric=args.metric,
        label=f"equal_count_percentile_buckets_q{args.quantiles}",
    )

    balanced_summary = balanced_bootstrap(
        df,
        bucket_col="prior_last15_fixed_bucket",
        metric=args.metric,
        sample_size=args.sample_size,
        iterations=args.iterations,
        seed=args.seed,
    )

    stem = path.stem
    out_fixed = path.with_name(f"{stem}_fixed_bucket_summary_{args.metric}.csv")
    out_pct = path.with_name(f"{stem}_percentile_q{args.quantiles}_summary_{args.metric}.csv")
    out_bal = path.with_name(f"{stem}_balanced_bootstrap_summary_{args.metric}.csv")

    fixed_summary.to_csv(out_fixed, index=False)
    percentile_summary.to_csv(out_pct, index=False)
    balanced_summary.to_csv(out_bal, index=False)

    print("input:", path)
    print("rows used:", len(df))
    print("metric:", args.metric)
    print()

    print("=== Fixed RVOL buckets ===")
    print(
        fixed_summary[
            [
                "bucket",
                "bucket_rows",
                "rvol_min",
                "rvol_median",
                "rvol_max",
                "median",
                "win_rate",
                "pct_ge_1",
                "pct_ge_2",
                "pct_le_minus_2",
                "pct_le_minus_3",
            ]
        ].to_string(index=False)
    )

    print()
    print(f"=== Equal-count percentile buckets q={args.quantiles} ===")
    print(
        percentile_summary[
            [
                "bucket",
                "bucket_rows",
                "rvol_min",
                "rvol_median",
                "rvol_max",
                "median",
                "win_rate",
                "pct_ge_1",
                "pct_ge_2",
                "pct_le_minus_2",
                "pct_le_minus_3",
            ]
        ].to_string(index=False)
    )

    print()
    print("=== Balanced repeated sample from fixed buckets ===")
    print(
        balanced_summary[
            [
                "bucket",
                "sample_size",
                "iterations",
                "median_mean",
                "median_p05",
                "median_p95",
                "win_rate_mean",
                "win_rate_p05",
                "win_rate_p95",
                "pct_ge_2_mean",
                "pct_le_minus_3_mean",
            ]
        ].to_string(index=False)
    )

    print()
    print("saved fixed:", out_fixed)
    print("saved percentile:", out_pct)
    print("saved balanced:", out_bal)


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
import re
from pathlib import Path

import numpy as np
import pandas as pd


FEATURE_DIR = Path("data/research/full_market_scanner_10y/high_price_full_universe_first15_checks")


def pearson_corr(x, y):
    temp = pd.DataFrame({"x": x, "y": y}).replace([np.inf, -np.inf], np.nan).dropna()
    if len(temp) < 3:
        return np.nan
    return temp["x"].corr(temp["y"])


def spearman_corr(x, y):
    temp = pd.DataFrame({"x": x, "y": y}).replace([np.inf, -np.inf], np.nan).dropna()
    if len(temp) < 3:
        return np.nan
    return temp["x"].rank(method="average").corr(temp["y"].rank(method="average"))


def log_corr(x, y):
    temp = pd.DataFrame({"x": x, "y": y}).replace([np.inf, -np.inf], np.nan).dropna()
    temp = temp[(temp["x"] > 0) & (temp["y"] > 0)]
    if len(temp) < 3:
        return np.nan
    return np.log1p(temp["x"]).corr(np.log1p(temp["y"]))


def find_feature_files(start_date: str, end_date: str) -> list[Path]:
    by_date = {}

    for path in FEATURE_DIR.glob("high_price_full_universe_first15_features_*.csv"):
        m = re.search(r"features_(\d{4}-\d{2}-\d{2})(?:_with_first15_rvol)?\.csv$", path.name)
        if not m:
            continue

        date = m.group(1)
        if not (start_date <= date <= end_date):
            continue

        # Prefer the richer file if it exists.
        current = by_date.get(date)
        if current is None:
            by_date[date] = path
        elif "_with_first15_rvol" in path.name:
            by_date[date] = path

    return [by_date[d] for d in sorted(by_date)]


def add_ratios(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    numeric_cols = [
        "prev_close",
        "volume",
        "dollar_volume",
        "volume_rvol_20d",
        "dollar_volume_rvol_20d",
        "avg_volume_20d_prior",
        "avg_dollar_volume_20d_prior",
        "premarket_volume",
        "premarket_dollar_volume",
        "first_15m_volume",
        "first_15m_dollar_volume",
        "first15_volume_rvol_20d",
        "first15_dollar_rvol_20d",
    ]

    for col in numeric_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    out["premarket_volume_vs_prior_daily_avg"] = np.where(
        out["avg_volume_20d_prior"] > 0,
        out["premarket_volume"] / out["avg_volume_20d_prior"],
        np.nan,
    )

    out["premarket_dollar_vs_prior_daily_avg"] = np.where(
        out["avg_dollar_volume_20d_prior"] > 0,
        out["premarket_dollar_volume"] / out["avg_dollar_volume_20d_prior"],
        np.nan,
    )

    out["first15_volume_vs_prior_daily_avg"] = np.where(
        out["avg_volume_20d_prior"] > 0,
        out["first_15m_volume"] / out["avg_volume_20d_prior"],
        np.nan,
    )

    out["first15_dollar_vs_prior_daily_avg"] = np.where(
        out["avg_dollar_volume_20d_prior"] > 0,
        out["first_15m_dollar_volume"] / out["avg_dollar_volume_20d_prior"],
        np.nan,
    )

    out["early_volume_vs_prior_daily_avg"] = np.where(
        out["avg_volume_20d_prior"] > 0,
        (out["premarket_volume"] + out["first_15m_volume"]) / out["avg_volume_20d_prior"],
        np.nan,
    )

    out["early_dollar_vs_prior_daily_avg"] = np.where(
        out["avg_dollar_volume_20d_prior"] > 0,
        (out["premarket_dollar_volume"] + out["first_15m_dollar_volume"]) / out["avg_dollar_volume_20d_prior"],
        np.nan,
    )

    return out


def stats_for_filter(df: pd.DataFrame, mask, label: str) -> dict:
    sub = df[mask].copy()

    if len(sub) == 0:
        return {
            "filter": label,
            "rows": 0,
            "median_daily_dollar_rvol": np.nan,
            "avg_daily_dollar_rvol": np.nan,
            "pct_daily_dollar_rvol_ge_1_5": np.nan,
            "pct_daily_dollar_rvol_ge_2": np.nan,
            "pct_daily_dollar_rvol_ge_3": np.nan,
            "pct_daily_dollar_rvol_ge_5": np.nan,
        }

    return {
        "filter": label,
        "rows": len(sub),
        "median_daily_dollar_rvol": sub["dollar_volume_rvol_20d"].median(),
        "avg_daily_dollar_rvol": sub["dollar_volume_rvol_20d"].mean(),
        "pct_daily_dollar_rvol_ge_1_5": (sub["dollar_volume_rvol_20d"] >= 1.5).mean() * 100,
        "pct_daily_dollar_rvol_ge_2": (sub["dollar_volume_rvol_20d"] >= 2).mean() * 100,
        "pct_daily_dollar_rvol_ge_3": (sub["dollar_volume_rvol_20d"] >= 3).mean() * 100,
        "pct_daily_dollar_rvol_ge_5": (sub["dollar_volume_rvol_20d"] >= 5).mean() * 100,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    args = parser.parse_args()

    files = find_feature_files(args.start_date, args.end_date)

    if not files:
        raise SystemExit(
            f"No feature files found in {FEATURE_DIR} for {args.start_date} to {args.end_date}"
        )

    frames = []

    print("files:")
    for path in files:
        print(" ", path)
        df = pd.read_csv(path)
        m = re.search(r"features_(\d{4}-\d{2}-\d{2})", path.name)
        if m:
            df["feature_file_date"] = m.group(1)
        frames.append(df)

    df = pd.concat(frames, ignore_index=True)
    df = add_ratios(df)

    if "download_status" in df.columns:
        df = df[df["download_status"].eq("ok")].copy()

    base = df[
        (pd.to_numeric(df["prev_close"], errors="coerce") >= 50)
        & (pd.to_numeric(df["dollar_volume_rvol_20d"], errors="coerce") > 0)
        & (pd.to_numeric(df["volume_rvol_20d"], errors="coerce") > 0)
    ].copy()

    print()
    print("rows used:", len(base))
    print("tickers:", base["ticker"].nunique())
    print("dates:", base["trade_date"].nunique() if "trade_date" in base.columns else base["feature_file_date"].nunique())

    pairs = [
        ("premarket_dollar_vs_prior_daily_avg", "dollar_volume_rvol_20d"),
        ("premarket_volume_vs_prior_daily_avg", "volume_rvol_20d"),
        ("first15_dollar_vs_prior_daily_avg", "dollar_volume_rvol_20d"),
        ("first15_volume_vs_prior_daily_avg", "volume_rvol_20d"),
        ("early_dollar_vs_prior_daily_avg", "dollar_volume_rvol_20d"),
        ("early_volume_vs_prior_daily_avg", "volume_rvol_20d"),
    ]

    if "first15_dollar_rvol_20d" in base.columns and base["first15_dollar_rvol_20d"].notna().sum() > 0:
        pairs.append(("first15_dollar_rvol_20d", "dollar_volume_rvol_20d"))

    corr_rows = []

    for x, y in pairs:
        corr_rows.append(
            {
                "x": x,
                "y": y,
                "rows": pd.DataFrame({"x": base[x], "y": base[y]}).dropna().shape[0],
                "pearson": pearson_corr(base[x], base[y]),
                "spearman": spearman_corr(base[x], base[y]),
                "log_corr": log_corr(base[x], base[y]),
            }
        )

    corr = pd.DataFrame(corr_rows)

    print()
    print("=== Correlations ===")
    print(corr.to_string(index=False))

    lift_rows = []

    lift_rows.append(stats_for_filter(base, base.index == base.index, "ALL $50+ baseline"))

    for th in [0.001, 0.003, 0.005, 0.01, 0.03, 0.05, 0.10, 0.20]:
        lift_rows.append(
            stats_for_filter(
                base,
                base["premarket_dollar_vs_prior_daily_avg"] >= th,
                f"pre-market dollar/prior daily >= {th}",
            )
        )

    for th in [0.01, 0.03, 0.05, 0.10, 0.20, 0.30, 0.50]:
        lift_rows.append(
            stats_for_filter(
                base,
                base["first15_dollar_vs_prior_daily_avg"] >= th,
                f"first15 dollar/prior daily >= {th}",
            )
        )

    for th in [0.01, 0.03, 0.05, 0.10, 0.20, 0.30, 0.50]:
        lift_rows.append(
            stats_for_filter(
                base,
                base["early_dollar_vs_prior_daily_avg"] >= th,
                f"pre-market + first15 dollar/prior daily >= {th}",
            )
        )

    # This checks your exact idea:
    # early volume appears in pre-market, then opening volume confirms.
    for pm_th in [0.003, 0.005, 0.01, 0.03, 0.05]:
        for f15_th in [0.03, 0.05, 0.10, 0.20]:
            lift_rows.append(
                stats_for_filter(
                    base,
                    (base["premarket_dollar_vs_prior_daily_avg"] >= pm_th)
                    & (base["first15_dollar_vs_prior_daily_avg"] >= f15_th),
                    f"pre-market >= {pm_th} + first15 >= {f15_th}",
                )
            )

    # This checks our current long setup idea:
    # quiet pre-market, sudden opening volume.
    for pm_max in [0.003, 0.005, 0.01, 0.03, 0.05, 0.10]:
        for f15_th in [0.03, 0.05, 0.10, 0.20]:
            lift_rows.append(
                stats_for_filter(
                    base,
                    (base["premarket_dollar_vs_prior_daily_avg"] <= pm_max)
                    & (base["first15_dollar_vs_prior_daily_avg"] >= f15_th),
                    f"quiet pre-market <= {pm_max} + first15 >= {f15_th}",
                )
            )

    lift = pd.DataFrame(lift_rows)
    lift = lift.sort_values(
        ["pct_daily_dollar_rvol_ge_2", "median_daily_dollar_rvol", "rows"],
        ascending=[False, False, False],
    )

    out_dir = FEATURE_DIR
    corr_path = out_dir / f"early_volume_daily_volume_correlation_{args.start_date}_to_{args.end_date}.csv"
    lift_path = out_dir / f"early_volume_daily_volume_lift_{args.start_date}_to_{args.end_date}.csv"

    corr.to_csv(corr_path, index=False)
    lift.to_csv(lift_path, index=False)

    print()
    print("=== Lift table sorted by pct daily dollar RVOL >= 2 ===")
    print(lift.head(80).to_string(index=False))

    print()
    print("saved correlation:", corr_path)
    print("saved lift:", lift_path)


if __name__ == "__main__":
    main()

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


def find_time_col(df: pd.DataFrame, kind: str) -> str | None:
    candidates = []
    for c in df.columns:
        cl = c.lower()
        if kind == "runup":
            if "runup" in cl and ("time" in cl or "minute" in cl or "min" in cl):
                candidates.append(c)
        elif kind == "drawdown":
            if ("drawdown" in cl or "dd" in cl) and ("time" in cl or "minute" in cl or "min" in cl):
                candidates.append(c)
    return candidates[0] if candidates else None


def simulate_exit(
    row: pd.Series,
    target: float,
    stop: float,
    runup_col: str,
    drawdown_col: str,
    eod_col: str,
    runup_time_col: str | None,
    drawdown_time_col: str | None,
    both_policy: str,
) -> tuple[float, str]:
    runup = row[runup_col]
    drawdown = row[drawdown_col]
    eod = row[eod_col]

    target_hit = pd.notna(runup) and runup >= target
    stop_hit = pd.notna(drawdown) and drawdown <= -stop

    if target_hit and stop_hit:
        if runup_time_col and drawdown_time_col:
            rt = row[runup_time_col]
            dt = row[drawdown_time_col]

            if pd.notna(rt) and pd.notna(dt):
                if rt < dt:
                    return target, "target"
                if dt < rt:
                    return -stop, "stop"

        if both_policy == "target_first":
            return target, "target"
        if both_policy == "stop_first":
            return -stop, "stop"

        return eod, "eod"

    if target_hit:
        return target, "target"

    if stop_hit:
        return -stop, "stop"

    return eod, "eod"


def summarize(g: pd.DataFrame) -> dict:
    vals = pd.to_numeric(g["net_pct"], errors="coerce").dropna()

    return {
        "rows": len(vals),
        "avg": vals.mean(),
        "median": vals.median(),
        "win_rate": (vals > 0).mean() * 100,
        "target_rate": (g["exit_type"] == "target").mean() * 100,
        "stop_rate": (g["exit_type"] == "stop").mean() * 100,
        "eod_rate": (g["exit_type"] == "eod").mean() * 100,
        "pct_ge_1": (vals >= 1).mean() * 100,
        "pct_ge_2": (vals >= 2).mean() * 100,
        "pct_le_minus_2": (vals <= -2).mean() * 100,
        "pct_le_minus_3": (vals <= -3).mean() * 100,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=str(DEFAULT_INPUT))
    parser.add_argument("--both-policy", choices=["stop_first", "target_first", "eod"], default="stop_first")
    args = parser.parse_args()

    path = Path(args.input)
    df = pd.read_csv(path)

    needed = [
        "prior_day_last15_dollar_rvol_20d",
        "long_max_runup_pct",
        "long_max_drawdown_pct",
        "long_eod_pct",
    ]

    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise SystemExit(f"Missing columns: {missing}")

    for c in needed:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=needed).copy()
    df["prior_last15_bucket"] = bucket_prior_last15(df["prior_day_last15_dollar_rvol_20d"])

    runup_time_col = find_time_col(df, "runup")
    drawdown_time_col = find_time_col(df, "drawdown")

    print("input:", path)
    print("rows used:", len(df))
    print("runup time col:", runup_time_col)
    print("drawdown time col:", drawdown_time_col)
    print("both-hit policy:", args.both_policy)
    print()

    combos = [
        (1.5, 2.0),
        (1.5, 2.5),
        (2.0, 2.5),
        (2.0, 3.0),
        (2.5, 3.0),
        (3.0, 4.0),
        (4.0, 5.0),
    ]

    all_rows = []

    for target, stop in combos:
        tmp = df.copy()

        exits = tmp.apply(
            lambda row: simulate_exit(
                row=row,
                target=target,
                stop=stop,
                runup_col="long_max_runup_pct",
                drawdown_col="long_max_drawdown_pct",
                eod_col="long_eod_pct",
                runup_time_col=runup_time_col,
                drawdown_time_col=drawdown_time_col,
                both_policy=args.both_policy,
            ),
            axis=1,
            result_type="expand",
        )

        tmp["net_pct"] = exits[0]
        tmp["exit_type"] = exits[1]

        for bucket, g in tmp.groupby("prior_last15_bucket", observed=True):
            row = {
                "target_pct": target,
                "stop_pct": stop,
                "bucket": str(bucket),
            }
            row.update(summarize(g))
            all_rows.append(row)

    out = pd.DataFrame(all_rows)

    out_path = path.with_name(
        f"{path.stem}_target_stop_by_prior_last15_{args.both_policy}.csv"
    )
    out.to_csv(out_path, index=False)

    print("=== Target/Stop by prior-day last15 RVOL bucket ===")
    print(
        out.sort_values(["target_pct", "stop_pct", "bucket"])
        .to_string(index=False)
    )

    print()
    print("=== Best by bucket, sorted by median then avg ===")
    best = (
        out.sort_values(["bucket", "median", "avg"], ascending=[True, False, False])
        .groupby("bucket", observed=True)
        .head(3)
    )
    print(best.to_string(index=False))

    print()
    print("saved:", out_path)


if __name__ == "__main__":
    main()

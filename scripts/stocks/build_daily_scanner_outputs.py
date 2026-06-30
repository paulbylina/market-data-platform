from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import pandas as pd


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--confirmed",
        default="data/reference/stocks/confirmed_first_15m_activated_dormant_latest.csv",
    )
    parser.add_argument("--output-dir", default="data/reference/stocks")
    parser.add_argument("--strict-first15-rvol", type=float, default=3.0)
    parser.add_argument("--watch-first15-rvol", type=float, default=1.5)
    parser.add_argument("--min-first15-dollar-volume", type=float, default=100_000)
    parser.add_argument("--strict-premarket-rvol", type=float, default=3.0)
    parser.add_argument("--min-premarket-dollar-volume", type=float, default=100_000)
    args = parser.parse_args()

    confirmed_path = Path(args.confirmed)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(confirmed_path)

    numeric_cols = [
        "gap_pct",
        "today_dollar_volume",
        "today_vs_prev_volume_pct",
        "today_first_15m_volume",
        "avg_prior_first_15m_volume",
        "first_15m_rvol",
        "today_first_15m_dollar_volume",
        "first_15m_return_pct",
        "today_premarket_volume",
        "premarket_rvol",
        "today_premarket_dollar_volume",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["strict_first15"] = (
        (df["first_15m_rvol"] >= args.strict_first15_rvol)
        & (df["today_first_15m_dollar_volume"] >= args.min_first15_dollar_volume)
    )

    df["watch_first15"] = (
        (df["first_15m_rvol"] >= args.watch_first15_rvol)
        & (df["today_first_15m_dollar_volume"] >= args.min_first15_dollar_volume)
    )

    df["strict_premarket"] = (
        (df["premarket_rvol"] >= args.strict_premarket_rvol)
        & (df["today_premarket_dollar_volume"] >= args.min_premarket_dollar_volume)
    )

    df["trade_candidate"] = df["strict_first15"] | df["strict_premarket"]
    df["watchlist_candidate"] = df["watch_first15"] | df["strict_premarket"]

    df["volume_signal"] = "none"
    df.loc[df["watch_first15"], "volume_signal"] = "watch_first15"
    df.loc[df["strict_premarket"], "volume_signal"] = "premarket"
    df.loc[df["strict_first15"], "volume_signal"] = "first15"
    df.loc[
        df["watch_first15"] & df["strict_premarket"],
        "volume_signal",
    ] = "premarket_and_watch_first15"
    df.loc[
        df["strict_first15"] & df["strict_premarket"],
        "volume_signal",
    ] = "premarket_and_first15"

    sort_cols = [
        "trade_candidate",
        "watchlist_candidate",
        "today_first_15m_dollar_volume",
        "first_15m_rvol",
        "today_dollar_volume",
    ]

    df = df.sort_values(sort_cols, ascending=[False, False, False, False, False])

    trade = df[df["trade_candidate"]].copy()
    watch = df[df["watchlist_candidate"]].copy()

    today = date.today().isoformat()

    outputs = {
        f"today_trade_candidates_{today}.csv": trade,
        "today_trade_candidates_latest.csv": trade,
        f"today_watchlist_{today}.csv": watch,
        "today_watchlist_latest.csv": watch,
        f"today_all_confirmed_scanner_rows_{today}.csv": df,
        "today_all_confirmed_scanner_rows_latest.csv": df,
    }

    for filename, out in outputs.items():
        out.to_csv(output_dir / filename, index=False)

    cols = [
        "ticker",
        "volume_signal",
        "prev_close",
        "gap_pct",
        "today_dollar_volume",
        "today_vs_prev_volume_pct",
        "first_15m_rvol",
        "today_first_15m_dollar_volume",
        "first_15m_return_pct",
        "premarket_rvol",
        "today_premarket_dollar_volume",
        "name",
    ]

    cols = [col for col in cols if col in df.columns]

    print("confirmed input:", confirmed_path)
    print()
    print("=== Counts ===")
    print("all rows:", len(df))
    print("trade candidates:", len(trade))
    print("watchlist:", len(watch))

    print()
    print("=== Trade candidates ===")
    if len(trade):
        print(trade[cols].to_string(index=False))
    else:
        print("No trade candidates")

    print()
    print("=== Watchlist ===")
    if len(watch):
        print(watch[cols].to_string(index=False))
    else:
        print("No watchlist candidates")

    print()
    print("saved:")
    for filename in outputs:
        print(output_dir / filename)


if __name__ == "__main__":
    main()

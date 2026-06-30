from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import pandas as pd


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--snapshot",
        default="data/reference/stocks/massive_snapshot_common_stocks_latest.csv",
    )
    parser.add_argument("--output-dir", default="data/reference/stocks")
    parser.add_argument("--min-today-dollar-volume", type=float, default=250_000)
    parser.add_argument("--min-today-vs-prev-volume-pct", type=float, default=200)
    parser.add_argument("--gap-up-pct", type=float, default=1.0)
    args = parser.parse_args()

    snapshot_path = Path(args.snapshot)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(snapshot_path)

    for col in [
        "prev_close",
        "prev_dollar_volume",
        "today_dollar_volume",
        "today_vs_prev_volume_pct",
        "gap_pct",
    ]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    activated = df[
        (df["today_dollar_volume"] >= args.min_today_dollar_volume)
        & (df["today_vs_prev_volume_pct"] >= args.min_today_vs_prev_volume_pct)
    ].copy()

    dormant_activated = activated[
        activated["activity_tier"] == "dormant"
    ].copy()

    gap_up_activated = activated[
        activated["gap_pct"] >= args.gap_up_pct
    ].copy()

    gap_up_dormant_activated = dormant_activated[
        dormant_activated["gap_pct"] >= args.gap_up_pct
    ].copy()

    sort_cols = ["today_dollar_volume", "today_vs_prev_volume_pct"]
    for out in [
        activated,
        dormant_activated,
        gap_up_activated,
        gap_up_dormant_activated,
    ]:
        out.sort_values(sort_cols, ascending=False, inplace=True)

    today = date.today().isoformat()

    outputs = {
        f"activated_common_stocks_{today}.csv": activated,
        "activated_common_stocks_latest.csv": activated,
        f"activated_dormant_common_stocks_{today}.csv": dormant_activated,
        "activated_dormant_common_stocks_latest.csv": dormant_activated,
        f"gap_up_activated_common_stocks_{today}.csv": gap_up_activated,
        "gap_up_activated_common_stocks_latest.csv": gap_up_activated,
        f"gap_up_activated_dormant_common_stocks_{today}.csv": gap_up_dormant_activated,
        "gap_up_activated_dormant_common_stocks_latest.csv": gap_up_dormant_activated,
    }

    for filename, out in outputs.items():
        path = output_dir / filename
        out.to_csv(path, index=False)

    print("snapshot:", snapshot_path)
    print()
    print("=== Candidate counts ===")
    print("activated:", len(activated))
    print("dormant activated:", len(dormant_activated))
    print("gap-up activated:", len(gap_up_activated))
    print("gap-up dormant activated:", len(gap_up_dormant_activated))

    print()
    print("=== Gap-up dormant activated ===")
    cols = [
        "ticker",
        "prev_close",
        "prev_dollar_volume",
        "today_dollar_volume",
        "today_vs_prev_volume_pct",
        "gap_pct",
        "name",
    ]

    print(gap_up_dormant_activated[cols].head(30).to_string(index=False))

    print()
    print("saved:")
    for filename in outputs:
        print(output_dir / filename)


if __name__ == "__main__":
    main()

from pathlib import Path

import pandas as pd


INPUT_PATH = Path(
    "data/research/full_market_scanner_10y/daily_regime_event_tasks/extended_hours_1m_regime_tasks.csv"
)

OUTPUT_DIR = Path(
    "data/research/full_market_scanner_10y/daily_regime_event_tasks"
)


CAPS = {
    "very_quiet_p0_p10": 100,
    "quiet_p10_p25": 100,
    "normal_p25_p75": 100,
    "elevated_p75_p95": 100,
    "extreme_p95_p99": 250,
    "mania_p99_p99_9": 250,
    "super_mania_p99_9_p100": 250,
}


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(INPUT_PATH)

    sampled = []

    for (price_regime, volume_regime), sub in df.groupby(
        ["price_regime", "dollar_volume_regime"],
        observed=True,
    ):
        cap = CAPS.get(volume_regime, 100)

        sample = sub.sample(
            n=min(len(sub), cap),
            random_state=42,
        )

        sampled.append(sample)

    out = pd.concat(sampled, ignore_index=True)

    out = out.sort_values(
        [
            "price_regime",
            "dollar_volume_regime",
            "trade_date",
            "ticker",
        ]
    ).copy()

    output_path = OUTPUT_DIR / "extended_hours_1m_regime_tasks_pilot.csv"
    out.to_csv(output_path, index=False)

    print("saved:", output_path)
    print("rows:", len(out))
    print("tickers:", out["ticker"].nunique())

    print()
    print("=== Pilot Task Counts ===")
    counts = (
        out.groupby(["price_regime", "dollar_volume_regime"], observed=True)
        .size()
        .reset_index(name="rows")
        .sort_values(["price_regime", "dollar_volume_regime"])
    )
    print(counts.to_string(index=False))


if __name__ == "__main__":
    main()

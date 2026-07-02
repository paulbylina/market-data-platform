from pathlib import Path

import pandas as pd


INPUT_PATH = Path(
    "data/research/full_market_scanner_10y/daily_regime_event_tasks/extended_hours_1m_regime_tasks.csv"
)

OUTPUT_PATH = Path(
    "data/research/full_market_scanner_10y/daily_regime_event_tasks/cheap_open_activation_1m_tasks.csv"
)


CAPS = {
    "very_quiet_p0_p10": 500,
    "quiet_p10_p25": 500,
    "normal_p25_p75": 500,
    "elevated_p75_p95": 500,
    "extreme_p95_p99": 3000,
    "mania_p99_p99_9": 3000,
    "super_mania_p99_9_p100": None,  # all
}


def main():
    df = pd.read_csv(INPUT_PATH)

    df = df[df["price_regime"] == "cheap_under_5"].copy()

    sampled = []

    for regime, sub in df.groupby("dollar_volume_regime", observed=True):
        cap = CAPS.get(regime, 500)

        if cap is None:
            sample = sub.copy()
        else:
            sample = sub.sample(n=min(len(sub), cap), random_state=42)

        sampled.append(sample)

    out = pd.concat(sampled, ignore_index=True)

    out = out.sort_values(
        ["dollar_volume_regime", "trade_date", "ticker"]
    ).copy()

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUTPUT_PATH, index=False)

    print("saved:", OUTPUT_PATH)
    print("rows:", len(out))
    print("tickers:", out["ticker"].nunique())

    print()
    print("=== Cheap Open Activation Task Counts ===")
    counts = (
        out.groupby(["price_regime", "dollar_volume_regime"], observed=True)
        .size()
        .reset_index(name="rows")
        .sort_values(["price_regime", "dollar_volume_regime"])
    )
    print(counts.to_string(index=False))


if __name__ == "__main__":
    main()

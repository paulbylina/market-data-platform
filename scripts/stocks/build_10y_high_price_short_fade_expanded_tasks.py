from pathlib import Path

import pandas as pd


INPUT_PATH = Path(
    "data/research/full_market_scanner_10y/daily_regime_event_tasks/extended_hours_1m_regime_tasks.csv"
)

OUTPUT_PATH = Path(
    "data/research/full_market_scanner_10y/daily_regime_event_tasks/high_price_short_fade_expanded_tasks.csv"
)


HIGH_DAILY_REGIMES = [
    "extreme_p95_p99",
    "mania_p99_p99_9",
    "super_mania_p99_9_p100",
]


CAPS = {
    "extreme_p95_p99": 20_000,
    "mania_p99_p99_9": None,        # keep all
    "super_mania_p99_9_p100": None, # keep all
}


def main():
    df = pd.read_csv(INPUT_PATH)

    df["prev_close"] = pd.to_numeric(df["prev_close"], errors="coerce")

    base = df[
        (df["price_regime"] == "high_50_plus")
        & (df["prev_close"] >= 50)
        & (df["dollar_volume_regime"].isin(HIGH_DAILY_REGIMES))
    ].copy()

    sampled = []

    for regime in HIGH_DAILY_REGIMES:
        sub = base[base["dollar_volume_regime"] == regime].copy()
        cap = CAPS[regime]

        if cap is None:
            sample = sub.copy()
        else:
            sample = sub.sample(
                n=min(len(sub), cap),
                random_state=42,
            )

        print(f"{regime}: source_rows={len(sub):,} selected_rows={len(sample):,}")
        sampled.append(sample)

    out = pd.concat(sampled, ignore_index=True)
    out = out.sort_values(["dollar_volume_regime", "trade_date", "ticker"]).reset_index(drop=True)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUTPUT_PATH, index=False)

    print()
    print("saved:", OUTPUT_PATH)
    print("rows:", len(out))
    print("tickers:", out["ticker"].nunique())
    print("date range:", out["trade_date"].min(), "to", out["trade_date"].max())

    print()
    print("=== Expanded $50+ Short-Fade Task Counts ===")
    counts = (
        out.groupby(["price_regime", "dollar_volume_regime"], observed=True)
        .size()
        .reset_index(name="rows")
        .sort_values(["price_regime", "dollar_volume_regime"])
    )
    print(counts.to_string(index=False))


if __name__ == "__main__":
    main()

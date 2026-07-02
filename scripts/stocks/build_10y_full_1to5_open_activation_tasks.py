from pathlib import Path

import pandas as pd


INPUT_PATH = Path(
    "data/research/full_market_scanner_10y/daily_regime_event_tasks/extended_hours_1m_regime_tasks.csv"
)

OUTPUT_PATH = Path(
    "data/research/full_market_scanner_10y/daily_regime_event_tasks/full_1to5_open_activation_1m_tasks.csv"
)


HIGH_DAILY_REGIMES = [
    "extreme_p95_p99",
    "mania_p99_p99_9",
    "super_mania_p99_9_p100",
]


def main():
    df = pd.read_csv(INPUT_PATH)

    df["prev_close"] = pd.to_numeric(df["prev_close"], errors="coerce")

    out = df[
        (df["price_regime"] == "cheap_under_5")
        & (df["prev_close"] >= 1.0)
        & (df["prev_close"] < 5.0)
        & (df["dollar_volume_regime"].isin(HIGH_DAILY_REGIMES))
    ].copy()

    out = out.sort_values(["trade_date", "ticker"]).reset_index(drop=True)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUTPUT_PATH, index=False)

    print("saved:", OUTPUT_PATH)
    print("rows:", len(out))
    print("tickers:", out["ticker"].nunique())
    print("date range:", out["trade_date"].min(), "to", out["trade_date"].max())

    print()
    print("=== Full $1-5 Open Activation Task Counts ===")
    counts = (
        out.groupby(["price_regime", "dollar_volume_regime"], observed=True)
        .size()
        .reset_index(name="rows")
        .sort_values(["price_regime", "dollar_volume_regime"])
    )
    print(counts.to_string(index=False))


if __name__ == "__main__":
    main()

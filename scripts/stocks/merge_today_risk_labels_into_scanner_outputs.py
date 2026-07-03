from pathlib import Path
import pandas as pd

RISK_PATH = Path("data/reference/stocks/today_watchlist_fundamentals_risk_latest.csv")

INPUTS = [
    Path("data/reference/stocks/today_watchlist_latest.csv"),
    Path("data/reference/stocks/today_trade_candidates_latest.csv"),
    Path("data/reference/stocks/today_all_confirmed_scanner_rows_latest.csv"),
]

RISK_COLS = [
    "ticker",
    "float",
    "free_float_percent",
    "free_float_market_cap",
    "pre_market_volume_to_float",
    "first_15m_volume_to_float",
    "short_interest",
    "short_interest_pct_float",
    "days_to_cover",
    "short_volume_ratio",
    "risk_flag_count",
    "risk_bucket",
    "risk_labels",
]

risk = pd.read_csv(RISK_PATH)
risk = risk[[c for c in RISK_COLS if c in risk.columns]].copy()
risk["ticker"] = risk["ticker"].astype(str).str.upper().str.strip()

for input_path in INPUTS:
    if not input_path.exists():
        print("missing:", input_path)
        continue

    df = pd.read_csv(input_path)

    if "ticker" not in df.columns:
        print("skipping no ticker column:", input_path)
        continue

    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()

    # Drop old risk columns if rerunning.
    drop_cols = [c for c in risk.columns if c != "ticker" and c in df.columns]
    if drop_cols:
        df = df.drop(columns=drop_cols)

    out = df.merge(risk, on="ticker", how="left")

    output_path = input_path.with_name(input_path.stem + "_fundamentals_risk.csv")
    out.to_csv(output_path, index=False)

    print()
    print("saved:", output_path)
    print("rows:", len(out))

    show_cols = [
        "ticker",
        "prev_close",
        "gap_pct",
        "first_15m_return_pct",
        "premarket_dollar_rvol",
        "first_15m_dollar_rvol",
        "float",
        "pre_market_volume_to_float",
        "first_15m_volume_to_float",
        "short_interest_pct_float",
        "short_volume_ratio",
        "risk_bucket",
        "risk_labels",
    ]

    show_cols = [c for c in show_cols if c in out.columns]

    print(out.sort_values(
        [c for c in ["risk_flag_count", "pre_market_volume_to_float"] if c in out.columns],
        ascending=False
    )[show_cols].to_string(index=False))

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from scripts.stocks.build_10y_high_price_short_fade_expanded_features import (
    get_api_key,
    build_features_for_event,
)
from scripts.stocks.add_first15_opening_rvol_for_date import (
    fetch_1m_range,
    first15_by_date,
)


OUT_DIR = Path("data/research/full_market_scanner_10y/high_price_full_universe_first15_checks")
PANEL_PATH = Path("data/research/full_market_scanner_10y/historical_full_market_daily_panel.csv")
PRIOR_CACHE_DIR = Path("data/cache/massive/first15_prior_1m")


def add_missing_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    if "first15_dollar_vs_prior_daily_avg" not in out.columns:
        out["first15_dollar_vs_prior_daily_avg"] = (
            pd.to_numeric(out["first_15m_dollar_volume"], errors="coerce")
            / pd.to_numeric(out["avg_dollar_volume_20d_prior"], errors="coerce")
        )

    if "first15_range_pct" not in out.columns:
        high = pd.to_numeric(out["first_15m_high"], errors="coerce")
        low = pd.to_numeric(out["first_15m_low"], errors="coerce")
        out["first15_range_pct"] = np.where(low > 0, (high / low - 1.0) * 100.0, np.nan)

    if "first15_close_position_in_range" not in out.columns:
        close = pd.to_numeric(out["first_15m_close"], errors="coerce")
        low = pd.to_numeric(out["first_15m_low"], errors="coerce")
        high = pd.to_numeric(out["first_15m_high"], errors="coerce")
        rng = high - low
        out["first15_close_position_in_range"] = np.where(rng > 0, (close - low) / rng, np.nan)

    return out


def label_raw_signals(df: pd.DataFrame) -> pd.DataFrame:
    out = add_missing_derived_columns(df)

    needed = [
        "prev_close",
        "gap_pct",
        "premarket_dollar_vs_prior_daily_avg",
        "first15_dollar_vs_prior_daily_avg",
        "first_15m_return_pct",
        "first15_range_pct",
        "first15_close_position_in_range",
    ]

    for col in needed:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    base = (
        (out["prev_close"] >= 50)
        & (out["premarket_dollar_vs_prior_daily_avg"] <= 0.10)
        & (out["first15_dollar_vs_prior_daily_avg"] >= 0.01)
        & (out["first_15m_return_pct"] >= 1)
    )

    b = (
        base
        & (out["gap_pct"] >= 0)
        & (out["gap_pct"] < 10)
    )

    a = (
        b
        & (out["first15_dollar_vs_prior_daily_avg"] >= 0.05)
        & (out["first15_dollar_vs_prior_daily_avg"] < 1.00)
        & (out["first_15m_return_pct"] >= 1)
        & (out["first_15m_return_pct"] < 8)
        & (out["first15_range_pct"] >= 2)
        & (out["first15_range_pct"] < 8)
    )

    aplus = (
        b
        & (out["first15_dollar_vs_prior_daily_avg"] >= 0.05)
        & (out["first15_dollar_vs_prior_daily_avg"] < 0.50)
        & (out["first_15m_return_pct"] >= 2)
        & (out["first_15m_return_pct"] < 8)
        & (out["first15_range_pct"] >= 2)
        & (out["first15_range_pct"] < 8)
    )

    out["signal_quality"] = ""
    out.loc[base, "signal_quality"] = "BASE"
    out.loc[b, "signal_quality"] = "B"
    out.loc[a, "signal_quality"] = "A"
    out.loc[aplus, "signal_quality"] = "A+"

    return out


def build_full_universe_features_for_date(panel: pd.DataFrame, date_str: str, api_key: str, sleep_seconds: float) -> pd.DataFrame:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    features_path = OUT_DIR / f"high_price_full_universe_first15_features_{date_str}.csv"

    if features_path.exists():
        print(f"using existing features: {features_path}")
        return pd.read_csv(features_path)

    today = panel[
        (panel["trade_date"] == date_str)
        & (panel["prev_close"] >= 50)
        & (panel["avg_dollar_volume_20d_prior"] > 0)
        & (panel["avg_volume_20d_prior"] > 0)
        & panel["prev_trade_date"].notna()
    ].copy()

    today["price_regime"] = "high_50_plus"
    today["volume_regime"] = "full_universe_unfiltered"
    today["dollar_volume_regime"] = "full_universe_unfiltered"

    print()
    print("date:", date_str)
    print("$50+ rows to check:", len(today))
    print("tickers:", today["ticker"].nunique())

    rows = []
    for i, row in today.reset_index(drop=True).iterrows():
        if i % 50 == 0:
            print(f"building features {i}/{len(today)}")

        try:
            rows.append(build_features_for_event(row, api_key))
        except Exception as exc:
            bad = row.to_dict()
            bad["download_status"] = f"error: {exc}"
            rows.append(bad)

    feat = pd.DataFrame(rows)
    feat.to_csv(features_path, index=False)

    print("saved features:", features_path)
    print("download status:")
    print(feat["download_status"].value_counts(dropna=False).head(20).to_string())

    return feat


def add_first15_rvol_to_raw_signals(
    raw: pd.DataFrame,
    panel: pd.DataFrame,
    date_str: str,
    api_key: str,
    lookback_days: int,
    sleep_seconds: float,
) -> pd.DataFrame:
    rows = []

    tickers = raw["ticker"].dropna().astype(str).unique().tolist()

    print("raw A/A+ tickers needing first15 RVOL:", len(tickers))

    for i, ticker in enumerate(tickers):
        if i % 20 == 0:
            print(f"first15 prior avg {i}/{len(tickers)}")

        ticker_dates = panel[
            (panel["ticker"].astype(str) == ticker)
            & (panel["trade_date"] < date_str)
        ]["trade_date"].drop_duplicates().sort_values()

        prior_dates = ticker_dates.tail(lookback_days).tolist()

        if len(prior_dates) == 0:
            rows.append(
                {
                    "ticker": ticker,
                    "avg_prior_20d_first15_volume": np.nan,
                    "avg_prior_20d_first15_dollar_volume": np.nan,
                    "prior_first15_days_used": 0,
                }
            )
            continue

        try:
            bars = fetch_1m_range(
                ticker=ticker,
                start_date=prior_dates[0],
                end_date=prior_dates[-1],
                api_key=api_key,
                cache_dir=PRIOR_CACHE_DIR,
                sleep_seconds=sleep_seconds,
            )

            first15 = first15_by_date(bars)
            first15 = first15[first15["trade_date"].isin(prior_dates)].copy()

            rows.append(
                {
                    "ticker": ticker,
                    "avg_prior_20d_first15_volume": first15["prior_first15_volume"].mean(),
                    "avg_prior_20d_first15_dollar_volume": first15["prior_first15_dollar_volume"].mean(),
                    "median_prior_20d_first15_volume": first15["prior_first15_volume"].median(),
                    "median_prior_20d_first15_dollar_volume": first15["prior_first15_dollar_volume"].median(),
                    "prior_first15_days_used": int(first15["trade_date"].nunique()),
                }
            )

        except Exception as exc:
            print(f"ERROR {ticker}: {exc}")
            rows.append(
                {
                    "ticker": ticker,
                    "avg_prior_20d_first15_volume": np.nan,
                    "avg_prior_20d_first15_dollar_volume": np.nan,
                    "median_prior_20d_first15_volume": np.nan,
                    "median_prior_20d_first15_dollar_volume": np.nan,
                    "prior_first15_days_used": 0,
                }
            )

    avg = pd.DataFrame(rows)
    out = raw.merge(avg, on="ticker", how="left")

    for col in [
        "first_15m_volume",
        "first_15m_dollar_volume",
        "avg_prior_20d_first15_volume",
        "avg_prior_20d_first15_dollar_volume",
    ]:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out["first15_volume_rvol_20d"] = np.where(
        out["avg_prior_20d_first15_volume"] > 0,
        out["first_15m_volume"] / out["avg_prior_20d_first15_volume"],
        np.nan,
    )

    out["first15_dollar_rvol_20d"] = np.where(
        out["avg_prior_20d_first15_dollar_volume"] > 0,
        out["first_15m_dollar_volume"] / out["avg_prior_20d_first15_dollar_volume"],
        np.nan,
    )

    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--min-first15-dollar-rvol", type=float, default=3.0)
    parser.add_argument("--min-prior-first15-days", type=int, default=15)
    parser.add_argument("--lookback-days", type=int, default=20)
    parser.add_argument("--sleep-seconds", type=float, default=0.05)
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    panel = pd.read_csv(PANEL_PATH)
    panel["trade_date"] = pd.to_datetime(panel["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    panel = panel.sort_values(["ticker", "trade_date"]).reset_index(drop=True)
    panel["prev_trade_date"] = panel.groupby("ticker")["trade_date"].shift(1)

    numeric_cols = [
        "prev_close",
        "avg_volume_20d_prior",
        "avg_dollar_volume_20d_prior",
        "gap_pct",
        "volume_rvol_20d",
        "dollar_volume_rvol_20d",
    ]

    for col in numeric_cols:
        if col in panel.columns:
            panel[col] = pd.to_numeric(panel[col], errors="coerce")

    dates = (
        panel[
            (panel["trade_date"] >= args.start_date)
            & (panel["trade_date"] <= args.end_date)
        ]["trade_date"]
        .drop_duplicates()
        .sort_values()
        .tolist()
    )

    api_key = get_api_key()

    all_raw = []
    all_final = []

    for date_str in dates:
        print()
        print("=" * 80)
        print("SCANNING", date_str)
        print("=" * 80)

        feat = build_full_universe_features_for_date(
            panel=panel,
            date_str=date_str,
            api_key=api_key,
            sleep_seconds=args.sleep_seconds,
        )

        if "download_status" in feat.columns:
            feat = feat[feat["download_status"].eq("ok")].copy()

        labeled = label_raw_signals(feat)

        raw = labeled[labeled["signal_quality"].isin(["A", "A+"])].copy()
        raw = raw.sort_values(["signal_quality", "ticker"])

        raw_path = OUT_DIR / f"high_price_first15_raw_A_Aplus_{date_str}.csv"
        raw.to_csv(raw_path, index=False)

        print("raw A/A+:", len(raw))
        print(raw["signal_quality"].value_counts(dropna=False).to_string() if len(raw) else "none")

        if raw.empty:
            continue

        enriched_raw = add_first15_rvol_to_raw_signals(
            raw=raw,
            panel=panel,
            date_str=date_str,
            api_key=api_key,
            lookback_days=args.lookback_days,
            sleep_seconds=args.sleep_seconds,
        )

        final = enriched_raw[
            (pd.to_numeric(enriched_raw["first15_dollar_rvol_20d"], errors="coerce") >= args.min_first15_dollar_rvol)
            & (pd.to_numeric(enriched_raw["prior_first15_days_used"], errors="coerce") >= args.min_prior_first15_days)
        ].copy()

        final = final.sort_values(
            ["trade_date", "first15_dollar_rvol_20d"],
            ascending=[True, False],
        )

        raw_rvol_path = OUT_DIR / f"high_price_first15_raw_A_Aplus_{date_str}_with_first15_rvol.csv"
        final_path = OUT_DIR / f"high_price_first15_final_A_Aplus_rvol_{date_str}.csv"

        enriched_raw.to_csv(raw_rvol_path, index=False)
        final.to_csv(final_path, index=False)

        print("final new-filter signals:", len(final))
        if len(final):
            print(
                final[
                    [
                        "trade_date",
                        "ticker",
                        "signal_quality",
                        "prev_close",
                        "gap_pct",
                        "premarket_dollar_vs_prior_daily_avg",
                        "first15_dollar_vs_prior_daily_avg",
                        "first15_dollar_rvol_20d",
                        "first15_volume_rvol_20d",
                        "prior_first15_days_used",
                        "first_15m_return_pct",
                        "first15_range_pct",
                        "first15_close_position_in_range",
                    ]
                ].to_string(index=False)
            )

        all_raw.append(enriched_raw)
        all_final.append(final)

    start = args.start_date
    end = args.end_date

    if all_raw:
        combined_raw = pd.concat(all_raw, ignore_index=True)
    else:
        combined_raw = pd.DataFrame()

    if all_final:
        combined_final = pd.concat(all_final, ignore_index=True)
    else:
        combined_final = pd.DataFrame()

    combined_raw_path = OUT_DIR / f"high_price_first15_raw_A_Aplus_{start}_to_{end}_with_first15_rvol.csv"
    combined_final_path = OUT_DIR / f"high_price_first15_final_A_Aplus_rvol_{start}_to_{end}.csv"

    combined_raw.to_csv(combined_raw_path, index=False)
    combined_final.to_csv(combined_final_path, index=False)

    print()
    print("=" * 80)
    print("FINAL WEEK SUMMARY")
    print("=" * 80)
    print("saved raw:", combined_raw_path)
    print("saved final:", combined_final_path)

    if combined_final.empty:
        print("No final signals.")
        return

    print()
    print("counts by date/quality:")
    print(
        combined_final.groupby(["trade_date", "signal_quality"], observed=True)
        .size()
        .rename("signals")
        .reset_index()
        .to_string(index=False)
    )

    print()
    print("final signals:")
    print(
        combined_final[
            [
                "trade_date",
                "ticker",
                "signal_quality",
                "prev_close",
                "gap_pct",
                "premarket_dollar_vs_prior_daily_avg",
                "first15_dollar_vs_prior_daily_avg",
                "first15_dollar_rvol_20d",
                "first15_volume_rvol_20d",
                "prior_first15_days_used",
                "first_15m_return_pct",
                "first15_range_pct",
                "first15_close_position_in_range",
            ]
        ]
        .sort_values(["trade_date", "first15_dollar_rvol_20d"], ascending=[True, False])
        .to_string(index=False)
    )


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
import re
from pathlib import Path

import numpy as np
import pandas as pd

from scripts.stocks.build_10y_high_price_short_fade_expanded_features import get_api_key
from scripts.stocks.add_first15_opening_rvol_for_date import fetch_1m_range


DEFAULT_INPUT = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_expanded_features/"
    "high_price_expanded_custom_setups_path_metrics.csv"
)

DEFAULT_PANEL = Path(
    "data/research/full_market_scanner_10y/historical_full_market_daily_panel.csv"
)

OUT_DIR = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_expanded_features"
)

LAST15_CACHE_DIR = Path("data/cache/massive/prior_day_last15_context_1m")


def to_date_str(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce").dt.strftime("%Y-%m-%d")


def add_missing_ratios(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    numeric_cols = [
        "prev_close",
        "premarket_dollar_volume",
        "premarket_volume",
        "avg_dollar_volume_20d_prior",
        "avg_volume_20d_prior",
        "first_15m_dollar_volume",
        "first_15m_volume",
    ]

    for col in numeric_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    if (
        "premarket_dollar_vs_prior_daily_avg" not in out.columns
        and {"premarket_dollar_volume", "avg_dollar_volume_20d_prior"}.issubset(out.columns)
    ):
        out["premarket_dollar_vs_prior_daily_avg"] = np.where(
            out["avg_dollar_volume_20d_prior"] > 0,
            out["premarket_dollar_volume"] / out["avg_dollar_volume_20d_prior"],
            np.nan,
        )

    if (
        "first15_dollar_vs_prior_daily_avg" not in out.columns
        and {"first_15m_dollar_volume", "avg_dollar_volume_20d_prior"}.issubset(out.columns)
    ):
        out["first15_dollar_vs_prior_daily_avg"] = np.where(
            out["avg_dollar_volume_20d_prior"] > 0,
            out["first_15m_dollar_volume"] / out["avg_dollar_volume_20d_prior"],
            np.nan,
        )

    return out


def find_perf_cols(df: pd.DataFrame) -> list[str]:
    preferred = [
        "net_pct",
        "avg_net",
        "long_15m_pct",
        "long_30m_pct",
        "long_45m_pct",
        "long_75m_pct",
        "long_eod_pct",
        "long_15m",
        "long_30m",
        "long_45m",
        "long_75m",
        "long_eod",
        "eod_return_pct",
        "entry_to_eod_pct",
        "fwd_1d_close_pct",
        "fwd_5d_close_pct",
    ]

    cols = [c for c in preferred if c in df.columns]

    # Add likely forward-return columns if preferred names differ.
    for c in df.columns:
        cl = c.lower()
        if c in cols:
            continue
        if (
            ("long" in cl or "fwd" in cl or "eod" in cl)
            and "pct" in cl
            and pd.api.types.is_numeric_dtype(pd.to_numeric(df[c], errors="coerce"))
        ):
            cols.append(c)

    return cols


def bucket_prior_day_rvol(s: pd.Series) -> pd.Series:
    return pd.cut(
        pd.to_numeric(s, errors="coerce"),
        bins=[-np.inf, 0.75, 1.50, 3.00, 5.00, np.inf],
        labels=["<=0.75 quiet", "0.75-1.5 normal", "1.5-3 active", "3-5 hot", "5+ extreme"],
    )


def bucket_premarket(s: pd.Series) -> pd.Series:
    return pd.cut(
        pd.to_numeric(s, errors="coerce"),
        bins=[-np.inf, 0.003, 0.01, 0.03, 0.10, np.inf],
        labels=["<=0.003 dead", "0.003-0.01 quiet", "0.01-0.03 mild", "0.03-0.10 active", "0.10+ mania"],
    )


def summarize_by_bucket(df: pd.DataFrame, bucket_col: str, context_name: str, perf_cols: list[str]) -> pd.DataFrame:
    rows = []

    for bucket, g in df.groupby(bucket_col, observed=True):
        if g.empty:
            continue

        base = {
            "context": context_name,
            "bucket": str(bucket),
            "rows": len(g),
            "tickers": g["ticker"].nunique() if "ticker" in g.columns else np.nan,
        }

        for metric in perf_cols:
            vals = pd.to_numeric(g[metric], errors="coerce").dropna()
            if vals.empty:
                continue

            r = dict(base)
            r.update(
                {
                    "metric": metric,
                    "avg": vals.mean(),
                    "median": vals.median(),
                    "win_rate": (vals > 0).mean() * 100,
                    "pct_ge_1": (vals >= 1).mean() * 100,
                    "pct_ge_2": (vals >= 2).mean() * 100,
                    "pct_le_minus_2": (vals <= -2).mean() * 100,
                    "pct_le_minus_3": (vals <= -3).mean() * 100,
                    "best": vals.max(),
                    "worst": vals.min(),
                }
            )
            rows.append(r)

    return pd.DataFrame(rows)


def last15_by_date(bars: pd.DataFrame) -> pd.DataFrame:
    if bars.empty or "timestamp_ms" not in bars.columns:
        return pd.DataFrame()

    out = bars.copy()

    for col in ["open", "high", "low", "close", "volume"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out["ts_utc"] = pd.to_datetime(
        pd.to_numeric(out["timestamp_ms"], errors="coerce"),
        unit="ms",
        utc=True,
        errors="coerce",
    )

    out = out[out["ts_utc"].notna()].copy()
    out["ts_et"] = out["ts_utc"].dt.tz_convert("America/New_York")
    out["trade_date"] = out["ts_et"].dt.strftime("%Y-%m-%d")
    out["time_et"] = out["ts_et"].dt.time

    start = pd.to_datetime("15:45").time()
    end = pd.to_datetime("16:00").time()

    last15 = out[(out["time_et"] >= start) & (out["time_et"] < end)].copy()

    if last15.empty:
        return pd.DataFrame()

    last15["dollar_volume"] = last15["close"] * last15["volume"]

    rows = []

    for date, g in last15.groupby("trade_date", sort=True):
        g = g.sort_values("ts_et")
        first = g.iloc[0]
        last = g.iloc[-1]

        open_px = first["open"]
        close_px = last["close"]

        rows.append(
            {
                "trade_date": date,
                "last15_volume": g["volume"].sum(),
                "last15_dollar_volume": g["dollar_volume"].sum(),
                "last15_open": open_px,
                "last15_high": g["high"].max(),
                "last15_low": g["low"].min(),
                "last15_close": close_px,
                "last15_return_pct": (close_px / open_px - 1.0) * 100.0 if open_px > 0 else np.nan,
            }
        )

    return pd.DataFrame(rows)


def compute_prior_day_last15_rvol(
    ticker: str,
    trade_date: str,
    panel: pd.DataFrame,
    api_key: str,
    lookback_days: int,
    sleep_seconds: float,
) -> dict:
    tpanel = panel[
        (panel["ticker"].astype(str) == str(ticker))
        & (panel["trade_date"] < trade_date)
    ].sort_values("trade_date")

    dates = tpanel["trade_date"].dropna().drop_duplicates().tolist()

    if len(dates) < lookback_days + 1:
        return {
            "ticker": ticker,
            "trade_date": trade_date,
            "prior_last15_days_used": len(dates),
            "prior_day_last15_dollar_rvol_20d": np.nan,
            "prior_day_last15_volume_rvol_20d": np.nan,
            "prior_day_last15_return_pct": np.nan,
        }

    prior_day = dates[-1]
    lookback = dates[-(lookback_days + 1):-1]

    bars = fetch_1m_range(
        ticker=ticker,
        start_date=lookback[0],
        end_date=prior_day,
        api_key=api_key,
        cache_dir=LAST15_CACHE_DIR,
        sleep_seconds=sleep_seconds,
    )

    l15 = last15_by_date(bars)

    if l15.empty:
        return {
            "ticker": ticker,
            "trade_date": trade_date,
            "prior_trade_date_for_last15": prior_day,
            "prior_last15_days_used": 0,
            "prior_day_last15_dollar_rvol_20d": np.nan,
            "prior_day_last15_volume_rvol_20d": np.nan,
            "prior_day_last15_return_pct": np.nan,
        }

    prior = l15[l15["trade_date"].eq(prior_day)].copy()
    avg = l15[l15["trade_date"].isin(lookback)].copy()

    if prior.empty or avg.empty:
        return {
            "ticker": ticker,
            "trade_date": trade_date,
            "prior_trade_date_for_last15": prior_day,
            "prior_last15_days_used": int(avg["trade_date"].nunique()) if not avg.empty else 0,
            "prior_day_last15_dollar_rvol_20d": np.nan,
            "prior_day_last15_volume_rvol_20d": np.nan,
            "prior_day_last15_return_pct": np.nan,
        }

    p = prior.iloc[0]
    avg_vol = avg["last15_volume"].mean()
    avg_dvol = avg["last15_dollar_volume"].mean()

    return {
        "ticker": ticker,
        "trade_date": trade_date,
        "prior_trade_date_for_last15": prior_day,
        "prior_last15_days_used": int(avg["trade_date"].nunique()),
        "prior_day_last15_volume": p["last15_volume"],
        "prior_day_last15_dollar_volume": p["last15_dollar_volume"],
        "avg_prior_20d_last15_volume": avg_vol,
        "avg_prior_20d_last15_dollar_volume": avg_dvol,
        "prior_day_last15_volume_rvol_20d": p["last15_volume"] / avg_vol if avg_vol > 0 else np.nan,
        "prior_day_last15_dollar_rvol_20d": p["last15_dollar_volume"] / avg_dvol if avg_dvol > 0 else np.nan,
        "prior_day_last15_return_pct": p["last15_return_pct"],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=str(DEFAULT_INPUT))
    parser.add_argument("--daily-panel", default=str(DEFAULT_PANEL))
    parser.add_argument("--setup", default="LONG_quiet_pm_first15_strong")
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--include-prior-last15", action="store_true")
    parser.add_argument("--lookback-days", type=int, default=20)
    parser.add_argument("--sleep-seconds", type=float, default=0.05)
    parser.add_argument("--max-rows", type=int, default=None)
    args = parser.parse_args()

    input_path = Path(args.input)
    panel_path = Path(args.daily_panel)

    if not input_path.exists():
        raise SystemExit(f"Missing input file: {input_path}")

    if not panel_path.exists():
        raise SystemExit(f"Missing daily panel: {panel_path}")

    df = pd.read_csv(input_path)
    panel = pd.read_csv(panel_path)

    if "trade_date" not in df.columns:
        raise SystemExit("Input file needs trade_date column.")

    df["trade_date"] = to_date_str(df["trade_date"])
    panel["trade_date"] = to_date_str(panel["trade_date"])
    panel = panel.sort_values(["ticker", "trade_date"]).reset_index(drop=True)

    df = add_missing_ratios(df)

    if args.setup and "setup" in df.columns:
        before = len(df)
        df = df[df["setup"].astype(str).eq(args.setup)].copy()
        print(f"setup filter: {args.setup} | {before} -> {len(df)} rows")

    if args.start_date:
        df = df[df["trade_date"] >= args.start_date].copy()

    if args.end_date:
        df = df[df["trade_date"] <= args.end_date].copy()

    if args.max_rows:
        df = df.sort_values("trade_date").tail(args.max_rows).copy()
        print(f"using most recent max rows: {len(df)}")

    # Add prev_trade_date if missing.
    prev_map = panel[["ticker", "trade_date"]].copy()
    prev_map = prev_map.sort_values(["ticker", "trade_date"])
    prev_map["prev_trade_date_from_panel"] = prev_map.groupby("ticker")["trade_date"].shift(1)

    df = df.merge(
        prev_map[["ticker", "trade_date", "prev_trade_date_from_panel"]],
        on=["ticker", "trade_date"],
        how="left",
    )

    if "prev_trade_date" not in df.columns:
        df["prev_trade_date"] = df["prev_trade_date_from_panel"]
    else:
        df["prev_trade_date"] = df["prev_trade_date"].fillna(df["prev_trade_date_from_panel"])

    # Merge previous day's daily RVOL.
    daily_prev = panel[
        [
            "ticker",
            "trade_date",
            "volume",
            "dollar_volume",
            "volume_rvol_20d",
            "dollar_volume_rvol_20d",
        ]
    ].copy()

    daily_prev = daily_prev.rename(
        columns={
            "trade_date": "prev_trade_date",
            "volume": "prior_day_volume",
            "dollar_volume": "prior_day_dollar_volume",
            "volume_rvol_20d": "prior_day_volume_rvol_20d",
            "dollar_volume_rvol_20d": "prior_day_dollar_volume_rvol_20d",
        }
    )

    df = df.merge(
        daily_prev,
        on=["ticker", "prev_trade_date"],
        how="left",
    )

    for col in [
        "prior_day_volume_rvol_20d",
        "prior_day_dollar_volume_rvol_20d",
        "premarket_dollar_vs_prior_daily_avg",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if args.include_prior_last15:
        print("Computing prior-day last15 RVOL. This may take time if cache is cold.")
        api_key = get_api_key()

        pairs = df[["ticker", "trade_date"]].drop_duplicates().sort_values(["trade_date", "ticker"])
        rows = []

        for i, row in pairs.reset_index(drop=True).iterrows():
            if i % 50 == 0:
                print(f"prior last15 {i}/{len(pairs)}")

            try:
                rows.append(
                    compute_prior_day_last15_rvol(
                        ticker=str(row["ticker"]),
                        trade_date=str(row["trade_date"]),
                        panel=panel,
                        api_key=api_key,
                        lookback_days=args.lookback_days,
                        sleep_seconds=args.sleep_seconds,
                    )
                )
            except Exception as exc:
                print(f"ERROR {row['ticker']} {row['trade_date']}: {exc}")
                rows.append(
                    {
                        "ticker": row["ticker"],
                        "trade_date": row["trade_date"],
                        "prior_last15_days_used": 0,
                        "prior_day_last15_dollar_rvol_20d": np.nan,
                        "prior_day_last15_volume_rvol_20d": np.nan,
                        "prior_day_last15_return_pct": np.nan,
                        "last15_error": str(exc),
                    }
                )

        l15 = pd.DataFrame(rows)

        df = df.merge(l15, on=["ticker", "trade_date"], how="left")

    perf_cols = find_perf_cols(df)

    if not perf_cols:
        print("No obvious performance columns found.")
        print("Columns:")
        print(df.columns.tolist())
        raise SystemExit(1)

    print()
    print("performance columns found:")
    for c in perf_cols:
        print(" ", c)

    df["prior_day_daily_rvol_bucket"] = bucket_prior_day_rvol(df["prior_day_dollar_volume_rvol_20d"])

    summaries = [
        summarize_by_bucket(
            df,
            bucket_col="prior_day_daily_rvol_bucket",
            context_name="prior_day_full_day_dollar_rvol",
            perf_cols=perf_cols,
        )
    ]

    if "premarket_dollar_vs_prior_daily_avg" in df.columns:
        df["premarket_bucket"] = bucket_premarket(df["premarket_dollar_vs_prior_daily_avg"])
        summaries.append(
            summarize_by_bucket(
                df,
                bucket_col="premarket_bucket",
                context_name="pre_market_dollar_vs_prior_daily_avg",
                perf_cols=perf_cols,
            )
        )

    if args.include_prior_last15 and "prior_day_last15_dollar_rvol_20d" in df.columns:
        df["prior_day_last15_rvol_bucket"] = bucket_prior_day_rvol(df["prior_day_last15_dollar_rvol_20d"])
        summaries.append(
            summarize_by_bucket(
                df,
                bucket_col="prior_day_last15_rvol_bucket",
                context_name="prior_day_last15_dollar_rvol",
                perf_cols=perf_cols,
            )
        )

    summary = pd.concat(summaries, ignore_index=True)

    suffix = "with_prior_last15" if args.include_prior_last15 else "daily_only"

    if args.start_date or args.end_date:
        date_suffix = f"{args.start_date or 'start'}_to_{args.end_date or 'end'}"
    else:
        date_suffix = "all_dates"

    enriched_path = OUT_DIR / f"old_research_prior_day_context_{suffix}_{date_suffix}.csv"
    summary_path = OUT_DIR / f"old_research_prior_day_context_summary_{suffix}_{date_suffix}.csv"

    df.to_csv(enriched_path, index=False)
    summary.to_csv(summary_path, index=False)

    print()
    print("rows analyzed:", len(df))
    print("tickers:", df["ticker"].nunique() if "ticker" in df.columns else "n/a")
    print("date range:", df["trade_date"].min(), "to", df["trade_date"].max())

    print()
    print("=== Summary ===")
    print(
        summary.sort_values(["context", "metric", "bucket"])
        .to_string(index=False)
    )

    print()
    print("saved enriched:", enriched_path)
    print("saved summary:", summary_path)


if __name__ == "__main__":
    main()

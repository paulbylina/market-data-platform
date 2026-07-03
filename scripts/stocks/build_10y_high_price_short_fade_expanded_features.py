from pathlib import Path
import os
import time

import numpy as np
import pandas as pd
import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


TASKS_PATH = Path(
    "data/research/full_market_scanner_10y/daily_regime_event_tasks/high_price_short_fade_expanded_tasks.csv"
)

OUTPUT_DIR = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_expanded_features"
)

CACHE_DIR = Path("data/cache/massive/extended_hours_1m")


BASE_URL = "https://api.massive.com"


def get_api_key():
    key = os.getenv("MASSIVE_API_KEY") or os.getenv("POLYGON_API_KEY")
    if not key:
        raise RuntimeError("Missing MASSIVE_API_KEY or POLYGON_API_KEY in environment/.env")
    return key



def get_with_retries(url, params, attempts=3):
    transient_statuses = {429, 500, 502, 503, 504}

    for attempt in range(1, attempts + 1):
        try:
            r = requests.get(url, params=params, timeout=(10, 90))
        except requests.exceptions.RequestException as e:
            if attempt == attempts:
                print(f"request failed after {attempts} attempts: {e}", flush=True)
                return None

            print(f"request error; retry {attempt}/{attempts}: {e}", flush=True)
            time.sleep(10 * attempt)
            continue

        if r.status_code in transient_statuses and attempt < attempts:
            print(f"transient HTTP {r.status_code}; retry {attempt}/{attempts}", flush=True)
            time.sleep(10 * attempt)
            continue

        return r

    return None


def fetch_1m_bars(ticker, start_date, end_date, api_key, sleep_seconds=0.15):
    import time

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = CACHE_DIR / f"{ticker}_{start_date}_{end_date}_1m.csv"

    if cache_path.exists():
        try:
            return pd.read_csv(cache_path)
        except pd.errors.EmptyDataError:
            return pd.DataFrame()

    url = (
        f"https://api.massive.com/v2/aggs/ticker/{ticker}/range/1/minute/"
        f"{start_date}/{end_date}"
    )

    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": 50000,
        "apiKey": api_key,
    }

    transient_statuses = {429, 500, 502, 503, 504}

    for attempt in range(1, 4):
        try:
            r = requests.get(url, params=params, timeout=(10, 90))
        except requests.exceptions.RequestException as e:
            if attempt == 3:
                print(
                    f"request failed after 3 attempts {ticker} "
                    f"{start_date}->{end_date}: {e}",
                    flush=True,
                )
                return pd.DataFrame()

            wait = 10 * attempt
            print(
                f"request error retry {attempt}/3 {ticker} "
                f"{start_date}->{end_date}: {e}; wait {wait}s",
                flush=True,
            )
            time.sleep(wait)
            continue

        if r.status_code in transient_statuses and attempt < 3:
            wait = 10 * attempt
            print(
                f"HTTP {r.status_code} retry {attempt}/3 {ticker} "
                f"{start_date}->{end_date}; wait {wait}s",
                flush=True,
            )
            time.sleep(wait)
            continue

        if r.status_code != 200:
            print(
                f"failed {ticker} {start_date}->{end_date}: "
                f"status {r.status_code} {r.text[:200]}",
                flush=True,
            )
            time.sleep(sleep_seconds)
            return pd.DataFrame()

        data = r.json()
        results = data.get("results", [])

        if not results:
            out = pd.DataFrame()
            # Write a tiny marker file with headers so future reads do not crash.
            out.to_csv(cache_path, index=False)
            time.sleep(sleep_seconds)
            return out

        df = pd.DataFrame(results)

        rename = {
            "t": "timestamp_ms",
            "o": "open",
            "h": "high",
            "l": "low",
            "c": "close",
            "v": "volume",
            "vw": "vwap",
            "n": "transactions",
        }
        df = df.rename(columns=rename)

        df["bar_start_utc"] = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True)
        df["bar_start_et"] = df["bar_start_utc"].dt.tz_convert("America/New_York")
        df["trade_date_et"] = df["bar_start_et"].dt.date.astype(str)
        df["time_et"] = df["bar_start_et"].dt.strftime("%H:%M")

        df.to_csv(cache_path, index=False)
        time.sleep(sleep_seconds)
        return df

    print(f"failed after retries {ticker} {start_date}->{end_date}", flush=True)
    return pd.DataFrame()


def prep_bars(bars):
    if bars.empty:
        return bars

    for col in ["open", "high", "low", "close", "volume", "vwap"]:
        if col in bars.columns:
            bars[col] = pd.to_numeric(bars[col], errors="coerce")

    if "vwap" in bars.columns:
        price_for_dollar = bars["vwap"].fillna(bars["close"])
    else:
        price_for_dollar = bars["close"]

    bars["dollar_volume"] = price_for_dollar * bars["volume"]

    return bars


def summarize_session(bars, label):
    if bars.empty:
        return {
            f"{label}_bars": 0,
            f"{label}_volume": 0.0,
            f"{label}_dollar_volume": 0.0,
            f"{label}_high": np.nan,
            f"{label}_low": np.nan,
            f"{label}_open": np.nan,
            f"{label}_close": np.nan,
        }

    return {
        f"{label}_bars": len(bars),
        f"{label}_volume": bars["volume"].sum(),
        f"{label}_dollar_volume": bars["dollar_volume"].sum(),
        f"{label}_high": bars["high"].max(),
        f"{label}_low": bars["low"].min(),
        f"{label}_open": bars.iloc[0]["open"],
        f"{label}_close": bars.iloc[-1]["close"],
    }


def pct(a, b):
    if pd.isna(a) or pd.isna(b) or b == 0:
        return np.nan
    return (a / b - 1.0) * 100.0


def position_in_range(x, low, high):
    if pd.isna(x) or pd.isna(low) or pd.isna(high) or high == low:
        return np.nan
    return (x - low) / (high - low)


def build_features_for_event(row, api_key):
    ticker = row["ticker"]
    trade_date = row["trade_date"]
    prev_trade_date = row["prev_trade_date"]

    bars = fetch_1m_bars(
        ticker=ticker,
        start_date=prev_trade_date,
        end_date=trade_date,
        api_key=api_key,
    )

    bars = prep_bars(bars)

    out = row.to_dict()

    if bars.empty:
        out["download_status"] = "no_bars"
        return out

    prev_ah = bars[
        (bars["trade_date_et"] == prev_trade_date)
        & (bars["time_et"] >= "16:00")
        & (bars["time_et"] < "20:00")
    ].copy()

    premarket = bars[
        (bars["trade_date_et"] == trade_date)
        & (bars["time_et"] >= "04:00")
        & (bars["time_et"] < "09:30")
    ].copy()

    regular = bars[
        (bars["trade_date_et"] == trade_date)
        & (bars["time_et"] >= "09:30")
        & (bars["time_et"] < "16:00")
    ].copy()

    first_15m = bars[
        (bars["trade_date_et"] == trade_date)
        & (bars["time_et"] >= "09:30")
        & (bars["time_et"] <= "09:44")
    ].copy()

    out.update(summarize_session(prev_ah, "prev_after_hours"))
    out.update(summarize_session(premarket, "premarket"))
    out.update(summarize_session(regular, "regular"))
    out.update(summarize_session(first_15m, "first_15m"))

    prev_close = row.get("prev_close", np.nan)
    regular_open = row.get("open", np.nan)
    avg_volume_20d_prior = row.get("avg_volume_20d_prior", np.nan)
    avg_dollar_volume_20d_prior = row.get("avg_dollar_volume_20d_prior", np.nan)

    # If the task file does not include these averages, use daily row's own RVOL fields where possible.
    # These two ratios are extended-hours activity relative to normal full-day activity.
    out["prev_after_hours_volume_vs_prior_daily_avg"] = (
        out["prev_after_hours_volume"] / avg_volume_20d_prior
        if pd.notna(avg_volume_20d_prior) and avg_volume_20d_prior > 0
        else np.nan
    )
    out["prev_after_hours_dollar_vs_prior_daily_avg"] = (
        out["prev_after_hours_dollar_volume"] / avg_dollar_volume_20d_prior
        if pd.notna(avg_dollar_volume_20d_prior) and avg_dollar_volume_20d_prior > 0
        else np.nan
    )
    out["premarket_volume_vs_prior_daily_avg"] = (
        out["premarket_volume"] / avg_volume_20d_prior
        if pd.notna(avg_volume_20d_prior) and avg_volume_20d_prior > 0
        else np.nan
    )
    out["premarket_dollar_vs_prior_daily_avg"] = (
        out["premarket_dollar_volume"] / avg_dollar_volume_20d_prior
        if pd.notna(avg_dollar_volume_20d_prior) and avg_dollar_volume_20d_prior > 0
        else np.nan
    )

    out["prev_after_hours_high_vs_prev_close_pct"] = pct(out["prev_after_hours_high"], prev_close)
    out["premarket_high_vs_prev_close_pct"] = pct(out["premarket_high"], prev_close)
    out["regular_open_vs_premarket_high_pct"] = pct(regular_open, out["premarket_high"])
    out["regular_open_vs_premarket_close_pct"] = pct(regular_open, out["premarket_close"])
    out["regular_open_position_in_premarket_range"] = position_in_range(
        regular_open,
        out["premarket_low"],
        out["premarket_high"],
    )

    if len(first_15m) > 0:
        first_open = first_15m.iloc[0]["open"]
        first_close = first_15m.iloc[-1]["close"]
        out["first_15m_return_pct"] = pct(first_close, first_open)
    else:
        out["first_15m_return_pct"] = np.nan

    out["download_status"] = "ok"
    return out


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    api_key = get_api_key()

    tasks = pd.read_csv(TASKS_PATH)
    tasks["trade_date"] = pd.to_datetime(tasks["trade_date"]).dt.date.astype(str)
    tasks["prev_trade_date"] = pd.to_datetime(tasks["prev_trade_date"]).dt.date.astype(str)

    # These prior averages are needed for extended-hours-vs-normal-daily ratios.
    # Pull them back from the 10y daily panel if not already present.
    if "avg_volume_20d_prior" not in tasks.columns or "avg_dollar_volume_20d_prior" not in tasks.columns:
        panel_path = Path("data/research/full_market_scanner_10y/historical_full_market_daily_panel.csv")
        panel_cols = [
            "ticker",
            "trade_date",
            "avg_volume_20d_prior",
            "avg_dollar_volume_20d_prior",
        ]
        panel = pd.read_csv(panel_path, usecols=panel_cols)
        panel["trade_date"] = pd.to_datetime(panel["trade_date"]).dt.date.astype(str)

        tasks = tasks.merge(
            panel,
            on=["ticker", "trade_date"],
            how="left",
        )

    print("tasks:", len(tasks))
    print("tickers:", tasks["ticker"].nunique())

    results = []
    for i, row in tasks.iterrows():
        if i % 50 == 0:
            print(f"processing {i}/{len(tasks)}")

        results.append(build_features_for_event(row, api_key))

    out = pd.DataFrame(results)

    features_path = OUTPUT_DIR / "high_price_short_fade_expanded_features.csv"
    out.to_csv(features_path, index=False)

    print()
    print("saved:", features_path)

    print()
    print("=== Download Status ===")
    print(out["download_status"].value_counts(dropna=False).to_string())

    print()
    print("=== Extended-Hours Feature Summary ===")
    summary = (
        out.groupby(["price_regime", "dollar_volume_regime"], observed=True)
        .agg(
            rows=("ticker", "size"),
            ok_rows=("download_status", lambda s: (s == "ok").sum()),
            median_prev_ah_dollar=("prev_after_hours_dollar_volume", "median"),
            median_premarket_dollar=("premarket_dollar_volume", "median"),
            median_premarket_dollar_vs_daily_avg=("premarket_dollar_vs_prior_daily_avg", "median"),
            median_premarket_high_vs_prev_close=("premarket_high_vs_prev_close_pct", "median"),
            median_open_vs_premarket_high=("regular_open_vs_premarket_high_pct", "median"),
            median_open_position_pm_range=("regular_open_position_in_premarket_range", "median"),
            median_first_15m_return=("first_15m_return_pct", "median"),
            median_fwd_1d=("fwd_1d_close_pct", "median"),
            median_fwd_5d=("fwd_5d_close_pct", "median"),
        )
        .reset_index()
    )

    print(summary.to_string(index=False))

    summary.to_csv(OUTPUT_DIR / "high_price_short_fade_expanded_features_summary.csv", index=False)
    print()
    print("saved:", OUTPUT_DIR / "high_price_short_fade_expanded_features_summary.csv")


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
import json
import os
import re
import time
from pathlib import Path
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

import numpy as np
import pandas as pd


BASE_URL = "https://api.massive.com/v2/aggs/ticker"


def load_env_file(path: Path = Path(".env")) -> None:
    if not path.exists():
        return

    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def get_api_key() -> str:
    load_env_file()
    key = os.environ.get("MASSIVE_API_KEY") or os.environ.get("POLYGON_API_KEY")
    if not key:
        raise SystemExit("Missing MASSIVE_API_KEY or POLYGON_API_KEY in .env")
    return key


def safe_name(ticker: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", ticker)


def fetch_1m_day(ticker: str, trade_date: str, api_key: str, cache_dir: Path, sleep_seconds: float) -> pd.DataFrame:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / f"{safe_name(ticker)}_{trade_date}_1m.csv"

    if cache_path.exists():
        return pd.read_csv(cache_path)

    encoded = quote(ticker, safe="")
    params = urlencode(
        {
            "adjusted": "false",
            "sort": "asc",
            "limit": 50000,
            "apiKey": api_key,
        }
    )

    url = f"{BASE_URL}/{encoded}/range/1/minute/{trade_date}/{trade_date}?{params}"
    req = Request(url, headers={"User-Agent": "market-data-platform/1.0"})

    with urlopen(req, timeout=90) as response:
        payload = json.loads(response.read().decode("utf-8"))

    results = payload.get("results", [])
    if not results:
        df = pd.DataFrame()
    else:
        df = pd.DataFrame(results).rename(
            columns={
                "o": "open",
                "h": "high",
                "l": "low",
                "c": "close",
                "v": "volume",
                "vw": "vwap",
                "n": "transactions",
                "t": "timestamp_ms",
            }
        )

    df.to_csv(cache_path, index=False)
    time.sleep(sleep_seconds)
    return df


def prep_intraday_bars(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "timestamp_ms" not in df.columns:
        return pd.DataFrame()

    out = df.copy()

    for col in ["open", "high", "low", "close", "volume"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    out["ts_utc"] = pd.to_datetime(
        pd.to_numeric(out["timestamp_ms"], errors="coerce"),
        unit="ms",
        utc=True,
        errors="coerce",
    )
    out = out[out["ts_utc"].notna()].copy()
    out["ts_et"] = out["ts_utc"].dt.tz_convert("America/New_York")
    out["time_et"] = out["ts_et"].dt.time

    return out.sort_values("ts_et").reset_index(drop=True)


def compute_atr14_pct(panel: pd.DataFrame, ticker: str, trade_date: str) -> float:
    sub = panel[panel["ticker"].astype(str).eq(str(ticker))].copy()
    sub["trade_date"] = pd.to_datetime(sub["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    sub = sub[sub["trade_date"] < trade_date].copy()
    sub = sub.sort_values("trade_date")

    for col in ["high", "low", "close"]:
        sub[col] = pd.to_numeric(sub[col], errors="coerce")

    sub["prev_close_for_tr"] = sub["close"].shift(1)

    tr1 = sub["high"] - sub["low"]
    tr2 = (sub["high"] - sub["prev_close_for_tr"]).abs()
    tr3 = (sub["low"] - sub["prev_close_for_tr"]).abs()

    sub["true_range"] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    last14 = sub["true_range"].dropna().tail(14)

    if len(last14) < 10:
        return np.nan

    prev_close = pd.to_numeric(sub["close"], errors="coerce").dropna().iloc[-1]
    atr14 = last14.mean()

    if prev_close <= 0:
        return np.nan

    return float((atr14 / prev_close) * 100.0)


def simulate_long_exit(
    bars: pd.DataFrame,
    trade_date: str,
    entry_price: float,
    target_pct: float,
    stop_pct: float,
) -> dict:
    if bars.empty or not np.isfinite(entry_price) or entry_price <= 0:
        return {
            "exit_status": "no_bars",
            "exit_price": np.nan,
            "net_pct": np.nan,
            "minutes_held": np.nan,
            "exit_time_et": None,
        }

    start_time = pd.to_datetime("09:45").time()
    end_time = pd.to_datetime("16:00").time()

    day = bars[
        (bars["time_et"] >= start_time)
        & (bars["time_et"] <= end_time)
    ].copy()

    if day.empty:
        return {
            "exit_status": "no_regular_bars_after_entry",
            "exit_price": np.nan,
            "net_pct": np.nan,
            "minutes_held": np.nan,
            "exit_time_et": None,
        }

    target_price = entry_price * (1.0 + target_pct / 100.0)
    stop_price = entry_price * (1.0 - stop_pct / 100.0)

    entry_ts = pd.Timestamp(f"{trade_date} 09:45:00", tz="America/New_York")

    for _, bar in day.iterrows():
        hi = bar["high"]
        lo = bar["low"]

        hit_stop = lo <= stop_price
        hit_target = hi >= target_price

        # Conservative if both target and stop hit in same 1m bar.
        if hit_stop and hit_target:
            exit_price = stop_price
            status = "both_hit_stop_first"
        elif hit_stop:
            exit_price = stop_price
            status = "stop"
        elif hit_target:
            exit_price = target_price
            status = "target"
        else:
            continue

        minutes_held = (bar["ts_et"] - entry_ts).total_seconds() / 60.0
        return {
            "exit_status": status,
            "exit_price": exit_price,
            "net_pct": (exit_price / entry_price - 1.0) * 100.0,
            "minutes_held": minutes_held,
            "exit_time_et": bar["ts_et"].strftime("%H:%M"),
        }

    last = day.iloc[-1]
    exit_price = last["close"]
    minutes_held = (last["ts_et"] - entry_ts).total_seconds() / 60.0

    return {
        "exit_status": "eod",
        "exit_price": exit_price,
        "net_pct": (exit_price / entry_price - 1.0) * 100.0,
        "minutes_held": minutes_held,
        "exit_time_et": last["ts_et"].strftime("%H:%M"),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--min-first15-dollar-rvol", type=float, default=3.0)
    parser.add_argument("--min-prior-first15-days", type=int, default=15)
    parser.add_argument(
        "--signals",
        default=None,
        help="Signals file with first15 RVOL columns.",
    )
    parser.add_argument(
        "--daily-panel",
        default="data/research/full_market_scanner_10y/historical_full_market_daily_panel.csv",
    )
    parser.add_argument(
        "--cache-dir",
        default="data/cache/massive/atr_exit_1m",
    )
    parser.add_argument("--sleep-seconds", type=float, default=0.05)
    args = parser.parse_args()

    trade_date = args.date

    if args.signals:
        signals_path = Path(args.signals)
    else:
        signals_path = Path(
            "data/research/full_market_scanner_10y/high_price_full_universe_first15_checks"
        ) / f"high_price_full_universe_first15_signals_{trade_date}_with_first15_rvol.csv"

    out_path = signals_path.with_name(
        f"high_price_first15_exit_comparison_{trade_date}.csv"
    )
    summary_path = signals_path.with_name(
        f"high_price_first15_exit_comparison_summary_{trade_date}.csv"
    )

    signals = pd.read_csv(signals_path)

    for col in [
        "prev_close",
        "first15_dollar_rvol_20d",
        "prior_first15_days_used",
        "first_15m_close",
        "first_15m_return_pct",
        "first15_range_pct",
        "first15_close_position_in_range",
    ]:
        if col in signals.columns:
            signals[col] = pd.to_numeric(signals[col], errors="coerce")

    candidates = signals[
        (signals["signal_quality"].isin(["A", "A+"]))
        & (signals["first15_dollar_rvol_20d"] >= args.min_first15_dollar_rvol)
        & (signals["prior_first15_days_used"] >= args.min_prior_first15_days)
    ].copy()

    print("date:", trade_date)
    print("signals file:", signals_path)
    print("candidates:", len(candidates))
    print(candidates[["ticker", "signal_quality", "first15_dollar_rvol_20d"]].to_string(index=False))

    if candidates.empty:
        raise SystemExit("No candidates after filter.")

    panel = pd.read_csv(args.daily_panel)
    api_key = get_api_key()
    cache_dir = Path(args.cache_dir)

    exit_configs = [
        {"exit_model": "fixed_2_0t_3_0s", "kind": "fixed", "target_pct": 2.0, "stop_pct": 3.0},
        {"exit_model": "fixed_2_5t_3_0s", "kind": "fixed", "target_pct": 2.5, "stop_pct": 3.0},
        {"exit_model": "fixed_3_0t_4_0s", "kind": "fixed", "target_pct": 3.0, "stop_pct": 4.0},

        {"exit_model": "atr_0_50t_0_75s_clip", "kind": "atr", "target_mult": 0.50, "stop_mult": 0.75},
        {"exit_model": "atr_0_75t_1_00s_clip", "kind": "atr", "target_mult": 0.75, "stop_mult": 1.00},
        {"exit_model": "atr_1_00t_1_25s_clip", "kind": "atr", "target_mult": 1.00, "stop_mult": 1.25},
    ]

    rows = []

    for _, sig in candidates.iterrows():
        ticker = str(sig["ticker"])
        entry_price = pd.to_numeric(sig.get("first_15m_close"), errors="coerce")

        atr14_pct = compute_atr14_pct(panel, ticker, trade_date)

        raw_bars = fetch_1m_day(
            ticker=ticker,
            trade_date=trade_date,
            api_key=api_key,
            cache_dir=cache_dir,
            sleep_seconds=args.sleep_seconds,
        )
        bars = prep_intraday_bars(raw_bars)

        for cfg in exit_configs:
            if cfg["kind"] == "fixed":
                target_pct = cfg["target_pct"]
                stop_pct = cfg["stop_pct"]
            else:
                if not np.isfinite(atr14_pct):
                    continue

                target_pct = float(np.clip(cfg["target_mult"] * atr14_pct, 1.5, 4.0))
                stop_pct = float(np.clip(cfg["stop_mult"] * atr14_pct, 2.0, 5.0))

            result = simulate_long_exit(
                bars=bars,
                trade_date=trade_date,
                entry_price=entry_price,
                target_pct=target_pct,
                stop_pct=stop_pct,
            )

            rows.append(
                {
                    "trade_date": trade_date,
                    "ticker": ticker,
                    "signal_quality": sig["signal_quality"],
                    "entry_price": entry_price,
                    "atr14_pct": atr14_pct,
                    "first15_dollar_rvol_20d": sig.get("first15_dollar_rvol_20d"),
                    "first_15m_return_pct": sig.get("first_15m_return_pct"),
                    "first15_range_pct": sig.get("first15_range_pct"),
                    "first15_close_position_in_range": sig.get("first15_close_position_in_range"),
                    "exit_model": cfg["exit_model"],
                    "target_pct": target_pct,
                    "stop_pct": stop_pct,
                    **result,
                }
            )

    trades = pd.DataFrame(rows)
    trades.to_csv(out_path, index=False)

    summary = (
        trades.groupby("exit_model", observed=True)
        .agg(
            trades=("ticker", "size"),
            avg_net=("net_pct", "mean"),
            median_net=("net_pct", "median"),
            win_rate=("net_pct", lambda s: (s > 0).mean() * 100),
            target_rate=("exit_status", lambda s: s.astype(str).str.contains("target").mean() * 100),
            stop_rate=("exit_status", lambda s: s.astype(str).str.contains("stop").mean() * 100),
            eod_rate=("exit_status", lambda s: (s == "eod").mean() * 100),
            median_minutes_held=("minutes_held", "median"),
            best=("net_pct", "max"),
            worst=("net_pct", "min"),
        )
        .reset_index()
        .sort_values(["median_net", "avg_net"], ascending=[False, False])
    )

    summary.to_csv(summary_path, index=False)

    print()
    print("saved trades:", out_path)
    print("saved summary:", summary_path)

    print()
    print("=== Exit Model Summary ===")
    print(summary.to_string(index=False))

    print()
    print("=== Trades ===")
    display_cols = [
        "ticker",
        "signal_quality",
        "exit_model",
        "entry_price",
        "atr14_pct",
        "target_pct",
        "stop_pct",
        "exit_status",
        "exit_time_et",
        "net_pct",
        "minutes_held",
    ]
    print(trades[display_cols].sort_values(["ticker", "exit_model"]).to_string(index=False))


if __name__ == "__main__":
    main()

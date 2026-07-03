from __future__ import annotations

import argparse
from functools import lru_cache
from pathlib import Path
from datetime import time

import pandas as pd


INPUT_PATH = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_expanded_features/high_price_expanded_custom_setups_path_metrics.csv"
)

OUTPUT_DIR = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_expanded_features"
)

TARGETS = [0.5, 0.75, 1.0, 1.25, 1.5, 2.0, 2.5, 3.0, 4.0]
STOPS = [0.5, 0.75, 1.0, 1.25, 1.5, 2.0, 2.5, 3.0, 4.0]


def find_col(df: pd.DataFrame, names: list[str]) -> str:
    for name in names:
        if name in df.columns:
            return name
    raise KeyError(f"Could not find any of {names}. Available columns: {list(df.columns)}")


def parse_ts(series: pd.Series, col_name: str) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series):
        med = pd.to_numeric(series, errors="coerce").dropna().median()

        if med > 1e17:
            unit = "ns"
        elif med > 1e14:
            unit = "us"
        elif med > 1e11:
            unit = "ms"
        else:
            unit = "s"

        ts = pd.to_datetime(series, unit=unit, utc=True, errors="coerce")
        return ts.dt.tz_convert("America/New_York")

    if "et" in col_name.lower() or "eastern" in col_name.lower():
        ts = pd.to_datetime(series, errors="coerce", format="mixed")
        if getattr(ts.dt, "tz", None) is None:
            return ts.dt.tz_localize("America/New_York", nonexistent="shift_forward", ambiguous="NaT")
        return ts.dt.tz_convert("America/New_York")

    ts = pd.to_datetime(series, utc=True, errors="coerce", format="mixed")
    return ts.dt.tz_convert("America/New_York")


@lru_cache(maxsize=None)
def load_bars(cache_file: str) -> pd.DataFrame:
    path = Path(cache_file)

    if not path.exists():
        raise FileNotFoundError(path)

    if path.suffix.lower() in {".parquet", ".pq"}:
        raw = pd.read_parquet(path)
    else:
        raw = pd.read_csv(path)

    ts_col = None
    for candidate in [
        "bar_start_et",
        "bar_start_utc",
        "timestamp_ms",
        "timestamp_et",
        "datetime_et",
        "timestamp",
        "datetime",
        "window_start",
        "sip_timestamp",
        "participant_timestamp",
        "t",
        "time",
        "time_et",
    ]:
        if candidate in raw.columns:
            ts_col = candidate
            break

    if ts_col is None:
        raise KeyError(f"No timestamp column found in {path}. Columns: {list(raw.columns)}")

    open_col = find_col(raw, ["open", "o"])
    high_col = find_col(raw, ["high", "h"])
    low_col = find_col(raw, ["low", "l"])
    close_col = find_col(raw, ["close", "c"])

    out = pd.DataFrame({
        "ts_et": parse_ts(raw[ts_col], ts_col),
        "open": pd.to_numeric(raw[open_col], errors="coerce"),
        "high": pd.to_numeric(raw[high_col], errors="coerce"),
        "low": pd.to_numeric(raw[low_col], errors="coerce"),
        "close": pd.to_numeric(raw[close_col], errors="coerce"),
    })

    out = out.dropna(subset=["ts_et", "open", "high", "low", "close"])
    out = out.sort_values("ts_et").reset_index(drop=True)
    return out


def simulate_trade(
    bars: pd.DataFrame,
    side: str,
    entry_idx: int,
    entry_price: float,
    target_pct: float,
    stop_pct: float,
    cost_bps: float,
) -> dict:
    target_px = None
    stop_px = None

    if side == "long":
        target_px = entry_price * (1.0 + target_pct / 100.0)
        stop_px = entry_price * (1.0 - stop_pct / 100.0)
    elif side == "short":
        target_px = entry_price * (1.0 - target_pct / 100.0)
        stop_px = entry_price * (1.0 + stop_pct / 100.0)
    else:
        raise ValueError(side)

    entry_time = bars.iloc[entry_idx]["ts_et"]

    for j in range(entry_idx, len(bars)):
        bar = bars.iloc[j]
        high = float(bar["high"])
        low = float(bar["low"])

        # Conservative same-bar rule: if both target and stop are possible in one bar, count stop first.
        if side == "long":
            hit_target = high >= target_px
            hit_stop = low <= stop_px

            if hit_target and hit_stop:
                exit_price = stop_px
                exit_reason = "stop_same_bar"
            elif hit_stop:
                exit_price = stop_px
                exit_reason = "stop"
            elif hit_target:
                exit_price = target_px
                exit_reason = "target"
            else:
                continue

            gross_pct = (exit_price / entry_price - 1.0) * 100.0
            net_pct = gross_pct - cost_bps / 100.0

            return {
                "exit_time": bar["ts_et"],
                "exit_price": exit_price,
                "exit_reason": exit_reason,
                "gross_pct": gross_pct,
                "net_pct": net_pct,
                "minutes_held": (bar["ts_et"] - entry_time).total_seconds() / 60.0,
            }

        if side == "short":
            hit_target = low <= target_px
            hit_stop = high >= stop_px

            if hit_target and hit_stop:
                exit_price = stop_px
                exit_reason = "stop_same_bar"
            elif hit_stop:
                exit_price = stop_px
                exit_reason = "stop"
            elif hit_target:
                exit_price = target_px
                exit_reason = "target"
            else:
                continue

            gross_pct = (entry_price / exit_price - 1.0) * 100.0
            net_pct = gross_pct - cost_bps / 100.0

            return {
                "exit_time": bar["ts_et"],
                "exit_price": exit_price,
                "exit_reason": exit_reason,
                "gross_pct": gross_pct,
                "net_pct": net_pct,
                "minutes_held": (bar["ts_et"] - entry_time).total_seconds() / 60.0,
            }

    last = bars.iloc[-1]
    exit_price = float(last["close"])

    if side == "long":
        gross_pct = (exit_price / entry_price - 1.0) * 100.0
    else:
        gross_pct = (entry_price / exit_price - 1.0) * 100.0

    return {
        "exit_time": last["ts_et"],
        "exit_price": exit_price,
        "exit_reason": "eod",
        "gross_pct": gross_pct,
        "net_pct": gross_pct - cost_bps / 100.0,
        "minutes_held": (last["ts_et"] - entry_time).total_seconds() / 60.0,
    }


def find_entry_idx(first15: pd.DataFrame, side: str, entry_price: float) -> int | None:
    if side == "long":
        hits = first15.index[first15["low"] <= entry_price].tolist()
    else:
        hits = first15.index[first15["high"] >= entry_price].tolist()

    if not hits:
        return None

    return hits[0]


def summarize(trades: pd.DataFrame) -> pd.DataFrame:
    rows = []

    group_cols = ["setup", "side", "entry_offset_pct", "target_pct", "stop_pct", "cost_bps"]

    for keys, sub in trades.groupby(group_cols, dropna=False):
        setup, side, entry_offset_pct, target_pct, stop_pct, cost_bps = keys

        exit_text = sub["exit_reason"].astype(str).str.lower()

        rows.append({
            "trades": len(sub),
            "tickers": sub["ticker"].nunique(),
            "avg_net": sub["net_pct"].mean(),
            "median_net": sub["net_pct"].median(),
            "win_rate": (sub["net_pct"] > 0).mean() * 100.0,
            "target_rate": exit_text.str.contains("target").mean() * 100.0,
            "stop_rate": exit_text.str.contains("stop").mean() * 100.0,
            "eod_rate": exit_text.str.contains("eod").mean() * 100.0,
            "median_minutes_held": sub["minutes_held"].median(),
            "best": sub["net_pct"].max(),
            "worst": sub["net_pct"].min(),
            "setup": setup,
            "side": side,
            "entry_offset_pct": entry_offset_pct,
            "target_pct": target_pct,
            "stop_pct": stop_pct,
            "cost_bps": cost_bps,
        })

    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=str(INPUT_PATH))
    parser.add_argument("--label", default="high_price_first15_premarket_level_trap_entries")
    parser.add_argument("--cost-bps", type=float, default=10.0)
    parser.add_argument("--entry-offset-pcts", default="0")
    parser.add_argument("--max-rows", type=int, default=None)
    args = parser.parse_args()

    input_path = Path(args.input)
    df = pd.read_csv(input_path)

    if "path_status" in df.columns:
        df = df[df["path_status"] == "ok"].copy()

    df = df[df["prev_close"] >= 50].copy()

    needed = ["ticker", "trade_date", "premarket_high", "premarket_low", "cache_file"]
    for col in needed:
        if col not in df.columns:
            raise KeyError(f"Missing required column: {col}")

    for col in ["premarket_high", "premarket_low", "prev_close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.date
    df = df.dropna(subset=["ticker", "trade_date", "premarket_high", "premarket_low", "cache_file"])

    if args.max_rows is not None:
        df = df.head(args.max_rows).copy()

    entry_offsets = [float(x.strip()) for x in args.entry_offset_pcts.split(",") if x.strip()]

    print(f"input rows to scan: {len(df)}")
    print(f"entry offsets: {entry_offsets}")
    print(f"targets: {TARGETS}")
    print(f"stops: {STOPS}")

    trades = []

    for i, row in enumerate(df.itertuples(index=False), start=1):
        if i % 1000 == 0:
            print(f"processed {i}/{len(df)}")

        ticker = str(getattr(row, "ticker")).upper()
        trade_date = getattr(row, "trade_date")
        premarket_high = float(getattr(row, "premarket_high"))
        premarket_low = float(getattr(row, "premarket_low"))
        cache_file = str(getattr(row, "cache_file"))

        try:
            bars_all = load_bars(cache_file)
        except Exception as e:
            continue

        bars = bars_all[bars_all["ts_et"].dt.date == trade_date].copy()
        if bars.empty:
            continue

        regular = bars[
            (bars["ts_et"].dt.time >= time(9, 30))
            & (bars["ts_et"].dt.time <= time(16, 0))
        ].copy().reset_index(drop=True)

        if regular.empty:
            continue

        first15 = regular[
            (regular["ts_et"].dt.time >= time(9, 30))
            & (regular["ts_et"].dt.time < time(9, 45))
        ].copy()

        if first15.empty:
            continue

        for entry_offset_pct in entry_offsets:
            setup_specs = [
                {
                    "setup": "LONG_pre_market_low_sweep_entry_first15",
                    "side": "long",
                    "entry_price": premarket_low * (1.0 - entry_offset_pct / 100.0),
                    "level": premarket_low,
                },
                {
                    "setup": "SHORT_pre_market_high_sweep_entry_first15",
                    "side": "short",
                    "entry_price": premarket_high * (1.0 + entry_offset_pct / 100.0),
                    "level": premarket_high,
                },
            ]

            for spec in setup_specs:
                entry_idx = find_entry_idx(first15, spec["side"], spec["entry_price"])

                if entry_idx is None:
                    continue

                entry_bar = regular.loc[entry_idx]
                entry_time = entry_bar["ts_et"]

                for target_pct in TARGETS:
                    for stop_pct in STOPS:
                        result = simulate_trade(
                            bars=regular,
                            side=spec["side"],
                            entry_idx=entry_idx,
                            entry_price=spec["entry_price"],
                            target_pct=target_pct,
                            stop_pct=stop_pct,
                            cost_bps=args.cost_bps,
                        )

                        trades.append({
                            "ticker": ticker,
                            "trade_date": trade_date,
                            "setup": spec["setup"],
                            "side": spec["side"],
                            "entry_offset_pct": entry_offset_pct,
                            "entry_time": entry_time,
                            "entry_price": spec["entry_price"],
                            "premarket_high": premarket_high,
                            "premarket_low": premarket_low,
                            "target_pct": target_pct,
                            "stop_pct": stop_pct,
                            "cost_bps": args.cost_bps,
                            **result,
                        })

    trades_df = pd.DataFrame(trades)

    if trades_df.empty:
        print("No trades found.")
        return

    summary = summarize(trades_df)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    trades_path = OUTPUT_DIR / f"{args.label}_trades.csv"
    summary_path = OUTPUT_DIR / f"{args.label}_summary.csv"

    trades_df.to_csv(trades_path, index=False)
    summary.to_csv(summary_path, index=False)

    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 240)

    print()
    print("saved trades:", trades_path)
    print("saved summary:", summary_path)

    print()
    print("=== Top Target/Stop Combos | Sorted By Median Net ===")
    print(
        summary.sort_values(["median_net", "avg_net"], ascending=False)
        .head(30)
        .to_string(index=False)
    )

    print()
    print("=== Top Target/Stop Combos | Min 55% Win Rate ===")
    filt = summary[summary["win_rate"] >= 55].copy()
    if filt.empty:
        print("No combos with win rate >= 55%.")
    else:
        print(
            filt.sort_values(["avg_net", "median_net"], ascending=False)
            .head(30)
            .to_string(index=False)
        )


if __name__ == "__main__":
    main()

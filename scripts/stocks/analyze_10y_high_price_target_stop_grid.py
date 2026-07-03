from argparse import ArgumentParser
from pathlib import Path
from functools import lru_cache

import numpy as np
import pandas as pd


INPUT_PATH = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_features/high_price_short_fade_post_first15_path_metrics.csv"
)

OUTPUT_DIR = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_features"
)

TARGETS = [0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 2.0, 2.5, 3.0, 4.0]
STOPS = [0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 2.0, 2.5, 3.0, 4.0]


def detect_time_col(df):
    for col in [
        "timestamp_ms",
        "timestamp",
        "bar_start_utc",
        "bar_start_et",
        "datetime",
        "window_start",
        "t",
    ]:
        if col in df.columns:
            return col
    return None


def normalize_bars(raw):
    df = raw.copy()

    rename = {
        "o": "open",
        "h": "high",
        "l": "low",
        "c": "close",
        "v": "volume",
        "vw": "vwap",
        "t": "timestamp_ms",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    time_col = detect_time_col(df)
    if time_col is None:
        return None

    required = ["open", "high", "low", "close"]
    if any(col not in df.columns for col in required):
        return None

    if time_col in ["t", "timestamp_ms"] or pd.api.types.is_numeric_dtype(df[time_col]):
        ts = pd.to_datetime(df[time_col], unit="ms", utc=True, errors="coerce")
    else:
        ts = pd.to_datetime(df[time_col], utc=True, errors="coerce")

    df["ts_et"] = ts.dt.tz_convert("America/New_York")

    for col in required:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["ts_et", "open", "high", "low", "close"])
    df = df.sort_values("ts_et").copy()

    return df[["ts_et", "open", "high", "low", "close"]]


@lru_cache(maxsize=None)
def load_normalized_bars(cache_file):
    p = Path(cache_file)

    if not p.exists():
        return "missing_cache", None

    try:
        raw = pd.read_csv(p)
    except pd.errors.EmptyDataError:
        return "empty_cache", None
    except Exception:
        return "read_error", None

    bars = normalize_bars(raw)
    if bars is None or bars.empty:
        return "bad_bars", None

    return "ok", bars


def simulate_trade(row, side, target_pct, stop_pct, cost_bps):
    cache_file = row.get("cache_file")
    trade_date = str(row.get("trade_date"))
    entry = pd.to_numeric(row.get("entry_px"), errors="coerce")

    if pd.isna(entry) or entry <= 0 or not cache_file or pd.isna(cache_file):
        return {
            "status": "bad_input",
            "exit_reason": None,
            "gross_return_pct": np.nan,
            "net_return_pct": np.nan,
            "minutes_held": np.nan,
        }

    status, bars = load_normalized_bars(str(cache_file))
    if status != "ok":
        return {
            "status": status,
            "exit_reason": None,
            "gross_return_pct": np.nan,
            "net_return_pct": np.nan,
            "minutes_held": np.nan,
        }

    trade_day = pd.to_datetime(trade_date).date()

    regular = bars[
        (bars["ts_et"].dt.date == trade_day)
        & (bars["ts_et"].dt.time >= pd.to_datetime("09:45").time())
        & (bars["ts_et"].dt.time <= pd.to_datetime("16:00").time())
    ].copy()

    if regular.empty:
        return {
            "status": "no_post_first15_bars",
            "exit_reason": None,
            "gross_return_pct": np.nan,
            "net_return_pct": np.nan,
            "minutes_held": np.nan,
        }

    start_ts = regular.iloc[0]["ts_et"]

    if side == "long":
        target_px = entry * (1.0 + target_pct / 100.0)
        stop_px = entry * (1.0 - stop_pct / 100.0)
    elif side == "short":
        target_px = entry * (1.0 - target_pct / 100.0)
        stop_px = entry * (1.0 + stop_pct / 100.0)
    else:
        raise ValueError(f"bad side: {side}")

    for _, bar in regular.iterrows():
        minutes_held = (bar["ts_et"] - start_ts).total_seconds() / 60.0

        if side == "long":
            target_hit = bar["high"] >= target_px
            stop_hit = bar["low"] <= stop_px
        else:
            target_hit = bar["low"] <= target_px
            stop_hit = bar["high"] >= stop_px

        # Conservative rule: if target and stop hit in same 1m bar, assume stop first.
        if target_hit and stop_hit:
            gross = -stop_pct
            return {
                "status": "ok",
                "exit_reason": "stop_same_bar",
                "gross_return_pct": gross,
                "net_return_pct": gross - cost_bps / 100.0,
                "minutes_held": minutes_held,
            }

        if stop_hit:
            gross = -stop_pct
            return {
                "status": "ok",
                "exit_reason": "stop",
                "gross_return_pct": gross,
                "net_return_pct": gross - cost_bps / 100.0,
                "minutes_held": minutes_held,
            }

        if target_hit:
            gross = target_pct
            return {
                "status": "ok",
                "exit_reason": "target",
                "gross_return_pct": gross,
                "net_return_pct": gross - cost_bps / 100.0,
                "minutes_held": minutes_held,
            }

    eod_close = regular.iloc[-1]["close"]

    if side == "long":
        gross = (eod_close / entry - 1.0) * 100.0
    else:
        gross = (entry / eod_close - 1.0) * 100.0

    return {
        "status": "ok",
        "exit_reason": "eod",
        "gross_return_pct": gross,
        "net_return_pct": gross - cost_bps / 100.0,
        "minutes_held": (regular.iloc[-1]["ts_et"] - start_ts).total_seconds() / 60.0,
    }


def summarize_results(results):
    df = pd.DataFrame(results)
    ok = df[df["status"] == "ok"].copy()

    if ok.empty:
        return {
            "trades": 0,
            "avg_net": np.nan,
            "median_net": np.nan,
            "win_rate": np.nan,
            "target_rate": np.nan,
            "stop_rate": np.nan,
            "eod_rate": np.nan,
            "median_minutes_held": np.nan,
            "best": np.nan,
            "worst": np.nan,
        }

    return {
        "trades": len(ok),
        "avg_net": ok["net_return_pct"].mean(),
        "median_net": ok["net_return_pct"].median(),
        "win_rate": (ok["net_return_pct"] > 0).mean() * 100,
        "target_rate": (ok["exit_reason"] == "target").mean() * 100,
        "stop_rate": ok["exit_reason"].isin(["stop", "stop_same_bar"]).mean() * 100,
        "eod_rate": (ok["exit_reason"] == "eod").mean() * 100,
        "median_minutes_held": ok["minutes_held"].median(),
        "best": ok["net_return_pct"].max(),
        "worst": ok["net_return_pct"].min(),
    }


def main():
    parser = ArgumentParser()
    parser.add_argument("--setup", required=True)
    parser.add_argument("--side", choices=["long", "short"], required=True)
    parser.add_argument("--cost-bps", type=float, default=10.0)
    parser.add_argument("--label", required=True)
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(INPUT_PATH)

    df = df[
        (df["path_status"] == "ok")
        & (df["setup"] == args.setup)
    ].copy()

    print("setup:", args.setup)
    print("side:", args.side)
    print("cost_bps:", args.cost_bps)
    print("input trades:", len(df))
    print("tickers:", df["ticker"].nunique())

    all_summaries = []
    all_trade_rows = []

    for target_pct in TARGETS:
        for stop_pct in STOPS:
            print(f'processing target={target_pct} stop={stop_pct}', flush=True)
            results = []

            for _, row in df.iterrows():
                r = simulate_trade(
                    row=row,
                    side=args.side,
                    target_pct=target_pct,
                    stop_pct=stop_pct,
                    cost_bps=args.cost_bps,
                )
                r.update(
                    {
                        "ticker": row["ticker"],
                        "trade_date": row["trade_date"],
                        "setup": args.setup,
                        "side": args.side,
                        "target_pct": target_pct,
                        "stop_pct": stop_pct,
                        "cost_bps": args.cost_bps,
                    }
                )
                results.append(r)

            summary = summarize_results(results)
            summary.update(
                {
                    "setup": args.setup,
                    "side": args.side,
                    "target_pct": target_pct,
                    "stop_pct": stop_pct,
                    "cost_bps": args.cost_bps,
                }
            )

            all_summaries.append(summary)
            all_trade_rows.extend(results)

    summary_df = pd.DataFrame(all_summaries)
    trades_df = pd.DataFrame(all_trade_rows)

    summary_df = summary_df.sort_values(
        ["median_net", "avg_net", "win_rate"],
        ascending=[False, False, False],
    ).reset_index(drop=True)

    summary_path = OUTPUT_DIR / f"{args.label}_target_stop_grid_summary.csv"
    trades_path = OUTPUT_DIR / f"{args.label}_target_stop_grid_trades.csv"

    summary_df.to_csv(summary_path, index=False)
    trades_df.to_csv(trades_path, index=False)

    print()
    print("saved summary:", summary_path)
    print("saved trades:", trades_path)

    print()
    print("=== Top Target/Stop Combos | Sorted By Median Net ===")
    print(summary_df.head(25).to_string(index=False))

    print()
    print("=== Top Target/Stop Combos | Min 55% Win Rate ===")
    filtered = summary_df[summary_df["win_rate"] >= 55].copy()
    print(filtered.head(25).to_string(index=False))


if __name__ == "__main__":
    main()

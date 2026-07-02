from pathlib import Path

import numpy as np
import pandas as pd


FEATURES_PATH = Path(
    "data/research/full_market_scanner_10y/cheap_open_activation_features/extended_hours_features_pilot.csv"
)

CACHE_DIR = Path("data/cache/massive/extended_hours_1m")

OUTPUT_DIR = Path(
    "data/research/full_market_scanner_10y/cheap_open_activation_features"
)


HIGH_DAILY_REGIMES = [
    "extreme_p95_p99",
    "mania_p99_p99_9",
    "super_mania_p99_9_p100",
]

TARGETS = [6, 8, 10, 12, 15, 18, 20, 25]
STOPS = [4, 5, 6, 8, 10, 12]
COST_BPS_LIST = [0, 50, 100]


def find_cache_file(ticker, trade_date, prev_trade_date=None):
    ticker = str(ticker)
    candidates = []

    if prev_trade_date is not None and not pd.isna(prev_trade_date):
        candidates.append(CACHE_DIR / f"{ticker}_{prev_trade_date}_to_{trade_date}_1m.csv")

    candidates.extend(CACHE_DIR.glob(f"{ticker}_*_to_{trade_date}_1m.csv"))
    candidates.extend(CACHE_DIR.glob(f"{ticker}*{trade_date}*.csv"))

    for p in candidates:
        if p.exists():
            return p

    return None


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

    time_col = detect_time_col(df)
    if time_col is None:
        return None

    close_col = "close" if "close" in df.columns else "c" if "c" in df.columns else None
    high_col = "high" if "high" in df.columns else "h" if "h" in df.columns else None
    low_col = "low" if "low" in df.columns else "l" if "l" in df.columns else None

    if close_col is None or high_col is None or low_col is None:
        return None

    if time_col in ["timestamp_ms", "t"] or pd.api.types.is_numeric_dtype(df[time_col]):
        ts = pd.to_datetime(df[time_col], unit="ms", utc=True, errors="coerce")
    else:
        ts = pd.to_datetime(df[time_col], utc=True, errors="coerce")

    df["ts_et"] = ts.dt.tz_convert("America/New_York")
    df["close_px"] = pd.to_numeric(df[close_col], errors="coerce")
    df["high_px"] = pd.to_numeric(df[high_col], errors="coerce")
    df["low_px"] = pd.to_numeric(df[low_col], errors="coerce")

    df = df.dropna(subset=["ts_et", "close_px", "high_px", "low_px"])
    df = df[
        np.isfinite(df["close_px"])
        & np.isfinite(df["high_px"])
        & np.isfinite(df["low_px"])
        & (df["close_px"] > 0)
        & (df["high_px"] > 0)
        & (df["low_px"] > 0)
    ].copy()

    df = df.sort_values("ts_et").copy()

    return df[["ts_et", "close_px", "high_px", "low_px"]]


def get_day_bars(row):
    p = find_cache_file(
        row["ticker"],
        str(row["trade_date"]),
        row.get("prev_trade_date", None),
    )

    if p is None:
        return None, "missing_cache"

    try:
        raw = pd.read_csv(p)
    except Exception:
        return None, "read_error"

    bars = normalize_bars(raw)

    if bars is None or bars.empty:
        return None, "bad_bars"

    trade_day = pd.to_datetime(str(row["trade_date"])).date()
    day_bars = bars[bars["ts_et"].dt.date == trade_day].copy()

    if day_bars.empty:
        return None, "no_trade_day_bars"

    return day_bars, "ok"


def simulate_long(row, day_bars, target_pct, stop_pct):
    entry_px = pd.to_numeric(row.get("first_15m_close", np.nan), errors="coerce")

    if pd.isna(entry_px) or entry_px <= 0 or not np.isfinite(entry_px):
        return {"sim_status": "bad_entry"}

    trade_bars = day_bars[
        (day_bars["ts_et"].dt.time >= pd.to_datetime("09:45").time())
        & (day_bars["ts_et"].dt.time <= pd.to_datetime("16:00").time())
    ].copy()

    if trade_bars.empty:
        return {"sim_status": "no_post_first15_bars"}

    entry_ts = trade_bars.iloc[0]["ts_et"]
    target_px = entry_px * (1.0 + target_pct / 100.0)
    stop_px = entry_px * (1.0 - stop_pct / 100.0)

    exit_px = trade_bars.iloc[-1]["close_px"]
    exit_ts = trade_bars.iloc[-1]["ts_et"]
    exit_reason = "time_eod"

    for _, bar in trade_bars.iterrows():
        stop_hit = bar["low_px"] <= stop_px
        target_hit = bar["high_px"] >= target_px

        # Conservative if both happen in same minute.
        if stop_hit:
            exit_px = stop_px
            exit_ts = bar["ts_et"]
            exit_reason = "stop"
            break

        if target_hit:
            exit_px = target_px
            exit_ts = bar["ts_et"]
            exit_reason = "target"
            break

    gross_return_pct = (exit_px / entry_px - 1.0) * 100.0
    minutes_held = (exit_ts - entry_ts).total_seconds() / 60.0

    return {
        "sim_status": "ok",
        "entry_px": entry_px,
        "exit_px": exit_px,
        "target_px": target_px,
        "stop_px": stop_px,
        "exit_reason": exit_reason,
        "minutes_held": minutes_held,
        "gross_return_pct": gross_return_pct,
    }


def summarize(df):
    winners = df[df["net_return_pct"] > 0]
    losers = df[df["net_return_pct"] <= 0]

    avg_win = winners["net_return_pct"].mean() if len(winners) else np.nan
    avg_loss = losers["net_return_pct"].mean() if len(losers) else np.nan

    return {
        "trades": len(df),
        "tickers": df["ticker"].nunique(),
        "median_net_return_pct": df["net_return_pct"].median(),
        "avg_net_return_pct": df["net_return_pct"].mean(),
        "net_win_rate": (df["net_return_pct"] > 0).mean() * 100,
        "avg_win_pct": avg_win,
        "avg_loss_pct": avg_loss,
        "median_minutes_held": df["minutes_held"].median(),
        "target_rate": (df["exit_reason"] == "target").mean() * 100,
        "stop_rate": (df["exit_reason"] == "stop").mean() * 100,
        "time_exit_rate": (df["exit_reason"].astype(str).str.startswith("time")).mean() * 100,
        "worst_net_return_pct": df["net_return_pct"].min(),
        "best_net_return_pct": df["net_return_pct"].max(),
    }


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(FEATURES_PATH)

    numeric_cols = [
        "prev_close",
        "avg_dollar_volume_20d_prior",
        "premarket_dollar_vs_prior_daily_avg",
        "first_15m_dollar_volume",
        "first_15m_close",
        "first_15m_return_pct",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df[df["download_status"] == "ok"].copy()

    df["first15_dollar_vs_prior_daily_avg"] = np.where(
        df["avg_dollar_volume_20d_prior"] > 0,
        df["first_15m_dollar_volume"] / df["avg_dollar_volume_20d_prior"],
        np.nan,
    )

    base = df[
        (df["dollar_volume_regime"].isin(HIGH_DAILY_REGIMES))
        & (df["prev_close"] >= 1.0)
        & (df["prev_close"] < 5.0)
        & (df["premarket_dollar_vs_prior_daily_avg"] <= 0.1)
        & (df["first15_dollar_vs_prior_daily_avg"] >= 0.01)
        & (df["first_15m_return_pct"] >= 1.0)
    ].copy()

    print("base events:", len(base))
    print("tickers:", base["ticker"].nunique())

    rows = []
    status_rows = []

    for i, row in base.reset_index(drop=True).iterrows():
        day_bars, bar_status = get_day_bars(row)

        if bar_status != "ok":
            status_rows.append({"status": bar_status})
            continue

        for target_pct in TARGETS:
            for stop_pct in STOPS:
                sim = simulate_long(row, day_bars, target_pct, stop_pct)

                if sim.get("sim_status") != "ok":
                    status_rows.append({"status": sim.get("sim_status")})
                    continue

                for cost_bps in COST_BPS_LIST:
                    out = {
                        "ticker": row["ticker"],
                        "trade_date": row["trade_date"],
                        "prev_close": row["prev_close"],
                        "dollar_volume_regime": row["dollar_volume_regime"],
                        "premarket_dollar_vs_prior_daily_avg": row["premarket_dollar_vs_prior_daily_avg"],
                        "first15_dollar_vs_prior_daily_avg": row["first15_dollar_vs_prior_daily_avg"],
                        "first_15m_return_pct": row["first_15m_return_pct"],
                        "target_pct": target_pct,
                        "stop_pct": stop_pct,
                        "cost_bps": cost_bps,
                        **sim,
                    }

                    out["net_return_pct"] = out["gross_return_pct"] - (cost_bps / 100.0)
                    rows.append(out)

                status_rows.append({"status": "ok"})

        if (i + 1) % 250 == 0:
            print("processed:", i + 1)

    trades = pd.DataFrame(rows)
    status = pd.DataFrame(status_rows)

    trades_path = OUTPUT_DIR / "cheap_long_target_stop_grid_trades.csv"
    summary_path = OUTPUT_DIR / "cheap_long_target_stop_grid_summary.csv"

    trades.to_csv(trades_path, index=False)

    summary_rows = []
    for keys, sub in trades.groupby(["target_pct", "stop_pct", "cost_bps"], observed=True):
        target_pct, stop_pct, cost_bps = keys
        row = {
            "target_pct": target_pct,
            "stop_pct": stop_pct,
            "cost_bps": cost_bps,
        }
        row.update(summarize(sub))
        summary_rows.append(row)

    summary = pd.DataFrame(summary_rows).sort_values(
        ["cost_bps", "median_net_return_pct", "avg_net_return_pct"],
        ascending=[True, False, False],
    )

    summary.to_csv(summary_path, index=False)

    print()
    print("saved trades:", trades_path)
    print("saved summary:", summary_path)

    print()
    print("=== Sim Status ===")
    print(status["status"].value_counts(dropna=False).to_string())

    focus = summary[summary["cost_bps"] == 100].copy()

    print()
    print("=== Top Target/Stop Combos | 100 bps | Sorted By Median Net ===")
    display_cols = [
        "target_pct",
        "stop_pct",
        "cost_bps",
        "trades",
        "tickers",
        "median_net_return_pct",
        "avg_net_return_pct",
        "net_win_rate",
        "target_rate",
        "stop_rate",
        "time_exit_rate",
        "median_minutes_held",
        "worst_net_return_pct",
        "best_net_return_pct",
    ]
    print(focus[display_cols].head(20).to_string(index=False))

    print()
    print("=== Median Net Pivot | 100 bps ===")
    pivot_median = focus.pivot_table(
        index="stop_pct",
        columns="target_pct",
        values="median_net_return_pct",
        aggfunc="first",
    ).sort_index()
    print(pivot_median.round(2).to_string())

    print()
    print("=== Avg Net Pivot | 100 bps ===")
    pivot_avg = focus.pivot_table(
        index="stop_pct",
        columns="target_pct",
        values="avg_net_return_pct",
        aggfunc="first",
    ).sort_index()
    print(pivot_avg.round(2).to_string())

    print()
    print("=== Win Rate Pivot | 100 bps ===")
    pivot_win = focus.pivot_table(
        index="stop_pct",
        columns="target_pct",
        values="net_win_rate",
        aggfunc="first",
    ).sort_index()
    print(pivot_win.round(1).to_string())


if __name__ == "__main__":
    main()

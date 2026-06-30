from pathlib import Path

import pandas as pd

from src.utils.path_builders import build_market_curated_output_path


INPUT_PATH = Path("data/research/intraday_gap_up/us_expanded/final_strategy_ge3_rank10_fb025.csv")
OUTPUT_DIR = Path("data/research/intraday_gap_up/us_expanded")

TARGET_PCT = 2.0
COST_BPS = 20

RULES = [
    {"name": "no_stop_eod", "stop_pct": None, "time_exit": None},
    {"name": "stop_2pct_eod", "stop_pct": 2.0, "time_exit": None},
    {"name": "stop_3pct_eod", "stop_pct": 3.0, "time_exit": None},
    {"name": "no_stop_exit_1200", "stop_pct": None, "time_exit": "12:00"},
    {"name": "no_stop_exit_1400", "stop_pct": None, "time_exit": "14:00"},
    {"name": "stop_2pct_exit_1200", "stop_pct": 2.0, "time_exit": "12:00"},
    {"name": "stop_2pct_exit_1400", "stop_pct": 2.0, "time_exit": "14:00"},
    {"name": "stop_3pct_exit_1400", "stop_pct": 3.0, "time_exit": "14:00"},
]


def get_rth(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["bar_start_utc"] = pd.to_datetime(out["bar_start"], utc=True)
    out["bar_start_et"] = out["bar_start_utc"].dt.tz_convert("America/New_York")
    out["time_et"] = out["bar_start_et"].dt.time

    return out[
        (out["time_et"] >= pd.Timestamp("09:30").time())
        & (out["time_et"] < pd.Timestamp("16:00").time())
    ].sort_values("bar_start_et").copy()


def load_15m(ticker: str, trade_date: str) -> pd.DataFrame:
    path = build_market_curated_output_path(
        symbol=ticker,
        start_date=trade_date,
        end_date=trade_date,
        timeframe="15m",
    )
    return pd.read_parquet(path)


def simulate(rth: pd.DataFrame, stop_pct: float | None, time_exit: str | None) -> dict:
    first_bar = rth.iloc[0]
    entry = first_bar["close"]
    target = entry * (1 + TARGET_PCT / 100)

    stop = None
    if stop_pct is not None:
        stop = entry * (1 - stop_pct / 100)

    exit_price = rth.iloc[-1]["close"]
    exit_reason = "eod"
    exit_time_et = str(rth.iloc[-1]["bar_start_et"])

    time_exit_time = None
    if time_exit is not None:
        time_exit_time = pd.Timestamp(time_exit).time()

    for _, bar in rth.iloc[1:].iterrows():
        if stop is not None and bar["low"] <= stop:
            exit_price = stop
            exit_reason = "stop"
            exit_time_et = str(bar["bar_start_et"])
            break

        if bar["high"] >= target:
            exit_price = target
            exit_reason = "target"
            exit_time_et = str(bar["bar_start_et"])
            break

        if time_exit_time is not None and bar["time_et"] >= time_exit_time:
            exit_price = bar["close"]
            exit_reason = f"time_exit_{time_exit.replace(':', '')}"
            exit_time_et = str(bar["bar_start_et"])
            break

    gross = (exit_price / entry - 1) * 100
    net = gross - (COST_BPS / 100)

    return {
        "entry_price": entry,
        "exit_price": exit_price,
        "gross_return_pct": gross,
        "net_return_pct": net,
        "exit_reason": exit_reason,
        "exit_time_et": exit_time_et,
    }


def main() -> None:
    trades = pd.read_csv(INPUT_PATH)
    rows = []

    for _, trade in trades.iterrows():
        ticker = trade["ticker"]
        trade_date = str(trade["trade_date"])[:10]

        bars = load_15m(ticker, trade_date)
        rth = get_rth(bars)

        if len(rth) < 2:
            continue

        for rule in RULES:
            result = simulate(
                rth=rth,
                stop_pct=rule["stop_pct"],
                time_exit=rule["time_exit"],
            )

            rows.append(
                {
                    "rule": rule["name"],
                    "split": trade["split"],
                    "ticker": ticker,
                    "trade_date": trade_date,
                    "daily_signal_rank": trade["daily_signal_rank"],
                    "relative_gap_vs_spy_pct": trade["relative_gap_vs_spy_pct"],
                    "first_bar_return_pct": trade["first_bar_return_pct"],
                    **result,
                }
            )

    results = pd.DataFrame(rows)
    results["trade_date"] = pd.to_datetime(results["trade_date"])
    results["year"] = results["trade_date"].dt.year

    summary = (
        results.groupby("rule")
        .agg(
            trades=("ticker", "count"),
            avg=("net_return_pct", "mean"),
            median=("net_return_pct", "median"),
            win_rate=("net_return_pct", lambda s: (s > 0).mean() * 100),
            target_rate=("exit_reason", lambda s: (s == "target").mean() * 100),
            stop_rate=("exit_reason", lambda s: (s == "stop").mean() * 100),
            worst=("net_return_pct", "min"),
            best=("net_return_pct", "max"),
            total=("net_return_pct", "sum"),
        )
        .reset_index()
        .sort_values(["avg", "median"], ascending=[False, False])
    )

    by_year = (
        results.groupby(["rule", "year"])
        .agg(
            trades=("ticker", "count"),
            avg=("net_return_pct", "mean"),
            median=("net_return_pct", "median"),
            win_rate=("net_return_pct", lambda s: (s > 0).mean() * 100),
            worst=("net_return_pct", "min"),
            total=("net_return_pct", "sum"),
        )
        .reset_index()
    )

    results_path = OUTPUT_DIR / "final_strategy_exit_sweep_results.csv"
    summary_path = OUTPUT_DIR / "final_strategy_exit_sweep_summary.csv"
    by_year_path = OUTPUT_DIR / "final_strategy_exit_sweep_by_year.csv"

    results.to_csv(results_path, index=False)
    summary.to_csv(summary_path, index=False)
    by_year.to_csv(by_year_path, index=False)

    print("=== Final strategy exit sweep ===")
    print(f"Input trades: {len(trades)}")
    print(f"Result rows: {len(results)}")
    print(f"Saved results: {results_path}")
    print(f"Saved summary: {summary_path}")
    print(f"Saved by year: {by_year_path}")
    print()
    print("=== Summary ===")
    print(summary.round(4).to_string(index=False))
    print()
    print("=== By year ===")
    print(by_year.round(4).to_string(index=False))


if __name__ == "__main__":
    main()

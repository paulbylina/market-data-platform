from pathlib import Path
import json

import pandas as pd

from src.utils.path_builders import build_market_curated_output_path


CONFIG_PATH = Path("configs/scanners/rs_scanner_us_expanded.json")
INPUT_PATH = Path("data/research/intraday_gap_up/us_expanded/no_oversold_relative_gap_daily_candidates.csv")
OUTPUT_DIR = Path("data/research/intraday_gap_up/us_expanded")

REL_GAP_THRESHOLDS = [3.5, 4.0]
RANK_LIMITS = [5]
FIRST_BAR_MAX_LIST = [-0.25, -0.50, -0.75]

TARGET_STOP_LIST = [
    (2.5, 3.0),
    (3.0, 3.0),
    (2.5, 2.5),
    (3.0, 2.5),
]

STOCK_FILTERS = [
    ("stock_any", None),
    ("stock_z_le_0", 0.0),
    ("stock_z_le_minus_0_5", -0.5),
    ("stock_z_le_minus_1", -1.0),
]

SPY_FILTERS = [
    ("spy_any", None),
    ("spy_z_le_0", 0.0),
    ("spy_z_le_minus_0_5", -0.5),
    ("spy_z_le_minus_1", -1.0),
]

COST_BPS = 20


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

    if not path.exists():
        raise FileNotFoundError(path)

    return pd.read_parquet(path)


def simulate_short(rth: pd.DataFrame, target_pct: float, stop_pct: float) -> dict:
    first_bar = rth.iloc[0]
    entry = first_bar["close"]

    target = entry * (1 - target_pct / 100)
    stop = entry * (1 + stop_pct / 100)

    exit_price = rth.iloc[-1]["close"]
    exit_reason = "eod"
    exit_time_et = str(rth.iloc[-1]["bar_start_et"])

    for _, bar in rth.iloc[1:].iterrows():
        # Conservative same-bar assumption for shorts: stop first.
        if bar["high"] >= stop:
            exit_price = stop
            exit_reason = "stop"
            exit_time_et = str(bar["bar_start_et"])
            break

        if bar["low"] <= target:
            exit_price = target
            exit_reason = "target"
            exit_time_et = str(bar["bar_start_et"])
            break

    gross = (entry / exit_price - 1) * 100
    net = gross - (COST_BPS / 100)

    return {
        "entry_price": entry,
        "exit_price": exit_price,
        "gross_return_pct": gross,
        "net_return_pct": net,
        "exit_reason": exit_reason,
        "exit_time_et": exit_time_et,
    }


def summarize(sub: pd.DataFrame) -> dict:
    years = sub["trade_date"].dt.year.nunique()

    return {
        "trades": len(sub),
        "years": years,
        "trades_per_year": len(sub) / years if years else float("nan"),
        "trades_per_day": len(sub) / (years * 252) if years else float("nan"),
        "avg": sub["net_return_pct"].mean(),
        "median": sub["net_return_pct"].median(),
        "win_rate": (sub["net_return_pct"] > 0).mean() * 100 if len(sub) else float("nan"),
        "target_rate": (sub["exit_reason"] == "target").mean() * 100 if len(sub) else float("nan"),
        "stop_rate": (sub["exit_reason"] == "stop").mean() * 100 if len(sub) else float("nan"),
        "worst": sub["net_return_pct"].min() if len(sub) else float("nan"),
        "best": sub["net_return_pct"].max() if len(sub) else float("nan"),
        "total": sub["net_return_pct"].sum(),
    }


def main() -> None:
    with CONFIG_PATH.open() as f:
        config = json.load(f)

    symbols = config["stock_symbols"]
    benchmark = config["benchmark_symbol"]
    start = config["start_date"]
    end = config["end_date"]
    timeframe = config["timeframe"]
    data_root = Path(config["data_root"])

    curated_dir = data_root / "curated" / "market" / timeframe

    candidates = pd.read_csv(INPUT_PATH)
    candidates["trade_date"] = pd.to_datetime(candidates["trade_date"])

    # Only use the downloaded short/fade pool.
    candidates = candidates[
        (candidates["relative_gap_vs_spy_pct"] >= 3.5)
        & (candidates["relative_gap_rank"] <= 5)
    ].copy()

    # Stock prior-day weakness.
    stock_feature_rows = []

    for symbol in symbols:
        path = curated_dir / f"{symbol}_{start}_{end}_curated.parquet"

        if not path.exists():
            continue

        df = pd.read_parquet(path).sort_values("date").reset_index(drop=True)
        df["date"] = pd.to_datetime(df["date"])
        df["ticker"] = symbol

        df["close_zscore_50d"] = (
            df["close"] - df["close"].rolling(50).mean()
        ) / df["close"].rolling(50).std()

        df["prior_close_zscore_50d"] = df["close_zscore_50d"].shift(1)

        df = df.rename(columns={"date": "trade_date"})

        stock_feature_rows.append(
            df[["ticker", "trade_date", "prior_close_zscore_50d"]].copy()
        )

    stock_features = pd.concat(stock_feature_rows).dropna().reset_index(drop=True)

    # SPY prior-day weakness.
    spy_path = curated_dir / f"{benchmark}_{start}_{end}_curated.parquet"
    spy = pd.read_parquet(spy_path).sort_values("date").reset_index(drop=True)
    spy["date"] = pd.to_datetime(spy["date"])

    spy["spy_zscore_200d"] = (
        spy["close"] - spy["close"].rolling(200).mean()
    ) / spy["close"].rolling(200).std()

    spy["prior_spy_zscore_200d"] = spy["spy_zscore_200d"].shift(1)

    spy = spy[["date", "prior_spy_zscore_200d"]].rename(columns={"date": "trade_date"})

    candidates = candidates.merge(stock_features, on=["ticker", "trade_date"], how="left")
    candidates = candidates.merge(spy, on="trade_date", how="left")
    candidates = candidates.dropna(
        subset=["prior_close_zscore_50d", "prior_spy_zscore_200d"]
    ).copy()

    trade_rows = []
    missing = 0

    for _, row in candidates.iterrows():
        ticker = row["ticker"]
        trade_date = row["trade_date"].strftime("%Y-%m-%d")

        try:
            bars = load_15m(ticker, trade_date)
        except FileNotFoundError:
            missing += 1
            continue

        rth = get_rth(bars)

        if len(rth) < 2:
            continue

        first_bar = rth.iloc[0]
        first_bar_return_pct = (first_bar["close"] / first_bar["open"] - 1) * 100

        for target_pct, stop_pct in TARGET_STOP_LIST:
            result = simulate_short(rth, target_pct, stop_pct)

            trade_rows.append(
                {
                    "ticker": ticker,
                    "trade_date": row["trade_date"],
                    "stock_gap_pct": row["stock_gap_pct"],
                    "spy_gap_pct": row["spy_gap_pct"],
                    "relative_gap_vs_spy_pct": row["relative_gap_vs_spy_pct"],
                    "relative_gap_rank": row["relative_gap_rank"],
                    "prior_close_zscore_50d": row["prior_close_zscore_50d"],
                    "prior_spy_zscore_200d": row["prior_spy_zscore_200d"],
                    "first_bar_return_pct": first_bar_return_pct,
                    "target_pct": target_pct,
                    "stop_pct": stop_pct,
                    **result,
                }
            )

    trades = pd.DataFrame(trade_rows)
    trades["trade_date"] = pd.to_datetime(trades["trade_date"])
    trades["year"] = trades["trade_date"].dt.year

    summary_rows = []

    for rel_gap in REL_GAP_THRESHOLDS:
        for rank_limit in RANK_LIMITS:
            for first_bar_max in FIRST_BAR_MAX_LIST:
                for target_pct, stop_pct in TARGET_STOP_LIST:
                    base = trades[
                        (trades["relative_gap_vs_spy_pct"] >= rel_gap)
                        & (trades["relative_gap_rank"] <= rank_limit)
                        & (trades["first_bar_return_pct"] <= first_bar_max)
                        & (trades["target_pct"] == target_pct)
                        & (trades["stop_pct"] == stop_pct)
                    ].copy()

                    for stock_filter_name, stock_max in STOCK_FILTERS:
                        for spy_filter_name, spy_max in SPY_FILTERS:
                            sub = base.copy()

                            if stock_max is not None:
                                sub = sub[sub["prior_close_zscore_50d"] <= stock_max]

                            if spy_max is not None:
                                sub = sub[sub["prior_spy_zscore_200d"] <= spy_max]

                            if len(sub) == 0:
                                continue

                            stats = summarize(sub)

                            summary_rows.append(
                                {
                                    "setup": f"short_relgap_ge_{rel_gap:g}_rank_le_{rank_limit}_first_bar_le_{first_bar_max:g}",
                                    "target_pct": target_pct,
                                    "stop_pct": stop_pct,
                                    "stock_filter": stock_filter_name,
                                    "spy_filter": spy_filter_name,
                                    **stats,
                                }
                            )

    summary = pd.DataFrame(summary_rows).sort_values(
        ["avg", "trades_per_day"],
        ascending=[False, False],
    )

    trades_path = OUTPUT_DIR / "no_oversold_short_weak_context_trades.csv"
    summary_path = OUTPUT_DIR / "no_oversold_short_weak_context_summary.csv"

    trades.to_csv(trades_path, index=False)
    summary.to_csv(summary_path, index=False)

    print("=== Short setup with weak stock / weak SPY filters ===")
    print("Input candidates:", len(candidates))
    print("Missing intraday files:", missing)
    print("Trade rows:", len(trades))
    print("Saved trades:", trades_path)
    print("Saved summary:", summary_path)
    print()

    print("=== Best avg results, minimum 100 trades ===")
    print(summary[summary["trades"] >= 100].head(40).round(4).to_string(index=False))
    print()

    print("=== Results near 0.5 trades/day or more ===")
    active = summary[summary["trades_per_day"] >= 0.5].copy()
    print(active.head(40).round(4).to_string(index=False))


if __name__ == "__main__":
    main()

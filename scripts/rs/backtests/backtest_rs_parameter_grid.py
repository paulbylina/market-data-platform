from pathlib import Path
import json
import pandas as pd

CONFIG_PATH = Path("configs/scanners/rs_scanner.json")
RS_DIR = Path("data/serving/scanners/rs")
SPY_PATH = Path("data/curated/market/1d/SPY_2016-01-01_2026-06-28_curated.parquet")

POSITION_SIZE = 0.10
MAX_POSITIONS = 10

THRESHOLD_QUANTILES = [0.10, 0.20, 0.30]
HOLD_DAYS_LIST = [3, 5, 10]

SPLITS = [
    {
        "name": "WF1",
        "train_end": "2020-01-01",
        "oos_start": "2022-01-01",
        "oos_end": "2024-01-01",
    },
    {
        "name": "WF2",
        "train_end": "2022-01-01",
        "oos_start": "2024-01-01",
        "oos_end": None,
    },
]

with CONFIG_PATH.open("r", encoding="utf-8") as f:
    config = json.load(f)

symbols = config["stock_symbols"]
benchmark = config["benchmark_symbol"]
start = config["start_date"]
end = config["end_date"]

spy = pd.read_parquet(SPY_PATH).sort_values("date").reset_index(drop=True)
spy["date"] = pd.to_datetime(spy["date"])
spy["spy_ma_200"] = spy["close"].rolling(200).mean()
spy["spy_above_200d"] = spy["close"] > spy["spy_ma_200"]
spy_regime = spy[["date", "spy_ma_200", "spy_above_200d"]].copy()

base_rows = []

for symbol in symbols:
    path = RS_DIR / f"{symbol}_vs_{benchmark}_{start}_{end}_rs_scan.parquet"
    df = pd.read_parquet(path).sort_values("date").reset_index(drop=True)
    df["date"] = pd.to_datetime(df["date"])

    df = df.merge(spy_regime, on="date", how="left")
    df = df.dropna(subset=["close_zscore_50d", "spy_ma_200", "spy_above_200d"]).copy()
    df["ticker"] = symbol

    base_rows.append(
        df[
            [
                "date",
                "ticker",
                "stock_return_pct",
                "benchmark_return_pct",
                "close_zscore_50d",
                "spy_above_200d",
            ]
        ]
    )

base_panel = pd.concat(base_rows).sort_values(["date", "ticker"]).reset_index(drop=True)

wide_returns = (
    base_panel
    .pivot(index="date", columns="ticker", values="stock_return_pct")
    .sort_index()
    / 100
)

basket_daily = wide_returns.mean(axis=1).fillna(0)
common_dates = sorted(wide_returns.index)


def get_dates(start_date, end_date=None):
    dates = [d for d in common_dates if d >= pd.Timestamp(start_date)]

    if end_date is not None:
        dates = [d for d in dates if d < pd.Timestamp(end_date)]

    return dates


def max_drawdown(equity):
    peak = equity.cummax()
    dd = equity / peak - 1
    return dd.min() * 100


def build_panel_for_split(train_end, threshold_quantile):
    frames = []
    threshold_rows = []

    for symbol in symbols:
        df = base_panel[base_panel["ticker"] == symbol].copy()
        train = df[df["date"] < pd.Timestamp(train_end)].copy()

        threshold = train["close_zscore_50d"].quantile(threshold_quantile)

        threshold_rows.append(
            {
                "ticker": symbol,
                "threshold_quantile": threshold_quantile,
                "train_rows": len(train),
                "threshold": threshold,
            }
        )

        df["threshold"] = threshold
        df["signal"] = (
            (df["close_zscore_50d"] <= threshold)
            & (df["spy_above_200d"])
        )

        frames.append(df)

    return (
        pd.concat(frames).sort_values(["date", "ticker"]).reset_index(drop=True),
        pd.DataFrame(threshold_rows),
    )


def simulate_basket_replacement(panel, dates, hold_days):
    open_positions = []
    rows = []
    trades = []
    equity = 1.0

    for date in dates:
        day_all = panel[panel["date"] == date].copy()
        day_signals = day_all[day_all["signal"]].copy()

        basket_r = basket_daily.loc[date]

        active_weight = len(open_positions) * POSITION_SIZE
        signal_stock_return = 0.0
        still_open = []

        for pos in open_positions:
            ticker = pos["ticker"]
            row = day_all[day_all["ticker"] == ticker]

            if len(row) > 0:
                stock_r = row.iloc[0]["stock_return_pct"] / 100

                signal_stock_return += POSITION_SIZE * stock_r

                pos["cum_stock_return"] = (
                    (1 + pos["cum_stock_return"]) * (1 + stock_r) - 1
                )

                pos["cum_basket_excess"] += POSITION_SIZE * (stock_r - basket_r)

            pos["days_held"] += 1

            if pos["days_held"] >= hold_days:
                trades.append(
                    {
                        "ticker": ticker,
                        "entry_date": pos["entry_date"],
                        "exit_date": date,
                        "stock_trade_return_pct": pos["cum_stock_return"] * 100,
                        "basket_excess_contribution_pct": pos["cum_basket_excess"] * 100,
                    }
                )
            else:
                still_open.append(pos)

        daily_return = (1 - active_weight) * basket_r + signal_stock_return
        equity *= (1 + daily_return)

        open_positions = still_open
        open_tickers = {pos["ticker"] for pos in open_positions}

        # If many signals fire on the same day, choose the most extreme z-scores first.
        day_signals = day_signals.sort_values("close_zscore_50d", ascending=True)

        for _, row in day_signals.iterrows():
            if len(open_positions) >= MAX_POSITIONS:
                break

            if row["ticker"] in open_tickers:
                continue

            open_positions.append(
                {
                    "ticker": row["ticker"],
                    "entry_date": date,
                    "days_held": 0,
                    "cum_stock_return": 0.0,
                    "cum_basket_excess": 0.0,
                }
            )

            open_tickers.add(row["ticker"])

        rows.append(
            {
                "date": date,
                "equity": equity,
                "active_positions": len(open_positions),
                "active_weight": len(open_positions) * POSITION_SIZE,
            }
        )

    return pd.DataFrame(rows), pd.DataFrame(trades)


summary_rows = []

for split in SPLITS:
    oos_dates = get_dates(split["oos_start"], split["oos_end"])

    basket_bh = (1 + basket_daily.loc[oos_dates]).cumprod()
    basket_return = (basket_bh.iloc[-1] - 1) * 100
    basket_dd = max_drawdown(basket_bh)

    for q in THRESHOLD_QUANTILES:
        panel, thresholds = build_panel_for_split(
            train_end=split["train_end"],
            threshold_quantile=q,
        )

        for hold_days in HOLD_DAYS_LIST:
            equity_df, trades_df = simulate_basket_replacement(
                panel=panel,
                dates=oos_dates,
                hold_days=hold_days,
            )

            strategy_return = (equity_df["equity"].iloc[-1] - 1) * 100
            strategy_dd = max_drawdown(equity_df["equity"])

            if len(trades_df) > 0:
                avg_trade = trades_df["stock_trade_return_pct"].mean()
                median_trade = trades_df["stock_trade_return_pct"].median()
                win_rate = (trades_df["stock_trade_return_pct"] > 0).mean() * 100
                basket_excess_total = trades_df["basket_excess_contribution_pct"].sum()
                trades = len(trades_df)
            else:
                avg_trade = float("nan")
                median_trade = float("nan")
                win_rate = float("nan")
                basket_excess_total = 0.0
                trades = 0

            summary_rows.append(
                {
                    "split": split["name"],
                    "train_end": split["train_end"],
                    "oos_start": split["oos_start"],
                    "oos_end": split["oos_end"] or "latest",
                    "threshold_q": q,
                    "hold_days": hold_days,
                    "trades": trades,
                    "basket_return": basket_return,
                    "strategy_return": strategy_return,
                    "excess_return": strategy_return - basket_return,
                    "basket_max_dd": basket_dd,
                    "strategy_max_dd": strategy_dd,
                    "dd_delta": strategy_dd - basket_dd,
                    "avg_active_weight": equity_df["active_weight"].mean() * 100,
                    "max_active_weight": equity_df["active_weight"].max() * 100,
                    "avg_trade": avg_trade,
                    "median_trade": median_trade,
                    "win_rate": win_rate,
                    "basket_excess_total": basket_excess_total,
                }
            )

summary = pd.DataFrame(summary_rows)

print("=== Parameter robustness: basket core + signal replacement ===")
print(f"Position size: {POSITION_SIZE * 100:.0f}%")
print(f"Max positions: {MAX_POSITIONS}")
print()

print("=== Full grid ===")
print(
    summary[
        [
            "split",
            "threshold_q",
            "hold_days",
            "trades",
            "basket_return",
            "strategy_return",
            "excess_return",
            "basket_max_dd",
            "strategy_max_dd",
            "dd_delta",
            "avg_active_weight",
            "max_active_weight",
            "avg_trade",
            "median_trade",
            "win_rate",
            "basket_excess_total",
        ]
    ]
    .round(4)
    .sort_values(["split", "threshold_q", "hold_days"])
    .to_string(index=False)
)

print("\n=== Configs that beat basket buy-hold ===")
beat = summary[summary["excess_return"] > 0].copy()
print(
    beat[
        [
            "split",
            "threshold_q",
            "hold_days",
            "trades",
            "excess_return",
            "strategy_return",
            "strategy_max_dd",
            "avg_active_weight",
            "avg_trade",
            "median_trade",
            "win_rate",
        ]
    ]
    .round(4)
    .sort_values(["split", "excess_return"], ascending=[True, False])
    .to_string(index=False)
)

print("\n=== Average excess return by parameter across WF1 + WF2 ===")
param_avg = (
    summary
    .groupby(["threshold_q", "hold_days"])
    .agg(
        avg_excess_return=("excess_return", "mean"),
        min_excess_return=("excess_return", "min"),
        max_excess_return=("excess_return", "max"),
        avg_dd_delta=("dd_delta", "mean"),
        avg_trades=("trades", "mean"),
        avg_active_weight=("avg_active_weight", "mean"),
        avg_trade=("avg_trade", "mean"),
        median_trade=("median_trade", "mean"),
        avg_win_rate=("win_rate", "mean"),
    )
    .reset_index()
)

print(
    param_avg
    .round(4)
    .sort_values("avg_excess_return", ascending=False)
    .to_string(index=False)
)
from pathlib import Path
import json
import pandas as pd

CONFIG_PATH = Path("configs/scanners/rs_scanner.json")
RS_DIR = Path("data/serving/scanners/rs")
SPY_PATH = Path("data/curated/market/1d/SPY_2016-01-01_2026-06-28_curated.parquet")

POSITION_SIZE = 0.10
STOCK_THRESHOLD_Q = 0.20
HOLD_DAYS = 3
MAX_POSITIONS_LIST = [3, 5, 10]

REGIME_COLS = [
    "regime_above_200sma",
    "regime_z200_gt_train_q20",
    "regime_z200_between_train_q20_q80",
]

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

spy["spy_close_zscore_200d"] = (
    spy["close"] - spy["close"].rolling(200).mean()
) / spy["close"].rolling(200).std()

spy_features = spy[
    [
        "date",
        "spy_ma_200",
        "spy_above_200d",
        "spy_close_zscore_200d",
    ]
].copy()

base_rows = []

for symbol in symbols:
    path = RS_DIR / f"{symbol}_vs_{benchmark}_{start}_{end}_rs_scan.parquet"

    df = pd.read_parquet(path).sort_values("date").reset_index(drop=True)
    df["date"] = pd.to_datetime(df["date"])

    df = df.merge(spy_features, on="date", how="left")

    df = df.dropna(
        subset=[
            "close_zscore_50d",
            "stock_return_pct",
            "spy_above_200d",
            "spy_close_zscore_200d",
        ]
    ).copy()

    df["ticker"] = symbol

    base_rows.append(
        df[
            [
                "date",
                "ticker",
                "stock_return_pct",
                "close_zscore_50d",
                "spy_above_200d",
                "spy_close_zscore_200d",
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


def build_panel_for_split(train_end):
    train_end = pd.Timestamp(train_end)

    regime_train = (
        base_panel[base_panel["date"] < train_end]
        .drop_duplicates("date")
        .copy()
    )

    z200_q20 = regime_train["spy_close_zscore_200d"].quantile(0.20)
    z200_q80 = regime_train["spy_close_zscore_200d"].quantile(0.80)

    frames = []
    threshold_rows = []

    for symbol in symbols:
        df = base_panel[base_panel["ticker"] == symbol].copy()
        train = df[df["date"] < train_end].copy()

        stock_threshold = train["close_zscore_50d"].quantile(STOCK_THRESHOLD_Q)

        threshold_rows.append(
            {
                "ticker": symbol,
                "train_rows": len(train),
                "stock_threshold": stock_threshold,
            }
        )

        df["stock_threshold"] = stock_threshold
        df["stock_signal"] = df["close_zscore_50d"] <= stock_threshold

        df["regime_above_200sma"] = df["spy_above_200d"]

        df["regime_z200_gt_train_q20"] = (
            df["spy_close_zscore_200d"] > z200_q20
        )

        df["regime_z200_between_train_q20_q80"] = (
            (df["spy_close_zscore_200d"] > z200_q20)
            & (df["spy_close_zscore_200d"] < z200_q80)
        )

        frames.append(df)

    panel = pd.concat(frames).sort_values(["date", "ticker"]).reset_index(drop=True)

    regime_thresholds = {
        "z200_q20": z200_q20,
        "z200_q80": z200_q80,
    }

    return panel, pd.DataFrame(threshold_rows), regime_thresholds


def simulate_basket_replacement(panel, dates, regime_col, max_positions):
    open_positions = []
    rows = []
    trades = []
    equity = 1.0

    for date in dates:
        day_all = panel[panel["date"] == date].copy()

        day_signals = day_all[
            (day_all["stock_signal"])
            & (day_all[regime_col])
        ].copy()

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

            if pos["days_held"] >= HOLD_DAYS:
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

        # If many signals fire on the same day, take the most extreme stock z-scores first.
        day_signals = day_signals.sort_values("close_zscore_50d", ascending=True)

        for _, row in day_signals.iterrows():
            if len(open_positions) >= max_positions:
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
threshold_output_rows = []

for split in SPLITS:
    panel, stock_thresholds, regime_thresholds = build_panel_for_split(split["train_end"])
    oos_dates = get_dates(split["oos_start"], split["oos_end"])

    basket_bh = (1 + basket_daily.loc[oos_dates]).cumprod()
    basket_return = (basket_bh.iloc[-1] - 1) * 100
    basket_dd = max_drawdown(basket_bh)

    threshold_output_rows.append(
        {
            "split": split["name"],
            "train_end": split["train_end"],
            "z200_q20": regime_thresholds["z200_q20"],
            "z200_q80": regime_thresholds["z200_q80"],
        }
    )

    for regime_col in REGIME_COLS:
        for max_positions in MAX_POSITIONS_LIST:
            equity_df, trades_df = simulate_basket_replacement(
                panel=panel,
                dates=oos_dates,
                regime_col=regime_col,
                max_positions=max_positions,
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
                    "regime": regime_col.replace("regime_", ""),
                    "max_positions": max_positions,
                    "max_signal_exposure": max_positions * POSITION_SIZE * 100,
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

print("=== Regime + exposure cap test ===")
print(f"Universe size: {len(symbols)}")
print(f"Stock threshold: bottom {STOCK_THRESHOLD_Q * 100:.0f}% close_zscore_50d")
print(f"Hold days: {HOLD_DAYS}")
print(f"Position size: {POSITION_SIZE * 100:.0f}%")
print()

print("=== Regime thresholds learned from train ===")
print(pd.DataFrame(threshold_output_rows).round(4).to_string(index=False))

print("\n=== Full results ===")
print(
    summary[
        [
            "split",
            "regime",
            "max_positions",
            "max_signal_exposure",
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
    .sort_values(["split", "regime", "max_positions"])
    .to_string(index=False)
)

print("\n=== Average by regime and exposure cap across WF1 + WF2 ===")
avg = (
    summary
    .groupby(["regime", "max_positions", "max_signal_exposure"])
    .agg(
        avg_excess_return=("excess_return", "mean"),
        min_excess_return=("excess_return", "min"),
        max_excess_return=("excess_return", "max"),
        avg_dd_delta=("dd_delta", "mean"),
        avg_active_weight=("avg_active_weight", "mean"),
        avg_trades=("trades", "mean"),
        avg_trade=("avg_trade", "mean"),
        median_trade=("median_trade", "mean"),
        avg_win_rate=("win_rate", "mean"),
    )
    .reset_index()
)

print(
    avg
    .round(4)
    .sort_values(["avg_excess_return"], ascending=False)
    .to_string(index=False)
)

print("\n=== Configs positive in both WF1 and WF2 ===")
positive_both = (
    summary
    .pivot_table(
        index=["regime", "max_positions", "max_signal_exposure"],
        columns="split",
        values="excess_return",
    )
    .reset_index()
)

positive_both["min_excess_return"] = positive_both[["WF1", "WF2"]].min(axis=1)
positive_both["avg_excess_return"] = positive_both[["WF1", "WF2"]].mean(axis=1)

positive_both = positive_both[positive_both["min_excess_return"] > 0]

print(
    positive_both
    .round(4)
    .sort_values("avg_excess_return", ascending=False)
    .to_string(index=False)
)

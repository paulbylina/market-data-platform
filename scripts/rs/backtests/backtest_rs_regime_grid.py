from pathlib import Path
import json
import pandas as pd

CONFIG_PATH = Path("configs/scanners/rs_scanner.json")
RS_DIR = Path("data/serving/scanners/rs")
SPY_PATH = Path("data/curated/market/1d/SPY_2016-01-01_2026-06-28_curated.parquet")

POSITION_SIZE = 0.10
STOCK_THRESHOLD_Q = 0.20
HOLD_DAYS = 3
MAX_POSITIONS = 3

SPLITS = [
    {
        "name": "WF1",
        "train_end": "2020-01-01",
        "valid_start": "2020-01-01",
        "valid_end": "2022-01-01",
        "oos_start": "2022-01-01",
        "oos_end": "2024-01-01",
    },
    {
        "name": "WF2",
        "train_end": "2022-01-01",
        "valid_start": "2022-01-01",
        "valid_end": "2024-01-01",
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

spy["spy_close_zscore_50d"] = (
    spy["close"] - spy["close"].rolling(50).mean()
) / spy["close"].rolling(50).std()

spy["spy_close_zscore_200d"] = (
    spy["close"] - spy["close"].rolling(200).mean()
) / spy["close"].rolling(200).std()

spy_features = spy[
    [
        "date",
        "spy_ma_200",
        "spy_above_200d",
        "spy_close_zscore_50d",
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
            "spy_close_zscore_50d",
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
                "spy_close_zscore_50d",
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

    regime_thresholds = {
        "z50_q20": regime_train["spy_close_zscore_50d"].quantile(0.20),
        "z50_median": regime_train["spy_close_zscore_50d"].quantile(0.50),
        "z50_q80": regime_train["spy_close_zscore_50d"].quantile(0.80),
        "z200_q20": regime_train["spy_close_zscore_200d"].quantile(0.20),
        "z200_median": regime_train["spy_close_zscore_200d"].quantile(0.50),
        "z200_q80": regime_train["spy_close_zscore_200d"].quantile(0.80),
    }

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

        # Candidate regime filters.
        df["regime_no_regime"] = True

        df["regime_above_200sma"] = df["spy_above_200d"]

        df["regime_z50_gt_train_q20"] = (
            df["spy_close_zscore_50d"] > regime_thresholds["z50_q20"]
        )

        df["regime_z50_gt_train_median"] = (
            df["spy_close_zscore_50d"] > regime_thresholds["z50_median"]
        )

        df["regime_z50_between_train_q20_q80"] = (
            (df["spy_close_zscore_50d"] > regime_thresholds["z50_q20"])
            & (df["spy_close_zscore_50d"] < regime_thresholds["z50_q80"])
        )

        df["regime_z200_gt_train_q20"] = (
            df["spy_close_zscore_200d"] > regime_thresholds["z200_q20"]
        )

        df["regime_z200_gt_train_median"] = (
            df["spy_close_zscore_200d"] > regime_thresholds["z200_median"]
        )

        df["regime_z200_between_train_q20_q80"] = (
            (df["spy_close_zscore_200d"] > regime_thresholds["z200_q20"])
            & (df["spy_close_zscore_200d"] < regime_thresholds["z200_q80"])
        )

        frames.append(df)

    panel = pd.concat(frames).sort_values(["date", "ticker"]).reset_index(drop=True)

    return panel, pd.DataFrame(threshold_rows), regime_thresholds


def simulate_basket_replacement(panel, dates, regime_col):
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


def summarize_strategy(split_name, period, regime_col, equity_df, trades_df, basket_return, basket_dd):
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

    return {
        "split": split_name,
        "period": period,
        "regime": regime_col.replace("regime_", ""),
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


regime_cols = [
    "regime_no_regime",
    "regime_above_200sma",
    "regime_z50_gt_train_q20",
    "regime_z50_gt_train_median",
    "regime_z50_between_train_q20_q80",
    "regime_z200_gt_train_q20",
    "regime_z200_gt_train_median",
    "regime_z200_between_train_q20_q80",
]

all_validation_rows = []
all_oos_rows = []
selected_rows = []

for split in SPLITS:
    panel, stock_thresholds, regime_thresholds = build_panel_for_split(split["train_end"])

    valid_dates = get_dates(split["valid_start"], split["valid_end"])
    oos_dates = get_dates(split["oos_start"], split["oos_end"])

    valid_basket = (1 + basket_daily.loc[valid_dates]).cumprod()
    valid_basket_return = (valid_basket.iloc[-1] - 1) * 100
    valid_basket_dd = max_drawdown(valid_basket)

    oos_basket = (1 + basket_daily.loc[oos_dates]).cumprod()
    oos_basket_return = (oos_basket.iloc[-1] - 1) * 100
    oos_basket_dd = max_drawdown(oos_basket)

    print("\n" + "=" * 80)
    print(f"=== {split['name']} ===")
    print(
        f"Train before {split['train_end']} | "
        f"Validation {split['valid_start']} to {split['valid_end']} | "
        f"OOS {split['oos_start']} to {split['oos_end'] or 'latest'}"
    )

    print("\nRegime thresholds learned from train:")
    print(pd.DataFrame([regime_thresholds]).round(4).to_string(index=False))

    validation_rows = []

    for regime_col in regime_cols:
        equity_df, trades_df = simulate_basket_replacement(
            panel=panel,
            dates=valid_dates,
            regime_col=regime_col,
        )

        row = summarize_strategy(
            split_name=split["name"],
            period="validation",
            regime_col=regime_col,
            equity_df=equity_df,
            trades_df=trades_df,
            basket_return=valid_basket_return,
            basket_dd=valid_basket_dd,
        )

        validation_rows.append(row)
        all_validation_rows.append(row)

    validation_df = pd.DataFrame(validation_rows)

    selected_regime = (
        validation_df
        .sort_values(["excess_return", "strategy_return"], ascending=False)
        .iloc[0]["regime"]
    )

    selected_regime_col = f"regime_{selected_regime}"

    print("\nValidation regime comparison:")
    print(
        validation_df[
            [
                "regime",
                "trades",
                "basket_return",
                "strategy_return",
                "excess_return",
                "basket_max_dd",
                "strategy_max_dd",
                "dd_delta",
                "avg_active_weight",
                "avg_trade",
                "median_trade",
                "win_rate",
            ]
        ]
        .round(4)
        .sort_values("excess_return", ascending=False)
        .to_string(index=False)
    )

    print(f"\nSelected regime for OOS based on validation: {selected_regime}")

    oos_rows = []

    for regime_col in regime_cols:
        equity_df, trades_df = simulate_basket_replacement(
            panel=panel,
            dates=oos_dates,
            regime_col=regime_col,
        )

        row = summarize_strategy(
            split_name=split["name"],
            period="oos",
            regime_col=regime_col,
            equity_df=equity_df,
            trades_df=trades_df,
            basket_return=oos_basket_return,
            basket_dd=oos_basket_dd,
        )

        row["selected_by_validation"] = regime_col == selected_regime_col

        oos_rows.append(row)
        all_oos_rows.append(row)

    oos_df = pd.DataFrame(oos_rows)

    selected_oos_row = oos_df[oos_df["selected_by_validation"]].iloc[0].to_dict()
    selected_rows.append(selected_oos_row)

    print("\nOOS regime comparison:")
    print(
        oos_df[
            [
                "regime",
                "selected_by_validation",
                "trades",
                "basket_return",
                "strategy_return",
                "excess_return",
                "basket_max_dd",
                "strategy_max_dd",
                "dd_delta",
                "avg_active_weight",
                "avg_trade",
                "median_trade",
                "win_rate",
            ]
        ]
        .round(4)
        .sort_values("excess_return", ascending=False)
        .to_string(index=False)
    )

print("\n" + "=" * 80)
print("=== Validation-selected OOS summary ===")
selected_summary = pd.DataFrame(selected_rows)
print(
    selected_summary[
        [
            "split",
            "regime",
            "trades",
            "basket_return",
            "strategy_return",
            "excess_return",
            "basket_max_dd",
            "strategy_max_dd",
            "dd_delta",
            "avg_active_weight",
            "avg_trade",
            "median_trade",
            "win_rate",
        ]
    ]
    .round(4)
    .to_string(index=False)
)

print("\n=== Average OOS result by regime across WF1 + WF2 ===")
oos_all = pd.DataFrame(all_oos_rows)

avg_oos = (
    oos_all
    .groupby("regime")
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
    avg_oos
    .round(4)
    .sort_values("avg_excess_return", ascending=False)
    .to_string(index=False)
)

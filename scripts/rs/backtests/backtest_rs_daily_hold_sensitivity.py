from pathlib import Path
import json
import pandas as pd

CONFIG_PATH = Path("configs/scanners/rs_scanner.json")
RS_DIR = Path("data/serving/scanners/rs")

POSITION_SIZE = 0.10
STOCK_THRESHOLD_Q = 0.20
SPY_REGIME_THRESHOLD_Q = 0.20
RVOL20_MIN = 1.20
MAX_POSITIONS = 3

HOLD_DAYS_LIST = [1, 2, 3, 5]

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


def load_config() -> dict:
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


config = load_config()

symbols = config["stock_symbols"]
benchmark = config["benchmark_symbol"]
start = config["start_date"]
end = config["end_date"]
timeframe = config["timeframe"]
data_root = Path(config["data_root"])

CURATED_DIR = data_root / "curated" / "market" / timeframe


def load_curated(symbol: str) -> pd.DataFrame:
    path = CURATED_DIR / f"{symbol}_{start}_{end}_curated.parquet"

    if not path.exists():
        raise FileNotFoundError(f"Missing curated file: {path}")

    df = pd.read_parquet(path).sort_values("date").reset_index(drop=True)
    df["date"] = pd.to_datetime(df["date"])

    if "volume" not in df.columns:
        raise ValueError(f"Missing volume column for {symbol}: {path}")

    return df


def zscore(series: pd.Series, window: int) -> pd.Series:
    return (series - series.rolling(window).mean()) / series.rolling(window).std()


def max_drawdown(equity: pd.Series) -> float:
    peak = equity.cummax()
    dd = equity / peak - 1
    return dd.min() * 100


def get_dates(common_dates, start_date, end_date=None):
    dates = [d for d in common_dates if d >= pd.Timestamp(start_date)]

    if end_date is not None:
        dates = [d for d in dates if d < pd.Timestamp(end_date)]

    return dates


spy = load_curated(benchmark)
spy = spy[["date", "close"]].rename(columns={"close": "spy_close"})
spy["spy_return"] = spy["spy_close"].pct_change().fillna(0)
spy["spy_zscore_200d"] = zscore(spy["spy_close"], 200)

rows = []

for symbol in symbols:
    rs_path = RS_DIR / f"{symbol}_vs_{benchmark}_{start}_{end}_rs_scan.parquet"

    if not rs_path.exists():
        raise FileNotFoundError(f"Missing RS scanner file: {rs_path}")

    rs = pd.read_parquet(rs_path).sort_values("date").reset_index(drop=True)
    rs["date"] = pd.to_datetime(rs["date"])

    stock = load_curated(symbol)
    stock = stock[["date", "close", "volume"]].rename(
        columns={
            "close": "stock_close",
            "volume": "stock_volume",
        }
    )

    df = (
        rs[
            [
                "date",
                "stock_return_pct",
                "close_zscore_50d",
            ]
        ]
        .merge(stock, on="date", how="left")
        .merge(
            spy[["date", "spy_return", "spy_zscore_200d"]],
            on="date",
            how="left",
        )
        .sort_values("date")
        .reset_index(drop=True)
    )

    df["ticker"] = symbol

    # Use prior 20-day average volume so today's volume is not inside the average.
    df["avg_volume_20d_prior"] = df["stock_volume"].shift(1).rolling(20).mean()
    df["volume_ratio_20d"] = df["stock_volume"] / df["avg_volume_20d_prior"]

    df = df.dropna(
        subset=[
            "stock_return_pct",
            "close_zscore_50d",
            "spy_zscore_200d",
            "volume_ratio_20d",
        ]
    ).copy()

    rows.append(df)

panel = pd.concat(rows).sort_values(["date", "ticker"]).reset_index(drop=True)

wide_returns = (
    panel
    .pivot(index="date", columns="ticker", values="stock_return_pct")
    .sort_index()
    / 100
)

basket_daily = wide_returns.mean(axis=1).fillna(0)

spy_daily = (
    spy
    .set_index("date")["spy_return"]
    .reindex(wide_returns.index)
    .fillna(0)
)

common_dates = sorted(wide_returns.index)


def build_panel_for_split(train_end: str) -> pd.DataFrame:
    train_end_ts = pd.Timestamp(train_end)

    train_regime = panel[
        panel["date"] < train_end_ts
    ].drop_duplicates("date").copy()

    spy_threshold = train_regime["spy_zscore_200d"].quantile(SPY_REGIME_THRESHOLD_Q)

    frames = []

    for symbol in symbols:
        df = panel[panel["ticker"] == symbol].copy()
        train = df[df["date"] < train_end_ts].copy()

        stock_threshold = train["close_zscore_50d"].quantile(STOCK_THRESHOLD_Q)

        df["stock_threshold"] = stock_threshold
        df["stock_signal"] = df["close_zscore_50d"] <= stock_threshold
        df["market_ok"] = df["spy_zscore_200d"] > spy_threshold
        df["volume_ok"] = df["volume_ratio_20d"] >= RVOL20_MIN

        frames.append(df)

    return pd.concat(frames).sort_values(["date", "ticker"]).reset_index(drop=True)


def simulate_strategy(panel: pd.DataFrame, dates, hold_days: int):
    panel_by_date = {d: df for d, df in panel.groupby("date")}

    open_positions = []
    rows = []
    trades = []
    equity = 1.0

    for date in dates:
        day_all = panel_by_date.get(date)

        if day_all is None:
            continue

        day_signals = day_all[
            (day_all["stock_signal"])
            & (day_all["market_ok"])
            & (day_all["volume_ok"])
        ].copy()

        # Tested candidate: if too many stocks trigger, choose most oversold first.
        day_signals = day_signals.sort_values("close_zscore_50d", ascending=True)

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

            pos["days_held"] += 1

            if pos["days_held"] >= hold_days:
                trades.append(
                    {
                        "ticker": ticker,
                        "stock_trade_return_pct": pos["cum_stock_return"] * 100,
                    }
                )
            else:
                still_open.append(pos)

        daily_return = (1 - active_weight) * basket_r + signal_stock_return
        equity *= (1 + daily_return)

        open_positions = still_open
        open_tickers = {pos["ticker"] for pos in open_positions}

        for _, row in day_signals.iterrows():
            if len(open_positions) >= MAX_POSITIONS:
                break

            if row["ticker"] in open_tickers:
                continue

            open_positions.append(
                {
                    "ticker": row["ticker"],
                    "days_held": 0,
                    "cum_stock_return": 0.0,
                }
            )

            open_tickers.add(row["ticker"])

        rows.append(
            {
                "date": date,
                "strategy_equity": equity,
                "active_positions": len(open_positions),
                "active_weight": len(open_positions) * POSITION_SIZE,
            }
        )

    return pd.DataFrame(rows), pd.DataFrame(trades)


summary_rows = []

for split in SPLITS:
    split_panel = build_panel_for_split(split["train_end"])
    oos_dates = get_dates(common_dates, split["oos_start"], split["oos_end"])

    basket_equity = (1 + basket_daily.loc[oos_dates]).cumprod()
    spy_equity = (1 + spy_daily.loc[oos_dates]).cumprod()

    basket_return = (basket_equity.iloc[-1] - 1) * 100
    spy_return = (spy_equity.iloc[-1] - 1) * 100

    basket_dd = max_drawdown(basket_equity)
    spy_dd = max_drawdown(spy_equity)

    for hold_days in HOLD_DAYS_LIST:
        strategy_df, trades_df = simulate_strategy(
            panel=split_panel,
            dates=oos_dates,
            hold_days=hold_days,
        )

        strategy_equity = strategy_df.set_index("date")["strategy_equity"]

        strategy_return = (strategy_equity.iloc[-1] - 1) * 100
        strategy_dd = max_drawdown(strategy_equity)

        if len(trades_df) > 0:
            avg_trade = trades_df["stock_trade_return_pct"].mean()
            median_trade = trades_df["stock_trade_return_pct"].median()
            win_rate = (trades_df["stock_trade_return_pct"] > 0).mean() * 100
            trades = len(trades_df)
        else:
            avg_trade = float("nan")
            median_trade = float("nan")
            win_rate = float("nan")
            trades = 0

        summary_rows.append(
            {
                "split": split["name"],
                "hold_days": hold_days,
                "trades": trades,
                "strategy_return": strategy_return,
                "spy_buy_hold_return": spy_return,
                "equal_weight_basket_return": basket_return,
                "strategy_minus_spy": strategy_return - spy_return,
                "strategy_minus_basket": strategy_return - basket_return,
                "strategy_max_dd": strategy_dd,
                "spy_max_dd": spy_dd,
                "basket_max_dd": basket_dd,
                "avg_active_weight": strategy_df["active_weight"].mean() * 100,
                "max_active_weight": strategy_df["active_weight"].max() * 100,
                "avg_trade": avg_trade,
                "median_trade": median_trade,
                "win_rate": win_rate,
            }
        )

summary = pd.DataFrame(summary_rows)

avg = (
    summary
    .groupby("hold_days")
    .agg(
        avg_strategy_return=("strategy_return", "mean"),
        avg_spy_buy_hold_return=("spy_buy_hold_return", "mean"),
        avg_basket_return=("equal_weight_basket_return", "mean"),
        avg_strategy_minus_spy=("strategy_minus_spy", "mean"),
        min_strategy_minus_spy=("strategy_minus_spy", "min"),
        avg_strategy_minus_basket=("strategy_minus_basket", "mean"),
        min_strategy_minus_basket=("strategy_minus_basket", "min"),
        avg_strategy_max_dd=("strategy_max_dd", "mean"),
        avg_spy_max_dd=("spy_max_dd", "mean"),
        avg_basket_max_dd=("basket_max_dd", "mean"),
        avg_trades=("trades", "mean"),
        avg_active_weight=("avg_active_weight", "mean"),
        avg_trade=("avg_trade", "mean"),
        median_trade=("median_trade", "mean"),
        avg_win_rate=("win_rate", "mean"),
    )
    .reset_index()
)

positive_both = (
    summary
    .pivot_table(
        index="hold_days",
        columns="split",
        values="strategy_minus_basket",
    )
    .reset_index()
)

positive_both["min_strategy_minus_basket"] = positive_both[["WF1", "WF2"]].min(axis=1)
positive_both["avg_strategy_minus_basket"] = positive_both[["WF1", "WF2"]].mean(axis=1)
positive_both = positive_both[positive_both["min_strategy_minus_basket"] > 0].copy()

print("=== Daily hold sensitivity test ===")
print("Signal: oversold pullback + healthy SPY + RVOL20 >= 1.2")
print(f"Position size: {POSITION_SIZE * 100:.0f}%")
print(f"Max positions: {MAX_POSITIONS}")
print(f"Hold days tested: {HOLD_DAYS_LIST}")
print()

print("=== Average across WF1 + WF2 ===")
print(avg.round(4).to_string(index=False))

print("\n=== Positive versus basket in both WF1 and WF2 ===")
if positive_both.empty:
    print("None")
else:
    print(positive_both.round(4).to_string(index=False))

print("\n=== Split detail ===")
print(summary.round(4).to_string(index=False))

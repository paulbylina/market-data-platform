from pathlib import Path
import json
import pandas as pd

CONFIG_PATH = Path("configs/scanners/rs_scanner.json")
RS_DIR = Path("data/serving/scanners/rs")

POSITION_SIZE = 0.10
STOCK_THRESHOLD_Q = 0.20

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

CANDIDATES = [
    {
        "name": "baseline_oversold_priority_hold5_max3",
        "hold_days": 5,
        "max_positions": 3,
        "sector_200d_cutoff": None,
        "priority": "oversold",
    },
    {
        "name": "baseline_rvol20_priority_hold5_max3",
        "hold_days": 5,
        "max_positions": 3,
        "sector_200d_cutoff": None,
        "priority": "rvol20_desc",
    },
    {
        "name": "sector_200d_top30_1d_priority_hold5_max3",
        "hold_days": 5,
        "max_positions": 3,
        "sector_200d_cutoff": 0.30,
        "priority": "sector_1d",
    },
    {
        "name": "sector_200d_top30_rvol20_priority_hold5_max3",
        "hold_days": 5,
        "max_positions": 3,
        "sector_200d_cutoff": 0.30,
        "priority": "rvol20_desc",
    },
]

VOLUME_FILTERS = [
    {
        "name": "no_volume_filter",
        "kind": "none",
        "column": None,
        "op": None,
        "value": None,
    },
    {
        "name": "hard_filter_high_rvol20_ge_1_2",
        "kind": "filter",
        "column": "volume_ratio_20d",
        "op": ">=",
        "value": 1.2,
    },
]


def load_config() -> dict:
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


config = load_config()

symbols = config["stock_symbols"]
benchmark = config["benchmark_symbol"]
sector_by_symbol = config["sector_by_symbol"]

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
        raise ValueError(f"Curated file for {symbol} has no volume column: {path}")

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


def rank_pct_desc(series: pd.Series) -> pd.Series:
    return series.rank(method="first", ascending=False, pct=True)


def apply_volume_filter(day_signals: pd.DataFrame, volume_filter: dict) -> pd.DataFrame:
    if volume_filter["kind"] == "none":
        return day_signals.copy()

    col = volume_filter["column"]
    op = volume_filter["op"]
    value = volume_filter["value"]

    if op == ">=":
        return day_signals[day_signals[col] >= value].copy()

    if op == "<=":
        return day_signals[day_signals[col] <= value].copy()

    raise ValueError(f"Unsupported operator: {op}")


spy = load_curated(benchmark)
spy = spy[["date", "close"]].rename(columns={"close": "spy_close"})
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
        .merge(spy, on="date", how="left")
        .sort_values("date")
        .reset_index(drop=True)
    )

    df["ticker"] = symbol
    df["sector"] = sector_by_symbol[symbol]

    df["stock_return_1d"] = df["stock_close"] / df["stock_close"].shift(1) - 1
    df["stock_return_200d"] = df["stock_close"] / df["stock_close"].shift(200) - 1

    # Today's volume versus prior average volume.
    # Prior average avoids comparing today's volume to an average that already includes today.
    df["avg_volume_20d_prior"] = df["stock_volume"].shift(1).rolling(20).mean()
    df["avg_volume_50d_prior"] = df["stock_volume"].shift(1).rolling(50).mean()
    df["std_volume_20d_prior"] = df["stock_volume"].shift(1).rolling(20).std()
    df["std_volume_50d_prior"] = df["stock_volume"].shift(1).rolling(50).std()

    df["volume_ratio_20d"] = df["stock_volume"] / df["avg_volume_20d_prior"]
    df["volume_ratio_50d"] = df["stock_volume"] / df["avg_volume_50d_prior"]

    df["volume_zscore_20d"] = (
        (df["stock_volume"] - df["avg_volume_20d_prior"])
        / df["std_volume_20d_prior"]
    )

    df["volume_zscore_50d"] = (
        (df["stock_volume"] - df["avg_volume_50d_prior"])
        / df["std_volume_50d_prior"]
    )

    # Recent average volume versus longer average volume.
    # This captures whether volume is drying up.
    df["avg_volume_5d"] = df["stock_volume"].rolling(5).mean()
    df["avg_volume_20d"] = df["stock_volume"].rolling(20).mean()
    df["volume_trend_5d_vs_20d"] = df["avg_volume_5d"] / df["avg_volume_20d"]

    df = df.dropna(
        subset=[
            "stock_return_pct",
            "close_zscore_50d",
            "spy_zscore_200d",
            "volume_ratio_20d",
            "volume_ratio_50d",
            "volume_zscore_20d",
            "volume_trend_5d_vs_20d",
        ]
    ).copy()

    rows.append(df)

base_panel = pd.concat(rows).sort_values(["date", "ticker"]).reset_index(drop=True)

base_panel["sector_stock_return_1d_rank_pct"] = (
    base_panel
    .groupby(["date", "sector"])["stock_return_1d"]
    .transform(rank_pct_desc)
)

base_panel["sector_stock_return_200d_rank_pct"] = (
    base_panel
    .groupby(["date", "sector"])["stock_return_200d"]
    .transform(rank_pct_desc)
)

wide_returns = (
    base_panel
    .pivot(index="date", columns="ticker", values="stock_return_pct")
    .sort_index()
    / 100
)

basket_daily = wide_returns.mean(axis=1).fillna(0)
common_dates = sorted(wide_returns.index)


def build_panel_for_split(train_end: str) -> pd.DataFrame:
    train_end_ts = pd.Timestamp(train_end)

    train_regime = base_panel[
        base_panel["date"] < train_end_ts
    ].drop_duplicates("date").copy()

    spy_q20 = train_regime["spy_zscore_200d"].quantile(0.20)

    frames = []

    for symbol in symbols:
        df = base_panel[base_panel["ticker"] == symbol].copy()
        train = df[df["date"] < train_end_ts].copy()

        stock_threshold = train["close_zscore_50d"].quantile(STOCK_THRESHOLD_Q)

        df["stock_threshold"] = stock_threshold
        df["stock_signal"] = df["close_zscore_50d"] <= stock_threshold
        df["regime_spy_z200_gt_q20"] = df["spy_zscore_200d"] > spy_q20

        frames.append(df)

    return pd.concat(frames).sort_values(["date", "ticker"]).reset_index(drop=True)


def apply_candidate_filters(day_signals: pd.DataFrame, candidate: dict) -> pd.DataFrame:
    out = day_signals.copy()

    if candidate["sector_200d_cutoff"] is not None:
        out = out[
            out["sector_stock_return_200d_rank_pct"]
            <= candidate["sector_200d_cutoff"]
        ].copy()

    return out


def sort_signals(day_signals: pd.DataFrame, candidate: dict) -> pd.DataFrame:
    if candidate["priority"] == "sector_1d":
        return day_signals.sort_values(
            ["sector_stock_return_1d_rank_pct", "close_zscore_50d"],
            ascending=[True, True],
        )

    if candidate["priority"] == "rvol20_desc":
        return day_signals.sort_values(
            ["volume_ratio_20d", "close_zscore_50d"],
            ascending=[False, True],
        )

    return day_signals.sort_values("close_zscore_50d", ascending=True)


def simulate_candidate(panel: pd.DataFrame, dates, candidate: dict, volume_filter: dict):
    hold_days = candidate["hold_days"]
    max_positions = candidate["max_positions"]

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
            & (day_all["regime_spy_z200_gt_q20"])
        ].copy()

        day_signals = apply_candidate_filters(day_signals, candidate)
        day_signals = apply_volume_filter(day_signals, volume_filter)

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
                        "entry_volume_ratio_20d": pos["entry_volume_ratio_20d"],
                        "entry_volume_ratio_50d": pos["entry_volume_ratio_50d"],
                        "entry_volume_zscore_20d": pos["entry_volume_zscore_20d"],
                        "entry_volume_trend_5d_vs_20d": pos["entry_volume_trend_5d_vs_20d"],
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

        day_signals = sort_signals(day_signals, candidate)

        for _, row in day_signals.iterrows():
            if len(open_positions) >= max_positions:
                break

            if row["ticker"] in open_tickers:
                continue

            open_positions.append(
                {
                    "ticker": row["ticker"],
                    "days_held": 0,
                    "cum_stock_return": 0.0,
                    "cum_basket_excess": 0.0,
                    "entry_volume_ratio_20d": row["volume_ratio_20d"],
                    "entry_volume_ratio_50d": row["volume_ratio_50d"],
                    "entry_volume_zscore_20d": row["volume_zscore_20d"],
                    "entry_volume_trend_5d_vs_20d": row["volume_trend_5d_vs_20d"],
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


def summarize(
    split_name,
    candidate,
    volume_filter,
    equity_df,
    trades_df,
    basket_return,
    basket_dd,
):
    strategy_return = (equity_df["equity"].iloc[-1] - 1) * 100
    strategy_dd = max_drawdown(equity_df["equity"])

    if len(trades_df) > 0:
        avg_trade = trades_df["stock_trade_return_pct"].mean()
        median_trade = trades_df["stock_trade_return_pct"].median()
        win_rate = (trades_df["stock_trade_return_pct"] > 0).mean() * 100
        basket_excess_total = trades_df["basket_excess_contribution_pct"].sum()

        avg_entry_rvol20 = trades_df["entry_volume_ratio_20d"].mean()
        avg_entry_rvol50 = trades_df["entry_volume_ratio_50d"].mean()
        avg_entry_vz20 = trades_df["entry_volume_zscore_20d"].mean()
        avg_entry_vtrend = trades_df["entry_volume_trend_5d_vs_20d"].mean()

        trades = len(trades_df)
    else:
        avg_trade = float("nan")
        median_trade = float("nan")
        win_rate = float("nan")
        basket_excess_total = 0.0

        avg_entry_rvol20 = float("nan")
        avg_entry_rvol50 = float("nan")
        avg_entry_vz20 = float("nan")
        avg_entry_vtrend = float("nan")

        trades = 0

    return {
        "split": split_name,
        "candidate": candidate["name"],
        "volume_filter": volume_filter["name"],
        "hold_days": candidate["hold_days"],
        "max_positions": candidate["max_positions"],
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
        "avg_entry_rvol20": avg_entry_rvol20,
        "avg_entry_rvol50": avg_entry_rvol50,
        "avg_entry_volume_z20": avg_entry_vz20,
        "avg_entry_volume_trend_5v20": avg_entry_vtrend,
    }


summary_rows = []

for split in SPLITS:
    panel = build_panel_for_split(split["train_end"])
    oos_dates = get_dates(common_dates, split["oos_start"], split["oos_end"])

    basket_bh = (1 + basket_daily.loc[oos_dates]).cumprod()
    basket_return = (basket_bh.iloc[-1] - 1) * 100
    basket_dd = max_drawdown(basket_bh)

    for candidate in CANDIDATES:
        for volume_filter in VOLUME_FILTERS:
            equity_df, trades_df = simulate_candidate(
                panel=panel,
                dates=oos_dates,
                candidate=candidate,
                volume_filter=volume_filter,
            )

            summary_rows.append(
                summarize(
                    split_name=split["name"],
                    candidate=candidate,
                    volume_filter=volume_filter,
                    equity_df=equity_df,
                    trades_df=trades_df,
                    basket_return=basket_return,
                    basket_dd=basket_dd,
                )
            )

summary = pd.DataFrame(summary_rows)

print("=== Volume priority test ===")
print(f"Universe size: {len(symbols)}")
print(f"Stock threshold: bottom {STOCK_THRESHOLD_Q * 100:.0f}% close_zscore_50d")
print("Regime: SPY zscore_200d > train q20")
print(f"Position size: {POSITION_SIZE * 100:.0f}%")
print()

print("=== Average across WF1 + WF2 ===")
avg = (
    summary
    .groupby(["candidate", "volume_filter", "hold_days", "max_positions"])
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
        basket_excess_total=("basket_excess_total", "sum"),
        avg_entry_rvol20=("avg_entry_rvol20", "mean"),
        avg_entry_rvol50=("avg_entry_rvol50", "mean"),
        avg_entry_volume_z20=("avg_entry_volume_z20", "mean"),
        avg_entry_volume_trend_5v20=("avg_entry_volume_trend_5v20", "mean"),
    )
    .reset_index()
)

print(
    avg
    .round(4)
    .sort_values(["candidate", "avg_excess_return"], ascending=[True, False])
    .to_string(index=False)
)

print("\n=== Positive in both WF1 and WF2 ===")
pivot = (
    summary
    .pivot_table(
        index=["candidate", "volume_filter", "hold_days", "max_positions"],
        columns="split",
        values="excess_return",
    )
    .reset_index()
)

pivot["min_excess_return"] = pivot[["WF1", "WF2"]].min(axis=1)
pivot["avg_excess_return"] = pivot[["WF1", "WF2"]].mean(axis=1)

positive_both = pivot[pivot["min_excess_return"] > 0].copy()

print(
    positive_both
    .round(4)
    .sort_values(["candidate", "avg_excess_return"], ascending=[True, False])
    .to_string(index=False)
)

print("\n=== Split detail ===")
print(
    summary
    .round(4)
    .sort_values(["candidate", "volume_filter", "split"])
    .to_string(index=False)
)

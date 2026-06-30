from pathlib import Path
import json
import pandas as pd

CONFIG_PATH = Path("configs/scanners/rs_scanner.json")
RS_DIR = Path("data/serving/scanners/rs")

POSITION_SIZE = 0.10
STOCK_THRESHOLD_Q = 0.20
HOLD_DAYS = 3
MAX_POSITIONS = 3
MIN_SECTOR_TRAIN_ROWS = 500

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

all_symbols = config["stock_symbols"]
benchmark = config["benchmark_symbol"]
start = config["start_date"]
end = config["end_date"]
timeframe = config["timeframe"]
data_root = Path(config["data_root"])
sector_etf_by_symbol = config["sector_etf_by_symbol"]

CURATED_DIR = data_root / "curated" / "market" / timeframe


def load_curated(symbol: str) -> pd.DataFrame:
    path = CURATED_DIR / f"{symbol}_{start}_{end}_curated.parquet"

    if not path.exists():
        raise FileNotFoundError(f"Missing curated file: {path}")

    df = pd.read_parquet(path).sort_values("date").reset_index(drop=True)
    df["date"] = pd.to_datetime(df["date"])
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
spy_features = spy[["date", "close"]].rename(columns={"close": "spy_close"})
spy_features["spy_zscore_200d"] = zscore(spy_features["spy_close"], 200)

sector_symbols = sorted(set(sector_etf_by_symbol.values()))
sector_rs_features_by_etf = {}

for sector_symbol in sector_symbols:
    sector = load_curated(sector_symbol)

    sector_features = (
        sector[["date", "close"]]
        .rename(columns={"close": "sector_close"})
        .merge(spy_features[["date", "spy_close"]], on="date", how="inner")
        .sort_values("date")
        .reset_index(drop=True)
    )

    sector_features["sector_rs_vs_spy"] = (
        sector_features["sector_close"] / sector_features["spy_close"]
    )

    sector_features["sector_rs_zscore_200d"] = zscore(
        sector_features["sector_rs_vs_spy"],
        200,
    )

    sector_rs_features_by_etf[sector_symbol] = sector_features[
        ["date", "sector_rs_zscore_200d"]
    ]


# Strict eligibility: actual sector ETF only; no SPY fallback.
eligibility_rows = []
eligible_symbols = []
excluded_symbols = []

for symbol in all_symbols:
    sector_symbol = sector_etf_by_symbol[symbol]
    sector_features = sector_rs_features_by_etf[sector_symbol]

    split_counts = {}

    for split in SPLITS:
        train_end = pd.Timestamp(split["train_end"])

        usable_train_rows = (
            sector_features[
                (sector_features["date"] < train_end)
                & (sector_features["sector_rs_zscore_200d"].notna())
            ]
            .drop_duplicates("date")
            .shape[0]
        )

        split_counts[f"{split['name']}_sector_rs_train_rows"] = usable_train_rows

    min_rows = min(split_counts.values())
    is_eligible = min_rows >= MIN_SECTOR_TRAIN_ROWS

    row = {
        "ticker": symbol,
        "sector_etf": sector_symbol,
        "min_sector_rs_train_rows": min_rows,
        "eligible": is_eligible,
    }
    row.update(split_counts)
    eligibility_rows.append(row)

    if is_eligible:
        eligible_symbols.append(symbol)
    else:
        excluded_symbols.append(symbol)


base_rows = []

for symbol in eligible_symbols:
    rs_path = RS_DIR / f"{symbol}_vs_{benchmark}_{start}_{end}_rs_scan.parquet"

    if not rs_path.exists():
        raise FileNotFoundError(f"Missing RS scanner file: {rs_path}")

    df = pd.read_parquet(rs_path).sort_values("date").reset_index(drop=True)
    df["date"] = pd.to_datetime(df["date"])

    sector_symbol = sector_etf_by_symbol[symbol]

    df = df.merge(spy_features[["date", "spy_zscore_200d"]], on="date", how="left")
    df = df.merge(sector_rs_features_by_etf[sector_symbol], on="date", how="left")

    df = df.dropna(
        subset=[
            "close_zscore_50d",
            "stock_return_pct",
            "spy_zscore_200d",
            "sector_rs_zscore_200d",
        ]
    ).copy()

    df["ticker"] = symbol
    df["sector_etf"] = sector_symbol

    base_rows.append(
        df[
            [
                "date",
                "ticker",
                "sector_etf",
                "stock_return_pct",
                "close_zscore_50d",
                "spy_zscore_200d",
                "sector_rs_zscore_200d",
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


def build_panel_for_split(train_end: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    train_end_ts = pd.Timestamp(train_end)

    frames = []
    threshold_rows = []

    unique_dates = base_panel.drop_duplicates("date").copy()
    regime_train = unique_dates[unique_dates["date"] < train_end_ts].copy()

    spy_q20 = regime_train["spy_zscore_200d"].quantile(0.20)

    sector_rs_q20_by_etf = {}

    for sector_symbol in sorted(set(sector_etf_by_symbol[s] for s in eligible_symbols)):
        sector_train = base_panel[
            (base_panel["sector_etf"] == sector_symbol)
            & (base_panel["date"] < train_end_ts)
        ].drop_duplicates("date")

        sector_rs_q20_by_etf[sector_symbol] = (
            sector_train["sector_rs_zscore_200d"].quantile(0.20)
        )

    for symbol in eligible_symbols:
        df = base_panel[base_panel["ticker"] == symbol].copy()
        train = df[df["date"] < train_end_ts].copy()

        stock_threshold = train["close_zscore_50d"].quantile(STOCK_THRESHOLD_Q)

        sector_symbol = sector_etf_by_symbol[symbol]
        sector_rs_q20 = sector_rs_q20_by_etf[sector_symbol]

        threshold_rows.append(
            {
                "ticker": symbol,
                "sector_etf": sector_symbol,
                "train_rows": len(train),
                "stock_threshold": stock_threshold,
                "spy_z200_q20": spy_q20,
                "sector_rs_z200_q20": sector_rs_q20,
            }
        )

        df["stock_threshold"] = stock_threshold
        df["stock_signal"] = df["close_zscore_50d"] <= stock_threshold

        df["regime_no_regime"] = True

        df["regime_spy_z200_gt_q20"] = df["spy_zscore_200d"] > spy_q20

        df["regime_sector_rs_z200_gt_q20"] = (
            df["sector_rs_zscore_200d"] > sector_rs_q20
        )

        df["regime_spy_and_sector_rs_z200_gt_q20"] = (
            (df["spy_zscore_200d"] > spy_q20)
            & (df["sector_rs_zscore_200d"] > sector_rs_q20)
        )

        df["regime_spy_or_sector_rs_z200_gt_q20"] = (
            (df["spy_zscore_200d"] > spy_q20)
            | (df["sector_rs_zscore_200d"] > sector_rs_q20)
        )

        frames.append(df)

    panel = pd.concat(frames).sort_values(["date", "ticker"]).reset_index(drop=True)

    return panel, pd.DataFrame(threshold_rows)


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


def summarize(split_name, regime_col, equity_df, trades_df, basket_return, basket_dd):
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
    "regime_spy_z200_gt_q20",
    "regime_sector_rs_z200_gt_q20",
    "regime_spy_and_sector_rs_z200_gt_q20",
    "regime_spy_or_sector_rs_z200_gt_q20",
]

summary_rows = []

for split in SPLITS:
    panel, thresholds = build_panel_for_split(split["train_end"])

    oos_dates = get_dates(common_dates, split["oos_start"], split["oos_end"])

    basket_bh = (1 + basket_daily.loc[oos_dates]).cumprod()
    basket_return = (basket_bh.iloc[-1] - 1) * 100
    basket_dd = max_drawdown(basket_bh)

    for regime_col in regime_cols:
        equity_df, trades_df = simulate_basket_replacement(
            panel=panel,
            dates=oos_dates,
            regime_col=regime_col,
        )

        summary_rows.append(
            summarize(
                split_name=split["name"],
                regime_col=regime_col,
                equity_df=equity_df,
                trades_df=trades_df,
                basket_return=basket_return,
                basket_dd=basket_dd,
            )
        )

summary = pd.DataFrame(summary_rows)

print("=== Sector relative-strength regime comparison ===")
print(f"Universe size in config: {len(all_symbols)}")
print(f"Eligible sector-RS universe: {len(eligible_symbols)}")
print(f"Excluded due to insufficient actual sector ETF history: {excluded_symbols}")
print(f"Minimum sector-RS training rows required: {MIN_SECTOR_TRAIN_ROWS}")
print(f"Stock threshold: bottom {STOCK_THRESHOLD_Q * 100:.0f}% close_zscore_50d")
print(f"Hold days: {HOLD_DAYS}")
print(f"Position size: {POSITION_SIZE * 100:.0f}%")
print(f"Max positions: {MAX_POSITIONS}")
print()

print("=== Sector-RS eligibility ===")
print(
    pd.DataFrame(eligibility_rows)
    .sort_values(["eligible", "ticker"], ascending=[True, True])
    .to_string(index=False)
)

print("\n=== Full results ===")
print(
    summary[
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
            "max_active_weight",
            "avg_trade",
            "median_trade",
            "win_rate",
            "basket_excess_total",
        ]
    ]
    .round(4)
    .sort_values(["split", "excess_return"], ascending=[True, False])
    .to_string(index=False)
)

print("\n=== Average by regime across WF1 + WF2 ===")
avg = (
    summary
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
    avg
    .round(4)
    .sort_values("avg_excess_return", ascending=False)
    .to_string(index=False)
)

print("\n=== Regimes positive in both WF1 and WF2 ===")
pivot = (
    summary
    .pivot_table(
        index="regime",
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
    .sort_values("avg_excess_return", ascending=False)
    .to_string(index=False)
)

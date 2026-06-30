from pathlib import Path
import json
import pandas as pd

CONFIG_PATH = Path("configs/scanners/rs_scanner.json")
RS_DIR = Path("data/serving/scanners/rs")

STOCK_THRESHOLD_Q = 0.20
SPY_REGIME_THRESHOLD_Q = 0.20
RVOL20_MIN = 1.20
MAX_PICKS = 3

ROUND_TRIP_COST_BPS_LIST = [0, 5, 10, 20]

SPLITS = [
    {"name": "WF1", "train_end": "2020-01-01", "oos_start": "2022-01-01", "oos_end": "2024-01-01"},
    {"name": "WF2", "train_end": "2022-01-01", "oos_start": "2024-01-01", "oos_end": None},
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

    required = {"date", "open", "close", "volume"}
    missing = required - set(df.columns)

    if missing:
        raise ValueError(f"{symbol} missing columns: {sorted(missing)}")

    return df


def zscore(series: pd.Series, window: int) -> pd.Series:
    return (series - series.rolling(window).mean()) / series.rolling(window).std()


def get_dates(common_dates, start_date, end_date=None):
    dates = [d for d in common_dates if d >= pd.Timestamp(start_date)]

    if end_date is not None:
        dates = [d for d in dates if d < pd.Timestamp(end_date)]

    return dates


def add_filter_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    out["filter_all"] = True
    out["filter_gap_up_1pct_or_more"] = out["next_gap_pct"] >= 1.0
    out["filter_gap_down_1pct_or_more"] = out["next_gap_pct"] <= -1.0
    out["filter_abs_gap_1pct_or_more"] = out["next_gap_pct"].abs() >= 1.0
    out["filter_no_large_gap"] = out["next_gap_pct"].abs() < 1.0
    out["filter_any_gap_up"] = out["next_gap_pct"] > 0
    out["filter_any_gap_down"] = out["next_gap_pct"] < 0

    return out


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

    stock = stock[["date", "open", "close", "volume"]].rename(
        columns={
            "open": "stock_open",
            "close": "stock_close",
            "volume": "stock_volume",
        }
    )

    df = (
        rs[["date", "close_zscore_50d"]]
        .merge(stock, on="date", how="left")
        .merge(spy, on="date", how="left")
        .sort_values("date")
        .reset_index(drop=True)
    )

    df["ticker"] = symbol
    df["avg_volume_20d_prior"] = df["stock_volume"].shift(1).rolling(20).mean()
    df["volume_ratio_20d"] = df["stock_volume"] / df["avg_volume_20d_prior"]

    df["signal_close"] = df["stock_close"]
    df["next_open"] = df["stock_open"].shift(-1)
    df["next_close"] = df["stock_close"].shift(-1)

    df["next_gap_pct"] = (df["next_open"] / df["signal_close"] - 1) * 100
    df["next_open_to_close_return_pct"] = (df["next_close"] / df["next_open"] - 1) * 100

    df = df.dropna(
        subset=[
            "close_zscore_50d",
            "spy_zscore_200d",
            "volume_ratio_20d",
            "next_gap_pct",
            "next_open_to_close_return_pct",
        ]
    ).copy()

    rows.append(df)

panel = pd.concat(rows).sort_values(["date", "ticker"]).reset_index(drop=True)
common_dates = sorted(panel["date"].unique())


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


picked_rows = []

for split in SPLITS:
    split_panel = build_panel_for_split(split["train_end"])
    oos_dates = get_dates(common_dates, split["oos_start"], split["oos_end"])

    oos = split_panel[split_panel["date"].isin(oos_dates)].copy()

    signals = oos[
        (oos["stock_signal"])
        & (oos["market_ok"])
        & (oos["volume_ok"])
    ].copy()

    signals = signals.sort_values(
        ["date", "close_zscore_50d"],
        ascending=[True, True],
    )

    picked = signals.groupby("date", group_keys=False).head(MAX_PICKS).copy()
    picked["split"] = split["name"]

    picked_rows.append(picked)

picked_all = pd.concat(picked_rows).reset_index(drop=True)
picked_all = add_filter_columns(picked_all)

filters = [
    "filter_all",
    "filter_gap_up_1pct_or_more",
    "filter_gap_down_1pct_or_more",
    "filter_abs_gap_1pct_or_more",
    "filter_no_large_gap",
    "filter_any_gap_up",
    "filter_any_gap_down",
]

rows = []

for filter_name in filters:
    for split in ["WF1", "WF2"]:
        subset = picked_all[
            (picked_all["split"] == split)
            & (picked_all[filter_name])
        ].copy()

        for cost_bps in ROUND_TRIP_COST_BPS_LIST:
            cost_pct = cost_bps / 100.0

            net_returns = subset["next_open_to_close_return_pct"] - cost_pct

            rows.append(
                {
                    "filter": filter_name.replace("filter_", ""),
                    "split": split,
                    "round_trip_cost_bps": cost_bps,
                    "trades": len(net_returns),
                    "avg_net_trade": net_returns.mean(),
                    "median_net_trade": net_returns.median(),
                    "win_rate_net": (net_returns > 0).mean() * 100 if len(net_returns) else float("nan"),
                    "total_net_return_sum": net_returns.sum(),
                    "best_net_trade": net_returns.max() if len(net_returns) else float("nan"),
                    "worst_net_trade": net_returns.min() if len(net_returns) else float("nan"),
                }
            )

summary = pd.DataFrame(rows)

avg = (
    summary
    .groupby(["filter", "round_trip_cost_bps"])
    .agg(
        avg_trades=("trades", "mean"),
        avg_net_trade=("avg_net_trade", "mean"),
        median_net_trade=("median_net_trade", "mean"),
        avg_win_rate_net=("win_rate_net", "mean"),
        min_split_avg_net_trade=("avg_net_trade", "min"),
        avg_total_net_return_sum=("total_net_return_sum", "mean"),
    )
    .reset_index()
    .sort_values(
        ["round_trip_cost_bps", "avg_net_trade"],
        ascending=[True, False],
    )
)

print("=== Gap filter cost test ===")
print("Signal day: daily oversold + healthy SPY + RVOL20 >= 1.2")
print("Entry: next trading day open")
print("Exit: same trading day close")
print(f"Max picks per day: {MAX_PICKS}")
print()

for cost_bps in ROUND_TRIP_COST_BPS_LIST:
    print(f"=== Average across WF1 + WF2 | round-trip cost: {cost_bps} bps ===")
    print(
        avg[avg["round_trip_cost_bps"] == cost_bps]
        .round(4)
        .to_string(index=False)
    )
    print()

print("=== Split detail ===")
print(summary.round(4).to_string(index=False))

from pathlib import Path
import json
import pandas as pd

CONFIG_PATH = Path("configs/scanners/rs_scanner.json")
RS_DIR = Path("data/serving/scanners/rs")

STOCK_THRESHOLD_Q = 0.20
SPY_REGIME_THRESHOLD_Q = 0.20
RVOL20_MIN = 1.20
MAX_PICKS = 3

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


def bucket_gap(gap_pct: float) -> str:
    if gap_pct <= -1.0:
        return "gap_down_1pct_or_more"
    if gap_pct < 0:
        return "gap_down_less_than_1pct"
    if gap_pct == 0:
        return "flat"
    if gap_pct < 1.0:
        return "gap_up_less_than_1pct"
    return "gap_up_1pct_or_more"


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

    df["next_open"] = df["stock_open"].shift(-1)
    df["next_close"] = df["stock_close"].shift(-1)
    df["signal_close"] = df["stock_close"]

    df["next_gap_pct"] = (df["next_open"] / df["signal_close"] - 1) * 100
    df["next_open_to_close_return_pct"] = (
        df["next_close"] / df["next_open"] - 1
    ) * 100
    df["next_close_to_close_return_pct"] = (
        df["next_close"] / df["signal_close"] - 1
    ) * 100

    df = df.dropna(
        subset=[
            "close_zscore_50d",
            "spy_zscore_200d",
            "volume_ratio_20d",
            "next_gap_pct",
            "next_open_to_close_return_pct",
            "next_close_to_close_return_pct",
        ]
    ).copy()

    df["gap_bucket"] = df["next_gap_pct"].map(bucket_gap)

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

    picked = (
        signals
        .groupby("date", group_keys=False)
        .head(MAX_PICKS)
        .copy()
    )

    picked["split"] = split["name"]
    picked_rows.append(picked)

picked_all = pd.concat(picked_rows).reset_index(drop=True)

summary = (
    picked_all
    .groupby(["split", "gap_bucket"])
    .agg(
        trades=("ticker", "count"),
        avg_gap_pct=("next_gap_pct", "mean"),
        avg_open_to_close=("next_open_to_close_return_pct", "mean"),
        median_open_to_close=("next_open_to_close_return_pct", "median"),
        win_rate_open_to_close=("next_open_to_close_return_pct", lambda s: (s > 0).mean() * 100),
        avg_close_to_close=("next_close_to_close_return_pct", "mean"),
        median_close_to_close=("next_close_to_close_return_pct", "median"),
        win_rate_close_to_close=("next_close_to_close_return_pct", lambda s: (s > 0).mean() * 100),
    )
    .reset_index()
)

avg_summary = (
    picked_all
    .groupby("gap_bucket")
    .agg(
        trades=("ticker", "count"),
        avg_gap_pct=("next_gap_pct", "mean"),
        avg_open_to_close=("next_open_to_close_return_pct", "mean"),
        median_open_to_close=("next_open_to_close_return_pct", "median"),
        win_rate_open_to_close=("next_open_to_close_return_pct", lambda s: (s > 0).mean() * 100),
        avg_close_to_close=("next_close_to_close_return_pct", "mean"),
        median_close_to_close=("next_close_to_close_return_pct", "median"),
        win_rate_close_to_close=("next_close_to_close_return_pct", lambda s: (s > 0).mean() * 100),
    )
    .reset_index()
)

bucket_order = [
    "gap_down_1pct_or_more",
    "gap_down_less_than_1pct",
    "flat",
    "gap_up_less_than_1pct",
    "gap_up_1pct_or_more",
]

avg_summary["bucket_order"] = avg_summary["gap_bucket"].map(
    {bucket: i for i, bucket in enumerate(bucket_order)}
)
avg_summary = avg_summary.sort_values("bucket_order").drop(columns="bucket_order")

summary["bucket_order"] = summary["gap_bucket"].map(
    {bucket: i for i, bucket in enumerate(bucket_order)}
)
summary = summary.sort_values(["split", "bucket_order"]).drop(columns="bucket_order")

print("=== Next-day gap behavior test ===")
print("Signal day: daily oversold + healthy SPY + RVOL20 >= 1.2")
print("Entry/exit comparison: next day open-to-close and signal close-to-next close")
print(f"Max picks per day: {MAX_PICKS}")
print()

print("=== Average by next-day gap bucket ===")
print(avg_summary.round(4).to_string(index=False))

print("\n=== Split detail by next-day gap bucket ===")
print(summary.round(4).to_string(index=False))

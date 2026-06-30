import argparse
import json
from pathlib import Path

import pandas as pd

DEFAULT_CONFIG_PATH = Path("configs/scanners/rs_scanner.json")
RS_DIR = Path("data/serving/scanners/rs")
OUTPUT_DIR = Path("data/research/intraday_gap_up")

STOCK_THRESHOLD_Q = 0.20
SPY_REGIME_THRESHOLD_Q = 0.20
RVOL20_MIN = 1.20
GAP_UP_MIN_PCT = 1.00
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


def load_config(config_path: Path = DEFAULT_CONFIG_PATH) -> dict:
    with config_path.open("r", encoding="utf-8") as f:
        return json.load(f)

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="Path to RS scanner config JSON.",
    )
    return parser.parse_args()

args = parse_args()
config = load_config(args.config)

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


def get_oos_dates(common_dates, start_date, end_date=None):
    dates = [d for d in common_dates if d >= pd.Timestamp(start_date)]

    if end_date is not None:
        dates = [d for d in dates if d < pd.Timestamp(end_date)]

    return dates


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

    # Daily signal-day volume filter.
    df["avg_volume_20d_prior"] = df["stock_volume"].shift(1).rolling(20).mean()
    df["volume_ratio_20d"] = df["stock_volume"] / df["avg_volume_20d_prior"]

    # Next trading day trade info.
    df["trade_date"] = df["date"].shift(-1)
    df["signal_close"] = df["stock_close"]
    df["next_open"] = df["stock_open"].shift(-1)
    df["next_close"] = df["stock_close"].shift(-1)

    df["next_gap_pct"] = (df["next_open"] / df["signal_close"] - 1) * 100
    df["next_open_to_close_return_pct"] = (
        df["next_close"] / df["next_open"] - 1
    ) * 100

    df = df.dropna(
        subset=[
            "close_zscore_50d",
            "spy_zscore_200d",
            "volume_ratio_20d",
            "trade_date",
            "next_gap_pct",
            "next_open_to_close_return_pct",
        ]
    ).copy()

    rows.append(df)

panel = pd.concat(rows).sort_values(["date", "ticker"]).reset_index(drop=True)
common_dates = sorted(pd.to_datetime(panel["date"].unique()))


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


all_signal_rows = []
top3_rows = []

for split in SPLITS:
    split_panel = build_panel_for_split(split["train_end"])
    oos_dates = get_oos_dates(common_dates, split["oos_start"], split["oos_end"])

    oos = split_panel[split_panel["date"].isin(oos_dates)].copy()

    signals = oos[
        (oos["stock_signal"])
        & (oos["market_ok"])
        & (oos["volume_ok"])
    ].copy()

    # Same ranking as tested: most oversold first.
    signals = signals.sort_values(
        ["date", "close_zscore_50d"],
        ascending=[True, True],
    )

    signals["daily_signal_rank"] = signals.groupby("date").cumcount() + 1
    signals["selected_top3"] = signals["daily_signal_rank"] <= MAX_PICKS
    signals["split"] = split["name"]

    gap_up_all_signals = signals[signals["next_gap_pct"] >= GAP_UP_MIN_PCT].copy()
    gap_up_top3 = signals[
        (signals["selected_top3"])
        & (signals["next_gap_pct"] >= GAP_UP_MIN_PCT)
    ].copy()

    all_signal_rows.append(gap_up_all_signals)
    top3_rows.append(gap_up_top3)

all_gap_up = pd.concat(all_signal_rows).reset_index(drop=True)
top3_gap_up = pd.concat(top3_rows).reset_index(drop=True)

columns = [
    "split",
    "date",
    "trade_date",
    "ticker",
    "daily_signal_rank",
    "selected_top3",
    "close_zscore_50d",
    "stock_threshold",
    "volume_ratio_20d",
    "signal_close",
    "next_open",
    "next_close",
    "next_gap_pct",
    "next_open_to_close_return_pct",
]

all_gap_up = all_gap_up[columns].copy()
top3_gap_up = top3_gap_up[columns].copy()

all_gap_up = all_gap_up.rename(columns={"date": "signal_date"})
top3_gap_up = top3_gap_up.rename(columns={"date": "signal_date"})

for df in [all_gap_up, top3_gap_up]:
    df["signal_date"] = pd.to_datetime(df["signal_date"]).dt.strftime("%Y-%m-%d")
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.strftime("%Y-%m-%d")

download_tasks = (
    top3_gap_up[["ticker", "trade_date"]]
    .drop_duplicates()
    .sort_values(["ticker", "trade_date"])
    .reset_index(drop=True)
)

download_tasks["start_date"] = download_tasks["trade_date"]
download_tasks["end_date"] = download_tasks["trade_date"]

summary = (
    top3_gap_up
    .groupby("split")
    .agg(
        trades=("ticker", "count"),
        unique_tickers=("ticker", "nunique"),
        first_trade_date=("trade_date", "min"),
        last_trade_date=("trade_date", "max"),
        avg_gap_pct=("next_gap_pct", "mean"),
        avg_open_to_close=("next_open_to_close_return_pct", "mean"),
        median_open_to_close=("next_open_to_close_return_pct", "median"),
        win_rate_open_to_close=("next_open_to_close_return_pct", lambda s: (s > 0).mean() * 100),
    )
    .reset_index()
)

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

top3_path = OUTPUT_DIR / "gap_up_candidates_top3.csv"
all_path = OUTPUT_DIR / "gap_up_candidates_all_signals.csv"
tasks_path = OUTPUT_DIR / "minute_download_tasks_top3.csv"
summary_path = OUTPUT_DIR / "gap_up_candidates_top3_summary.csv"

top3_gap_up.to_csv(top3_path, index=False)
all_gap_up.to_csv(all_path, index=False)
download_tasks.to_csv(tasks_path, index=False)
summary.to_csv(summary_path, index=False)

print("=== Gap-up candidate trade builder ===")
print("Daily signal: oversold + healthy SPY + RVOL20 >= 1.2")
print(f"Gap filter: next day gap up >= {GAP_UP_MIN_PCT:.2f}%")
print(f"Top picks per signal day: {MAX_PICKS}")
print()
print("Saved:")
print(f"Top-3 candidates:     {top3_path}")
print(f"All signal candidates:{all_path}")
print(f"Download tasks:       {tasks_path}")
print(f"Summary:              {summary_path}")
print()
print("=== Top-3 candidate summary ===")
print(summary.round(4).to_string(index=False))
print()
print("=== Download task count ===")
print(f"Rows: {len(download_tasks)}")
print(f"Unique tickers: {download_tasks['ticker'].nunique()}")
print()
print("=== First 25 download tasks ===")
print(download_tasks.head(25).to_string(index=False))

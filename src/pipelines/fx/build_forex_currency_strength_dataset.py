from pathlib import Path
import json

import pandas as pd


def parse_forex_ticker(ticker: str) -> tuple[str, str, str]:
    """
    Parse Massive forex ticker format.

    Example:
        C:EURUSD -> pair=EURUSD, base=EUR, quote=USD
    """
    if not ticker or ":" not in ticker:
        raise ValueError(f"Invalid forex ticker: {ticker}")

    pair = ticker.split(":", maxsplit=1)[1]

    if len(pair) != 6:
        raise ValueError(f"Expected 6-character forex pair, got: {pair}")

    base_currency = pair[:3]
    quote_currency = pair[3:]

    return pair, base_currency, quote_currency


def load_raw_forex_bars(raw_dir: Path) -> pd.DataFrame:
    """
    Load raw Massive forex aggregate JSON files into a pair-level DataFrame.
    """
    rows: list[dict] = []

    for path in sorted(raw_dir.glob("*_raw.json")):
        with path.open("r", encoding="utf-8") as f:
            raw = json.load(f)

        ticker = raw.get("ticker")
        pair, base_currency, quote_currency = parse_forex_ticker(ticker)

        for result in raw.get("results", []):
            rows.append(
                {
                    "date": pd.to_datetime(result["t"], unit="ms").normalize(),
                    "ticker": ticker,
                    "pair": pair,
                    "base_currency": base_currency,
                    "quote_currency": quote_currency,
                    "close": result["c"],
                }
            )

    if not rows:
        raise ValueError(f"No raw forex rows found in: {raw_dir}")

    df = pd.DataFrame(rows)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")

    return df.sort_values(["pair", "date"]).reset_index(drop=True)


def calculate_currency_strength(pair_df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert pair returns into individual currency strength scores.

    Logic:
        If EURUSD rises, EUR gets positive contribution and USD gets negative contribution.
        If USDJPY rises, USD gets positive contribution and JPY gets negative contribution.
    """
    df = pair_df.copy()

    df["return_1d"] = df.groupby("pair")["close"].pct_change()
    df = df.dropna(subset=["return_1d"]).reset_index(drop=True)

    base_contributions = df[
        ["date", "pair", "base_currency", "return_1d"]
    ].rename(
        columns={
            "base_currency": "currency",
            "return_1d": "contribution",
        }
    )

    quote_contributions = df[
        ["date", "pair", "quote_currency", "return_1d"]
    ].rename(
        columns={
            "quote_currency": "currency",
            "return_1d": "contribution",
        }
    )
    quote_contributions["contribution"] = -quote_contributions["contribution"]

    contribution_df = pd.concat(
        [base_contributions, quote_contributions],
        ignore_index=True,
    )

    strength_df = (
        contribution_df.groupby(["date", "currency"], as_index=False)
        .agg(
            strength_1d=("contribution", "mean"),
            pair_count=("pair", "nunique"),
        )
        .reset_index(drop=True)
    )

    strength_df["strength_1d_pct"] = strength_df["strength_1d"] * 100

    strength_df = strength_df.sort_values(["currency", "date"]).reset_index(drop=True)

    for window in (5, 20):
        strength_df[f"strength_{window}d"] = (
            strength_df.groupby("currency")["strength_1d"]
            .transform(lambda s: s.rolling(window=window, min_periods=window).sum())
        )
        strength_df[f"strength_{window}d_pct"] = (
            strength_df[f"strength_{window}d"] * 100
        )

    strength_df = strength_df.sort_values(["date", "currency"]).reset_index(drop=True)

    for strength_column, rank_column in [
        ("strength_1d", "rank_1d"),
        ("strength_5d", "rank_5d"),
        ("strength_20d", "rank_20d"),
    ]:
        strength_df[rank_column] = (
            strength_df.groupby("date")[strength_column]
            .rank(ascending=False, method="dense")
            .astype("Int64")
        )

    return strength_df.sort_values(["date", "rank_1d", "currency"]).reset_index(
        drop=True
    )

def build_forex_currency_strength_dataset(
    raw_dir: Path,
    output_path: Path,
) -> pd.DataFrame:
    """
    Build a serving-layer currency strength dataset from raw Massive forex JSON files.
    """
    pair_df = load_raw_forex_bars(raw_dir)
    strength_df = calculate_currency_strength(pair_df)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    strength_df.to_parquet(output_path, index=False)

    print(f"Currency strength dataset written to {output_path}")

    return strength_df


if __name__ == "__main__":
    result = build_forex_currency_strength_dataset(
        raw_dir=Path("data/raw/massive/forex/1d"),
        output_path=Path("data/serving/market/forex/currency_strength_daily.parquet"),
    )

    latest_date = result["date"].max()
    latest = result[result["date"] == latest_date].sort_values("rank_1d")

    print()
    print(f"Latest currency strength ranking: {latest_date.date()}")
    print(
        latest[
            [
                "currency",
                "strength_1d_pct",
                "rank_1d",
                "strength_5d_pct",
                "rank_5d",
                "strength_20d_pct",
                "rank_20d",
                "pair_count",
            ]
        ].to_string(index=False)
    )
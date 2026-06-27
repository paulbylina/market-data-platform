from pathlib import Path

import pandas as pd

from src.utils.load_symbols import load_symbols


def parse_forex_symbol(symbol: str) -> tuple[str, str, str]:
    pair = symbol.split(":", maxsplit=1)[1]
    base_currency = pair[:3]
    quote_currency = pair[3:]

    return pair, base_currency, quote_currency


def build_forex_pair_strength_dataset(
    currency_strength_path: Path,
    symbols_file: Path,
    output_path: Path,
) -> pd.DataFrame:
    currency_df = pd.read_parquet(currency_strength_path)
    symbols = load_symbols(symbols_file)

    pair_rows = []

    for symbol in symbols:
        pair, base_currency, quote_currency = parse_forex_symbol(symbol)

        pair_rows.append(
            {
                "ticker": symbol,
                "pair": pair,
                "base_currency": base_currency,
                "quote_currency": quote_currency,
            }
        )

    pairs_df = pd.DataFrame(pair_rows)

    base_df = currency_df.rename(
        columns={
            "currency": "base_currency",
            "strength_1d": "base_strength_1d",
            "strength_1d_pct": "base_strength_1d_pct",
            "strength_5d": "base_strength_5d",
            "strength_5d_pct": "base_strength_5d_pct",
            "strength_20d": "base_strength_20d",
            "strength_20d_pct": "base_strength_20d_pct",
            "rank_1d": "base_rank_1d",
            "rank_5d": "base_rank_5d",
            "rank_20d": "base_rank_20d",
        }
    )

    quote_df = currency_df.rename(
        columns={
            "currency": "quote_currency",
            "strength_1d": "quote_strength_1d",
            "strength_1d_pct": "quote_strength_1d_pct",
            "strength_5d": "quote_strength_5d",
            "strength_5d_pct": "quote_strength_5d_pct",
            "strength_20d": "quote_strength_20d",
            "strength_20d_pct": "quote_strength_20d_pct",
            "rank_1d": "quote_rank_1d",
            "rank_5d": "quote_rank_5d",
            "rank_20d": "quote_rank_20d",
        }
    )

    df = pairs_df.merge(base_df, on="base_currency", how="left")
    df = df.merge(
        quote_df,
        on=["date", "quote_currency"],
        how="left",
    )

    df["pair_score_1d"] = df["base_strength_1d"] - df["quote_strength_1d"]
    df["pair_score_5d"] = df["base_strength_5d"] - df["quote_strength_5d"]
    df["pair_score_20d"] = df["base_strength_20d"] - df["quote_strength_20d"]

    df["pair_score_1d_pct"] = df["pair_score_1d"] * 100
    df["pair_score_5d_pct"] = df["pair_score_5d"] * 100
    df["pair_score_20d_pct"] = df["pair_score_20d"] * 100

    def weighted_combined_pair_score(row: pd.Series):
        weights = {
            "pair_score_1d": 0.20,
            "pair_score_5d": 0.30,
            "pair_score_20d": 0.50,
        }

        weighted_sum = 0.0
        weight_total = 0.0

        for column, weight in weights.items():
            if pd.notna(row[column]):
                weighted_sum += row[column] * weight
                weight_total += weight

        if weight_total == 0:
            return pd.NA

        return weighted_sum / weight_total


    df["pair_score_combined"] = df.apply(weighted_combined_pair_score, axis=1)
    df["pair_score_combined_pct"] = df["pair_score_combined"] * 100

    df["rank_pair_combined"] = (
        df.groupby("date")["pair_score_combined"]
        .rank(ascending=False, method="dense")
        .astype("Int64")
    )

    df["rank_pair_1d"] = (
        df.groupby("date")["pair_score_1d"]
        .rank(ascending=False, method="dense")
        .astype("Int64")
    )

    df["rank_pair_5d"] = (
        df.groupby("date")["pair_score_5d"]
        .rank(ascending=False, method="dense")
        .astype("Int64")
    )

    df["rank_pair_20d"] = (
        df.groupby("date")["pair_score_20d"]
        .rank(ascending=False, method="dense")
        .astype("Int64")
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)

    print(f"Forex pair strength dataset written to {output_path}")

    return df


if __name__ == "__main__":
    result = build_forex_pair_strength_dataset(
        currency_strength_path=Path(
            "data/serving/market/forex/currency_strength_daily.parquet"
        ),
        symbols_file=Path("config/symbols_forex_major.txt"),
        output_path=Path("data/serving/market/forex/forex_pair_strength_daily.parquet"),
    )

    latest_date = result["date"].max()
    latest = result[result["date"] == latest_date].copy()

    latest["bias"] = latest["pair_score_combined"].apply(
        lambda score: "bullish" if score > 0 else "bearish"
    )

    print()
    print(f"Top combined forex pair scores: {latest_date.date()}")
    print(
        latest.sort_values("rank_pair_combined")
        [
            [
                "pair",
                "bias",
                "pair_score_combined_pct",
                "rank_pair_combined",
                "pair_score_1d_pct",
                "pair_score_5d_pct",
                "pair_score_20d_pct",
            ]
        ]
        .head(15)
        .to_string(index=False)
    )

    print()
    print(f"Bottom combined forex pair scores: {latest_date.date()}")
    print(
        latest.sort_values("pair_score_combined")
        [
            [
                "pair",
                "bias",
                "pair_score_combined_pct",
                "rank_pair_combined",
                "pair_score_1d_pct",
                "pair_score_5d_pct",
                "pair_score_20d_pct",
            ]
        ]
        .head(15)
        .to_string(index=False)
    )

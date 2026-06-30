from __future__ import annotations

from pathlib import Path
import argparse
import json
from datetime import datetime, timezone

import pandas as pd


DEFAULT_STRATEGY_CONFIG = Path("configs/strategies/daily_pullback_rvol.json")


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def zscore(series: pd.Series, window: int) -> pd.Series:
    return (series - series.rolling(window).mean()) / series.rolling(window).std()


def load_curated_symbol(
    symbol: str,
    curated_dir: Path,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    path = curated_dir / f"{symbol}_{start_date}_{end_date}_curated.parquet"

    if not path.exists():
        raise FileNotFoundError(f"Missing curated file for {symbol}: {path}")

    df = pd.read_parquet(path).sort_values("date").reset_index(drop=True)
    df["date"] = pd.to_datetime(df["date"])

    required = {"date", "close", "volume"}
    missing = required - set(df.columns)

    if missing:
        raise ValueError(f"{symbol} curated file is missing columns: {sorted(missing)}")

    return df


def load_rs_symbol(
    symbol: str,
    benchmark: str,
    rs_dir: Path,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    path = rs_dir / f"{symbol}_vs_{benchmark}_{start_date}_{end_date}_rs_scan.parquet"

    if not path.exists():
        raise FileNotFoundError(f"Missing RS serving file for {symbol}: {path}")

    df = pd.read_parquet(path).sort_values("date").reset_index(drop=True)
    df["date"] = pd.to_datetime(df["date"])

    required = {"date", "close_zscore_50d"}
    missing = required - set(df.columns)

    if missing:
        raise ValueError(f"{symbol} RS file is missing columns: {sorted(missing)}")

    return df


def prepare_spy_regime(
    benchmark: str,
    curated_dir: Path,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    spy = load_curated_symbol(
        symbol=benchmark,
        curated_dir=curated_dir,
        start_date=start_date,
        end_date=end_date,
    )

    spy = spy[["date", "close"]].rename(columns={"close": "spy_close"})
    spy["spy_zscore_200d"] = zscore(spy["spy_close"], 200)

    return spy


def make_signal_rows(strategy_config: dict) -> pd.DataFrame:
    scanner_config = load_json(Path(strategy_config["scanner_config_path"]))

    symbols = scanner_config["stock_symbols"]
    sector_by_symbol = scanner_config.get("sector_by_symbol", {})

    benchmark = scanner_config["benchmark_symbol"]
    timeframe = scanner_config["timeframe"]
    start_date = scanner_config["start_date"]
    end_date = scanner_config["end_date"]
    data_root = Path(scanner_config["data_root"])

    curated_dir = data_root / "curated" / "market" / timeframe
    rs_dir = data_root / "serving" / "scanners" / "rs"

    stock_threshold_q = strategy_config["stock_threshold_q"]
    spy_regime_threshold_q = strategy_config["spy_regime_threshold_q"]
    volume_ratio_20d_min = strategy_config["volume_ratio_20d_min"]

    spy = prepare_spy_regime(
        benchmark=benchmark,
        curated_dir=curated_dir,
        start_date=start_date,
        end_date=end_date,
    )

    rows = []

    for symbol in symbols:
        stock = load_curated_symbol(
            symbol=symbol,
            curated_dir=curated_dir,
            start_date=start_date,
            end_date=end_date,
        )

        rs = load_rs_symbol(
            symbol=symbol,
            benchmark=benchmark,
            rs_dir=rs_dir,
            start_date=start_date,
            end_date=end_date,
        )

        stock = stock[["date", "close", "volume"]].rename(
            columns={
                "close": "stock_close",
                "volume": "stock_volume",
            }
        )

        df = (
            rs[["date", "close_zscore_50d"]]
            .merge(stock, on="date", how="inner")
            .merge(spy, on="date", how="inner")
            .sort_values("date")
            .reset_index(drop=True)
        )

        df["avg_volume_20d_prior"] = df["stock_volume"].shift(1).rolling(20).mean()
        df["volume_ratio_20d"] = df["stock_volume"] / df["avg_volume_20d_prior"]

        df = df.dropna(
            subset=[
                "close_zscore_50d",
                "spy_zscore_200d",
                "volume_ratio_20d",
            ]
        ).copy()

        if df.empty:
            continue

        latest = df.iloc[-1].copy()
        latest_date = latest["date"]

        history_before_latest = df[df["date"] < latest_date].copy()

        if history_before_latest.empty:
            continue

        stock_threshold = history_before_latest["close_zscore_50d"].quantile(
            stock_threshold_q
        )

        spy_threshold = (
            history_before_latest
            .drop_duplicates("date")["spy_zscore_200d"]
            .quantile(spy_regime_threshold_q)
        )

        oversold_signal = latest["close_zscore_50d"] <= stock_threshold
        market_ok = latest["spy_zscore_200d"] > spy_threshold
        volume_ok = latest["volume_ratio_20d"] >= volume_ratio_20d_min
        signal = oversold_signal and market_ok and volume_ok

        rows.append(
            {
                "as_of_date": latest_date.date().isoformat(),
                "ticker": symbol,
                "sector": sector_by_symbol.get(symbol, "Unknown"),
                "close": float(latest["stock_close"]),
                "close_zscore_50d": float(latest["close_zscore_50d"]),
                "stock_pullback_threshold": float(stock_threshold),
                "oversold_signal": bool(oversold_signal),
                "spy_zscore_200d": float(latest["spy_zscore_200d"]),
                "spy_regime_threshold": float(spy_threshold),
                "market_ok": bool(market_ok),
                "volume": float(latest["stock_volume"]),
                "avg_volume_20d_prior": float(latest["avg_volume_20d_prior"]),
                "volume_ratio_20d": float(latest["volume_ratio_20d"]),
                "volume_ok": bool(volume_ok),
                "signal": bool(signal),
            }
        )

    signals = pd.DataFrame(rows)

    if signals.empty:
        raise RuntimeError("No signal rows generated.")

    signals = signals.sort_values(
        ["signal", "close_zscore_50d", "volume_ratio_20d"],
        ascending=[False, True, False],
    ).reset_index(drop=True)

    signals["signal_rank"] = None

    signal_mask = signals["signal"]
    signals.loc[signal_mask, "signal_rank"] = range(1, int(signal_mask.sum()) + 1)

    return signals


def write_outputs(signals: pd.DataFrame, strategy_config: dict) -> None:
    output_dir = Path(strategy_config["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)

    latest_csv = output_dir / "latest_signals.csv"
    latest_json = output_dir / "latest_signals.json"
    candidates_csv = output_dir / "latest_candidates.csv"
    candidates_json = output_dir / "latest_candidates.json"

    generated_at = datetime.now(timezone.utc).isoformat()

    signals_out = signals.copy()
    signals_out.insert(0, "generated_at_utc", generated_at)

    signal_rows = signals_out[signals_out["signal"]].copy()

    signals_out.to_csv(candidates_csv, index=False)
    signal_rows.to_csv(latest_csv, index=False)

    candidates_payload = {
        "generated_at_utc": generated_at,
        "strategy_name": strategy_config["strategy_name"],
        "as_of_date": str(signals["as_of_date"].max()),
        "total_symbols_checked": int(len(signals)),
        "signal_count": int(signals["signal"].sum()),
        "signals": signal_rows.to_dict(orient="records"),
        "all_candidates": signals_out.to_dict(orient="records"),
    }

    latest_payload = {
        "generated_at_utc": generated_at,
        "strategy_name": strategy_config["strategy_name"],
        "as_of_date": str(signals["as_of_date"].max()),
        "total_symbols_checked": int(len(signals)),
        "signal_count": int(signals["signal"].sum()),
        "signals": signal_rows.to_dict(orient="records"),
    }

    latest_json.write_text(json.dumps(latest_payload, indent=2), encoding="utf-8")
    candidates_json.write_text(json.dumps(candidates_payload, indent=2), encoding="utf-8")

    print("=== Daily pullback RVOL signals ===")
    print(f"Generated at UTC: {generated_at}")
    print(f"As of date: {signals['as_of_date'].max()}")
    print(f"Symbols checked: {len(signals)}")
    print(f"Signals found: {int(signals['signal'].sum())}")
    print()
    print(f"Saved signals CSV: {latest_csv}")
    print(f"Saved signals JSON: {latest_json}")
    print(f"Saved candidates CSV: {candidates_csv}")
    print(f"Saved candidates JSON: {candidates_json}")

    if len(signal_rows) > 0:
        print("\n=== Current signals ===")
        cols = [
            "signal_rank",
            "ticker",
            "sector",
            "close",
            "close_zscore_50d",
            "stock_pullback_threshold",
            "volume_ratio_20d",
        ]
        print(signal_rows[cols].round(4).to_string(index=False))
    else:
        print("\nNo current signals.")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_STRATEGY_CONFIG,
        help="Path to strategy config JSON.",
    )

    args = parser.parse_args()

    strategy_config = load_json(args.config)
    signals = make_signal_rows(strategy_config)
    write_outputs(signals, strategy_config)


if __name__ == "__main__":
    main()

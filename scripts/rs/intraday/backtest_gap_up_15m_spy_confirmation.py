import json
from pathlib import Path

import pandas as pd

from src.utils.path_builders import build_market_curated_output_path


CONFIG_PATH = Path("configs/scanners/rs_scanner.json")
RESULTS_PATH = Path("data/research/intraday_gap_up/gap_up_15m_wide_stop_results.csv")
OUTPUT_DIR = Path("data/research/intraday_gap_up")

RULE_NAME = "no_stop_target_2pct"
COST_BPS = 10


def get_rth(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["bar_start_utc"] = pd.to_datetime(out["bar_start"], utc=True)
    out["bar_start_et"] = out["bar_start_utc"].dt.tz_convert("America/New_York")
    out["time_et"] = out["bar_start_et"].dt.time

    return out[
        (out["time_et"] >= pd.Timestamp("09:30").time())
        & (out["time_et"] < pd.Timestamp("16:00").time())
    ].sort_values("bar_start_et").copy()


def load_spy_15m(trade_date: str) -> pd.DataFrame:
    path = build_market_curated_output_path(
        symbol="SPY",
        start_date=trade_date,
        end_date=trade_date,
        timeframe="15m",
    )

    if not path.exists():
        raise FileNotFoundError(path)

    return pd.read_parquet(path)


def load_spy_daily() -> pd.DataFrame:
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        config = json.load(f)

    start = config["start_date"]
    end = config["end_date"]
    data_root = Path(config["data_root"])

    spy_path = data_root / "curated" / "market" / "1d" / f"SPY_{start}_{end}_curated.parquet"

    spy = pd.read_parquet(spy_path).copy()
    spy["trade_date"] = pd.to_datetime(spy["date"])
    spy = spy[["trade_date", "open", "close"]].rename(
        columns={
            "open": "spy_daily_open",
            "close": "spy_daily_close",
        }
    )

    spy["spy_prev_close"] = spy["spy_daily_close"].shift(1)
    spy["spy_gap_pct"] = (spy["spy_daily_open"] / spy["spy_prev_close"] - 1) * 100

    return spy[["trade_date", "spy_daily_open", "spy_prev_close", "spy_gap_pct"]]


def add_spy_first_bar_features(df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for trade_date in sorted(df["trade_date"].dt.strftime("%Y-%m-%d").unique()):
        spy_15m = load_spy_15m(trade_date)
        rth = get_rth(spy_15m)

        if len(rth) == 0:
            continue

        first = rth.iloc[0]

        rows.append(
            {
                "trade_date": pd.to_datetime(trade_date),
                "spy_first_open": first["open"],
                "spy_first_high": first["high"],
                "spy_first_low": first["low"],
                "spy_first_close": first["close"],
                "spy_first_bar_return_pct": (first["close"] / first["open"] - 1) * 100,
                "spy_first_bar_range_pct": (first["high"] / first["low"] - 1) * 100,
            }
        )

    spy_features = pd.DataFrame(rows)

    out = df.merge(spy_features, on="trade_date", how="left")

    out["spy_first_close_vs_prev_close_pct"] = (
        out["spy_first_close"] / out["spy_prev_close"] - 1
    ) * 100

    return out


def summarize_filter(df: pd.DataFrame, name: str, mask: pd.Series) -> dict:
    sub = df[mask].copy()

    wf1 = sub[sub["split"] == "WF1"]["net_return_pct"]
    wf2 = sub[sub["split"] == "WF2"]["net_return_pct"]

    return {
        "filter": name,
        "trades": len(sub),
        "avg": sub["net_return_pct"].mean(),
        "median": sub["net_return_pct"].median(),
        "win_rate": (sub["net_return_pct"] > 0).mean() * 100 if len(sub) else float("nan"),
        "best": sub["net_return_pct"].max() if len(sub) else float("nan"),
        "worst": sub["net_return_pct"].min() if len(sub) else float("nan"),
        "wf1_trades": len(wf1),
        "wf1_avg": wf1.mean(),
        "wf2_trades": len(wf2),
        "wf2_avg": wf2.mean(),
        "min_split_avg": min(wf1.mean(), wf2.mean()),
    }


def main() -> None:
    df = pd.read_csv(RESULTS_PATH)

    df = df[df["rule_name"] == RULE_NAME].copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["net_return_pct"] = df["gross_return_pct"] - (COST_BPS / 100)

    spy_daily = load_spy_daily()
    df = df.merge(spy_daily, on="trade_date", how="left")
    df = add_spy_first_bar_features(df)

    filters = {
        "base_all": df["net_return_pct"].notna(),

        # Known at market open
        "spy_gap_up": df["spy_gap_pct"] > 0,
        "spy_gap_down": df["spy_gap_pct"] < 0,
        "spy_gap_ge_minus_0_25": df["spy_gap_pct"] >= -0.25,

        # Known at first 15m close
        "spy_first_bar_green": df["spy_first_bar_return_pct"] > 0,
        "spy_first_bar_gt_0_05": df["spy_first_bar_return_pct"] > 0.05,
        "spy_first_bar_gt_0_10": df["spy_first_bar_return_pct"] > 0.10,
        "spy_first_bar_ge_minus_0_05": df["spy_first_bar_return_pct"] >= -0.05,
        "spy_first_bar_ge_minus_0_10": df["spy_first_bar_return_pct"] >= -0.10,

        # SPY first 15m close relative to previous close
        "spy_first_close_above_prev_close": df["spy_first_close_vs_prev_close_pct"] > 0,
        "spy_first_close_ge_minus_0_25_prev_close": df["spy_first_close_vs_prev_close_pct"] >= -0.25,

        # Combined entry-valid filters
        "spy_gap_up_and_first_bar_green": (df["spy_gap_pct"] > 0) & (df["spy_first_bar_return_pct"] > 0),
        "spy_gap_down_and_first_bar_green": (df["spy_gap_pct"] < 0) & (df["spy_first_bar_return_pct"] > 0),
        "spy_not_bad_and_first_bar_green": (df["spy_gap_pct"] >= -0.25) & (df["spy_first_bar_return_pct"] > 0),
        "spy_first_bar_green_and_above_prev_close": (df["spy_first_bar_return_pct"] > 0)
        & (df["spy_first_close_vs_prev_close_pct"] > 0),
    }

    rows = [summarize_filter(df, name, mask) for name, mask in filters.items()]
    summary = pd.DataFrame(rows).sort_values("avg", ascending=False)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    enriched_path = OUTPUT_DIR / "gap_up_15m_spy_confirmation_trades.csv"
    summary_path = OUTPUT_DIR / "gap_up_15m_spy_confirmation_summary.csv"

    df.to_csv(enriched_path, index=False)
    summary.to_csv(summary_path, index=False)

    print("=== SPY first-15m confirmation test ===")
    print("Rule: daily signal + stock gap up >= 1% + stock first 15m <= -0.50% + 2% target/EOD")
    print("All filters are entry-valid at first 15m close.")
    print("Cost:", COST_BPS, "bps")
    print()
    print("Saved trades:", enriched_path)
    print("Saved summary:", summary_path)
    print()
    print("=== Summary ===")
    print(summary.round(4).to_string(index=False))

    print("\n=== By year for likely filters ===")
    df["year"] = df["trade_date"].dt.year

    for name in [
        "base_all",
        "spy_first_bar_green",
        "spy_first_bar_gt_0_10",
        "spy_gap_down_and_first_bar_green",
        "spy_first_bar_green_and_above_prev_close",
    ]:
        sub = df[filters[name]].copy()
        print(f"\n--- {name} ---")
        print(
            sub.groupby("year")
            .agg(
                trades=("ticker", "count"),
                avg=("net_return_pct", "mean"),
                median=("net_return_pct", "median"),
                win_rate=("net_return_pct", lambda s: (s > 0).mean() * 100),
                best=("net_return_pct", "max"),
                worst=("net_return_pct", "min"),
            )
            .round(4)
            .to_string()
        )


if __name__ == "__main__":
    main()

import argparse
import json
from pathlib import Path

import pandas as pd

from src.utils.path_builders import build_market_curated_output_path


DEFAULT_CONFIG_PATH = Path("configs/scanners/rs_scanner.json")
DEFAULT_OUTPUT_DIR = Path("data/research/intraday_gap_up")

RELATIVE_GAP_THRESHOLDS = [2.0, 2.5, 3.0]
RANK_LIMITS = [5, 10, None]
FIRST_BAR_PULLBACK_THRESHOLD = -0.50
TARGET_PCT = 2.0
COST_BPS_LIST = [10, 20]
REMOVE_TOP_N_LIST = [0, 1, 3, 5]


def get_rth(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["bar_start_utc"] = pd.to_datetime(out["bar_start"], utc=True)
    out["bar_start_et"] = out["bar_start_utc"].dt.tz_convert("America/New_York")
    out["time_et"] = out["bar_start_et"].dt.time

    return out[
        (out["time_et"] >= pd.Timestamp("09:30").time())
        & (out["time_et"] < pd.Timestamp("16:00").time())
    ].sort_values("bar_start_et").copy()


def load_15m(ticker: str, trade_date: str) -> pd.DataFrame:
    path = build_market_curated_output_path(
        symbol=ticker,
        start_date=trade_date,
        end_date=trade_date,
        timeframe="15m",
    )

    if not path.exists():
        raise FileNotFoundError(path)

    return pd.read_parquet(path)

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="Path to RS scanner config JSON.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory containing gap-up candidate inputs and sweep outputs.",
    )
    return parser.parse_args()

def load_spy_daily(config_path: Path = DEFAULT_CONFIG_PATH) -> pd.DataFrame:
    with config_path.open("r", encoding="utf-8") as f:
        config = json.load(f)

    start = config["start_date"]
    end = config["end_date"]
    data_root = Path(config["data_root"])

    spy_path = data_root / "curated" / "market" / "1d" / f"SPY_{start}_{end}_curated.parquet"

    spy = pd.read_parquet(spy_path).copy()
    spy["trade_date"] = pd.to_datetime(spy["date"])
    spy = spy[["trade_date", "open", "close"]].rename(
        columns={
            "open": "spy_open",
            "close": "spy_close",
        }
    )

    spy["spy_prev_close"] = spy["spy_close"].shift(1)
    spy["spy_gap_pct"] = (spy["spy_open"] / spy["spy_prev_close"] - 1) * 100

    return spy[["trade_date", "spy_gap_pct"]]


def simulate_target_or_eod(rth: pd.DataFrame) -> dict:
    first_bar = rth.iloc[0]
    entry_price = first_bar["close"]
    target_price = entry_price * (1 + TARGET_PCT / 100)

    exit_price = rth.iloc[-1]["close"]
    exit_reason = "eod"
    exit_time_et = str(rth.iloc[-1]["bar_start_et"])

    for _, bar in rth.iloc[1:].iterrows():
        if bar["high"] >= target_price:
            exit_price = target_price
            exit_reason = "target"
            exit_time_et = str(bar["bar_start_et"])
            break

    gross_return_pct = (exit_price / entry_price - 1) * 100

    return {
        "entry_price": entry_price,
        "exit_price": exit_price,
        "gross_return_pct": gross_return_pct,
        "exit_reason": exit_reason,
        "exit_time_et": exit_time_et,
    }


def summarize(sub: pd.DataFrame, label: str, cost_bps: int) -> dict:
    net_col = f"net_return_{cost_bps}bps"

    wf1 = sub[sub["split"] == "WF1"][net_col]
    wf2 = sub[sub["split"] == "WF2"][net_col]

    return {
        "label": label,
        "cost_bps": cost_bps,
        "trades": len(sub),
        "avg": sub[net_col].mean(),
        "median": sub[net_col].median(),
        "win_rate": (sub[net_col] > 0).mean() * 100 if len(sub) else float("nan"),
        "target_rate": (sub["exit_reason"] == "target").mean() * 100 if len(sub) else float("nan"),
        "best": sub[net_col].max() if len(sub) else float("nan"),
        "worst": sub[net_col].min() if len(sub) else float("nan"),
        "wf1_trades": len(wf1),
        "wf1_avg": wf1.mean(),
        "wf2_trades": len(wf2),
        "wf2_avg": wf2.mean(),
        "min_split_avg": min(wf1.mean(), wf2.mean()) if len(wf1) and len(wf2) else float("nan"),
    }


def make_label(relative_gap_threshold: float, rank_limit: int | None) -> str:
    if rank_limit is None:
        return f"ge_{relative_gap_threshold:g}_all_ranks"
    return f"ge_{relative_gap_threshold:g}_rank_le_{rank_limit}"


def main() -> None:
    args = parse_args()
    output_dir = args.output_dir
    candidates_path = output_dir / "gap_up_candidates_all_signals.csv"

    candidates = pd.read_csv(candidates_path).copy()
    candidates["trade_date"] = pd.to_datetime(candidates["trade_date"])

    spy = load_spy_daily(args.config)
    candidates = candidates.merge(spy, on="trade_date", how="left")
    candidates["relative_gap_vs_spy_pct"] = candidates["next_gap_pct"] - candidates["spy_gap_pct"]

    # Only load candidates that could pass the broadest tested threshold.
    candidates = candidates[
        candidates["relative_gap_vs_spy_pct"] >= min(RELATIVE_GAP_THRESHOLDS)
    ].copy()

    rows = []
    missing = []

    for _, row in candidates.iterrows():
        ticker = row["ticker"]
        trade_date = row["trade_date"].strftime("%Y-%m-%d")

        try:
            bars = load_15m(ticker, trade_date)
        except FileNotFoundError:
            missing.append((ticker, trade_date))
            continue

        rth = get_rth(bars)

        if len(rth) < 2:
            continue

        first_bar = rth.iloc[0]
        first_bar_return_pct = (first_bar["close"] / first_bar["open"] - 1) * 100

        if first_bar_return_pct > FIRST_BAR_PULLBACK_THRESHOLD:
            continue

        result = simulate_target_or_eod(rth)

        rows.append(
            {
                "split": row["split"],
                "ticker": ticker,
                "signal_date": row["signal_date"],
                "trade_date": trade_date,
                "daily_signal_rank": row["daily_signal_rank"],
                "next_gap_pct": row["next_gap_pct"],
                "spy_gap_pct": row["spy_gap_pct"],
                "relative_gap_vs_spy_pct": row["relative_gap_vs_spy_pct"],
                "first_bar_return_pct": first_bar_return_pct,
                **result,
            }
        )

    results = pd.DataFrame(rows)

    for cost_bps in COST_BPS_LIST:
        results[f"net_return_{cost_bps}bps"] = results["gross_return_pct"] - (cost_bps / 100)

    output_dir.mkdir(parents=True, exist_ok=True)

    results_path = output_dir / "relative_gap_sweep_intraday_results.csv"
    summary_path = output_dir / "relative_gap_sweep_summary.csv"
    robustness_path = output_dir / "relative_gap_sweep_robustness.csv"

    results.to_csv(results_path, index=False)

    summary_rows = []

    for threshold in RELATIVE_GAP_THRESHOLDS:
        for rank_limit in RANK_LIMITS:
            mask = results["relative_gap_vs_spy_pct"] >= threshold

            if rank_limit is not None:
                mask = mask & (results["daily_signal_rank"] <= rank_limit)

            sub = results[mask].copy()
            label = make_label(threshold, rank_limit)

            for cost_bps in COST_BPS_LIST:
                summary_rows.append(summarize(sub, label, cost_bps))

    summary = pd.DataFrame(summary_rows)
    summary.to_csv(summary_path, index=False)

    robustness_rows = []

    for threshold in RELATIVE_GAP_THRESHOLDS:
        for rank_limit in RANK_LIMITS:
            mask = results["relative_gap_vs_spy_pct"] >= threshold

            if rank_limit is not None:
                mask = mask & (results["daily_signal_rank"] <= rank_limit)

            base = results[mask].copy()
            label = make_label(threshold, rank_limit)

            for cost_bps in COST_BPS_LIST:
                net_col = f"net_return_{cost_bps}bps"

                for remove_top_n in REMOVE_TOP_N_LIST:
                    test = base.sort_values(net_col, ascending=False).iloc[remove_top_n:].copy()

                    robustness_rows.append(
                        summarize(test, f"{label}_remove_top_{remove_top_n}", cost_bps)
                        | {
                            "base_label": label,
                            "remove_top_n": remove_top_n,
                        }
                    )

    robustness = pd.DataFrame(robustness_rows)
    robustness.to_csv(robustness_path, index=False)

    print("=== Relative gap sweep ===")
    print("Candidate file:", candidates_path)
    print("Relative gap thresholds:", RELATIVE_GAP_THRESHOLDS)
    print("Rank limits:", RANK_LIMITS)
    print("First 15m threshold:", FIRST_BAR_PULLBACK_THRESHOLD)
    print("Target/EOD:", TARGET_PCT)
    print()
    print("daily candidates after broadest relative gap filter:", len(candidates))
    print("missing intraday files:", len(missing))
    print("final intraday trades after first-bar filter:", len(results))
    print("saved results:", results_path)
    print("saved summary:", summary_path)
    print("saved robustness:", robustness_path)
    print()

    if missing:
        print("First missing examples:")
        print(missing[:20])
        print()

    print("=== Summary | cost 10 bps ===")
    view = summary[summary["cost_bps"] == 10].copy()
    print(
        view
        .round(4)
        .sort_values(["trades", "avg"], ascending=[False, False])
        .to_string(index=False)
    )

    print("\n=== Summary | cost 20 bps ===")
    view = summary[summary["cost_bps"] == 20].copy()
    print(
        view
        .round(4)
        .sort_values(["trades", "avg"], ascending=[False, False])
        .to_string(index=False)
    )

    print("\n=== Robustness | cost 20 bps | remove top 3 ===")
    view = robustness[
        (robustness["cost_bps"] == 20)
        & (robustness["remove_top_n"] == 3)
    ].copy()
    print(
        view
        .round(4)
        .sort_values(["trades", "avg"], ascending=[False, False])
        .to_string(index=False)
    )

    results["year"] = pd.to_datetime(results["trade_date"]).dt.year

    print("\n=== By year | likely candidates at 10 bps ===")
    likely = [
        ("ge_2.5_rank_le_10", 2.5, 10),
        ("ge_2_rank_le_10", 2.0, 10),
        ("ge_3_rank_le_10", 3.0, 10),
    ]

    for label, threshold, rank_limit in likely:
        sub = results[
            (results["relative_gap_vs_spy_pct"] >= threshold)
            & (results["daily_signal_rank"] <= rank_limit)
        ].copy()

        print(f"\n--- {label} ---")
        print(
            sub.groupby("year")
            .agg(
                trades=("ticker", "count"),
                avg=("net_return_10bps", "mean"),
                median=("net_return_10bps", "median"),
                win_rate=("net_return_10bps", lambda s: (s > 0).mean() * 100),
                target_rate=("exit_reason", lambda s: (s == "target").mean() * 100),
                best=("net_return_10bps", "max"),
                worst=("net_return_10bps", "min"),
            )
            .round(4)
            .to_string()
        )


if __name__ == "__main__":
    main()

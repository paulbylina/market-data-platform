from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd


INPUT = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_expanded_features/"
    "abc_gap_early_bar_behavior_trades_2024_2026.csv"
)

OUT = Path(
    "data/research/full_market_scanner_10y/high_price_short_fade_expanded_features/"
    "a_vs_equal_size_c_comparison_2024_2026.csv"
)


N_RANDOM = 10000
SEED = 42


def add_c_quality_score(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    for c in [
        "gap_pct",
        "premarket_dollar_vs_prior_daily_avg",
        "first_15m_return_pct",
        "first15_range_pct",
        "first15_close_position_in_range",
        "first15_dollar_rvol_20d",
        "first15_dollar_vs_prior_daily_avg",
    ]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")

    score = pd.Series(0, index=out.index, dtype=float)

    # C preferred: real gap up, not flat.
    if "gap_pct" in out.columns:
        score += np.where((out["gap_pct"] >= 1) & (out["gap_pct"] <= 3), 4, 0)
        score += np.where((out["gap_pct"] > 3) & (out["gap_pct"] <= 5), 3, 0)
        score += np.where((out["gap_pct"] >= 0) & (out["gap_pct"] < 1), 1, 0)

    # Quiet pre-market is important for C.
    if "premarket_dollar_vs_prior_daily_avg" in out.columns:
        score += np.where(out["premarket_dollar_vs_prior_daily_avg"] <= 0.003, 3, 0)
        score += np.where(
            (out["premarket_dollar_vs_prior_daily_avg"] > 0.003)
            & (out["premarket_dollar_vs_prior_daily_avg"] <= 0.01),
            2,
            0,
        )
        score += np.where(
            (out["premarket_dollar_vs_prior_daily_avg"] > 0.01)
            & (out["premarket_dollar_vs_prior_daily_avg"] <= 0.03),
            1,
            0,
        )

    # First15 structure.
    if "first_15m_return_pct" in out.columns:
        score += np.where((out["first_15m_return_pct"] >= 1.5) & (out["first_15m_return_pct"] <= 4), 3, 0)
        score += np.where((out["first_15m_return_pct"] >= 1) & (out["first_15m_return_pct"] < 1.5), 1, 0)

    if "first15_range_pct" in out.columns:
        score += np.where((out["first15_range_pct"] >= 2) & (out["first15_range_pct"] <= 4), 3, 0)
        score += np.where((out["first15_range_pct"] > 4) & (out["first15_range_pct"] <= 6), 1, 0)

    if "first15_close_position_in_range" in out.columns:
        score += np.where(out["first15_close_position_in_range"] >= 0.90, 3, 0)
        score += np.where(
            (out["first15_close_position_in_range"] >= 0.75)
            & (out["first15_close_position_in_range"] < 0.90),
            1,
            0,
        )

    # First15 RVOL if available.
    if "first15_dollar_rvol_20d" in out.columns:
        score += np.where(out["first15_dollar_rvol_20d"] >= 5, 2, 0)
        score += np.where((out["first15_dollar_rvol_20d"] >= 3) & (out["first15_dollar_rvol_20d"] < 5), 1, 0)

    out["c_quality_score"] = score
    return out


def summarize(label: str, g: pd.DataFrame) -> dict:
    return {
        "sample": label,
        "trades": len(g),
        "dates": g["trade_date"].nunique(),
        "tickers": g["ticker"].nunique(),

        "avg_net": g["first_exit_net"].mean(),
        "median_net": g["first_exit_net"].median(),
        "win_rate": (g["first_exit_net"] > 0).mean() * 100,

        "target_rate": g["target_hit"].mean() * 100,
        "stop_rate": g["stop_hit"].mean() * 100,

        "target_first15_rate": g["target_in_first_15m_after_entry"].mean() * 100,
        "target_first30_rate": g["target_in_first_30m_after_entry"].mean() * 100,

        "median_minutes_to_target": g.loc[g["target_hit"], "minutes_to_target"].median(),

        "median_first1m_drawdown": g["first1m_drawdown_pct"].median(),
        "median_first2m_drawdown": g["first2m_drawdown_pct"].median(),
        "median_first5m_drawdown": g["first5m_drawdown_pct"].median(),

        "median_bar1_drawdown": g["09:45_10:00_drawdown_pct"].median(),
        "median_bar1_runup": g["09:45_10:00_runup_pct"].median(),
        "median_bar1_close": g["09:45_10:00_close_pct"].median(),

        "median_bar2_drawdown": g["10:00_10:15_drawdown_pct"].median(),
        "median_bar2_runup": g["10:00_10:15_runup_pct"].median(),
        "median_bar2_close": g["10:00_10:15_close_pct"].median(),

        "bar1_dd_le_minus1_rate": (g["09:45_10:00_drawdown_pct"] <= -1).mean() * 100,
        "bar1_dd_le_minus2_rate": (g["09:45_10:00_drawdown_pct"] <= -2).mean() * 100,
    }


def random_c_samples(c: pd.DataFrame, n: int, rng: np.random.Generator) -> pd.DataFrame:
    rows = []

    for i in range(N_RANDOM):
        s = c.sample(n=n, replace=False, random_state=int(rng.integers(0, 2**32 - 1)))
        rows.append(
            {
                "sample_id": i,
                "avg_net": s["first_exit_net"].mean(),
                "median_net": s["first_exit_net"].median(),
                "win_rate": (s["first_exit_net"] > 0).mean() * 100,
                "target_rate": s["target_hit"].mean() * 100,
                "stop_rate": s["stop_hit"].mean() * 100,
                "target_first30_rate": s["target_in_first_30m_after_entry"].mean() * 100,
                "median_bar1_drawdown": s["09:45_10:00_drawdown_pct"].median(),
                "median_bar2_drawdown": s["10:00_10:15_drawdown_pct"].median(),
                "bar1_dd_le_minus2_rate": (s["09:45_10:00_drawdown_pct"] <= -2).mean() * 100,
            }
        )

    return pd.DataFrame(rows)


def pct_rank(value: float, dist: pd.Series, higher_is_better: bool = True) -> float:
    if higher_is_better:
        return (dist <= value).mean() * 100
    return (dist >= value).mean() * 100


def main() -> None:
    if not INPUT.exists():
        raise SystemExit(f"Missing input: {INPUT}")

    df = pd.read_csv(INPUT)
    df = add_c_quality_score(df)

    a = df[df["abc"].eq("A")].copy()
    c = df[df["abc"].eq("C")].copy()

    n = len(a)
    if n == 0:
        raise SystemExit("No A trades found.")
    if len(c) < n:
        raise SystemExit(f"Not enough C trades. A={n}, C={len(c)}")

    c_top = c.sort_values(
        [
            "c_quality_score",
            "gap_pct",
            "first15_close_position_in_range",
            "first_15m_return_pct",
        ],
        ascending=[False, True, False, False],
    ).head(n).copy()

    rng = np.random.default_rng(SEED)
    rand = random_c_samples(c, n, rng)

    rows = [
        summarize("A_all_41", a),
        summarize("C_all_171", c),
        summarize("C_top41_live_safe_score", c_top),
    ]

    comp = pd.DataFrame(rows)

    # Add random C 41 distribution summary.
    random_summary = {
        "sample": "C_random41_median_of_10000_samples",
        "trades": n,
        "dates": np.nan,
        "tickers": np.nan,
        "avg_net": rand["avg_net"].median(),
        "median_net": rand["median_net"].median(),
        "win_rate": rand["win_rate"].median(),
        "target_rate": rand["target_rate"].median(),
        "stop_rate": rand["stop_rate"].median(),
        "target_first30_rate": rand["target_first30_rate"].median(),
        "median_bar1_drawdown": rand["median_bar1_drawdown"].median(),
        "median_bar2_drawdown": rand["median_bar2_drawdown"].median(),
        "bar1_dd_le_minus2_rate": rand["bar1_dd_le_minus2_rate"].median(),
    }

    comp = pd.concat([comp, pd.DataFrame([random_summary])], ignore_index=True)

    a_stats = summarize("A_all_41", a)

    print("input:", INPUT)
    print(f"A trades: {len(a)}")
    print(f"C trades: {len(c)}")
    print()
    print("=== A vs equal-size C comparison ===")
    print(comp.to_string(index=False))

    print()
    print("=== Where A ranks versus random 41-trade C samples ===")
    print(f"A avg_net percentile vs random C: {pct_rank(a_stats['avg_net'], rand['avg_net'], True):.1f}%")
    print(f"A median_net percentile vs random C: {pct_rank(a_stats['median_net'], rand['median_net'], True):.1f}%")
    print(f"A win_rate percentile vs random C: {pct_rank(a_stats['win_rate'], rand['win_rate'], True):.1f}%")
    print(f"A target_rate percentile vs random C: {pct_rank(a_stats['target_rate'], rand['target_rate'], True):.1f}%")
    print(f"A stop_rate percentile vs random C: {pct_rank(a_stats['stop_rate'], rand['stop_rate'], False):.1f}%")
    print(f"A bar1 drawdown percentile vs random C, less negative better: {pct_rank(a_stats['median_bar1_drawdown'], rand['median_bar1_drawdown'], True):.1f}%")
    print(f"A bar2 drawdown percentile vs random C, less negative better: {pct_rank(a_stats['median_bar2_drawdown'], rand['median_bar2_drawdown'], True):.1f}%")

    print()
    print("=== Top 41 C tickers by live-safe score ===")
    show_cols = [
        "trade_date",
        "ticker",
        "c_quality_score",
        "gap_pct",
        "premarket_dollar_vs_prior_daily_avg",
        "first_15m_return_pct",
        "first15_range_pct",
        "first15_close_position_in_range",
        "first_exit_net",
        "target_hit",
        "minutes_to_target",
        "09:45_10:00_drawdown_pct",
        "10:00_10:15_drawdown_pct",
    ]
    show_cols = [x for x in show_cols if x in c_top.columns]
    print(c_top[show_cols].to_string(index=False))

    comp.to_csv(OUT, index=False)
    print()
    print("saved:", OUT)


if __name__ == "__main__":
    main()

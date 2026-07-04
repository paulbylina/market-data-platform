from __future__ import annotations

from pathlib import Path
import time

import numpy as np
import pandas as pd

from scripts.stocks.build_10y_high_price_short_fade_expanded_features import get_api_key
from scripts.stocks.add_first15_opening_rvol_for_date import fetch_1m_range


SIGNALS_PATH = Path(
    "data/research/full_market_scanner_10y/high_price_full_universe_first15_checks/"
    "high_price_first15_final_A_Aplus_rvol_2026-06-29_to_2026-07-02.csv"
)

PANEL_PATH = Path(
    "data/research/full_market_scanner_10y/historical_full_market_daily_panel.csv"
)

CACHE_DIR = Path("data/cache/massive/past_week_prior_last15_direct_1m")

OUT_PATH = Path(
    "data/research/full_market_scanner_10y/high_price_full_universe_first15_checks/"
    "past_week_prior_last15_direct_ranked_signals_2026-06-29_to_2026-07-02.csv"
)


def to_date(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce").dt.strftime("%Y-%m-%d")


def last15_by_date(bars: pd.DataFrame) -> pd.DataFrame:
    if bars.empty or "timestamp_ms" not in bars.columns:
        return pd.DataFrame()

    out = bars.copy()

    for col in ["open", "high", "low", "close", "volume"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out["ts_utc"] = pd.to_datetime(
        pd.to_numeric(out["timestamp_ms"], errors="coerce"),
        unit="ms",
        utc=True,
        errors="coerce",
    )

    out = out[out["ts_utc"].notna()].copy()
    out["ts_et"] = out["ts_utc"].dt.tz_convert("America/New_York")
    out["trade_date"] = out["ts_et"].dt.strftime("%Y-%m-%d")
    out["time_et"] = out["ts_et"].dt.time

    start = pd.to_datetime("15:45").time()
    end = pd.to_datetime("16:00").time()

    l15 = out[(out["time_et"] >= start) & (out["time_et"] < end)].copy()

    if l15.empty:
        return pd.DataFrame()

    l15["dollar_volume"] = l15["close"] * l15["volume"]

    rows = []

    for date, g in l15.groupby("trade_date", sort=True):
        g = g.sort_values("ts_et")
        first = g.iloc[0]
        last = g.iloc[-1]

        open_px = first["open"]
        close_px = last["close"]

        rows.append(
            {
                "trade_date": date,
                "last15_volume": g["volume"].sum(),
                "last15_dollar_volume": g["dollar_volume"].sum(),
                "last15_return_pct": (close_px / open_px - 1.0) * 100 if open_px > 0 else np.nan,
            }
        )

    return pd.DataFrame(rows)


def compute_prior_last15_for_signal(
    ticker: str,
    trade_date: str,
    panel: pd.DataFrame,
    api_key: str,
    lookback_days: int = 20,
    sleep_seconds: float = 0.05,
) -> dict:
    tpanel = panel[
        (panel["ticker"].astype(str) == str(ticker))
        & (panel["trade_date"] < trade_date)
    ].sort_values("trade_date")

    dates = tpanel["trade_date"].dropna().drop_duplicates().tolist()

    if len(dates) < lookback_days + 1:
        return {
            "ticker": ticker,
            "trade_date": trade_date,
            "prior_last15_days_used": len(dates),
            "prior_day_last15_dollar_rvol_20d": np.nan,
            "prior_day_last15_volume_rvol_20d": np.nan,
            "prior_day_last15_return_pct": np.nan,
        }

    prior_day = dates[-1]
    lookback = dates[-(lookback_days + 1):-1]

    bars = fetch_1m_range(
        ticker=ticker,
        start_date=lookback[0],
        end_date=prior_day,
        api_key=api_key,
        cache_dir=CACHE_DIR,
        sleep_seconds=sleep_seconds,
    )

    l15 = last15_by_date(bars)

    if l15.empty:
        return {
            "ticker": ticker,
            "trade_date": trade_date,
            "prior_trade_date_for_last15": prior_day,
            "prior_last15_days_used": 0,
            "prior_day_last15_dollar_rvol_20d": np.nan,
            "prior_day_last15_volume_rvol_20d": np.nan,
            "prior_day_last15_return_pct": np.nan,
        }

    prior = l15[l15["trade_date"].eq(prior_day)]
    avg = l15[l15["trade_date"].isin(lookback)]

    if prior.empty or avg.empty:
        return {
            "ticker": ticker,
            "trade_date": trade_date,
            "prior_trade_date_for_last15": prior_day,
            "prior_last15_days_used": int(avg["trade_date"].nunique()) if not avg.empty else 0,
            "prior_day_last15_dollar_rvol_20d": np.nan,
            "prior_day_last15_volume_rvol_20d": np.nan,
            "prior_day_last15_return_pct": np.nan,
        }

    p = prior.iloc[0]
    avg_vol = avg["last15_volume"].mean()
    avg_dvol = avg["last15_dollar_volume"].mean()

    return {
        "ticker": ticker,
        "trade_date": trade_date,
        "prior_trade_date_for_last15": prior_day,
        "prior_last15_days_used": int(avg["trade_date"].nunique()),
        "prior_day_last15_volume": p["last15_volume"],
        "prior_day_last15_dollar_volume": p["last15_dollar_volume"],
        "avg_prior_20d_last15_volume": avg_vol,
        "avg_prior_20d_last15_dollar_volume": avg_dvol,
        "prior_day_last15_volume_rvol_20d": p["last15_volume"] / avg_vol if avg_vol > 0 else np.nan,
        "prior_day_last15_dollar_rvol_20d": p["last15_dollar_volume"] / avg_dvol if avg_dvol > 0 else np.nan,
        "prior_day_last15_return_pct": p["last15_return_pct"],
    }


def label_prior_last15(x: float) -> str:
    if pd.isna(x):
        return "missing"
    if x >= 5:
        return "5+ extreme"
    if x >= 3:
        return "3-5 hot"
    if x >= 1.5:
        return "1.5-3 active"
    if x >= 0.75:
        return "0.75-1.5 normal"
    return "<=0.75 quiet"


def label_premarket(x: float) -> str:
    if pd.isna(x):
        return "missing"
    if x <= 0.003:
        return "dead"
    if x <= 0.01:
        return "quiet"
    if x <= 0.03:
        return "mild"
    if x <= 0.10:
        return "active"
    return "mania"


def classify(row: pd.Series) -> tuple[str, int, str]:
    prior = row.get("prior_day_last15_dollar_rvol_20d", np.nan)
    pm = row.get("premarket_dollar_vs_prior_daily_avg", np.nan)
    f15_daily = row.get("first15_dollar_vs_prior_daily_avg", np.nan)
    f15_rvol = row.get("first15_dollar_rvol_20d", np.nan)
    ret = row.get("first_15m_return_pct", np.nan)
    rng = row.get("first15_range_pct", np.nan)
    close_pos = row.get("first15_close_position_in_range", np.nan)

    score = 0
    notes = []

    if pd.notna(prior):
        if prior >= 5:
            score += 2
            notes.append("5+ extreme prior close")
        elif prior >= 3:
            score += 1
            notes.append("3-5 hot prior close")
        elif prior <= 0.75:
            score -= 1
            notes.append("quiet prior close")

    if pd.notna(pm):
        if pm <= 0.003:
            score += 2
            notes.append("dead pre-market")
        elif pm <= 0.01:
            score += 1
            notes.append("quiet pre-market")
        elif pm <= 0.03:
            notes.append("mild pre-market")
        elif pm <= 0.10:
            score -= 1
            notes.append("active pre-market")
        else:
            score -= 3
            notes.append("pre-market mania")

    if pd.notna(ret):
        if 2 <= ret < 8:
            score += 2
            notes.append("first15 return 2-8 sweet spot")
        elif 1 <= ret < 2:
            score += 1
            notes.append("first15 return okay")
        elif ret >= 8:
            score -= 3
            notes.append("first15 too hot")

    if pd.notna(rng):
        if 2 <= rng < 4:
            score += 2
            notes.append("clean 2-4 range")
        elif 4 <= rng < 8:
            score += 1
            notes.append("wide but acceptable range")
        elif rng >= 8:
            score -= 2
            notes.append("range too wide")

    if pd.notna(f15_daily):
        if 0.05 <= f15_daily < 0.50:
            score += 1
            notes.append("meaningful first15 dollar volume")
        elif f15_daily >= 0.50:
            score -= 1
            notes.append("extreme first15 dollar volume")

    if pd.notna(f15_rvol):
        if f15_rvol >= 3:
            score += 2
            notes.append("first15 RVOL >= 3")
        elif f15_rvol >= 2:
            score += 1
            notes.append("first15 RVOL >= 2")

    if pd.notna(close_pos):
        if close_pos >= 0.75:
            score += 1
            notes.append("closed near high")
        elif close_pos < 0.50:
            score -= 1
            notes.append("weak first15 close position")

    if (
        pd.notna(prior) and prior >= 3
        and pd.notna(pm) and pm <= 0.003
        and pd.notna(ret) and 2 <= ret < 8
        and pd.notna(rng) and 2 <= rng < 8
    ):
        label = "A+ clean prior-close shock"
    elif (
        pd.notna(prior) and prior >= 3
        and pd.notna(pm) and pm <= 0.01
        and pd.notna(ret) and 2 <= ret < 8
        and pd.notna(rng) and 2 <= rng < 8
    ):
        label = "A clean"
    elif (
        pd.notna(prior) and prior >= 1.5
        and pd.notna(pm) and pm <= 0.03
        and pd.notna(ret) and 2 <= ret < 8
        and pd.notna(rng) and 2 <= rng < 8
    ):
        label = "B active but valid"
    elif pd.notna(ret) and ret >= 8:
        label = "DOWNRANK first15 too hot"
    elif pd.notna(pm) and pm > 0.03:
        label = "DOWNRANK active pre-market"
    else:
        label = "watchlist only"

    return label, score, "; ".join(notes)


def main() -> None:
    if not SIGNALS_PATH.exists():
        raise SystemExit(f"Missing signals file: {SIGNALS_PATH}")
    if not PANEL_PATH.exists():
        raise SystemExit(f"Missing panel file: {PANEL_PATH}")

    sig = pd.read_csv(SIGNALS_PATH)
    panel = pd.read_csv(PANEL_PATH)

    sig["trade_date"] = to_date(sig["trade_date"])
    panel["trade_date"] = to_date(panel["trade_date"])

    panel = panel.sort_values(["ticker", "trade_date"]).reset_index(drop=True)

    api_key = get_api_key()

    rows = []

    pairs = sig[["ticker", "trade_date"]].drop_duplicates().sort_values(["trade_date", "ticker"])

    for i, row in pairs.reset_index(drop=True).iterrows():
        print(f"{i+1}/{len(pairs)} {row['trade_date']} {row['ticker']}")
        rows.append(
            compute_prior_last15_for_signal(
                ticker=str(row["ticker"]),
                trade_date=str(row["trade_date"]),
                panel=panel,
                api_key=api_key,
                lookback_days=20,
                sleep_seconds=0.05,
            )
        )

    prior = pd.DataFrame(rows)

    df = sig.merge(prior, on=["ticker", "trade_date"], how="left")

    for col in df.columns:
        if any(x in col.lower() for x in ["pct", "rvol", "volume", "dollar", "range", "position", "gap"]):
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["prior_last15_bucket"] = df["prior_day_last15_dollar_rvol_20d"].map(label_prior_last15)
    df["premarket_bucket"] = df["premarket_dollar_vs_prior_daily_avg"].map(label_premarket)

    labels = df.apply(classify, axis=1, result_type="expand")
    df["context_signal"] = labels[0]
    df["context_score"] = labels[1]
    df["context_notes"] = labels[2]

    cols = [
        "trade_date",
        "ticker",
        "signal_quality",
        "context_signal",
        "context_score",
        "prior_last15_bucket",
        "premarket_bucket",
        "prior_day_last15_dollar_rvol_20d",
        "prior_last15_days_used",
        "premarket_dollar_vs_prior_daily_avg",
        "first15_dollar_vs_prior_daily_avg",
        "first15_dollar_rvol_20d",
        "first_15m_return_pct",
        "first15_range_pct",
        "first15_close_position_in_range",
        "gap_pct",
        "context_notes",
    ]

    cols = [c for c in cols if c in df.columns]

    out = df.sort_values(["trade_date", "context_score"], ascending=[True, False])[cols]
    out.to_csv(OUT_PATH, index=False)

    print()
    print("=== Counts ===")
    print(out["context_signal"].value_counts(dropna=False).to_string())
    print()
    print("=== Ranked past-week signals with direct prior-last15 ===")
    print(out.to_string(index=False))
    print()
    print("saved:", OUT_PATH)


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd


DEFAULT_FEATURES = Path(
    "data/research/full_market_scanner_10y/extended_hours_features_pilot/extended_hours_features_pilot.csv"
)
DEFAULT_CACHE_DIR = Path("data/cache/massive/extended_hours_1m")
DEFAULT_OUT_DIR = Path("data/research/full_market_scanner_10y/mid_5_to_50_strategy_pilot")

TARGET_STOPS = [(2.0, 2.5), (3.0, 4.0), (4.0, 5.0)]
COST_BPS = 10.0
COST_PCT = COST_BPS / 100.0

SETUP_ORDER = [
    "GD_SHORT_RRR_continuation",
    "GD_SHORT_RRR_clean",
    "GD_SHORT_RGR_bounce_fail",
    "GD_SHORT_RGR_clean",
    "GD_SHORT_RRG_weak_bounce",
    "GD_SHORT_RRG_failed_reclaim",
]


@dataclass(frozen=True)
class FiveMinBar:
    open: float
    high: float
    low: float
    close: float
    volume: float

    @property
    def ret_pct(self) -> float:
        if not np.isfinite(self.open) or self.open == 0:
            return np.nan
        return (self.close / self.open - 1.0) * 100.0

    @property
    def range_pct(self) -> float:
        if not np.isfinite(self.open) or self.open == 0:
            return np.nan
        return (self.high - self.low) / self.open * 100.0

    @property
    def close_pos(self) -> float:
        rng = self.high - self.low
        if not np.isfinite(rng) or rng == 0:
            return 0.5
        return (self.close - self.low) / rng


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Test $5-$50 gap-down short RRR/RGR/RRG setups with coverage-aware frequency diagnostics."
    )
    parser.add_argument("--features", type=Path, default=DEFAULT_FEATURES)
    parser.add_argument(
        "--daily-source",
        type=Path,
        default=None,
        help="Optional full daily source with ticker, trade_date, dollar_volume. Used to compute true previous-day volume.",
    )
    parser.add_argument(
        "--daily-source-root",
        type=Path,
        action="append",
        default=None,
        help="Optional root to search for a daily source. Can be repeated.",
    )
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--max-rows", type=int, default=None, help="Optional debug limit.")
    return parser.parse_args()


def period_bucket(year: int) -> str:
    if year <= 2022:
        return "train_2016_2022"
    if year <= 2024:
        return "validation_2023_2024"
    return "test_2025_2026"


def gap_bucket(gap_pct: float) -> str | None:
    if pd.isna(gap_pct):
        return None
    if -10 <= gap_pct < -5:
        return "gap_down_10_to_5"
    if -5 <= gap_pct < -2:
        return "gap_down_5_to_2"
    if -2 <= gap_pct < 0:
        return "gap_down_2_to_0"
    return None


def volume_grade(metric: float) -> str:
    if pd.isna(metric):
        return "MISSING"
    if metric <= 2:
        return "LOWER_le_2x"
    if metric <= 5:
        return "HIGH_2_to_5x"
    return "EXTREME_gt_5x"


def infer_prev_day_volume_metric(df: pd.DataFrame) -> pd.Series:
    """
    Lookahead-safe fallback only.

    Do NOT use same-day dollar_volume_rvol_20d here. That is not known at the
    open and creates lookahead bias for an intraday scanner.
    """
    if "prev_day_volume_metric" in df.columns:
        return pd.to_numeric(df["prev_day_volume_metric"], errors="coerce")

    if {"prev_day_dollar_volume", "avg_dollar_volume_20d_prior"}.issubset(df.columns):
        prev = pd.to_numeric(df["prev_day_dollar_volume"], errors="coerce")
        avg = pd.to_numeric(df["avg_dollar_volume_20d_prior"], errors="coerce")
        return prev / avg.replace(0, np.nan)

    return pd.Series(np.nan, index=df.index)


def normalize_date_text(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce").dt.strftime("%Y-%m-%d")


def default_daily_source_roots(custom_roots: list[Path] | None) -> list[Path]:
    if custom_roots:
        return custom_roots

    return [
        Path("data/research/full_market_scanner_10y"),
        Path("data/research"),
        Path("data/processed"),
        Path("data/reference/stocks"),
        Path("data/reference"),
    ]


def read_daily_source_columns(source: Path) -> pd.DataFrame:
    required = {"ticker", "trade_date", "dollar_volume"}

    if source.suffix.lower() == ".parquet":
        try:
            df = pd.read_parquet(source, columns=list(required))
        except Exception:
            return pd.DataFrame()
    else:
        try:
            df = pd.read_csv(source, usecols=lambda c: c in required)
        except Exception:
            return pd.DataFrame()

    if not required.issubset(df.columns):
        return pd.DataFrame()

    df = df[list(required)].copy()
    df["ticker"] = df["ticker"].astype(str).str.upper()
    df["trade_date"] = normalize_date_text(df["trade_date"])
    df["dollar_volume"] = pd.to_numeric(df["dollar_volume"], errors="coerce")
    df = df.dropna(subset=["ticker", "trade_date", "dollar_volume"])
    return df


def candidate_daily_paths(roots: list[Path]) -> list[Path]:
    candidates: list[Path] = []
    seen: set[Path] = set()

    excluded_parts = {
        ".git",
        ".venv",
        "cache",
        "extended_hours_1m",
        "mid_5_to_50_strategy_pilot",
        "cheap_1_to_5_extended_hours_features",
        "extended_hours_features_pilot",
    }

    excluded_name_bits = [
        "gapdown_short",
        "train_test",
        "best_rows",
        "focus_candidates",
        "trade_frequency",
        "after_close",
        "coverage",
        "trades.csv",
    ]

    for root in roots:
        if not root.exists():
            continue

        for suffix in ("*.csv", "*.parquet"):
            for p in root.rglob(suffix):
                if p in seen:
                    continue

                parts = {part.lower() for part in p.parts}
                name = p.name.lower()

                if parts & excluded_parts:
                    continue

                if any(bit in name for bit in excluded_name_bits):
                    continue

                seen.add(p)
                candidates.append(p)

    return candidates


def discover_daily_source(features: pd.DataFrame, roots: list[Path]) -> Path | None:
    needed = features[["ticker", "prev_trade_date"]].dropna().drop_duplicates().copy()

    if needed.empty:
        return None

    if len(needed) > 5000:
        needed_sample = needed.sample(5000, random_state=42)
    else:
        needed_sample = needed

    best_path: Path | None = None
    best_score = 0.0
    best_rows = 0

    paths = candidate_daily_paths(roots)
    print(f"daily source candidates checked: {len(paths)}")

    for p in paths:
        daily = read_daily_source_columns(p)

        if daily.empty:
            continue

        keys = daily[["ticker", "trade_date"]].drop_duplicates()
        keys = keys.rename(columns={"trade_date": "prev_trade_date"})

        matched = needed_sample.merge(keys, on=["ticker", "prev_trade_date"], how="inner")
        score = len(matched) / len(needed_sample) if len(needed_sample) else 0.0

        if score > best_score:
            best_score = score
            best_path = p
            best_rows = len(daily)

    if best_path is not None:
        print(f"selected daily source: {best_path}")
        print(f"daily source sample coverage: {best_score:.2%}")
        print(f"daily source rows: {best_rows}")

    return best_path


def attach_prev_day_volume_metric(
    features: pd.DataFrame,
    daily_source: Path | None,
    daily_source_roots: list[Path] | None,
) -> pd.DataFrame:
    features = features.copy()

    roots = default_daily_source_roots(daily_source_roots)

    if daily_source is None:
        daily_source = discover_daily_source(features, roots)

    if daily_source is None:
        print("WARNING: no daily source found. prev_day_volume_metric will be missing; volume grade will be MISSING.")
        features["prev_day_volume_metric"] = infer_prev_day_volume_metric(features)
        return features

    if not daily_source.exists():
        raise FileNotFoundError(f"Daily source does not exist: {daily_source}")

    daily = read_daily_source_columns(daily_source)

    if daily.empty:
        raise RuntimeError(
            f"Daily source is empty or missing required columns ticker/trade_date/dollar_volume: {daily_source}"
        )

    daily = daily.sort_values(["ticker", "trade_date"])
    daily = daily.drop_duplicates(["ticker", "trade_date"], keep="last")

    prev = daily.rename(
        columns={
            "trade_date": "prev_trade_date",
            "dollar_volume": "prev_day_dollar_volume",
        }
    )[["ticker", "prev_trade_date", "prev_day_dollar_volume"]]

    features = features.drop(columns=["prev_day_dollar_volume"], errors="ignore")
    features = features.merge(prev, on=["ticker", "prev_trade_date"], how="left")

    avg = pd.to_numeric(features["avg_dollar_volume_20d_prior"], errors="coerce")
    prev_dollar = pd.to_numeric(features["prev_day_dollar_volume"], errors="coerce")

    features["prev_day_volume_metric"] = prev_dollar / avg.replace(0, np.nan)

    coverage = features["prev_day_volume_metric"].notna().mean() * 100
    print(f"prev_day_volume_metric coverage: {coverage:.2f}%")
    print("prev_day_volume_metric source: true prev_trade_date dollar_volume / avg_dollar_volume_20d_prior")

    return features

def build_cache_index(cache_dir: Path) -> dict[tuple[str, str], Path]:
    """
    Index extended-hours 1m cache files by (ticker, trade_date).

    Cache names look like:
      TICKER_2016-11-17_to_2016-11-18_1m.csv

    The first date is the previous/extended-hours start date.
    The second date after "_to_" is the actual trade_date.
    """
    index: dict[tuple[str, str], Path] = {}

    pattern = re.compile(
        r"^(?P<ticker>[A-Z0-9.\-]+)_(?P<from_date>20\d{2}-\d{2}-\d{2})_to_(?P<trade_date>20\d{2}-\d{2}-\d{2})_1m$",
        re.IGNORECASE,
    )

    for path in cache_dir.rglob("*.csv"):
        m = pattern.match(path.stem)
        if not m:
            continue

        ticker = m.group("ticker").upper()
        trade_date = m.group("trade_date")
        index.setdefault((ticker, trade_date), path)

    return index

def read_minute_cache(path: Path) -> pd.DataFrame:
    try:
        bars = pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()

    if bars.empty:
        return bars

    lower_map = {c.lower(): c for c in bars.columns}

    # Prefer the normalized ET columns created by our cache builder.
    if "bar_start_et" in lower_map:
        dt_col = lower_map["bar_start_et"]
        bars["dt_et"] = pd.to_datetime(bars[dt_col], errors="coerce", utc=True).dt.tz_convert("America/New_York")
    elif "timestamp_ms" in lower_map:
        t = pd.to_numeric(bars[lower_map["timestamp_ms"]], errors="coerce")
        bars["dt_et"] = (
            pd.to_datetime(t, unit="ms", errors="coerce", utc=True)
            .dt.tz_convert("America/New_York")
        )
    elif "t" in lower_map:
        t = pd.to_numeric(bars[lower_map["t"]], errors="coerce")
        unit = "ms" if t.dropna().median() > 10_000_000_000 else "s"
        bars["dt_et"] = (
            pd.to_datetime(t, unit=unit, errors="coerce", utc=True)
            .dt.tz_convert("America/New_York")
        )
    else:
        dt_col = None
        for candidate in ["datetime", "timestamp", "time", "date", "window_start"]:
            if candidate in lower_map:
                dt_col = lower_map[candidate]
                break

        if dt_col is None:
            return pd.DataFrame()

        bars["dt_et"] = pd.to_datetime(bars[dt_col], errors="coerce")

    rename = {}
    for canonical, aliases in {
        "open": ["open", "o"],
        "high": ["high", "h"],
        "low": ["low", "l"],
        "close": ["close", "c"],
        "volume": ["volume", "v"],
    }.items():
        for alias in aliases:
            if alias in lower_map:
                rename[lower_map[alias]] = canonical
                break

    bars = bars.rename(columns=rename)

    needed = {"dt_et", "open", "high", "low", "close"}
    if not needed.issubset(bars.columns):
        return pd.DataFrame()

    if "volume" not in bars.columns:
        bars["volume"] = np.nan

    for c in ["open", "high", "low", "close", "volume"]:
        bars[c] = pd.to_numeric(bars[c], errors="coerce")

    if "trade_date_et" in lower_map:
        bars["trade_date_et"] = bars[lower_map["trade_date_et"]].astype(str)

    bars = bars.dropna(subset=["dt_et", "open", "high", "low", "close"])
    return bars.sort_values("dt_et")

def get_window_bar(bars: pd.DataFrame, start_time: str, end_time: str) -> FiveMinBar | None:
    times = bars["dt_et"].dt.strftime("%H:%M")
    w = bars[(times >= start_time) & (times < end_time)]

    if w.empty:
        return None

    return FiveMinBar(
        open=float(w.iloc[0]["open"]),
        high=float(w["high"].max()),
        low=float(w["low"].min()),
        close=float(w.iloc[-1]["close"]),
        volume=float(w["volume"].sum()) if "volume" in w.columns else np.nan,
    )


def regular_session_after_entry(bars: pd.DataFrame) -> pd.DataFrame:
    times = bars["dt_et"].dt.strftime("%H:%M")
    return bars[(times >= "09:45") & (times <= "15:59")].copy()


def identify_setups(first: FiveMinBar, second: FiveMinBar, third: FiveMinBar) -> list[str]:
    setups: list[str] = []

    first_red = first.close < first.open
    second_red = second.close < second.open
    third_red = third.close < third.open
    second_green = second.close > second.open
    third_green = third.close > third.open

    rrr_continuation = (
        first_red
        and second_red
        and third_red
        and second.close < first.close
        and third.close < second.close
    )

    if rrr_continuation:
        setups.append("GD_SHORT_RRR_continuation")
        if first.close_pos <= 0.50 and third.close_pos <= 0.50:
            setups.append("GD_SHORT_RRR_clean")

    rgr_bounce_fail = (
        first_red
        and second_green
        and third_red
        and third.close < second.close
        and third.close <= first.close
    )

    if rgr_bounce_fail:
        setups.append("GD_SHORT_RGR_bounce_fail")
        if first.close_pos <= 0.50 and third.close_pos <= 0.50:
            setups.append("GD_SHORT_RGR_clean")

    # RRG = red / red / green.
    # Bearish only if the third green bar is a weak bounce / failed reclaim.
    rrg = first_red and second_red and third_green

    if rrg:
        if third.close < first.close:
            setups.append("GD_SHORT_RRG_weak_bounce")
        if third.close < first.open and third.high < first.high:
            setups.append("GD_SHORT_RRG_failed_reclaim")

    return setups


def simulate_short(entry_px: float, after_entry: pd.DataFrame, target_pct: float, stop_pct: float) -> dict[str, float | str]:
    target_px = entry_px * (1 - target_pct / 100.0)
    stop_px = entry_px * (1 + stop_pct / 100.0)

    lows = pd.to_numeric(after_entry["low"], errors="coerce")
    highs = pd.to_numeric(after_entry["high"], errors="coerce")
    closes = pd.to_numeric(after_entry["close"], errors="coerce")

    if after_entry.empty or not closes.notna().any():
        return {
            "net_pct": np.nan,
            "exit_type": "bad_bars",
            "short_eod_pct": np.nan,
            "short_max_runup_pct": np.nan,
            "short_max_drawdown_pct": np.nan,
        }

    eod_close = float(closes.dropna().iloc[-1])
    short_eod_pct = (entry_px / eod_close - 1.0) * 100.0 if eod_close else np.nan
    short_max_runup_pct = (entry_px / lows.min() - 1.0) * 100.0 if lows.notna().any() and lows.min() else np.nan
    short_max_drawdown_pct = (entry_px / highs.max() - 1.0) * 100.0 if highs.notna().any() and highs.max() else np.nan

    for _, bar in after_entry.iterrows():
        hit_target = float(bar["low"]) <= target_px
        hit_stop = float(bar["high"]) >= stop_px

        # Conservative same-bar rule.
        if hit_target and hit_stop:
            return {
                "net_pct": -stop_pct - COST_PCT,
                "exit_type": "stop_same_bar",
                "short_eod_pct": short_eod_pct,
                "short_max_runup_pct": short_max_runup_pct,
                "short_max_drawdown_pct": short_max_drawdown_pct,
            }

        if hit_stop:
            return {
                "net_pct": -stop_pct - COST_PCT,
                "exit_type": "stop",
                "short_eod_pct": short_eod_pct,
                "short_max_runup_pct": short_max_runup_pct,
                "short_max_drawdown_pct": short_max_drawdown_pct,
            }

        if hit_target:
            return {
                "net_pct": target_pct - COST_PCT,
                "exit_type": "target",
                "short_eod_pct": short_eod_pct,
                "short_max_runup_pct": short_max_runup_pct,
                "short_max_drawdown_pct": short_max_drawdown_pct,
            }

    return {
        "net_pct": short_eod_pct - COST_PCT if np.isfinite(short_eod_pct) else np.nan,
        "exit_type": "eod",
        "short_eod_pct": short_eod_pct,
        "short_max_runup_pct": short_max_runup_pct,
        "short_max_drawdown_pct": short_max_drawdown_pct,
    }


def summarize_perf(g: pd.DataFrame) -> pd.Series:
    return pd.Series(
        {
            "trades": len(g),
            "dates": g["trade_date"].nunique(),
            "tickers": g["ticker"].nunique(),
            "avg_net": g["net_pct"].mean(),
            "median_net": g["net_pct"].median(),
            "win_rate": (g["net_pct"] > 0).mean() * 100,
            "target_rate": (g["exit_type"].astype(str) == "target").mean() * 100,
            "stop_rate": g["exit_type"].astype(str).isin(["stop", "stop_same_bar"]).mean() * 100,
            "eod_rate": (g["exit_type"].astype(str) == "eod").mean() * 100,
            "median_gap": g["gap_pct"].median(),
            "median_prev_day_vol_metric": g["prev_day_volume_metric"].median(),
            "median_pm_vs_daily": g.get("premarket_dollar_vs_prior_daily_avg", pd.Series(np.nan)).median(),
            "median_first5_ret": g["first5_ret"].median(),
            "median_second5_ret": g["second5_ret"].median(),
            "median_third5_ret": g["third5_ret"].median(),
            "median_short_eod_raw": g["short_eod_pct"].median(),
            "median_short_runup_raw": g["short_max_runup_pct"].median(),
            "median_short_drawdown_raw": g["short_max_drawdown_pct"].median(),
            "median_fwd_1d_long": g.get("fwd_1d_close_pct", pd.Series(np.nan)).median(),
            "median_fwd_5d_long": g.get("fwd_5d_close_pct", pd.Series(np.nan)).median(),
            "best": g["net_pct"].max(),
            "worst": g["net_pct"].min(),
        }
    )


def main() -> None:
    args = parse_args()

    if not args.features.exists():
        raise FileNotFoundError(f"Missing features file: {args.features}")

    if not args.cache_dir.exists():
        raise FileNotFoundError(f"Missing minute-cache dir: {args.cache_dir}")

    args.out_dir.mkdir(parents=True, exist_ok=True)

    features = pd.read_csv(args.features)
    features["ticker"] = features["ticker"].astype(str).str.upper()
    features["trade_date"] = pd.to_datetime(features["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")

    if "prev_trade_date" not in features.columns:
        raise RuntimeError("features file must contain prev_trade_date to compute true previous-day volume.")

    features["prev_trade_date"] = pd.to_datetime(features["prev_trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    features["trade_dt"] = pd.to_datetime(features["trade_date"], errors="coerce")
    features["year"] = features["trade_dt"].dt.year

    for col in [
        "prev_close",
        "gap_pct",
        "premarket_dollar_vs_prior_daily_avg",
        "fwd_1d_close_pct",
        "fwd_5d_close_pct",
    ]:
        if col in features.columns:
            features[col] = pd.to_numeric(features[col], errors="coerce")

    features = attach_prev_day_volume_metric(features, args.daily_source, args.daily_source_root)
    features["prev_day_volume_grade"] = features["prev_day_volume_metric"].apply(volume_grade)
    features["gap_bucket"] = features["gap_pct"].apply(gap_bucket)
    features["period"] = features["year"].apply(lambda y: period_bucket(int(y)) if pd.notna(y) else "unknown")

    base = features[
        (features["prev_close"] >= 5)
        & (features["prev_close"] < 50)
        & (features["gap_pct"] >= -10)
        & (features["gap_pct"] < 0)
        & features["gap_bucket"].notna()
    ].copy()

    if args.max_rows:
        base = base.head(args.max_rows).copy()

    print("features:", args.features)
    print("minute cache:", args.cache_dir)
    print("input feature rows:", len(features))
    print("base $5-$50 gap-down rows:", len(base))

    print("building cache index...")
    cache_index = build_cache_index(args.cache_dir)
    print("indexed cache files:", len(cache_index))

    trades = []
    coverage = []

    for i, row in enumerate(base.itertuples(index=False), start=1):
        ticker = str(getattr(row, "ticker")).upper()
        trade_date = str(getattr(row, "trade_date"))
        year = int(getattr(row, "year")) if pd.notna(getattr(row, "year")) else np.nan

        cache_path = cache_index.get((ticker, trade_date))

        if cache_path is None:
            coverage.append(
                {
                    "ticker": ticker,
                    "trade_date": trade_date,
                    "year": year,
                    "status": "missing_cache",
                    "setup_count": 0,
                }
            )
            continue

        bars = read_minute_cache(cache_path)

        # Cache files contain the previous session plus the actual trade_date.
        # Keep only the actual trade_date before building 5-minute bars.
        if not bars.empty and "trade_date_et" in bars.columns:
            bars = bars[bars["trade_date_et"].astype(str) == trade_date].copy()

        first = get_window_bar(bars, "09:30", "09:35") if not bars.empty else None
        second = get_window_bar(bars, "09:35", "09:40") if not bars.empty else None
        third = get_window_bar(bars, "09:40", "09:45") if not bars.empty else None
        after_entry = regular_session_after_entry(bars) if not bars.empty else pd.DataFrame()

        if first is None or second is None or third is None or after_entry.empty:
            coverage.append(
                {
                    "ticker": ticker,
                    "trade_date": trade_date,
                    "year": year,
                    "status": "bad_bars",
                    "setup_count": 0,
                }
            )
            continue

        setups = identify_setups(first, second, third)

        coverage.append(
            {
                "ticker": ticker,
                "trade_date": trade_date,
                "year": year,
                "status": "ok",
                "setup_count": len(setups),
            }
        )

        if not setups:
            continue

        entry_px = third.close

        for setup in setups:
            for target_pct, stop_pct in TARGET_STOPS:
                sim = simulate_short(entry_px, after_entry, target_pct, stop_pct)

                record = {
                    "ticker": ticker,
                    "trade_date": trade_date,
                    "year": year,
                    "period": getattr(row, "period"),
                    "setup": setup,
                    "gap_bucket": getattr(row, "gap_bucket"),
                    "target_pct": target_pct,
                    "stop_pct": stop_pct,
                    "target_stop": f"{target_pct}/{stop_pct}",
                    "entry_px": entry_px,
                    "prev_close": getattr(row, "prev_close"),
                    "gap_pct": getattr(row, "gap_pct"),
                    "premarket_dollar_vs_prior_daily_avg": getattr(
                        row, "premarket_dollar_vs_prior_daily_avg", np.nan
                    ),
                    "prev_day_volume_metric": getattr(row, "prev_day_volume_metric"),
                    "prev_day_volume_grade": getattr(row, "prev_day_volume_grade"),
                    "first5_ret": first.ret_pct,
                    "second5_ret": second.ret_pct,
                    "third5_ret": third.ret_pct,
                    "first5_range": first.range_pct,
                    "second5_range": second.range_pct,
                    "third5_range": third.range_pct,
                    "first5_close_pos": first.close_pos,
                    "second5_close_pos": second.close_pos,
                    "third5_close_pos": third.close_pos,
                    "fwd_1d_close_pct": getattr(row, "fwd_1d_close_pct", np.nan),
                    "fwd_5d_close_pct": getattr(row, "fwd_5d_close_pct", np.nan),
                }
                record.update(sim)
                trades.append(record)

        if i % 1000 == 0:
            print(f"processed {i}/{len(base)} | trades={len(trades)}")

    trades_df = pd.DataFrame(trades)
    coverage_df = pd.DataFrame(coverage)

    trades_path = args.out_dir / "mid_5_to_50_gapdown_short_rrr_rgr_rrg_trades.csv"
    perf_path = args.out_dir / "mid_5_to_50_gapdown_short_rrr_rgr_rrg_train_test_performance.csv"
    best_path = args.out_dir / "mid_5_to_50_gapdown_short_rrr_rgr_rrg_best_rows.csv"
    focus_path = args.out_dir / "mid_5_to_50_gapdown_short_rrr_rgr_rrg_focus_candidates.csv"
    freq_path = args.out_dir / "mid_5_to_50_gapdown_short_rrr_rgr_rrg_trade_frequency.csv"
    recent_freq_path = args.out_dir / "mid_5_to_50_gapdown_short_rrr_rgr_rrg_recent_frequency.csv"
    yearly_freq_path = args.out_dir / "mid_5_to_50_gapdown_short_rrr_rgr_rrg_yearly_frequency.csv"
    hold_path = args.out_dir / "mid_5_to_50_gapdown_short_rrr_rgr_rrg_after_close_short_hold_summary.csv"
    coverage_path = args.out_dir / "mid_5_to_50_gapdown_short_rrr_rgr_rrg_data_coverage_by_year.csv"

    trades_df.to_csv(trades_path, index=False)

    coverage_summary = (
        coverage_df.groupby("year", observed=True)
        .agg(
            base_rows=("ticker", "size"),
            unique_dates=("trade_date", "nunique"),
            unique_tickers=("ticker", "nunique"),
            ok_rows=("status", lambda s: (s == "ok").sum()),
            missing_cache=("status", lambda s: (s == "missing_cache").sum()),
            bad_bars=("status", lambda s: (s == "bad_bars").sum()),
            rows_with_setup=("setup_count", lambda s: (s > 0).sum()),
        )
        .reset_index()
    )

    if trades_df.empty:
        coverage_summary.to_csv(coverage_path, index=False)
        print("No trades generated.")
        print("saved trades:", trades_path)
        print("saved coverage:", coverage_path)
        return

    trades_df["is_stop"] = trades_df["exit_type"].astype(str).isin(["stop", "stop_same_bar"])

    event_keys = ["ticker", "trade_date", "setup", "gap_bucket", "prev_day_volume_grade"]
    events = trades_df.sort_values(event_keys + ["target_stop"]).drop_duplicates(event_keys).copy()

    event_counts_by_year = events.groupby("year", observed=True).size().rename("events").reset_index()
    coverage_summary = coverage_summary.merge(event_counts_by_year, on="year", how="left")
    coverage_summary["events"] = coverage_summary["events"].fillna(0).astype(int)
    coverage_summary["events_per_ok_row"] = coverage_summary["events"] / coverage_summary["ok_rows"].replace(0, np.nan)
    coverage_summary.to_csv(coverage_path, index=False)

    perf = (
        trades_df.groupby(
            ["setup", "gap_bucket", "prev_day_volume_grade", "target_stop", "period"],
            observed=True,
        )
        .apply(summarize_perf)
        .reset_index()
    )

    overall_perf = (
        trades_df.groupby(
            ["setup", "gap_bucket", "prev_day_volume_grade", "target_stop"],
            observed=True,
        )
        .apply(summarize_perf)
        .reset_index()
    )
    overall_perf.insert(4, "period", "ALL")

    perf_out = pd.concat([perf, overall_perf], ignore_index=True)
    perf_out.to_csv(perf_path, index=False)

    best = perf_out[(perf_out["period"] == "ALL") & (perf_out["trades"] >= 5)].sort_values(
        ["median_net", "avg_net", "trades"], ascending=[False, False, False]
    )
    best.to_csv(best_path, index=False)

    focus = perf_out[
        perf_out["setup"].isin(SETUP_ORDER)
        & perf_out["gap_bucket"].isin(["gap_down_10_to_5", "gap_down_5_to_2"])
        & (perf_out["prev_day_volume_grade"] == "LOWER_le_2x")
    ].copy()
    focus.to_csv(focus_path, index=False)

    denominator_by_period = (
        coverage_df.assign(period=coverage_df["year"].apply(lambda y: period_bucket(int(y)) if pd.notna(y) else "unknown"))
        .drop_duplicates(["trade_date", "period"])
        .groupby("period", observed=True)["trade_date"]
        .nunique()
        .rename("sample_market_days")
        .reset_index()
    )

    freq = (
        events.groupby(["setup", "gap_bucket", "prev_day_volume_grade", "period"], observed=True)
        .agg(
            events=("ticker", "size"),
            signal_dates=("trade_date", "nunique"),
            tickers=("ticker", "nunique"),
        )
        .reset_index()
        .merge(denominator_by_period, on="period", how="left")
    )
    freq["annualized_events_per_252_days"] = freq["events"] / freq["sample_market_days"].replace(0, np.nan) * 252
    freq.to_csv(freq_path, index=False)

    yearly_denominator = (
        coverage_df.drop_duplicates(["year", "trade_date"])
        .groupby("year", observed=True)["trade_date"]
        .nunique()
        .rename("sample_market_days")
        .reset_index()
    )

    yearly_freq = (
        events.groupby(["setup", "gap_bucket", "prev_day_volume_grade", "year"], observed=True)
        .agg(events=("ticker", "size"), signal_dates=("trade_date", "nunique"), tickers=("ticker", "nunique"))
        .reset_index()
        .merge(yearly_denominator, on="year", how="left")
    )
    yearly_freq["annualized_events_per_252_days"] = yearly_freq["events"] / yearly_freq["sample_market_days"].replace(0, np.nan) * 252
    yearly_freq.to_csv(yearly_freq_path, index=False)

    recent_rows = []
    for label, start_year in [("2023_2026", 2023), ("2024_2026", 2024), ("2025_2026", 2025)]:
        ev = events[events["year"] >= start_year]
        den = coverage_df[coverage_df["year"] >= start_year]["trade_date"].nunique()

        if ev.empty:
            continue

        tmp = (
            ev.groupby(["setup", "gap_bucket", "prev_day_volume_grade"], observed=True)
            .agg(events=("ticker", "size"), signal_dates=("trade_date", "nunique"), tickers=("ticker", "nunique"))
            .reset_index()
        )
        tmp["recent_window"] = label
        tmp["sample_market_days"] = den
        tmp["annualized_events_per_252_days"] = tmp["events"] / tmp["sample_market_days"].replace(0, np.nan) * 252
        recent_rows.append(tmp)

    recent_freq = pd.concat(recent_rows, ignore_index=True) if recent_rows else pd.DataFrame()
    recent_freq.to_csv(recent_freq_path, index=False)

    hold = events.copy()
    hold["short_hold_1d_net"] = -pd.to_numeric(hold.get("fwd_1d_close_pct", np.nan), errors="coerce") - COST_PCT
    hold["short_hold_5d_net"] = -pd.to_numeric(hold.get("fwd_5d_close_pct", np.nan), errors="coerce") - COST_PCT

    hold_summary = (
        hold.groupby(["setup", "gap_bucket", "prev_day_volume_grade"], observed=True)
        .agg(
            events=("ticker", "size"),
            dates=("trade_date", "nunique"),
            tickers=("ticker", "nunique"),
            avg_short_1d_net=("short_hold_1d_net", "mean"),
            median_short_1d_net=("short_hold_1d_net", "median"),
            win_short_1d_rate=("short_hold_1d_net", lambda s: (s > 0).mean() * 100),
            avg_short_5d_net=("short_hold_5d_net", "mean"),
            median_short_5d_net=("short_hold_5d_net", "median"),
            win_short_5d_rate=("short_hold_5d_net", lambda s: (s > 0).mean() * 100),
            median_fwd_1d_long=("fwd_1d_close_pct", "median"),
            median_fwd_5d_long=("fwd_5d_close_pct", "median"),
        )
        .reset_index()
    )
    hold_summary.to_csv(hold_path, index=False)

    print("\n=== Data Coverage By Year ===")
    print(coverage_summary.to_string(index=False))

    print("\n=== Best Overall Rows trades>=5 ===")
    print(best.to_string(index=False))

    print("\n=== Focus Candidates ===")
    print(focus.sort_values(["setup", "gap_bucket", "target_stop", "period"]).to_string(index=False))

    print("\n=== Recent Frequency ===")
    if recent_freq.empty:
        print("No recent frequency rows.")
    else:
        print(
            recent_freq.sort_values(
                ["setup", "gap_bucket", "prev_day_volume_grade", "recent_window"]
            ).to_string(index=False)
        )

    print("\nsaved trades:", trades_path)
    print("saved performance:", perf_path)
    print("saved best:", best_path)
    print("saved focus:", focus_path)
    print("saved frequency:", freq_path)
    print("saved yearly frequency:", yearly_freq_path)
    print("saved recent frequency:", recent_freq_path)
    print("saved hold:", hold_path)
    print("saved coverage:", coverage_path)


if __name__ == "__main__":
    main()

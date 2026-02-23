# src/build_sector_rankings_gold.py
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from src.utils.config import load_config


def max_drawdown(prices: pd.Series) -> float:
    """
    Max drawdown over a window.
    Returns a negative number (example: -0.12 means -12%).
    """
    prices = prices.dropna()
    if prices.empty:
        return float("nan")

    peak = prices.cummax()
    drawdown = (prices / peak) - 1.0
    return float(drawdown.min())


def make_weekly_table(df: pd.DataFrame, resample_rule: str) -> pd.DataFrame:
    """
    Convert daily prices to weekly close prices (week ending Friday).
    """
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["symbol", "sector", "date", "adj_close"])

    weekly = (
        df[["symbol", "sector", "date", "adj_close"]]
        .set_index("date")
        .groupby(["symbol", "sector"])
        .resample(resample_rule)["adj_close"]
        .last()
        .reset_index()
        .rename(columns={"date": "week_end", "adj_close": "weekly_close"})
    )

    weekly = weekly.sort_values(["symbol", "week_end"])
    weekly["weekly_ret"] = weekly.groupby("symbol")["weekly_close"].pct_change()
    return weekly


def build_rankings(weekly: pd.DataFrame, lookback_weeks: int, weights: dict) -> pd.DataFrame:
    """
    Build metrics + score + rank per week.
    """
    w_ret = float(weights["return"])
    w_vol = float(weights["volatility"])
    w_dd = float(weights["drawdown"])

    # Lookback return (price change over N weeks)
    weekly[f"ret_{lookback_weeks}w"] = weekly.groupby("symbol")["weekly_close"].pct_change(lookback_weeks)

    # Volatility over last N weeks (std of weekly returns)
    weekly[f"vol_{lookback_weeks}w"] = (
        weekly.groupby("symbol")["weekly_ret"]
        .rolling(lookback_weeks)
        .std()
        .reset_index(level=0, drop=True)
    )

    # Drawdown over last N weeks
    weekly[f"dd_{lookback_weeks}w"] = (
        weekly.groupby("symbol")["weekly_close"]
        .rolling(lookback_weeks)
        .apply(max_drawdown, raw=False)
        .reset_index(level=0, drop=True)
    )

    ret_col = f"ret_{lookback_weeks}w"
    vol_col = f"vol_{lookback_weeks}w"
    dd_col = f"dd_{lookback_weeks}w"

    # Score (simple weighted sum)
    weekly["score"] = (w_ret * weekly[ret_col]) + (w_vol * weekly[vol_col]) + (w_dd * weekly[dd_col])

    # Rank within each week (1 = best)
    weekly["rank"] = weekly.groupby("week_end")["score"].rank(ascending=False, method="dense")

    # Keep only output columns
    out = weekly[["week_end", "sector", "symbol", "weekly_close", ret_col, vol_col, dd_col, "score", "rank"]]
    out = out.sort_values(["week_end", "rank"])
    return out


def write_latest_json(rankings: pd.DataFrame, out_json: Path, top_n: int, lookback_weeks: int) -> None:
    latest_week = rankings["week_end"].max()

    ret_col = f"ret_{lookback_weeks}w"
    vol_col = f"vol_{lookback_weeks}w"
    dd_col = f"dd_{lookback_weeks}w"

    top = rankings[(rankings["week_end"] == latest_week) & (rankings["rank"] <= top_n)].copy()
    top = top.sort_values("rank")

    payload = {
        "week_end": str(pd.to_datetime(latest_week).date()),
        "top_n": int(top_n),
        "sectors": [
            {
                "rank": float(r["rank"]),
                "sector": r["sector"],
                "symbol": r["symbol"],
                "score": float(r["score"]),
                ret_col: float(r[ret_col]),
                vol_col: float(r[vol_col]),
                dd_col: float(r[dd_col]),
            }
            for _, r in top.iterrows()
        ],
    }

    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(payload, indent=2))


def main() -> None:
    cfg = load_config("configs/config.yaml")

    # Paths from config
    silver_path = Path(cfg["paths"]["silver"]["sector_indexes"])
    gold_rankings_path = Path(cfg["paths"]["gold"]["weekly_sector_rankings"])
    latest_json_path = Path(cfg["paths"]["gold"]["latest_top_sectors_json"])

    # Parameters from config
    resample_rule = cfg["gold"]["sector_rankings"]["resample_rule"]
    lookback_weeks = int(cfg["gold"]["sector_rankings"]["lookback_weeks"])
    weights = cfg["gold"]["sector_rankings"]["score_weights"]
    top_n = int(cfg["gold"]["sector_rankings"]["top_n"])

    # Load silver data
    df = pd.read_parquet(silver_path)

    # Build weekly + rankings
    weekly = make_weekly_table(df, resample_rule=resample_rule)
    rankings = build_rankings(weekly, lookback_weeks=lookback_weeks, weights=weights)

    # Write outputs
    gold_rankings_path.parent.mkdir(parents=True, exist_ok=True)
    rankings.to_parquet(gold_rankings_path, index=False)
    write_latest_json(rankings, latest_json_path, top_n=top_n, lookback_weeks=lookback_weeks)

    # Print latest top N (nice feedback)
    latest_week = rankings["week_end"].max()
    ret_col = f"ret_{lookback_weeks}w"
    vol_col = f"vol_{lookback_weeks}w"
    dd_col = f"dd_{lookback_weeks}w"

    top = rankings[(rankings["week_end"] == latest_week) & (rankings["rank"] <= top_n)].copy()
    top = top.sort_values("rank")

    print(f"✅ Wrote Gold rankings: {gold_rankings_path}")
    print(f"✅ Wrote Latest Top {top_n} JSON: {latest_json_path}")
    print(f"Latest week_end: {pd.to_datetime(latest_week).date()}")
    print("\nTop sectors (latest week):")
    print(top[["rank", "sector", "symbol", ret_col, vol_col, dd_col, "score"]].to_string(index=False))


if __name__ == "__main__":
    main()
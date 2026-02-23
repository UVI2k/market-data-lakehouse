from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yfinance as yf

from src.utils.config import load_config


def fetch_etf_history(symbol: str, start: str, interval: str) -> pd.DataFrame:
    df = yf.download(
        tickers=symbol,
        start=start,
        interval=interval,
        auto_adjust=False,
        progress=False,
        group_by="column",
        threads=True,
    )

    if df is None or df.empty:
        return pd.DataFrame()

    # If yfinance returns MultiIndex columns, flatten them (('Open','XLK') -> 'Open')
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df = df.reset_index()
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]

    # Standardize expected schema
    if "date" not in df.columns and "datetime" in df.columns:
        df = df.rename(columns={"datetime": "date"})

    required = {"date", "close"}
    if not required.issubset(set(df.columns)):
        raise ValueError(f"{symbol}: missing required columns: {required - set(df.columns)}")

    for col in ["open", "high", "low", "adj_close", "volume"]:
        if col not in df.columns:
            df[col] = pd.NA

    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
    df.insert(0, "symbol", symbol)

    return df[["symbol", "date", "open", "high", "low", "close", "adj_close", "volume"]]


def main() -> None:
    # 1) Load config ONCE
    cfg = load_config("configs/config.yaml")

    # 2) Read only what this script needs
    symbols = cfg["ingestion"]["sector_indexes"]["symbols"]      # dict: { "XLK": "Information Technology", ... }
    start_date = cfg["ingestion"]["sector_indexes"]["start_date"]
    interval = cfg["ingestion"]["sector_indexes"]["interval"]
    bronze_base = cfg["paths"]["bronze"]["sector_indexes_raw"]   # e.g. data_lake/raw/sector_indexes

    # 3) Decide the run folder
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_dir = Path(bronze_base) / f"run_date={run_date}"
    out_dir.mkdir(parents=True, exist_ok=True)

    total_rows = 0
    success = 0
    failed = []

    for symbol, sector_name in symbols.items():
        try:
            df = fetch_etf_history(symbol=symbol, start=start_date, interval=interval)
            if df.empty:
                print(f"⚠️ {symbol}: no data returned")
                failed.append(symbol)
                continue

            df["sector"] = sector_name
            out_path = out_dir / f"{symbol}.parquet"
            df.to_parquet(out_path, index=False)

            print(f"✅ {symbol}: wrote {len(df):,} rows -> {out_path}")
            total_rows += len(df)
            success += 1

        except Exception as e:
            print(f"❌ {symbol}: failed due to {e}")
            failed.append(symbol)

    print("\n--- Summary ---")
    print(f"Run date (UTC): {run_date}")
    print(f"Success: {success}/{len(symbols)}")
    print(f"Total rows written: {total_rows:,}")
    if failed:
        print(f"Failed symbols: {failed}")


if __name__ == "__main__":
    main()
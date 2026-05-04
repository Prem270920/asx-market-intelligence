"""
ASX Market Intelligence Pipeline
Bronze Layer - Data Ingestion

Downloads ASX50 OHLCV data via yfinance and saves as Parquet files.

OHLCV = Open, High, Low, Close, Volume — standard stock market data format.
"""

import yfinance as yf
import pandas as pd
from pathlib import Path
from datetime import datetime
import logging
import sys

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger(__name__)


# .AX suffix = Australian Securities Exchange in Yahoo Finance
ASX50_TICKERS = [
    "BHP.AX", "CBA.AX", "CSL.AX", "NAB.AX", "WBC.AX",
    "ANZ.AX", "WES.AX", "MQG.AX", "TLS.AX", "WOW.AX",
    "RIO.AX", "FMG.AX", "GMG.AX", "REA.AX", "COL.AX",
    "AGL.AX", "APA.AX", "ASX.AX", "BXB.AX", "IAG.AX",
    "IEL.AX", "JHX.AX", "LLC.AX", "MPL.AX", "NEM.AX",
    "NST.AX", "ORG.AX", "ORI.AX", "QAN.AX", "QBE.AX",
    "RMD.AX", "S32.AX", "SCG.AX", "SEK.AX", "SHL.AX",
    "STO.AX", "SUN.AX", "TCL.AX", "TWE.AX", "VCX.AX",
    "WDS.AX", "WPR.AX", "XRO.AX", "ALL.AX", "ALD.AX",
    "AMC.AX", "AMP.AX", "MIN.AX", "AZJ.AX", "BOQ.AX",
]

OUTPUT_DIR = Path("data/bronze/raw")
START_DATE = "2022-01-01"
END_DATE   = datetime.today().strftime("%Y-%m-%d")

def fetch_ticker(ticker: str) -> pd.DataFrame | None:
    """Download OHLCV data for one ticker. Returns None if it fails."""
    try:
        df = yf.download(ticker, start=START_DATE, end=END_DATE,
                         auto_adjust=True, progress=False)
        if df.empty:
            log.warning(f"  No data returned for {ticker}")
            return None

        # yfinance sometimes returns a MultiIndex — flatten it
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        df = df.reset_index()
        df["ticker"] = ticker.replace(".AX", "")  # store clean name e.g. "BHP"
        log.info(f"  {ticker}: {len(df)} rows")
        return df

    except Exception as e:
        log.error(f"  Failed {ticker}: {e}")
        return None
    
def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    log.info(f" Fetching ASX50: {START_DATE} → {END_DATE} ")

    success, failed = [], []

    for ticker in ASX50_TICKERS:
        log.info(f"Fetching {ticker}...")
        df = fetch_ticker(ticker)
        if df is not None:
            # Save as Parquet — faster and smaller than CSV
            out_path = OUTPUT_DIR / f"{ticker.replace('.AX', '')}.parquet"
            df.to_parquet(out_path, index=False, engine="pyarrow")
            log.info(f"  Saved → {out_path}")
            success.append(ticker)
        else:
            failed.append(ticker)

    log.info(f"\n Done: {len(success)} succeeded, {len(failed)} failed ")
    if failed:
        log.warning(f"Failed tickers: {failed}")

if __name__ == "__main__":
    main()

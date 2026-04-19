#!/usr/bin/env python3
"""
NSE Stock Screener - F&O Segment
Place this file in the root directory of your repository
"""

import sys
import time
import logging
import argparse
import pandas as pd
import numpy as np
import yfinance as yf
from typing import List, Dict, Tuple, Optional
import os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from io import BytesIO

# ============= CONFIGURATION =============
# Run mode - set via environment variable
RUN_MODE = os.environ.get("RUN_MODE", "local")  # 'cloud' or 'local'
SAVE_LOCAL_FILES = (RUN_MODE == "local")

# Telegram configuration
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# Screening parameters
ROUND_PRECI = 2
LOOKBACK_WEEKS = 15
THRESHOLD = 0.01
ODD_SQUARE_TOLERANCE = 0.5
RSI_PERIOD = 14
MAX_WORKERS = 5  # Reduced for GitHub Actions
BATCH_SIZE = 25  # Reduced for GitHub Actions

# Optional nselib import
try:
    from nselib import capital_market
    NSELIB_AVAILABLE = True
except ImportError:
    NSELIB_AVAILABLE = False

# ============= LOGGING =============
import io as _io
_stream_handler = logging.StreamHandler(
    stream=_io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    if hasattr(sys.stdout, "buffer") else sys.stdout
)
_stream_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        _stream_handler,
        logging.FileHandler("screener.log", mode="a", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# Fallback F&O list
FALLBACK_FNO_STOCKS = [
    "RELIANCE", "HDFCBANK", "ICICIBANK", "INFY", "TCS",
    "SBIN", "LT", "HINDUNILVR", "BHARTIARTL", "KOTAKBANK",
    "ITC", "AXISBANK", "BAJFINANCE", "WIPRO", "TATAMOTORS",
]

SECTOR_MAPPING: Dict[str, str] = {
    "HDFCBANK": "Banking", "ICICIBANK": "Banking", "SBIN": "Banking",
    "RELIANCE": "Energy", "INFY": "IT", "TCS": "IT",
    "TATAMOTORS": "Automobile", "BHARTIARTL": "Telecom",
    # Add more mappings as needed
}

# Pre-computed odd squares
ODD_SQUARES: List[Tuple[int, int]] = [(n, n * n) for n in range(1, 301, 2)]

# ============= HELPER FUNCTIONS =============

def scalar(val) -> float:
    """Safely extract a Python float from any numpy scalar."""
    try:
        if val is None:
            return 0.0
        if np.isscalar(val):
            return float(val)
        arr = np.asarray(val)
        if arr.size == 0:
            return 0.0
        return float(arr.flatten()[0])
    except Exception:
        return 0.0

def extract_close_series(df: pd.DataFrame) -> Optional[pd.Series]:
    """Extract closing prices from yfinance DataFrame."""
    if df is None or df.empty:
        return None
    
    if 'Close' in df.columns:
        series = df['Close'].dropna()
    elif 'Adj Close' in df.columns:
        series = df['Adj Close'].dropna()
    else:
        # Try to find any price column
        price_cols = [col for col in df.columns if 'close' in col.lower() or 'adj' in col.lower()]
        if price_cols:
            series = df[price_cols[0]].dropna()
        else:
            return None
    
    if isinstance(series, pd.DataFrame):
        series = series.iloc[:, 0]
    
    return series.astype(float)

def extract_open_series(df: pd.DataFrame) -> Optional[pd.Series]:
    """Extract opening prices from yfinance DataFrame."""
    if df is None or df.empty:
        return None
    
    if 'Open' in df.columns:
        series = df['Open'].dropna()
    else:
        open_cols = [col for col in df.columns if 'open' in col.lower()]
        if open_cols:
            series = df[open_cols[0]].dropna()
        else:
            return None
    
    if isinstance(series, pd.DataFrame):
        series = series.iloc[:, 0]
    
    return series.astype(float)

def calculate_rsi(df: pd.DataFrame, period: int = 14) -> Optional[float]:
    """Calculate RSI using Wilder's smoothing."""
    closes = extract_close_series(df)
    if closes is None or len(closes) < period * 2:
        return None
    
    delta = closes.diff().dropna()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    
    avg_gain = gain.ewm(com=period - 1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period).mean()
    
    last_gain = scalar(avg_gain.iloc[-1])
    last_loss = scalar(avg_loss.iloc[-1])
    
    if last_loss == 0:
        return 100.0
    if last_gain == 0:
        return 0.0
    
    rs = last_gain / last_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return round(rsi, 2)

def calculate_volatility_levels(df: pd.DataFrame) -> Optional[Tuple[float, float, float, float]]:
    """Calculate volatility levels."""
    closes = extract_close_series(df)
    opens = extract_open_series(df)
    
    if closes is None or opens is None or len(closes) < LOOKBACK_WEEKS + 2:
        return None
    
    try:
        dc = scalar(closes.iloc[-2])
        dc_list = closes.iloc[-(LOOKBACK_WEEKS + 1):-1].values
        dc_list = np.asarray(dc_list).flatten()
        
        if len(dc_list) < 2:
            return None
        
        log_returns = np.log(dc_list[1:] / dc_list[:-1])
        volatility = float(np.sqrt(np.var(log_returns)))
        
        dop = scalar(opens.iloc[-1])
        range1 = round(dc * volatility, ROUND_PRECI)
        doKdc = dop if abs(dop - dc) > (0.382 * range1) else dc
        
        adj_list = np.append(dc_list[:-1], doKdc)
        log_returns = np.log(adj_list[1:] / adj_list[:-1])
        volatility = float(np.sqrt(np.var(log_returns)))
        range1 = round(doKdc * volatility, ROUND_PRECI)
        
        buy_above = round(doKdc + range1 * 0.236, ROUND_PRECI)
        buy_confirm = round(doKdc + range1 * 0.382, ROUND_PRECI)
        sell_below = round(doKdc - range1 * 0.236, ROUND_PRECI)
        sell_confirm = round(doKdc - range1 * 0.382, ROUND_PRECI)
        
        return buy_above, buy_confirm, sell_below, sell_confirm
    except Exception as e:
        log.debug(f"Error calculating volatility levels: {e}")
        return None

def is_near_level(price: float, level: float, threshold: float = 0.01) -> bool:
    """Check if price is near a level."""
    if level == 0:
        return False
    return abs(price - level) / level <= threshold

def is_price_within_tolerance(price: float, target: float, pct: float = 0.5) -> bool:
    """Check if price is within percentage tolerance of target."""
    if price <= 0 or target <= 0:
        return False
    lo = target * (1 - pct / 100)
    hi = target * (1 + pct / 100)
    return lo <= price <= hi

# ============= SCREENER FUNCTIONS =============

def find_odd_square_matches(stock_data: Dict, tolerance: float = 0.5) -> Dict:
    """Find stocks near odd squares."""
    matches = {}
    for symbol, data in stock_data.items():
        price = data["price"]
        for odd_num, square in ODD_SQUARES:
            if is_price_within_tolerance(price, square, tolerance):
                matches[symbol] = {
                    "price": price,
                    "odd_number": odd_num,
                    "odd_square": square,
                    "deviation_percent": round(abs(price - square) / square * 100, 2),
                    "levels": data["levels"],
                    "weekly_rsi": data.get("weekly_rsi"),
                    "daily_rsi": data.get("daily_rsi"),
                    "sector": data.get("sector", "Other"),
                }
                break
    return matches

def find_level_matches(stock_data: Dict, threshold: float = 0.01) -> Dict:
    """Find stocks near volatility levels."""
    matches = {}
    for symbol, data in stock_data.items():
        if not data["has_levels"]:
            continue
        price = data["price"]
        buy_above, buy_confirm, sell_below, sell_confirm = data["levels"]
        
        level_hits = []
        if is_near_level(price, buy_above, threshold):
            level_hits.append(("BUY_ABOVE", buy_above))
        if is_near_level(price, buy_confirm, threshold):
            level_hits.append(("BUY_CONFIRM", buy_confirm))
        if is_near_level(price, sell_below, threshold):
            level_hits.append(("SELL_BELOW", sell_below))
        if is_near_level(price, sell_confirm, threshold):
            level_hits.append(("SELL_CONFIRM", sell_confirm))
        
        if level_hits:
            matches[symbol] = {
                "price": price,
                "levels": data["levels"],
                "matched_levels": level_hits,
                "deviation": min(abs(price - lv[1]) / lv[1] * 100 for lv in level_hits),
                "weekly_rsi": data.get("weekly_rsi"),
                "daily_rsi": data.get("daily_rsi"),
                "sector": data.get("sector", "Other"),
            }
    return matches

# ============= DATA FETCHER =============

class BatchDataFetcher:
    def __init__(self, max_workers: int = 5):
        self.max_workers = max_workers
    
    def fetch_single_stock(self, symbol: str) -> Tuple[str, Optional[Dict]]:
        try:
            yf_symbol = f"{symbol}.NS"
            
            # Weekly data
            df_week = yf.download(yf_symbol, period="1y", interval="1wk", 
                                  progress=False, auto_adjust=True, threads=False)
            if df_week.empty:
                return symbol, None
            
            closes_week = extract_close_series(df_week)
            if closes_week is None or closes_week.empty:
                return symbol, None
            
            current_price = scalar(closes_week.iloc[-1])
            if current_price <= 0:
                return symbol, None
            
            weekly_rsi = calculate_rsi(df_week)
            levels = calculate_volatility_levels(df_week)
            
            # Daily data for RSI
            df_day = yf.download(yf_symbol, period="6mo", interval="1d",
                                 progress=False, auto_adjust=True, threads=False)
            daily_rsi = calculate_rsi(df_day) if not df_day.empty else None
            
            sector = SECTOR_MAPPING.get(symbol, "Other")
            
            return symbol, {
                "price": current_price,
                "levels": levels,
                "has_levels": levels is not None,
                "weekly_rsi": weekly_rsi,
                "daily_rsi": daily_rsi,
                "sector": sector,
            }
        except Exception as e:
            log.debug(f"Error fetching {symbol}: {e}")
            return symbol, None
    
    def fetch_batch(self, symbols: List[str], batch_label: str = "") -> Dict[str, Dict]:
        stock_data = {}
        total = len(symbols)
        log.info(f"Fetching batch {batch_label} - {total} stocks")
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self.fetch_single_stock, s): s for s in symbols}
            completed = 0
            for future in as_completed(futures):
                symbol = futures[future]
                completed += 1
                try:
                    sym, data = future.result(timeout=30)
                    if data:
                        stock_data[sym] = data
                        log.info(f"  OK {sym:15} ({completed}/{total}) Rs {data['price']:.2f}")
                    else:
                        log.info(f"  -- {sym:15} ({completed}/{total}) no data")
                except Exception as e:
                    log.error(f"  !! {symbol} -- {e}")
        return stock_data

# ============= TELEGRAM FUNCTIONS =============

def send_telegram_message(text: str) -> bool:
    """Send message to Telegram."""
    if not BOT_TOKEN or not CHAT_ID:
        log.warning("Telegram not configured")
        return False
    
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(url, json={"chat_id": CHAT_ID, "text": text}, timeout=30)
        return resp.ok
    except Exception as e:
        log.error(f"Telegram error: {e}")
        return False

def send_telegram_file_bytes(file_bytes: bytes, filename: str, caption: str = "") -> bool:
    """Send file from bytes to Telegram."""
    if not BOT_TOKEN or not CHAT_ID:
        return False
    
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument"
    try:
        files = {"document": (filename, BytesIO(file_bytes), 
                              "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")}
        data = {"chat_id": CHAT_ID, "caption": caption[:1024]}
        resp = requests.post(url, data=data, files=files, timeout=60)
        return resp.ok
    except Exception as e:
        log.error(f"Telegram upload error: {e}")
        return False

def dataframe_to_excel_bytes(df: pd.DataFrame, sheet_name: str) -> bytes:
    """Convert DataFrame to Excel bytes."""
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
    return output.getvalue()

# ============= MAIN FUNCTION =============

def get_fno_stocks() -> List[str]:
    """Get list of F&O stocks."""
    if NSELIB_AVAILABLE:
        try:
            fno_data = capital_market.fno_equity_list()
            if not fno_data.empty:
                col = next((c for c in fno_data.columns if c.upper() == "SYMBOL"), None)
                if col:
                    return fno_data[col].tolist()
        except Exception as e:
            log.warning(f"nselib failed: {e}")
    
    log.info(f"Using fallback list of {len(FALLBACK_FNO_STOCKS)} stocks")
    return FALLBACK_FNO_STOCKS

def main(dry_run: bool = False, override_symbols: Optional[List[str]] = None):
    print("=" * 80)
    print(f"NSE STOCK SCREENER - {RUN_MODE.upper()} MODE")
    print("=" * 80)
    
    # Get stock list
    if override_symbols:
        symbols = override_symbols
    else:
        symbols = get_fno_stocks()
    
    if dry_run:
        symbols = symbols[:10]
        print(f"DRY RUN: Limiting to {len(symbols)} stocks")
    
    print(f"Processing {len(symbols)} stocks...")
    
    # Fetch data
    fetcher = BatchDataFetcher(max_workers=MAX_WORKERS)
    all_data = fetcher.fetch_batch(symbols)
    
    if not all_data:
        log.error("No data retrieved")
        return
    
    # Find matches
    odd_matches = find_odd_square_matches(all_data, ODD_SQUARE_TOLERANCE)
    level_matches = find_level_matches(all_data, THRESHOLD)
    both_matches = set(odd_matches) & set(level_matches)
    
    # Print summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"  Stocks screened: {len(symbols)}")
    print(f"  Data retrieved: {len(all_data)}")
    print(f"  Odd Square matches: {len(odd_matches)}")
    print(f"  Level matches: {len(level_matches)}")
    print(f"  Both criteria: {len(both_matches)}")
    
    # Send Telegram summary
    now = datetime.now().strftime("%d %b %Y %H:%M")
    summary = f"NSE Screener {now}\n"
    summary += f"Odd Square: {len(odd_matches)}\n"
    summary += f"Level matches: {len(level_matches)}\n"
    summary += f"Both criteria: {len(both_matches)}"
    
    send_telegram_message(summary)
    
    # Create and send Excel files
    if odd_matches:
        rows = [{"Symbol": s, "Price": d["price"], "Odd Square": d["odd_square"], 
                 "Deviation %": d["deviation_percent"]} 
                for s, d in odd_matches.items()]
        df = pd.DataFrame(rows)
        bytes_data = dataframe_to_excel_bytes(df, "Odd Square Matches")
        send_telegram_file_bytes(bytes_data, f"odd_square_{now.replace(' ', '_')}.xlsx",
                                f"Odd Square: {len(odd_matches)} stocks")
    
    if level_matches:
        rows = [{"Symbol": s, "Price": d["price"], "Matched Levels": d["matched_levels"][0][0]} 
                for s, d in level_matches.items()]
        df = pd.DataFrame(rows)
        bytes_data = dataframe_to_excel_bytes(df, "Level Matches")
        send_telegram_file_bytes(bytes_data, f"level_matches_{now.replace(' ', '_')}.xlsx",
                                f"Level matches: {len(level_matches)} stocks")
    
    print("\n✅ Done!")
    print("=" * 80)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NSE Stock Screener")
    parser.add_argument("--dry-run", action="store_true", help="Test with fewer stocks")
    parser.add_argument("--symbols", nargs="+", help="Specific symbols to test")
    args = parser.parse_args()
    
    main(dry_run=args.dry_run, override_symbols=args.symbols)

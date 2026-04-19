#!/usr/bin/env python3
"""
NSE Stock Screener - F&O Segment
Odd Square Pattern + Volatility Levels + RSI Analysis
Supports both local and cloud (GitHub Actions) execution
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

# Try to import settings - fall back to environment variables if not found
try:
    from config.settings import (
        RUN_MODE, SAVE_LOCAL_FILES, BOT_TOKEN, CHAT_ID,
        ROUND_PRECI, LOOKBACK_WEEKS, THRESHOLD, 
        ODD_SQUARE_TOLERANCE, RSI_PERIOD, MAX_WORKERS, BATCH_SIZE
    )
except ImportError:
    # Fallback to environment variables
    RUN_MODE = os.environ.get("RUN_MODE", "local")
    SAVE_LOCAL_FILES = (RUN_MODE == "local")
    BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
    ROUND_PRECI = 2
    LOOKBACK_WEEKS = 15
    THRESHOLD = 0.01
    ODD_SQUARE_TOLERANCE = 0.5
    RSI_PERIOD = 14
    MAX_WORKERS = 10
    BATCH_SIZE = 50

# Optional nselib import
try:
    from nselib import capital_market
    NSELIB_AVAILABLE = True
except ImportError:
    NSELIB_AVAILABLE = False

# ============= LOGGING =============
# Force UTF-8 on the stream handler so Windows cp1252 consoles don't choke
# on Indian Rupee signs, tick marks, or any other non-ASCII characters.
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

# Fallback F&O list used when nselib is unavailable
FALLBACK_FNO_STOCKS = [
    "RELIANCE", "HDFCBANK", "ICICIBANK", "INFY", "TCS",
    "SBIN", "LT", "HINDUNILVR", "BHARTIARTL", "KOTAKBANK",
    "ITC", "AXISBANK", "BAJFINANCE", "WIPRO", "TATAMOTORS",
]

SECTOR_MAPPING: Dict[str, str] = {
    "HDFCBANK": "Banking",     "ICICIBANK": "Banking",   "SBIN": "Banking",
    "KOTAKBANK": "Banking",    "AXISBANK": "Banking",    "INDUSINDBK": "Banking",
    "YESBANK": "Banking",      "BANKBARODA": "Banking",  "PNB": "Banking",
    "BAJFINANCE": "Financial Services", "BAJFINSERV": "Financial Services",
    "HDFC": "Financial Services",
    "POWERGRID": "Power",      "NTPC": "Power",
    "INFY": "IT",   "TCS": "IT",      "WIPRO": "IT",
    "HCLTECH": "IT","TECHM": "IT",    "LTIM": "IT",
    "MPHASIS": "IT","COFORGE": "IT",
    "RELIANCE": "Energy",  "ONGC": "Energy",    "BPCL": "Energy",
    "IOC": "Energy",       "HINDPETRO": "Energy","GAIL": "Energy",
    "TATAMOTORS": "Automobile","MARUTI": "Automobile","M&M": "Automobile",
    "BAJAJ-AUTO": "Automobile","HEROMOTOCO": "Automobile","EICHERMOT": "Automobile",
    "TATASTEEL": "Metals", "JSWSTEEL": "Metals","HINDALCO": "Metals",
    "VEDL": "Metals",      "NATIONALUM": "Metals","COALINDIA": "Mining",
    "SUNPHARMA": "Pharmaceuticals","DRREDDY": "Pharmaceuticals",
    "CIPLA": "Pharmaceuticals",   "DIVISLAB": "Pharmaceuticals",
    "HINDUNILVR": "FMCG","ITC": "FMCG","NESTLEIND": "FMCG",
    "BRITANNIA": "FMCG",  "DABUR": "FMCG","GODREJCP": "FMCG",
    "BHARTIARTL": "Telecom","JIOFIN": "Telecom","IDEA": "Telecom",
    "LT": "Construction",  "ULTRACEMCO": "Cement","GRASIM": "Cement",
    "AMBUJACEM": "Cement", "ACC": "Cement",
    "ASIANPAINT": "Paints","TITAN": "Retail",
    "HDFCLIFE": "Insurance","SBILIFE": "Insurance","ICICIPRULI": "Insurance",
    "HINDZINC": "Metals",
}

# Pre-computed at module level — no need to regenerate each run
ODD_SQUARES: List[Tuple[int, int]] = [
    (n, n * n) for n in range(1, 301, 2)
]


# ============= HELPERS =============

def scalar(val) -> float:
    """
    Safely extract a Python float from any numpy scalar, array, or Series.
    Handles 0D, 1D, and multi-dimensional arrays properly.
    """
    try:
        # If it's None or NaN
        if val is None or (hasattr(val, '__len__') and len(val) == 0):
            return 0.0
            
        # If it's already a scalar/number
        if np.isscalar(val):
            return float(val)
        
        # Convert to numpy array if not already
        arr = np.asarray(val)
        
        # Handle empty arrays
        if arr.size == 0:
            return 0.0
        
        # Flatten to 1D and take first element
        flat_arr = arr.flatten()
        return float(flat_arr[0])
    
    except Exception as e:
        log.warning(f"scalar() failed for {type(val)}: {e}")
        return 0.0


def extract_close_series(df: pd.DataFrame) -> Optional[pd.Series]:
    """
    Return a clean 1-D float Series of closing prices from a yfinance DataFrame.

    Handles three layouts produced by different yfinance versions:
      - MultiIndex ('Close', 'RELIANCE.NS')  — yfinance >= 0.2.x, auto_adjust=True
      - MultiIndex ('Adj Close', ticker)     — yfinance >= 0.2.x, auto_adjust=False
      - Flat 'Close' column                  — older yfinance / auto_adjust=True
    Column matching is case-insensitive to guard against yfinance inconsistencies.
    """
    if df is None or df.empty:
        return None

    cols = df.columns
    series = None

    if isinstance(cols, pd.MultiIndex):
        # Normalise level-0 names to title-case for robust matching
        level0 = [str(c[0]).strip() for c in cols]
        # Try exact 'Close' first, then 'Adj Close' as fallback
        for target in ("Close", "Adj Close"):
            matches = [cols[i] for i, name in enumerate(level0)
                       if name.lower() == target.lower()]
            if matches:
                series = df[matches[0]].dropna()
                break
    else:
        # Flat columns — case-insensitive search
        col_map = {str(c).lower(): c for c in cols}
        key = col_map.get("close") or col_map.get("adj close")
        if key is not None:
            series = df[key].dropna()

    if series is None:
        return None
    
    # Ensure we have a 1D Series
    if isinstance(series, pd.DataFrame):
        # Take the first column if it's a DataFrame
        series = series.iloc[:, 0]
    
    # Ensure it's a Series and flatten if needed
    if isinstance(series, pd.Series):
        # Remove any nested structures
        try:
            series = series.apply(lambda x: scalar(x))
        except Exception as e:
            log.debug(f"Error applying scalar to series: {e}")
            return None
    else:
        return None
    
    return series.astype(float)


def extract_open_series(df: pd.DataFrame) -> Optional[pd.Series]:
    """Same case-insensitive extraction logic as extract_close_series but for 'Open'."""
    if df is None or df.empty:
        return None
    cols = df.columns
    series = None
    
    if isinstance(cols, pd.MultiIndex):
        level0 = [str(c[0]).strip() for c in cols]
        matches = [cols[i] for i, name in enumerate(level0)
                   if name.lower() == "open"]
        if matches:
            series = df[matches[0]].dropna()
    else:
        col_map = {str(c).lower(): c for c in cols}
        key = col_map.get("open")
        if key is not None:
            series = df[key].dropna()
            
    if series is None:
        return None
        
    if isinstance(series, pd.DataFrame):
        series = series.iloc[:, 0]
        
    if isinstance(series, pd.Series):
        try:
            series = series.apply(lambda x: scalar(x))
        except Exception:
            return None
    else:
        return None
        
    return series.astype(float)


def is_data_stale(df: pd.DataFrame, max_days: int = 10) -> bool:
    """Return True if the most recent bar is older than max_days calendar days."""
    if df is None or df.empty:
        return True
    last_ts = df.index[-1]
    if hasattr(last_ts, "date"):
        last_ts = last_ts.date()
    return (datetime.now().date() - last_ts) > timedelta(days=max_days)


def autofit_worksheet_columns(worksheet) -> None:
    """Auto-size every column in an openpyxl worksheet to its content width."""
    for col in worksheet.columns:
        max_len = max(
            (len(str(cell.value)) for cell in col if cell.value is not None),
            default=0,
        )
        worksheet.column_dimensions[col[0].column_letter].width = min(max_len + 2, 50)


def sanitize_sheet_name(name: str) -> str:
    """Strip characters that are invalid in Excel sheet names and cap at 31 chars."""
    for ch in r":\/?\*[]":
        name = name.replace(ch, "")
    return name[:31]


def is_near_level(price: float, level: float, threshold: float = THRESHOLD) -> bool:
    if level == 0:
        return False
    return abs(price - level) / level <= threshold


def is_price_within_tolerance(price: float, target: float, pct: float = 1.0) -> bool:
    if price <= 0 or target <= 0:
        return False
    lo = target * (1 - pct / 100)
    hi = target * (1 + pct / 100)
    return lo <= price <= hi


# ============= RSI =============

def calculate_rsi(df: pd.DataFrame, period: int = RSI_PERIOD) -> Optional[float]:
    """
    Compute RSI using Wilder's smoothed moving average. Works on any timeframe
    (daily, weekly) — the caller is responsible for passing the right DataFrame.

      - Handles yfinance MultiIndex and flat column layouts (case-insensitive).
      - Uses EMA with com=(period-1) for Wilder smoothing — matches TradingView/Bloomberg.
      - Requires at least period*2 bars for a stable reading.
      - Guards against divide-by-zero (all-gain -> 100.0, all-loss -> 0.0).
    """
    closes = extract_close_series(df)
    if closes is None:
        log.debug("RSI: extract_close_series returned None")
        return None
    if len(closes) < period * 2:
        log.debug("RSI: only %d bars available, need %d", len(closes), period * 2)
        return None

    delta = closes.diff().dropna()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)

    # Wilder's smoothing: alpha = 1/period  =>  com = period - 1
    avg_gain = gain.ewm(com=period - 1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period).mean()

    # Extract scalar values safely
    try:
        last_gain = scalar(avg_gain.iloc[-1])
        last_loss = scalar(avg_loss.iloc[-1])
    except (ValueError, IndexError, TypeError) as e:
        log.debug(f"RSI extraction failed: {e}")
        return None

    if last_loss == 0:
        return 100.0
    if last_gain == 0:
        return 0.0

    rs = last_gain / last_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return round(rsi, 2)


def calculate_weekly_rsi(df: pd.DataFrame, period: int = RSI_PERIOD) -> Optional[float]:
    """Alias kept for backward compatibility — delegates to calculate_rsi."""
    return calculate_rsi(df, period)


# ============= VOLATILITY LEVELS =============

def calculate_volatility_levels(
    df: pd.DataFrame,
) -> Optional[Tuple[float, float, float, float]]:
    """
    Calculate Fibonacci-scaled volatility buy/sell levels.
    Returns (buy_above, buy_confirm, sell_below, sell_confirm) or None.
    """
    closes = extract_close_series(df)
    opens = extract_open_series(df)

    if closes is None or opens is None:
        return None
    if len(closes) < LOOKBACK_WEEKS + 2:
        return None

    try:
        dc = scalar(closes.iloc[-2])
        dc_list = closes.iloc[-(LOOKBACK_WEEKS + 1):-1].values

        if len(dc_list) < 2:
            return None

        # Ensure dc_list is flat
        dc_list = np.asarray(dc_list).flatten()
        
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


# ============= SCREENER LOGIC =============

def find_odd_square_matches(
    stock_data: Dict, tolerance: float = ODD_SQUARE_TOLERANCE
) -> Dict:
    matches = {}
    for symbol, data in stock_data.items():
        price = data["price"]
        for odd_num, square in ODD_SQUARES:
            if is_price_within_tolerance(price, square, tolerance):
                matches[symbol] = {
                    "price":            price,
                    "odd_number":       odd_num,
                    "odd_square":       square,
                    "deviation_percent": round(abs(price - square) / square * 100, 2),
                    "levels":           data["levels"],
                    "weekly_rsi":       data.get("weekly_rsi"),
                    "daily_rsi":        data.get("daily_rsi"),
                    "sector":           data.get("sector", "Other"),
                }
                break
    return matches


def find_level_matches(stock_data: Dict, threshold: float = THRESHOLD) -> Dict:
    matches = {}
    for symbol, data in stock_data.items():
        if not data["has_levels"]:
            continue
        price = data["price"]
        buy_above, buy_confirm, sell_below, sell_confirm = data["levels"]

        level_hits = []
        if is_near_level(price, buy_above,    threshold): level_hits.append(("BUY_ABOVE",    buy_above))
        if is_near_level(price, buy_confirm,  threshold): level_hits.append(("BUY_CONFIRM",  buy_confirm))
        if is_near_level(price, sell_below,   threshold): level_hits.append(("SELL_BELOW",   sell_below))
        if is_near_level(price, sell_confirm, threshold): level_hits.append(("SELL_CONFIRM", sell_confirm))

        if level_hits:
            matches[symbol] = {
                "price":          price,
                "levels":         data["levels"],
                "matched_levels": level_hits,
                "deviation":      min(abs(price - lv[1]) / lv[1] * 100 for lv in level_hits),
                "weekly_rsi":     data.get("weekly_rsi"),
                "daily_rsi":      data.get("daily_rsi"),
                "sector":         data.get("sector", "Other"),
            }
    return matches


# ============= BATCH DATA FETCHER =============

class BatchDataFetcher:
    """Fetches and preprocesses NSE stock data in parallel."""

    def __init__(self, max_workers: int = MAX_WORKERS):
        self.max_workers = max_workers

    def fetch_single_stock(self, symbol: str) -> Tuple[str, Optional[Dict]]:
        try:
            yf_symbol = f"{symbol}.NS"

            # ── Weekly data (1y) — used for RSI, volatility levels, current price ──
            df_week = yf.download(
                yf_symbol,
                period="1y",        # ~52 weekly bars — enough for Wilder RSI warmup
                interval="1wk",
                progress=False,
                auto_adjust=True,
                threads=False,
            )

            if df_week.empty:
                return symbol, None

            # Staleness guard — skip if last weekly bar is more than 10 days old
            if is_data_stale(df_week):
                log.warning("%s: weekly data appears stale, skipping", symbol)
                return symbol, None

            closes_week = extract_close_series(df_week)
            if closes_week is None or closes_week.empty:
                return symbol, None
            
            # Ensure we get a scalar value
            current_price = scalar(closes_week.iloc[-1])
            
            if current_price <= 0:
                return symbol, None

            weekly_rsi = calculate_weekly_rsi(df_week)
            levels = calculate_volatility_levels(df_week)

            # ── Daily data (6mo) — used only for Daily RSI ──
            df_day = yf.download(
                yf_symbol,
                period="6mo",
                interval="1d",
                progress=False,
                auto_adjust=True,
                threads=False,
            )
            daily_rsi = calculate_rsi(df_day) if not df_day.empty else None

            sector = SECTOR_MAPPING.get(symbol, "Other")

            return symbol, {
                "price":      current_price,
                "levels":     levels,
                "has_levels": levels is not None,
                "weekly_rsi": weekly_rsi,
                "daily_rsi":  daily_rsi,
                "sector":     sector,
            }

        except Exception as e:
            log.exception("Error fetching %s: %s", symbol, str(e))
            return symbol, None

    def fetch_batch(self, symbols: List[str], batch_label: str = "") -> Dict[str, Dict]:
        stock_data: Dict[str, Dict] = {}
        total = len(symbols)
        log.info("Fetching batch %s - %d stocks, %d threads", batch_label, total, self.max_workers)

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
                        w = data["weekly_rsi"]
                        d = data["daily_rsi"]
                        rsi_str = f"  W-RSI {w}  D-RSI {d}" if (w or d) else ""
                        log.info("  OK  %-15s (%d/%d)  Rs %.2f%s",
                                 sym, completed, total, data["price"], rsi_str)
                    else:
                        log.info("  --  %-15s (%d/%d)  no data", symbol, completed, total)
                except Exception as e:
                    log.exception("  !!  %s -- future raised: %s", symbol, str(e))

                if completed % 20 == 0:
                    time.sleep(0.5)   # gentle rate-limit backoff

        return stock_data


# ============= EXCEL OUTPUT =============

def _rsi_status(rsi: Optional[float]) -> str:
    if rsi is None:
        return "N/A"
    if rsi >= 70:
        return "Overbought"
    if rsi <= 30:
        return "Oversold"
    return "Neutral"


def _fmt_rsi(rsi: Optional[float]) -> str:
    return str(rsi) if rsi is not None else "N/A"


def create_dataframes(
    odd_matches: Dict,
    level_matches: Dict,
    both_matches: set,
) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    """Create DataFrames for each match type without writing to disk."""
    
    odd_df = None
    level_df = None
    both_df = None

    # ── Odd Square Matches ──────────────────────────────────────────────
    if odd_matches:
        rows = [
            {
                "Symbol":         sym,
                "Sector":         d["sector"],
                "Current Price":  d["price"],
                "Daily RSI":      _fmt_rsi(d.get("daily_rsi")),
                "Daily RSI Status": _rsi_status(d.get("daily_rsi")),
                "Weekly RSI":     _fmt_rsi(d.get("weekly_rsi")),
                "Weekly RSI Status": _rsi_status(d.get("weekly_rsi")),
                "Odd Number":     d["odd_number"],
                "Odd Square":     d["odd_square"],
                "Deviation %":    d["deviation_percent"],
                "Buy Above":      d["levels"][0] if d["levels"] else "N/A",
                "Buy Confirm":    d["levels"][1] if d["levels"] else "N/A",
                "Sell Below":     d["levels"][2] if d["levels"] else "N/A",
                "Sell Confirm":   d["levels"][3] if d["levels"] else "N/A",
            }
            for sym, d in odd_matches.items()
        ]
        odd_df = pd.DataFrame(rows).sort_values("Deviation %")

    # ── Volatility Level Matches ─────────────────────────────────────────
    if level_matches:
        rows = [
            {
                "Symbol":                    sym,
                "Sector":                    d["sector"],
                "Current Price":             d["price"],
                "Daily RSI":                 _fmt_rsi(d.get("daily_rsi")),
                "Daily RSI Status":          _rsi_status(d.get("daily_rsi")),
                "Weekly RSI":                _fmt_rsi(d.get("weekly_rsi")),
                "Weekly RSI Status":         _rsi_status(d.get("weekly_rsi")),
                "Matched Levels":            ", ".join(m[0] for m in d["matched_levels"]),
                "Buy Above":                 d["levels"][0],
                "Buy Confirm":               d["levels"][1],
                "Sell Below":                d["levels"][2],
                "Sell Confirm":              d["levels"][3],
                "Nearest Level Deviation %": round(d["deviation"], 2),
            }
            for sym, d in level_matches.items()
        ]
        level_df = pd.DataFrame(rows).sort_values("Nearest Level Deviation %")

    # ── Both Criteria (Signal Sheet) ─────────────────────────────────────
    if both_matches:
        rows = []
        for sym in both_matches:
            od = odd_matches[sym]
            ld = level_matches[sym]
            buys = [m for m in ld["matched_levels"] if "BUY" in m[0]]
            sells = [m for m in ld["matched_levels"] if "SELL" in m[0]]
            if buys:
                signal = "BUY"
                strength = "STRONG" if "CONFIRM" in buys[0][0] else "MODERATE"
            elif sells:
                signal = "SELL"
                strength = "STRONG" if "CONFIRM" in sells[0][0] else "MODERATE"
            else:
                signal, strength = "NEUTRAL", "WEAK"

            drsi = od.get("daily_rsi")
            wrsi = od.get("weekly_rsi")
            rows.append({
                "Symbol":                    sym,
                "Sector":                    od["sector"],
                "Signal":                    signal,
                "Strength":                  strength,
                "Current Price":             od["price"],
                "Daily RSI":                 _fmt_rsi(drsi),
                "Daily RSI Status":          _rsi_status(drsi),
                "Weekly RSI":                _fmt_rsi(wrsi),
                "Weekly RSI Status":         _rsi_status(wrsi),
                "Odd Square":                f"{od['odd_number']}^2 = {od['odd_square']}",
                "Odd Square Deviation %":    od["deviation_percent"],
                "Matched Levels":            ", ".join(m[0] for m in ld["matched_levels"]),
                "Nearest Level Deviation %": round(ld["deviation"], 2),
                "Buy Above":                 ld["levels"][0],
                "Buy Confirm":               ld["levels"][1],
                "Sell Below":                ld["levels"][2],
                "Sell Confirm":              ld["levels"][3],
            })
        both_df = pd.DataFrame(rows).sort_values(["Signal", "Strength"],
                                                ascending=[True, False])

    return odd_df, level_df, both_df


def dataframe_to_excel_bytes(df: pd.DataFrame, sheet_name: str) -> bytes:
    """Convert a DataFrame to Excel bytes with auto-fitted columns."""
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        clean_name = sanitize_sheet_name(sheet_name)
        df.to_excel(writer, sheet_name=clean_name, index=False)
        # Auto-fit columns
        worksheet = writer.sheets[clean_name]
        for col in worksheet.columns:
            max_len = max(
                (len(str(cell.value)) for cell in col if cell.value is not None),
                default=0,
            )
            worksheet.column_dimensions[col[0].column_letter].width = min(max_len + 2, 50)
    return output.getvalue()


def save_local_excel_files(
    odd_df: Optional[pd.DataFrame],
    level_df: Optional[pd.DataFrame],
    both_df: Optional[pd.DataFrame],
    timestamp: str,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Save DataFrames to local Excel files."""
    odd_file, level_file, both_file = None, None, None

    if odd_df is not None:
        odd_file = f"odd_square_matches_{timestamp}.xlsx"
        with pd.ExcelWriter(odd_file, engine="openpyxl") as writer:
            odd_df.to_excel(writer, sheet_name="Odd Square Matches", index=False)
            autofit_worksheet_columns(writer.sheets["Odd Square Matches"])
        log.info("Created: %s", odd_file)

    if level_df is not None:
        level_file = f"volatility_level_matches_{timestamp}.xlsx"
        with pd.ExcelWriter(level_file, engine="openpyxl") as writer:
            level_df.to_excel(writer, sheet_name="Level Matches", index=False)
            autofit_worksheet_columns(writer.sheets["Level Matches"])
        log.info("Created: %s", level_file)

    if both_df is not None:
        both_file = f"both_criteria_signals_{timestamp}.xlsx"
        with pd.ExcelWriter(both_file, engine="openpyxl") as writer:
            both_df.to_excel(writer, sheet_name="Buy Sell Signals", index=False)
            autofit_worksheet_columns(writer.sheets["Buy Sell Signals"])
        log.info("Created: %s", both_file)

    return odd_file, level_file, both_file


# ============= TELEGRAM =============

def _tg_post(endpoint: str, **kwargs) -> bool:
    """POST to a Telegram Bot API endpoint. Returns True on HTTP 200."""
    if not BOT_TOKEN or not CHAT_ID:
        log.debug("Telegram not configured - skipping")
        return False
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{endpoint}"
    try:
        resp = requests.post(url, timeout=60, **kwargs)
        if not resp.ok:
            log.warning("Telegram %s failed: %s %s", endpoint, resp.status_code, resp.text[:200])
        return resp.ok
    except Exception:
        log.exception("Telegram %s raised", endpoint)
        return False


def send_telegram_message(text: str) -> bool:
    """Send a plain-text message (Markdown parse_mode disabled for safety)."""
    return _tg_post("sendMessage", json={"chat_id": CHAT_ID, "text": text})


def send_telegram_file(filepath: str, caption: str = "") -> bool:
    """Upload a file to Telegram using sendDocument (supports up to 50 MB)."""
    if not os.path.exists(filepath):
        log.warning("Telegram upload: file not found: %s", filepath)
        return False
    filename = os.path.basename(filepath)
    with open(filepath, "rb") as fh:
        return _tg_post(
            "sendDocument",
            data={"chat_id": CHAT_ID, "caption": caption[:1024]},
            files={"document": (filename, fh,
                                "application/vnd.openxmlformats-officedocument"
                                ".spreadsheetml.sheet")},
        )


def send_telegram_file_bytes(file_bytes: bytes, filename: str, caption: str = "") -> bool:
    """Send a file from bytes to Telegram (no disk write) - for cloud mode."""
    if not BOT_TOKEN or not CHAT_ID:
        log.debug("Telegram not configured - skipping")
        return False
    
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument"
    try:
        files = {
            "document": (filename, BytesIO(file_bytes), 
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        }
        data = {"chat_id": CHAT_ID, "caption": caption[:1024]}
        resp = requests.post(url, data=data, files=files, timeout=60)
        if not resp.ok:
            log.warning("Telegram upload failed: %s %s", resp.status_code, resp.text[:200])
        return resp.ok
    except Exception as e:
        log.exception(f"Telegram upload failed: {e}")
        return False


def send_telegram_summary(
    odd_matches: Dict,
    level_matches: Dict,
    both_matches: set,
    odd_data: Optional[any],
    level_data: Optional[any],
    both_data: Optional[any],
) -> None:
    """Send the signal summary message then upload all three files."""
    if not BOT_TOKEN or not CHAT_ID:
        log.debug("Telegram not configured - skipping summary")
        return

    now = datetime.now().strftime("%d %b %Y %H:%M")
    run_mode_str = "☁️ CLOUD" if RUN_MODE == "cloud" else "💻 LOCAL"

    # ── 1. Summary text message ───────────────────────────────────────
    lines = [
        f"NSE F&O Screener  |  {now}",
        f"Mode: {run_mode_str}",
        f"Odd Square matches   : {len(odd_matches)}",
        f"Level matches        : {len(level_matches)}",
        f"Both-criteria hits   : {len(both_matches)}",
    ]

    if both_matches:
        lines.append("")
        lines.append("--- TOP SIGNALS ---")
        for sym in sorted(both_matches)[:10]:  # Top 10 signals
            od = odd_matches[sym]
            ld = level_matches[sym]
            sig = "BUY" if any("BUY" in m[0] for m in ld["matched_levels"]) else "SELL"
            drsi = od.get("daily_rsi")
            wrsi = od.get("weekly_rsi")
            d_str = f"D-RSI {drsi}" if drsi is not None else "D-RSI N/A"
            w_str = f"W-RSI {wrsi}" if wrsi is not None else "W-RSI N/A"
            d_tag = f" [{_rsi_status(drsi)}]" if drsi is not None else ""
            w_tag = f" [{_rsi_status(wrsi)}]" if wrsi is not None else ""
            lines.append(
                f"{sig}  {sym}  Rs {od['price']:.0f}"
                f"  {d_str}{d_tag}  {w_str}{w_tag}"
            )
        if len(both_matches) > 10:
            lines.append(f"... and {len(both_matches) - 10} more")
    else:
        lines.append("No both-criteria signals today.")

    send_telegram_message("\n".join(lines))

    # ── 2. Upload each file ────────────────────────────────────────────
    if RUN_MODE == "cloud" and odd_data is not None:
        # Cloud mode: send from bytes
        if odd_data:
            send_telegram_file_bytes(odd_data, f"odd_square_matches_{now.replace(' ', '_')}.xlsx",
                                    f"Odd Square matches ({len(odd_matches)} stocks)")
        if level_data:
            send_telegram_file_bytes(level_data, f"volatility_level_matches_{now.replace(' ', '_')}.xlsx",
                                    f"Volatility Level matches ({len(level_matches)} stocks)")
        if both_data:
            send_telegram_file_bytes(both_data, f"both_criteria_signals_{now.replace(' ', '_')}.xlsx",
                                    f"Both-criteria signals ({len(both_matches)} stocks)")
    elif SAVE_LOCAL_FILES:
        # Local mode: send from files
        for filepath, caption in [(odd_data, f"Odd Square matches ({len(odd_matches)} stocks)"),
                                   (level_data, f"Volatility Level matches ({len(level_matches)} stocks)"),
                                   (both_data, f"Both-criteria signals ({len(both_matches)} stocks)")]:
            if filepath:
                ok = send_telegram_file(filepath, caption=caption)
                if ok:
                    log.info("Sent to Telegram: %s", os.path.basename(filepath))
                else:
                    log.warning("Failed to send to Telegram: %s", filepath)


# ============= ENTRY POINT =============

def get_fno_stocks() -> List[str]:
    if NSELIB_AVAILABLE:
        try:
            fno_data = capital_market.fno_equity_list()
            if hasattr(fno_data, "empty") and not fno_data.empty:
                col = next((c for c in fno_data.columns if c.upper() == "SYMBOL"), None)
                if col:
                    return fno_data[col].tolist()
        except Exception:
            log.exception("nselib fetch failed, falling back")
    log.warning("Using fallback list of %d F&O stocks", len(FALLBACK_FNO_STOCKS))
    return FALLBACK_FNO_STOCKS


def main(dry_run: bool = False, override_symbols: Optional[List[str]] = None) -> None:
    print("=" * 80)
    print(f"STOCK SCREENER v2 - NSE F&O Segment [{RUN_MODE.upper()} MODE]")
    print("Odd Square +/-0.5%  |  Volatility Levels +/-2%  |  Wilder RSI-14 Daily + Weekly")
    print("=" * 80)
    
    log.info(f"Running in {RUN_MODE.upper()} mode")
    log.info(f"Save local files: {SAVE_LOCAL_FILES}")

    fetcher = BatchDataFetcher(max_workers=MAX_WORKERS)

    # ── 1. Stock list ──────────────────────────────────────────────────
    log.info("[1/5] Fetching F&O stock list...")
    symbols = override_symbols or get_fno_stocks()
    if dry_run:
        symbols = symbols[:10]
        log.info("Dry-run mode: limiting to first %d symbols", len(symbols))
    log.info("Stocks to screen: %d", len(symbols))

    # ── 2. Batch fetch ─────────────────────────────────────────────────
    log.info("[2/5] Fetching price data, RSI and volatility levels...")
    batches = [symbols[i:i + BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]
    all_data: Dict[str, Dict] = {}

    for i, batch in enumerate(batches, 1):
        all_data.update(fetcher.fetch_batch(batch, batch_label=f"{i}/{len(batches)}"))
        log.info("Progress: %d / %d stocks collected", len(all_data), len(symbols))

    if not all_data:
        log.error("No stock data retrieved. Exiting.")
        sys.exit(0)

    log.info("Retrieved data for %d / %d stocks", len(all_data), len(symbols))

    # ── 3 & 4. Screening ───────────────────────────────────────────────
    log.info("[3/5] Finding Odd Square matches (+/-%.1f%%)...", ODD_SQUARE_TOLERANCE)
    odd_matches = find_odd_square_matches(all_data, ODD_SQUARE_TOLERANCE)
    log.info("Odd Square matches: %d", len(odd_matches))

    log.info("[4/5] Finding Volatility Level matches (+/-%.0f%%)...", THRESHOLD * 100)
    level_matches = find_level_matches(all_data, THRESHOLD)
    log.info("Volatility Level matches: %d", len(level_matches))

    # ── 5. Cross-reference ─────────────────────────────────────────────
    both_matches = set(odd_matches) & set(level_matches)
    log.info("[5/5] Both-criteria matches: %d", len(both_matches))

    # ── Create DataFrames ──────────────────────────────────────────────
    odd_df, level_df, both_df = create_dataframes(odd_matches, level_matches, both_matches)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # ── Output based on mode ───────────────────────────────────────────
    odd_file = level_file = both_file = None
    odd_bytes = level_bytes = both_bytes = None
    
    if SAVE_LOCAL_FILES:
        # Local mode: Save to disk
        log.info("Saving Excel files locally...")
        odd_file, level_file, both_file = save_local_excel_files(odd_df, level_df, both_df, timestamp)
    else:
        # Cloud mode: Convert to bytes without saving to disk
        log.info("Cloud mode - creating Excel files in memory...")
        if odd_df is not None:
            odd_bytes = dataframe_to_excel_bytes(odd_df, "Odd Square Matches")
            log.info("Created odd square Excel in memory (%d bytes)", len(odd_bytes))
        if level_df is not None:
            level_bytes = dataframe_to_excel_bytes(level_df, "Level Matches")
            log.info("Created level matches Excel in memory (%d bytes)", len(level_bytes))
        if both_df is not None:
            both_bytes = dataframe_to_excel_bytes(both_df, "Buy Sell Signals")
            log.info("Created both criteria Excel in memory (%d bytes)", len(both_bytes))

    # ── Console summary ────────────────────────────────────────────────
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"  Run mode            : {RUN_MODE.upper()}")
    print(f"  F&O stocks screened : {len(symbols)}")
    print(f"  Valid data retrieved: {len(all_data)}")
    print(f"  Odd Square matches  : {len(odd_matches)}")
    print(f"  Level matches       : {len(level_matches)}")
    print(f"  Both-criteria hits  : {len(both_matches)}")

    if both_matches:
        print("\nTOP SIGNALS:")
        rows = []
        for sym in sorted(both_matches)[:20]:
            od = odd_matches[sym]
            ld = level_matches[sym]
            sig = "BUY" if any("BUY" in m[0] for m in ld["matched_levels"]) else "SELL"
            rows.append({
                "Symbol":    sym,
                "Sector":    od["sector"],
                "Signal":    sig,
                "Price":     od["price"],
                "Daily RSI": _fmt_rsi(od.get("daily_rsi")),
                "D-Status":  _rsi_status(od.get("daily_rsi")),
                "Weekly RSI":_fmt_rsi(od.get("weekly_rsi")),
                "W-Status":  _rsi_status(od.get("weekly_rsi")),
            })
        
        # Create DataFrame and display signals
        signals_df = pd.DataFrame(rows)
        print(f"\n📊 Buy Signals: {len(signals_df[signals_df['Signal'] == 'BUY'])}")
        print(f"📉 Sell Signals: {len(signals_df[signals_df['Signal'] == 'SELL'])}")
        print("\n" + signals_df.to_string(index=False))
    else:
        print("\n⚠️ No both-criteria signals found today.")
        print("   Try running with --symbols to test specific stocks, or adjust thresholds.")

    # ── Telegram: message + files ────────────────────────────────────
    if RUN_MODE == "cloud":
        send_telegram_summary(odd_matches, level_matches, both_matches,
                              odd_bytes, level_bytes, both_bytes)
    else:
        send_telegram_summary(odd_matches, level_matches, both_matches,
                              odd_file, level_file, both_file)

    print("\nOutput:")
    if SAVE_LOCAL_FILES:
        for f in [odd_file, level_file, both_file]:
            if f:
                print(f"  - {f}")
    else:
        print("  - Files sent directly to Telegram (no local files saved)")
    print("=" * 80)


if __name__ == "__main__":
    required = ["pandas", "numpy", "yfinance", "openpyxl", "requests"]
    missing = [lib for lib in required if not __import__("importlib").util.find_spec(lib)]
    if missing:
        print(f"❌ Missing libraries: {', '.join(missing)}")
        print("Install with: pip install " + " ".join(missing))
        sys.exit(1)

    parser = argparse.ArgumentParser(description="NSE F&O Stock Screener")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch only the first 10 symbols (for testing)")
    parser.add_argument("--symbols", nargs="+", metavar="SYM",
                        help="Override symbol list, e.g. --symbols RELIANCE TCS INFY")
    parser.add_argument("--local", action="store_true",
                        help="Force local mode (save files to disk)")
    parser.add_argument("--cloud", action="store_true",
                        help="Force cloud mode (no disk save)")
    args = parser.parse_args()
    
    # Override mode if specified
    if args.local:
        os.environ["RUN_MODE"] = "local"
    elif args.cloud:
        os.environ["RUN_MODE"] = "cloud"
    
    main(dry_run=args.dry_run, override_symbols=args.symbols)

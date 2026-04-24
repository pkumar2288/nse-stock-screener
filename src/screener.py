"""
AIScan India v5 — NSE F&O Screener
────────────────────────────────────────────────────────────────────────────────
Changes from v4:
  • F&O list: Zerodha Kite API (api.kite.trade/instruments) with daily cache
  • Parallel batch processing: concurrent weekly + daily fetches per stock
  • Sector + Industry columns in all 3 Excel sheets and Telegram messages
  • Weekly RSI + Daily RSI added to signal sheet and Telegram buy/sell message
  • Beautified Telegram message with emojis, sections, run-time summary
  • Always generate 3 separate Excel files in local/desktop mode
  • Cloud mode: send files as Telegram documents (no disk save)
────────────────────────────────────────────────────────────────────────────────
"""

import sys
import time
import json
import logging
import argparse
import pandas as pd
import numpy as np
import yfinance as yf
from typing import List, Dict, Tuple, Optional
import os
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from io import BytesIO

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


# ============= CONFIGURATION =============
RUN_MODE         = os.environ.get("RUN_MODE", "local")
IS_CLOUD         = RUN_MODE in ("cloud", "github")
SAVE_LOCAL_FILES = not IS_CLOUD      # always save 3 files on desktop/local

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "8736775563:AAH7jG4aHyUUfmtKJUxs-ChSXvOHp3Tm7Wk")
CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "5732340968, 545110825")
CHAT_ID_LIST = [cid.strip() for cid in CHAT_IDS.split(",")]

# Screening parameters
ROUND_PRECI          = 2
THRESHOLD            = 0.01      # 1 % proximity for VF level hits
ODD_SQUARE_TOLERANCE = 0.5       # 0.5 % proximity for odd-square filter
RSI_PERIOD           = 14

# Pine Script weekly VF replication
WEEKLY_CLOSES_NEEDED = 11
MIN_WEEKLY_BARS      = 13

# Daily indicator parameters
ATR_PERIOD         = 14
ATR_PERCENTILE_WIN = 52
ADX_PERIOD         = 14
SUPERTREND_FACTOR  = 3
SUPERTREND_ATR     = 10

# Score bands
SCORE_STRONG   = 75
SCORE_MODERATE = 55
SCORE_WEAK     = 40

# Parallel processing
MAX_WORKERS = 5 if IS_CLOUD else 12
BATCH_SIZE  = 25 if IS_CLOUD else 50

# Kite instruments cache
KITE_CACHE_FILE = "fno_cache.json"
KITE_CACHE_TTL  = 24   # hours — refresh once per day

ODD_SQUARES: List[Tuple[int, int]] = [(n, n * n) for n in range(1, 301, 2)]

# ── Sector + Industry mapping ─────────────────────────────────────────────────
# Format: symbol -> (Sector, Industry)
SECTOR_INDUSTRY: Dict[str, Tuple[str, str]] = {
    # Banking
    "HDFCBANK":   ("Banking", "Private Bank"),
    "ICICIBANK":  ("Banking", "Private Bank"),
    "KOTAKBANK":  ("Banking", "Private Bank"),
    "AXISBANK":   ("Banking", "Private Bank"),
    "INDUSINDBK": ("Banking", "Private Bank"),
    "YESBANK":    ("Banking", "Private Bank"),
    "SBIN":       ("Banking", "Public Sector Bank"),
    "BANKBARODA": ("Banking", "Public Sector Bank"),
    "PNB":        ("Banking", "Public Sector Bank"),
    "CANBK":      ("Banking", "Public Sector Bank"),
    "UNIONBANK":  ("Banking", "Public Sector Bank"),
    # Financial Services
    "BAJFINANCE": ("Financial Services", "NBFC"),
    "BAJAJFINSV": ("Financial Services", "NBFC"),
    "BAJFINSERV": ("Financial Services", "NBFC"),
    "HDFC":       ("Financial Services", "Housing Finance"),
    "LICHSGFIN":  ("Financial Services", "Housing Finance"),
    "MUTHOOTFIN": ("Financial Services", "Gold Loan"),
    "HDFCLIFE":   ("Financial Services", "Life Insurance"),
    "SBILIFE":    ("Financial Services", "Life Insurance"),
    "ICICIPRULI": ("Financial Services", "Life Insurance"),
    "ICICIGI":    ("Financial Services", "General Insurance"),
    "SBICARD":    ("Financial Services", "Credit Card"),
    # IT
    "TCS":        ("IT", "IT Services"),
    "INFY":       ("IT", "IT Services"),
    "WIPRO":      ("IT", "IT Services"),
    "HCLTECH":    ("IT", "IT Services"),
    "TECHM":      ("IT", "IT Services"),
    "LTIM":       ("IT", "IT Consulting"),
    "MPHASIS":    ("IT", "IT Services"),
    "COFORGE":    ("IT", "IT Services"),
    "PERSISTENT": ("IT", "IT Services"),
    "KPITTECH":   ("IT", "IT - Auto"),
    # Energy
    "RELIANCE":   ("Energy", "Oil & Gas - Integrated"),
    "ONGC":       ("Energy", "Oil & Gas - Upstream"),
    "IOC":        ("Energy", "Oil & Gas - Downstream"),
    "BPCL":       ("Energy", "Oil & Gas - Downstream"),
    "HINDPETRO":  ("Energy", "Oil & Gas - Downstream"),
    "GAIL":       ("Energy", "Gas Distribution"),
    "MGL":        ("Energy", "Gas Distribution"),
    "IGL":        ("Energy", "Gas Distribution"),
    # Power
    "POWERGRID":  ("Power", "Power Transmission"),
    "NTPC":       ("Power", "Power Generation"),
    "TATAPOWER":  ("Power", "Power Generation"),
    "ADANIGREEN": ("Power", "Renewable Energy"),
    "ADANIPOWER": ("Power", "Power Generation"),
    "CESC":       ("Power", "Power Generation"),
    # Automobile
    "TATAMOTORS": ("Automobile", "Auto - 4 Wheeler"),
    "MARUTI":     ("Automobile", "Auto - 4 Wheeler"),
    "M&M":        ("Automobile", "Auto - 4 Wheeler"),
    "BAJAJ-AUTO": ("Automobile", "Auto - 2 Wheeler"),
    "HEROMOTOCO": ("Automobile", "Auto - 2 Wheeler"),
    "EICHERMOT":  ("Automobile", "Auto - 2 Wheeler"),
    "ASHOKLEY":   ("Automobile", "Auto - Commercial Vehicle"),
    "TIINDIA":    ("Automobile", "Auto Ancillary"),
    "MOTHERSON":  ("Automobile", "Auto Ancillary"),
    "BALKRISIND": ("Automobile", "Tyres"),
    "MRF":        ("Automobile", "Tyres"),
    # Metals & Mining
    "TATASTEEL":  ("Metals", "Steel"),
    "JSWSTEEL":   ("Metals", "Steel"),
    "HINDALCO":   ("Metals", "Aluminium"),
    "VEDL":       ("Metals", "Diversified Metals"),
    "NATIONALUM": ("Metals", "Aluminium"),
    "HINDZINC":   ("Metals", "Zinc"),
    "NMDC":       ("Metals", "Iron Ore"),
    "COALINDIA":  ("Mining", "Coal"),
    # Pharmaceuticals & Healthcare
    "SUNPHARMA":  ("Pharmaceuticals", "Pharma - Branded Generic"),
    "DRREDDY":    ("Pharmaceuticals", "Pharma - Generic"),
    "CIPLA":      ("Pharmaceuticals", "Pharma - Generic"),
    "DIVISLAB":   ("Pharmaceuticals", "API / CDMO"),
    "LUPIN":      ("Pharmaceuticals", "Pharma - Generic"),
    "AUROPHARMA": ("Pharmaceuticals", "Pharma - Generic"),
    "BIOCON":     ("Pharmaceuticals", "Biosimilars"),
    "APOLLOHOSP": ("Healthcare", "Hospitals"),
    "MAXHEALTH":  ("Healthcare", "Hospitals"),
    # FMCG
    "HINDUNILVR": ("FMCG", "Personal Care"),
    "ITC":        ("FMCG", "Diversified FMCG"),
    "NESTLEIND":  ("FMCG", "Food & Beverages"),
    "BRITANNIA":  ("FMCG", "Food & Beverages"),
    "DABUR":      ("FMCG", "Ayurvedic / Health"),
    "GODREJCP":   ("FMCG", "Personal Care"),
    "MARICO":     ("FMCG", "Personal Care"),
    "COLPAL":     ("FMCG", "Personal Care"),
    "EMAMILTD":   ("FMCG", "Ayurvedic / Health"),
    "TATACONSUM": ("FMCG", "Food & Beverages"),
    # Telecom
    "BHARTIARTL": ("Telecom", "Integrated Telecom"),
    "JIOFIN":     ("Telecom", "Telecom - Finance"),
    "IDEA":       ("Telecom", "Wireless Telecom"),
    # Construction & Infra
    "LT":         ("Construction", "EPC - Diversified"),
    "LTTS":       ("Construction", "Engineering Services"),
    "ADANIPORTS": ("Infrastructure", "Ports & Logistics"),
    "GMRINFRA":   ("Infrastructure", "Airports"),
    # Cement
    "ULTRACEMCO": ("Cement", "Large Cap Cement"),
    "GRASIM":     ("Cement", "Diversified - Cement / Chemicals"),
    "AMBUJACEM":  ("Cement", "Large Cap Cement"),
    "ACC":        ("Cement", "Large Cap Cement"),
    "SHREECEM":   ("Cement", "Large Cap Cement"),
    "RAMCOCEM":   ("Cement", "Mid Cap Cement"),
    # Paints & Retail
    "ASIANPAINT": ("Paints", "Decorative Paints"),
    "BERGEPAINT": ("Paints", "Decorative Paints"),
    "PIDILITIND": ("Paints", "Adhesives & Chemicals"),
    "TITAN":      ("Retail", "Jewellery & Watches"),
    "DMART":      ("Retail", "Supermarkets"),
    "TRENT":      ("Retail", "Apparel"),
    # Conglomerate
    "ADANIENT":   ("Conglomerate", "Adani Group"),
    "ADANIENT":   ("Conglomerate", "Adani Group"),
    # Chemicals & Agri
    "UPL":        ("Agrochemicals", "Crop Protection"),
    "PIIND":      ("Chemicals", "Specialty Chemicals"),
    "AARTIIND":   ("Chemicals", "Specialty Chemicals"),
    "SRF":        ("Chemicals", "Fluorochemicals"),
    "DEEPAKNTR":  ("Chemicals", "Specialty Chemicals"),
    # Real Estate
    "DLF":        ("Real Estate", "Residential"),
    "GODREJPROP": ("Real Estate", "Residential"),
    "OBEROIRLTY": ("Real Estate", "Residential"),
    "PRESTIGE":   ("Real Estate", "Residential"),
}

FALLBACK_FNO_STOCKS = list(SECTOR_INDUSTRY.keys())


# ============= HELPERS =============

def scalar(val) -> float:
    try:
        if val is None: return 0.0
        if hasattr(val, "__len__") and len(val) == 0: return 0.0
        if np.isscalar(val): return float(val)
        arr = np.asarray(val)
        if arr.size == 0: return 0.0
        return float(arr.flatten()[0])
    except Exception as e:
        log.warning("scalar() failed: %s", e)
        return 0.0


def get_sector_industry(symbol: str) -> Tuple[str, str]:
    return SECTOR_INDUSTRY.get(symbol, ("Other", "Other"))


def _extract_series(df: pd.DataFrame, col_names: Tuple[str, ...]) -> Optional[pd.Series]:
    if df is None or df.empty:
        return None
    cols = df.columns
    series = None
    if isinstance(cols, pd.MultiIndex):
        level0 = [str(c[0]).strip().lower() for c in cols]
        for target in col_names:
            matches = [cols[i] for i, name in enumerate(level0) if name == target.lower()]
            if matches:
                series = df[matches[0]].dropna()
                break
    else:
        col_map = {str(c).lower(): c for c in cols}
        for target in col_names:
            key = col_map.get(target.lower())
            if key is not None:
                series = df[key].dropna()
                break
    if series is None: return None
    if isinstance(series, pd.DataFrame): series = series.iloc[:, 0]
    if not isinstance(series, pd.Series): return None
    try:
        series = series.apply(lambda x: scalar(x))
    except Exception:
        return None
    return series.astype(float)


def extract_close_series(df): return _extract_series(df, ("Close", "Adj Close"))
def extract_open_series(df):  return _extract_series(df, ("Open",))
def extract_high_series(df):  return _extract_series(df, ("High",))
def extract_low_series(df):   return _extract_series(df, ("Low",))


def is_data_stale(df: pd.DataFrame, max_days: int = 10) -> bool:
    if df is None or df.empty: return True
    last_ts = df.index[-1]
    if hasattr(last_ts, "date"): last_ts = last_ts.date()
    return (datetime.now().date() - last_ts) > timedelta(days=max_days)


def autofit_worksheet_columns(ws) -> None:
    for col in ws.columns:
        max_len = max((len(str(c.value)) for c in col if c.value is not None), default=0)
        ws.column_dimensions[col[0].column_letter].width = min(max_len + 2, 55)


def sanitize_sheet_name(name: str) -> str:
    for ch in r":\/?\*[]": name = name.replace(ch, "")
    return name[:31]


def is_near_level(price: float, level: float, threshold: float = THRESHOLD) -> bool:
    if level == 0: return False
    return abs(price - level) / level <= threshold


def is_price_within_tolerance(price: float, target: float, pct: float = 1.0) -> bool:
    if price <= 0 or target <= 0: return False
    return target * (1 - pct / 100) <= price <= target * (1 + pct / 100)


def _fmt(val, fmt=".2f") -> str:
    if val is None: return "N/A"
    try: return format(float(val), fmt)
    except: return str(val)


def _rsi_status(rsi: Optional[float]) -> str:
    if rsi is None: return "N/A"
    if rsi >= 70:   return "Overbought"
    if rsi <= 30:   return "Oversold"
    return "Neutral"


def _rsi_emoji(rsi: Optional[float]) -> str:
    if rsi is None: return ""
    if rsi >= 70:   return "🔴"
    if rsi <= 30:   return "🟢"
    return "🟡"


def _st_label(data: Dict) -> str:
    st = data.get("supertrend")
    if not st: return "N/A"
    return "BULL 🟢" if st["bullish"] else "BEAR 🔴"


# ============= F&O STOCK LIST — Kite API with daily cache =============

def _load_cache() -> Optional[Dict]:
    """Load cached F&O list if it exists and is less than KITE_CACHE_TTL hours old."""
    if not os.path.exists(KITE_CACHE_FILE):
        return None
    try:
        with open(KITE_CACHE_FILE, "r", encoding="utf-8") as f:
            cache = json.load(f)
        cached_date = cache.get("date", "")
        if cached_date == date.today().isoformat():
            symbols = cache.get("symbols", [])
            if symbols:
                log.info("F&O cache hit: %d symbols (cached %s)", len(symbols), cached_date)
                return cache
        log.info("F&O cache expired (cached=%s, today=%s) — refreshing",
                 cached_date, date.today().isoformat())
    except Exception as e:
        log.warning("Cache read failed: %s", e)
    return None


def _save_cache(symbols: List[str]) -> None:
    try:
        with open(KITE_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump({"date": date.today().isoformat(), "symbols": symbols}, f)
        log.info("F&O cache saved: %d symbols → %s", len(symbols), KITE_CACHE_FILE)
    except Exception as e:
        log.warning("Cache save failed: %s", e)


def get_fno_stocks_kite() -> Optional[List[str]]:
    """
    Fetch NSE F&O stock list from Zerodha Kite instruments API.
    URL: https://api.kite.trade/instruments
    Filters for: exchange=NFO, instrument_type=FUT, segment=NFO-FUT
    Extracts the underlying equity symbol (strip expiry/lot info).
    No API key required — this is a public endpoint.
    """
    url = "https://api.kite.trade/instruments"
    try:
        log.info("Fetching F&O list from Kite API...")
        resp = requests.get(url, timeout=30,
                            headers={"User-Agent": "Mozilla/5.0"})
        if not resp.ok:
            log.warning("Kite API returned %d", resp.status_code)
            return None

        # Response is a CSV — parse with pandas
        df = pd.read_csv(BytesIO(resp.content))
        log.debug("Kite instruments columns: %s", df.columns.tolist())

        # Filter NSE F&O futures on equities
        # Columns: instrument_token, exchange_token, tradingsymbol, name,
        #          last_price, expiry, strike, tick_size, lot_size,
        #          instrument_type, segment, exchange
        nfo = df[
            (df["exchange"].str.upper()       == "NFO") &
            (df["instrument_type"].str.upper() == "FUT") &
            (df["segment"].str.upper()         == "NFO-FUT")
        ].copy()

        if nfo.empty:
            log.warning("Kite: no NFO FUT rows found after filter")
            return None

        # 'name' column holds the underlying equity symbol (e.g. RELIANCE, TCS)
        symbols = sorted(nfo["name"].dropna().unique().tolist())
        # Remove index futures (NIFTY, BANKNIFTY, etc.) — keep only equity FnO
        equity_symbols = [s for s in symbols
                          if s.upper() not in (
                              "NIFTY", "BANKNIFTY", "FINNIFTY",
                              "MIDCPNIFTY", "NIFTYNXT50", "SENSEX",
                          )]
        if equity_symbols:
            log.info("Kite API: %d NSE F&O equity symbols", len(equity_symbols))
            return equity_symbols

    except Exception as e:
        log.warning("Kite API fetch failed: %s", e)
    return None


def get_fno_stocks_nse_api() -> Optional[List[str]]:
    """Fallback: NSE website API."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
        "Referer":    "https://www.nseindia.com/",
    }
    session = requests.Session()
    try:
        session.get("https://www.nseindia.com", headers=headers, timeout=10)
        time.sleep(1)
        resp = session.get(
            "https://www.nseindia.com/api/equity-stockIndices"
            "?index=Securities%20in%20F%26O",
            headers=headers, timeout=15,
        )
        if not resp.ok: return None
        symbols = [
            item["symbol"] for item in resp.json().get("data", [])
            if item.get("symbol") and item["symbol"] not in
               ("NIFTY 50", "NIFTY", "BANKNIFTY")
        ]
        if symbols:
            log.info("NSE API fallback: %d symbols", len(symbols))
            return symbols
    except Exception as e:
        log.warning("NSE API fallback failed: %s", e)
    return None


def get_fno_stocks(force_refresh: bool = False) -> List[str]:
    """
    Priority:
      1. Daily cache (fno_cache.json) — skip fetch if today's cache exists
      2. Kite instruments API (primary)
      3. NSE website API (secondary fallback)
      4. Hardcoded FALLBACK_FNO_STOCKS (last resort)
    """
    if not force_refresh:
        cache = _load_cache()
        if cache:
            return cache["symbols"]

    # Try Kite first
    symbols = get_fno_stocks_kite()

    # Try NSE API as fallback
    if not symbols:
        symbols = get_fno_stocks_nse_api()

    if symbols:
        _save_cache(symbols)
        return symbols

    log.warning("All F&O list sources failed — using hardcoded fallback (%d stocks)",
                len(FALLBACK_FNO_STOCKS))
    return FALLBACK_FNO_STOCKS


# ============= RSI =============

def calculate_rsi(df: pd.DataFrame, period: int = RSI_PERIOD) -> Optional[float]:
    closes = extract_close_series(df)
    if closes is None or len(closes) < period * 2: return None
    delta    = closes.diff().dropna()
    avg_gain = delta.clip(lower=0).ewm(com=period - 1, min_periods=period).mean()
    avg_loss = (-delta).clip(lower=0).ewm(com=period - 1, min_periods=period).mean()
    try:
        lg = scalar(avg_gain.iloc[-1])
        ll = scalar(avg_loss.iloc[-1])
    except Exception:
        return None
    if ll == 0: return 100.0
    if lg == 0: return 0.0
    return round(100.0 - (100.0 / (1.0 + lg / ll)), 2)


# ============= VOLATILITY LEVELS — Pine Script accurate =============

def calculate_volatility_levels(df: pd.DataFrame) -> Optional[Tuple[float, float, float, float]]:
    """
    Exact Pine Script VF-ST-EMA-CPR Weekly replication.
    Uses 11 weekly closes (close[1]..close[11]) → 10 log returns.
    Two-pass population variance: mean(r²) − mean(r)²
    """
    closes = extract_close_series(df)
    opens  = extract_open_series(df)
    if closes is None or opens is None: return None
    if len(closes) < MIN_WEEKLY_BARS or len(opens) < MIN_WEEKLY_BARS: return None
    try:
        dc_vals = [scalar(closes.iloc[-(i + 2)]) for i in range(WEEKLY_CLOSES_NEEDED)]
        dop     = scalar(opens.iloc[-1])
        if any(v <= 0 for v in dc_vals) or dop <= 0: return None
        dc = dc_vals[0]

        lr0      = [np.log(dc_vals[i] / dc_vals[i + 1]) for i in range(10)]
        mean_lr0 = sum(lr0) / 10
        vol0     = np.sqrt(max(sum(r*r for r in lr0)/10 - mean_lr0**2, 0.0))
        range11  = round(dc * vol0, ROUND_PRECI)
        doKdc    = dop if abs(dop - dc) > (0.382 * range11) else dc

        adj     = [doKdc] + dc_vals[1:]
        lr      = [np.log(adj[i] / adj[i + 1]) for i in range(10)]
        mean_lr = sum(lr) / 10
        vol     = np.sqrt(max(sum(r*r for r in lr)/10 - mean_lr**2, 0.0))
        range1  = round(doKdc * vol, ROUND_PRECI)

        return (
            round(doKdc + range1 * 0.236, ROUND_PRECI),
            round(doKdc + range1 * 0.382, ROUND_PRECI),
            round(doKdc - range1 * 0.236, ROUND_PRECI),
            round(doKdc - range1 * 0.382, ROUND_PRECI),
        )
    except Exception as e:
        log.debug("VF levels error: %s", e)
        return None


# ============= DAILY INDICATORS =============

def _true_range(df: pd.DataFrame) -> Optional[pd.Series]:
    h = extract_high_series(df)
    l = extract_low_series(df)
    c = extract_close_series(df)
    if h is None or l is None or c is None: return None
    common = h.index.intersection(l.index).intersection(c.index)
    if len(common) < 2: return None
    h, l, c = h.loc[common], l.loc[common], c.loc[common]
    cp = c.shift(1)
    return pd.concat([h - l, (h - cp).abs(), (l - cp).abs()], axis=1).max(axis=1)


def _wilder_atr(df, period=ATR_PERIOD):
    tr = _true_range(df)
    if tr is None or len(tr) < period * 2: return None
    return tr.ewm(com=period - 1, min_periods=period).mean()


def calculate_atr_percentile_rank(df, atr_period=ATR_PERIOD, window=ATR_PERCENTILE_WIN):
    atr = _wilder_atr(df, atr_period)
    if atr is None: return None
    clean = atr.dropna()
    if len(clean) < window: return None
    last_val = scalar(clean.iloc[-1])
    return round(float((clean.iloc[-window:] < last_val).sum()) / window * 100, 1)


def calculate_adx(df, period=ADX_PERIOD) -> Optional[Dict]:
    h = extract_high_series(df)
    l = extract_low_series(df)
    c = extract_close_series(df)
    if h is None or l is None or c is None: return None
    common = h.index.intersection(l.index).intersection(c.index)
    if len(common) < period * 3: return None
    h, l, c = h.loc[common], l.loc[common], c.loc[common]

    up, dn = h.diff(), (-l.diff())
    pdm = pd.Series(np.where((up > dn) & (up > 0), up, 0.0), index=common)
    mdm = pd.Series(np.where((dn > up) & (dn > 0), dn, 0.0), index=common)
    cp  = c.shift(1)
    tr  = pd.concat([h - l, (h - cp).abs(), (l - cp).abs()], axis=1).max(axis=1)
    atr = tr.ewm(com=period - 1, min_periods=period).mean()
    pdi = (pdm.ewm(com=period - 1, min_periods=period).mean() / atr) * 100
    mdi = (mdm.ewm(com=period - 1, min_periods=period).mean() / atr) * 100
    dx  = ((pdi - mdi).abs() / (pdi + mdi).replace(0, np.nan)) * 100
    adx = dx.ewm(com=period - 1, min_periods=period).mean()
    try:
        return {
            "adx":      round(scalar(adx.iloc[-1]), 2),
            "plus_di":  round(scalar(pdi.iloc[-1]),  2),
            "minus_di": round(scalar(mdi.iloc[-1]),  2),
        }
    except Exception:
        return None


def calculate_supertrend(df, factor=SUPERTREND_FACTOR, period=SUPERTREND_ATR) -> Optional[Dict]:
    h = extract_high_series(df)
    l = extract_low_series(df)
    c = extract_close_series(df)
    if h is None or l is None or c is None: return None
    common = h.index.intersection(l.index).intersection(c.index)
    if len(common) < period * 3: return None

    hv, lv, cv = h.loc[common].values, l.loc[common].values, c.loc[common].values
    n = len(cv)
    tr  = np.zeros(n)
    atr = np.zeros(n)
    for i in range(1, n):
        tr[i] = max(hv[i]-lv[i], abs(hv[i]-cv[i-1]), abs(lv[i]-cv[i-1]))
    if period <= n:
        atr[period-1] = np.mean(tr[1:period])
        alpha = 1.0 / period
        for i in range(period, n):
            atr[i] = atr[i-1]*(1-alpha) + tr[i]*alpha

    mid  = (hv + lv) / 2.0
    bu   = mid + factor * atr
    bl   = mid - factor * atr
    fu   = bu.copy()
    fl   = bl.copy()
    dirn = np.ones(n, dtype=int)

    for i in range(1, n):
        fu[i] = bu[i] if bu[i] < fu[i-1] or cv[i-1] > fu[i-1] else fu[i-1]
        fl[i] = bl[i] if bl[i] > fl[i-1] or cv[i-1] < fl[i-1] else fl[i-1]
        if dirn[i-1] == 1:
            dirn[i] = -1 if cv[i] > fu[i-1] else 1
        else:
            dirn[i] =  1 if cv[i] < fl[i-1] else -1

    last_dir = int(dirn[-1])
    last_st  = fl[-1] if last_dir == -1 else fu[-1]
    return {
        "value":   round(float(last_st),                             2),
        "direction": last_dir,
        "bullish": bool(last_dir == -1),
        "price":   round(float(cv[-1]),                              2),
        "gap_pct": round(abs(cv[-1] - last_st) / last_st * 100,     2),
    }


# ============= SCORE ENGINE =============

def compute_signal_score(
    price, levels, matched_levels, odd_deviation,
    supertrend, atr_rank, adx, daily_rsi, weekly_rsi,
) -> Dict:
    """
    100-point score engine:
      VF Level hit + type   30 pts
      Odd Square proximity  15 pts
      SuperTrend alignment  20 pts
      ATR Percentile Rank   20 pts
      ADX strength + DI     15 pts
    """
    buy_score = sell_score = 0
    breakdown = {}
    reasons   = []

    # 1. VF Level (30 pts)
    vf_buy  = [m for m in matched_levels if "BUY"  in m[0]]
    vf_sell = [m for m in matched_levels if "SELL" in m[0]]
    vf_conf = any("CONFIRM" in m[0] for m in matched_levels)
    if vf_buy:
        pts = 25 if vf_conf else 18
        buy_score += pts
        breakdown["vf_level"] = f"+{pts} BUY {'CONFIRM' if vf_conf else 'ABOVE'}"
        reasons.append(f"VF {'confirmed' if vf_conf else 'initial'} buy")
    elif vf_sell:
        pts = 25 if vf_conf else 18
        sell_score += pts
        breakdown["vf_level"] = f"+{pts} SELL {'CONFIRM' if vf_conf else 'BELOW'}"
        reasons.append(f"VF {'confirmed' if vf_conf else 'initial'} sell")
    else:
        breakdown["vf_level"] = "0"

    if matched_levels:
        min_dev  = min(abs(price - lv[1]) / lv[1] * 100 for lv in matched_levels)
        prox_pts = int(round(5 * max(0.0, 1.0 - min_dev / (THRESHOLD * 100))))
        if vf_buy:   buy_score  += prox_pts
        elif vf_sell: sell_score += prox_pts
        breakdown["vf_proximity"] = f"+{prox_pts}"

    # 2. Odd Square (15 pts)
    if odd_deviation is not None:
        odd_pts = int(round(15 * max(0.0, 1.0 - odd_deviation / ODD_SQUARE_TOLERANCE)))
        if buy_score >= sell_score: buy_score  += odd_pts
        else:                       sell_score += odd_pts
        breakdown["odd_square"] = f"+{odd_pts} (dev={odd_deviation:.2f}%)"
        reasons.append(f"OddSq ±{odd_deviation:.2f}%")
    else:
        breakdown["odd_square"] = "0"

    # 3. SuperTrend (20 pts)
    if supertrend:
        gap    = supertrend["gap_pct"]
        st_pts = 20 if gap > 0.5 else 10
        if supertrend["bullish"]:
            buy_score += st_pts
            breakdown["supertrend"] = f"+{st_pts} BULL ST={supertrend['value']} gap={gap}%"
            reasons.append("ST bullish")
        else:
            sell_score += st_pts
            breakdown["supertrend"] = f"+{st_pts} BEAR ST={supertrend['value']} gap={gap}%"
            reasons.append("ST bearish")
    else:
        breakdown["supertrend"] = "N/A"

    # 4. ATR Rank (20 pts)
    if atr_rank is not None:
        if atr_rank >= 60:   atr_pts, regime = 20, "EXPANDING"
        elif atr_rank >= 30: atr_pts, regime = 10, "NORMAL"
        else:                atr_pts, regime =  0, "SQUEEZE"
        if buy_score >= sell_score: buy_score  += atr_pts
        else:                       sell_score += atr_pts
        breakdown["atr_rank"] = f"+{atr_pts} {regime} rank={atr_rank:.0f}%"
        reasons.append(f"ATR {regime.lower()} ({atr_rank:.0f}th)")
    else:
        breakdown["atr_rank"] = "N/A"

    # 5. ADX (15 pts)
    if adx:
        av, pdi, mdi = adx["adx"], adx["plus_di"], adx["minus_di"]
        if av >= 40:   adx_pts = 15
        elif av >= 25: adx_pts = 10
        elif av >= 20: adx_pts =  5
        else:          adx_pts =  0
        di_bull = pdi > mdi
        if di_bull: buy_score  += adx_pts
        else:       sell_score += adx_pts
        breakdown["adx"] = f"+{adx_pts} {'BULL' if di_bull else 'BEAR'} ADX={av} +DI={pdi} -DI={mdi}"
        reasons.append(f"ADX={av:.0f} {'up' if di_bull else 'dn'}")
    else:
        breakdown["adx"] = "N/A"

    if buy_score >= sell_score:
        signal = "BUY"
        score  = min(buy_score, 100)
    else:
        signal = "SELL"
        score  = min(sell_score, 100)

    if score >= SCORE_STRONG:       confidence = "STRONG"
    elif score >= SCORE_MODERATE:   confidence = "MODERATE"
    elif score >= SCORE_WEAK:       confidence = "WEAK"
    else:                           confidence, signal = "NO SIGNAL", "NEUTRAL"

    return {
        "signal": signal, "score": score, "confidence": confidence,
        "breakdown": breakdown, "reason": " | ".join(reasons) or "—",
        "buy_raw": buy_score, "sell_raw": sell_score,
    }


# ============= SCREENER FILTERS =============

def find_odd_square_matches(stock_data: Dict, tolerance: float = ODD_SQUARE_TOLERANCE) -> Dict:
    matches = {}
    for symbol, data in stock_data.items():
        price = data["price"]
        for odd_num, square in ODD_SQUARES:
            if is_price_within_tolerance(price, square, tolerance):
                sector, industry = get_sector_industry(symbol)
                matches[symbol] = {
                    **data,
                    "odd_number":        odd_num,
                    "odd_square":        square,
                    "deviation_percent": round(abs(price - square) / square * 100, 2),
                    "sector":            sector,
                    "industry":          industry,
                }
                break
    return matches


def find_level_matches(stock_data: Dict, threshold: float = THRESHOLD) -> Dict:
    matches = {}
    for symbol, data in stock_data.items():
        if not data["has_levels"]: continue
        price = data["price"]
        ba, bc, sb, sc = data["levels"]
        hits = []
        if is_near_level(price, ba, threshold): hits.append(("BUY_ABOVE",    ba))
        if is_near_level(price, bc, threshold): hits.append(("BUY_CONFIRM",  bc))
        if is_near_level(price, sb, threshold): hits.append(("SELL_BELOW",   sb))
        if is_near_level(price, sc, threshold): hits.append(("SELL_CONFIRM", sc))
        if hits:
            sector, industry = get_sector_industry(symbol)
            matches[symbol] = {
                **data,
                "matched_levels": hits,
                "deviation": min(abs(price - lv[1]) / lv[1] * 100 for lv in hits),
                "sector":    sector,
                "industry":  industry,
            }
    return matches


# ============= BATCH DATA FETCHER — parallel per stock =============

class BatchDataFetcher:
    def __init__(self, max_workers: int = MAX_WORKERS):
        self.max_workers = max_workers

    def _fetch_weekly(self, yf_symbol: str):
        return yf.download(yf_symbol, period="6mo", interval="1wk",
                           progress=False, auto_adjust=True, threads=False)

    def _fetch_daily(self, yf_symbol: str):
        return yf.download(yf_symbol, period="1y", interval="1d",
                           progress=False, auto_adjust=True, threads=False)

    def fetch_single_stock(self, symbol: str) -> Tuple[str, Optional[Dict]]:
        try:
            yf_symbol = f"{symbol}.NS"

            # ── Fetch weekly and daily in parallel ───────────────────────
            with ThreadPoolExecutor(max_workers=2) as inner:
                f_week  = inner.submit(self._fetch_weekly, yf_symbol)
                f_daily = inner.submit(self._fetch_daily,  yf_symbol)
                df_week = f_week.result(timeout=30)
                df_day  = f_daily.result(timeout=30)

            # ── Weekly processing ────────────────────────────────────────
            if df_week is None or df_week.empty or is_data_stale(df_week):
                return symbol, None
            closes_week = extract_close_series(df_week)
            if closes_week is None or closes_week.empty:
                return symbol, None
            current_price = scalar(closes_week.iloc[-1])
            if current_price <= 0:
                return symbol, None

            weekly_rsi = calculate_rsi(df_week)
            levels     = calculate_volatility_levels(df_week)

            # ── Daily processing ─────────────────────────────────────────
            daily_rsi = supertrend = atr_rank = adx = None
            if df_day is not None and not df_day.empty:
                daily_rsi  = calculate_rsi(df_day)
                supertrend = calculate_supertrend(df_day)
                atr_rank   = calculate_atr_percentile_rank(df_day)
                adx        = calculate_adx(df_day)

            sector, industry = get_sector_industry(symbol)

            return symbol, {
                "price":      current_price,
                "levels":     levels,
                "has_levels": levels is not None,
                "weekly_rsi": weekly_rsi,
                "daily_rsi":  daily_rsi,
                "supertrend": supertrend,
                "atr_rank":   atr_rank,
                "adx":        adx,
                "sector":     sector,
                "industry":   industry,
            }

        except Exception as e:
            log.exception("Error fetching %s: %s", symbol, e)
            return symbol, None

    def fetch_batch(self, symbols: List[str], batch_label: str = "") -> Dict[str, Dict]:
        stock_data: Dict[str, Dict] = {}
        total = len(symbols)
        log.info("Batch %s — %d stocks  |  %d outer threads (2 inner per stock)",
                 batch_label, total, self.max_workers)

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures   = {executor.submit(self.fetch_single_stock, s): s for s in symbols}
            completed = 0
            for future in as_completed(futures):
                symbol    = futures[future]
                completed += 1
                try:
                    sym, data = future.result(timeout=60)
                    if data:
                        stock_data[sym] = data
                        st  = data.get("supertrend")
                        ar  = data.get("atr_rank")
                        adx = data.get("adx")
                        log.info(
                            "  ✓  %-15s (%d/%d)  Rs %-9.2f  "
                            "ST=%-4s  ATR%%=%-5s  ADX=%-5s  "
                            "W-RSI=%-5s  D-RSI=%-5s  [%s / %s]",
                            sym, completed, total, data["price"],
                            "BULL" if st and st["bullish"] else "BEAR" if st else "N/A",
                            f"{ar:.0f}" if ar is not None else "N/A",
                            f"{adx['adx']:.1f}" if adx else "N/A",
                            _fmt(data.get("weekly_rsi")),
                            _fmt(data.get("daily_rsi")),
                            data["sector"], data["industry"],
                        )
                    else:
                        log.info("  ✗  %-15s (%d/%d)  no data", symbol, completed, total)
                except Exception as e:
                    log.exception("  !!  %s — %s", symbol, e)
                if completed % 20 == 0:
                    time.sleep(0.3)

        return stock_data


# ============= EXCEL OUTPUT =============

def _write_excel(filename: str, sheet_name: str, df: pd.DataFrame) -> None:
    with pd.ExcelWriter(filename, engine="openpyxl") as writer:
        clean = sanitize_sheet_name(sheet_name)
        df.to_excel(writer, sheet_name=clean, index=False)
        autofit_worksheet_columns(writer.sheets[clean])
    log.info("Saved: %s  (%d rows)", filename, len(df))


def _write_excel_to_bytes(sheet_name: str, df: pd.DataFrame) -> bytes:
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        clean = sanitize_sheet_name(sheet_name)
        df.to_excel(writer, sheet_name=clean, index=False)
        autofit_worksheet_columns(writer.sheets[clean])
    return buf.getvalue()


def _base_cols(d: Dict, sc: Dict) -> Dict:
    """Common columns shared by all 3 Excel sheets."""
    return {
        "Symbol":            d.get("symbol", ""),
        "Sector":            d.get("sector",   "Other"),
        "Industry":          d.get("industry", "Other"),
        "Current Price":     d["price"],
        "Signal":            sc.get("signal",     "—"),
        "Score (0-100)":     sc.get("score",      "—"),
        "Confidence":        sc.get("confidence", "—"),
        "Daily RSI":         _fmt(d.get("daily_rsi")),
        "Daily RSI Status":  _rsi_status(d.get("daily_rsi")),
        "Weekly RSI":        _fmt(d.get("weekly_rsi")),
        "Weekly RSI Status": _rsi_status(d.get("weekly_rsi")),
        "SuperTrend":        "BULL" if d.get("supertrend") and d["supertrend"]["bullish"] else
                             "BEAR" if d.get("supertrend") else "N/A",
        "ST Value":          _fmt(d["supertrend"]["value"]   if d.get("supertrend") else None),
        "ST Gap %":          _fmt(d["supertrend"]["gap_pct"] if d.get("supertrend") else None),
        "ATR Rank %":        _fmt(d.get("atr_rank"), ".1f"),
        "ADX":               _fmt(d["adx"]["adx"]      if d.get("adx") else None),
        "+DI":               _fmt(d["adx"]["plus_di"]  if d.get("adx") else None),
        "-DI":               _fmt(d["adx"]["minus_di"] if d.get("adx") else None),
        "Buy Above":         d["levels"][0] if d.get("levels") else "N/A",
        "Buy Confirm":       d["levels"][1] if d.get("levels") else "N/A",
        "Sell Below":        d["levels"][2] if d.get("levels") else "N/A",
        "Sell Confirm":      d["levels"][3] if d.get("levels") else "N/A",
    }


def create_excel_files(
    odd_matches:   Dict,
    level_matches: Dict,
    both_matches:  set,
    scored:        Dict,
    timestamp:     str,
) -> Tuple[
    Optional[str], Optional[str], Optional[str],
    Optional[bytes], Optional[bytes], Optional[bytes],
]:
    odd_file = level_file = both_file = None
    odd_bytes = level_bytes = both_bytes = None

    # ── Sheet 1: Odd Square matches ─────────────────────────────────────
    if odd_matches:
        rows = []
        for sym, d in odd_matches.items():
            d["symbol"] = sym
            sc = scored.get(sym, {})
            row = _base_cols(d, sc)
            row.update({
                "Odd Number":  d["odd_number"],
                "Odd Square":  d["odd_square"],
                "Deviation %": d["deviation_percent"],
            })
            rows.append(row)
        df = pd.DataFrame(rows).sort_values(["Score (0-100)", "Deviation %"],
                                            ascending=[False, True])
        if SAVE_LOCAL_FILES:
            odd_file = f"odd_square_matches_{timestamp}.xlsx"
            _write_excel(odd_file, "Odd Square Matches", df)
        else:
            odd_bytes = _write_excel_to_bytes("Odd Square Matches", df)

    # ── Sheet 2: VF Level matches ───────────────────────────────────────
    if level_matches:
        rows = []
        for sym, d in level_matches.items():
            d["symbol"] = sym
            sc = scored.get(sym, {})
            row = _base_cols(d, sc)
            row.update({
                "Matched Levels":            ", ".join(m[0] for m in d["matched_levels"]),
                "Nearest Level Deviation %": round(d["deviation"], 2),
            })
            rows.append(row)
        df = pd.DataFrame(rows).sort_values(["Score (0-100)", "Nearest Level Deviation %"],
                                            ascending=[False, True])
        if SAVE_LOCAL_FILES:
            level_file = f"volatility_level_matches_{timestamp}.xlsx"
            _write_excel(level_file, "Level Matches", df)
        else:
            level_bytes = _write_excel_to_bytes("Level Matches", df)

    # ── Sheet 3: Both criteria — full score breakdown ───────────────────
    if both_matches:
        rows = []
        for sym in both_matches:
            od = odd_matches[sym]
            ld = level_matches[sym]
            sc = scored.get(sym, {})
            if sc.get("confidence") == "NO SIGNAL":
                continue
            od["symbol"] = sym
            bd = sc.get("breakdown", {})
            row = _base_cols(od, sc)
            row.update({
                "Reason":                    sc.get("reason", "—"),
                "Score: VF Level":           bd.get("vf_level",    "—"),
                "Score: VF Proximity":       bd.get("vf_proximity","—"),
                "Score: Odd Square":         bd.get("odd_square",  "—"),
                "Score: SuperTrend":         bd.get("supertrend",  "—"),
                "Score: ATR Rank":           bd.get("atr_rank",    "—"),
                "Score: ADX":                bd.get("adx",         "—"),
                "Odd Square":                f"{od['odd_number']}^2={od['odd_square']}",
                "Odd Sq Deviation %":        od["deviation_percent"],
                "Matched VF Levels":         ", ".join(m[0] for m in ld["matched_levels"]),
                "VF Level Deviation %":      round(ld["deviation"], 2),
            })
            rows.append(row)
        if rows:
            df = pd.DataFrame(rows).sort_values(
                ["Signal", "Score (0-100)"], ascending=[True, False])
            if SAVE_LOCAL_FILES:
                both_file = f"both_criteria_signals_{timestamp}.xlsx"
                _write_excel(both_file, "Buy Sell Signals", df)
            else:
                both_bytes = _write_excel_to_bytes("Buy Sell Signals", df)
        else:
            log.warning("All both-criteria hits scored below threshold — no signal file")

    return odd_file, level_file, both_file, odd_bytes, level_bytes, both_bytes


# ============= TELEGRAM — beautified =============

def _tg_post(endpoint: str, **kwargs) -> bool:
    if not BOT_TOKEN or not CHAT_ID_LIST:
        return False
    
    success = True
    for chat_id in CHAT_ID_LIST:
        try:
            # Copy kwargs to avoid modifying original
            kwargs_copy = kwargs.copy()
            
            # Handle both json and data/files differently
            if 'json' in kwargs_copy:
                kwargs_copy['json']['chat_id'] = chat_id
            elif 'data' in kwargs_copy:
                kwargs_copy['data']['chat_id'] = chat_id
            else:
                # If neither, add json with chat_id
                kwargs_copy['json'] = {'chat_id': chat_id}
            
            resp = requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/{endpoint}",
                timeout=60, **kwargs_copy
            )
            if not resp.ok:
                log.warning(f"Telegram {endpoint} to {chat_id}: {resp.status_code}")
                success = False
        except Exception as e:
            log.exception(f"Telegram {endpoint} to {chat_id} raised: {e}")
            success = False
    
    return success


def send_telegram_message(text: str) -> bool:
    """Send message to all configured chat IDs"""
    success = True
    for chat_id in CHAT_ID_LIST:
        ok = _tg_post("sendMessage", json={"chat_id": chat_id, "text": text})
        if not ok:
            success = False
    return success

def send_telegram_file(filepath: str, caption: str = "") -> bool:
    """Send file to all configured chat IDs"""
    if not os.path.exists(filepath):
        return False
    
    success = True
    with open(filepath, "rb") as fh:
        for chat_id in CHAT_ID_LIST:
            ok = _tg_post(
                "sendDocument",
                data={"chat_id": chat_id, "caption": caption[:1024]},
                files={"document": (
                    os.path.basename(filepath), fh,
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )},
            )
            if not ok:
                success = False
            fh.seek(0)  # Reset file pointer for next upload
    return success

def send_telegram_file_bytes(file_bytes: bytes, filename: str, caption: str = "") -> bool:
    """Send file bytes to all configured chat IDs"""
    if not BOT_TOKEN or not CHAT_ID_LIST:
        return False
    
    success = True
    for chat_id in CHAT_ID_LIST:
        try:
            resp = requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument",
                data={"chat_id": chat_id, "caption": caption[:1024]},
                files={"document": (
                    filename, BytesIO(file_bytes),
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )},
                timeout=60,
            )
            if not resp.ok:
                success = False
        except Exception as e:
            log.exception(f"Telegram bytes upload to {chat_id}: {e}")
            success = False
    
    return success


def _signal_emoji(signal: str) -> str:
    return {"BUY": "🟢", "SELL": "🔴", "NEUTRAL": "⚪"}.get(signal, "⚪")


def _conf_emoji(conf: str) -> str:
    return {"STRONG": "🔥", "MODERATE": "✅", "WEAK": "⚠️"}.get(conf, "")


def build_telegram_summary(
    odd_matches:   Dict,
    level_matches: Dict,
    both_matches:  set,
    scored:        Dict,
    run_start:     datetime,
    run_end:       datetime,
    total_screened: int,
    data_retrieved: int,
) -> str:
    """
    Build a beautifully formatted Telegram message.
    """
    now_str   = run_end.strftime("%d %b %Y  %H:%M:%S IST")
    elapsed   = (run_end - run_start).seconds
    mode_str  = "☁️ Cloud / GitHub Actions" if IS_CLOUD else "💻 Local Desktop"
    date_str  = run_end.strftime("%A, %d %B %Y")

    # Header
    lines = [
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "📊  *AIScan India v5*  |  NSE F&O",
        f"🗓  {date_str}",
        f"🕐  Run completed: {now_str}",
        f"⏱  Time taken: {elapsed}s",
        f"🖥  Mode: {mode_str}",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "",
        "📋  *EXECUTION SUMMARY*",
        f"  • Stocks screened     : {total_screened}",
        f"  • Data retrieved      : {data_retrieved}",
        f"  • Odd Square matches  : {len(odd_matches)}  (±{ODD_SQUARE_TOLERANCE}%)",
        f"  • VF Level matches    : {len(level_matches)}  (±{int(THRESHOLD*100)}%)",
        f"  • Both-criteria hits  : {len(both_matches)}",
        "",
    ]

    # Actionable signals
    actionable = sorted(
        [(sym, scored[sym]) for sym in both_matches
         if scored.get(sym, {}).get("confidence") in ("STRONG", "MODERATE")],
        key=lambda x: x[1]["score"], reverse=True,
    )
    weak_sigs = [
        (sym, scored[sym]) for sym in both_matches
        if scored.get(sym, {}).get("confidence") == "WEAK"
    ]

    if actionable:
        buy_sigs  = [(s, sc) for s, sc in actionable if sc["signal"] == "BUY"]
        sell_sigs = [(s, sc) for s, sc in actionable if sc["signal"] == "SELL"]

        lines += [
            f"🎯  *ACTIONABLE SIGNALS  ({len(actionable)})*",
            f"   🟢 BUY: {len(buy_sigs)}   🔴 SELL: {len(sell_sigs)}",
            "─────────────────────────────",
        ]

        for sym, sc in actionable:
            od       = odd_matches[sym]
            ld       = level_matches[sym]
            sector   = od.get("sector", "Other")
            industry = od.get("industry", "Other")
            price    = od["price"]
            drsi     = od.get("daily_rsi")
            wrsi     = od.get("weekly_rsi")
            st       = od.get("supertrend")
            atr_r    = od.get("atr_rank")
            adx_d    = od.get("adx")
            lvls     = ", ".join(m[0] for m in ld["matched_levels"])

            sig_e  = _signal_emoji(sc["signal"])
            conf_e = _conf_emoji(sc["confidence"])

            lines += [
                f"{sig_e} *{sym}*  {conf_e} {sc['confidence']}  [{sc['score']}/100]",
                f"   💰 Price: ₹{price:.2f}   📂 {sector} / {industry}",
                f"   📈 D-RSI: {_fmt(drsi)} {_rsi_emoji(drsi)} [{_rsi_status(drsi)}]"
                f"   📉 W-RSI: {_fmt(wrsi)} {_rsi_emoji(wrsi)} [{_rsi_status(wrsi)}]",
                f"   🔁 ST: {'🟢 BULL' if st and st['bullish'] else '🔴 BEAR' if st else 'N/A'}"
                f"   ATR%: {_fmt(atr_r, '.0f') if atr_r else 'N/A'}"
                f"   ADX: {_fmt(adx_d['adx'],'.1f') if adx_d else 'N/A'}",
                f"   🎯 VF: {lvls}",
                f"   💡 {sc['reason']}",
                "",
            ]
    else:
        lines += ["🎯  *ACTIONABLE SIGNALS*", "   No strong/moderate signals today.", ""]

    if weak_sigs:
        lines += [f"⚠️  *WEAK SIGNALS ({len(weak_sigs)})* — watch list:"]
        for sym, sc in weak_sigs:
            od = odd_matches[sym]
            lines.append(
                f"   ⚪ {sym}  [{sc['score']}/100]"
                f"  ₹{od['price']:.0f}"
                f"  {od.get('sector','')}"
                f"  D-RSI:{_fmt(od.get('daily_rsi'))} W-RSI:{_fmt(od.get('weekly_rsi'))}"
            )
        lines.append("")

    lines += [
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"📎 Score bands: 🔥≥{SCORE_STRONG} STRONG  ✅≥{SCORE_MODERATE} MOD  ⚠️≥{SCORE_WEAK} WEAK",
        "📁 Excel files attached below",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
    ]

    return "\n".join(lines)


def send_telegram_summary(
    odd_matches:    Dict,
    level_matches:  Dict,
    both_matches:   set,
    scored:         Dict,
    run_start:      datetime,
    run_end:        datetime,
    total_screened: int,
    data_retrieved: int,
    odd_file=None,  level_file=None,  both_file=None,
    odd_bytes=None, level_bytes=None, both_bytes=None,
) -> None:
    if not BOT_TOKEN or not CHAT_ID:
        log.debug("Telegram not configured")
        return

    msg = build_telegram_summary(
        odd_matches, level_matches, both_matches, scored,
        run_start, run_end, total_screened, data_retrieved,
    )
    send_telegram_message(msg)
    time.sleep(2)

    now_str = run_end.strftime("%d%b%Y_%H%M")
    if IS_CLOUD:
        for byt, fname, cap in [
            (odd_bytes,   f"odd_square_{now_str}.xlsx",
             f"📊 Odd Square Matches ({len(odd_matches)} stocks)"),
            (level_bytes, f"vf_levels_{now_str}.xlsx",
             f"📉 VF Level Matches ({len(level_matches)} stocks)"),
            (both_bytes,  f"signals_{now_str}.xlsx",
             f"🎯 Buy/Sell Signals ({len(both_matches)} hits)"),
        ]:
            if byt:
                send_telegram_file_bytes(byt, fname, cap)
    else:
        for fp, cap in [
            (odd_file,   f"📊 Odd Square Matches ({len(odd_matches)} stocks)"),
            (level_file, f"📉 VF Level Matches ({len(level_matches)} stocks)"),
            (both_file,  f"🎯 Buy/Sell Signals ({len(both_matches)} hits)"),
        ]:
            if fp:
                ok = send_telegram_file(fp, caption=cap)
                log.info("%s → Telegram: %s", os.path.basename(fp),
                         "✓ sent" if ok else "✗ failed")


# ============= MAIN =============

def main(dry_run: bool = False, override_symbols: Optional[List[str]] = None,
         force_refresh: bool = False) -> None:

    run_start = datetime.now()

    print("=" * 80)
    print(f"  AISCAN INDIA v5  —  NSE F&O Screener  [{RUN_MODE.upper()} MODE]")
    print("  VF Levels (Pine) + Odd Squares + SuperTrend + ATR Rank + ADX → Score 0-100")
    print("  Parallel fetch: weekly + daily per stock simultaneously")
    print("=" * 80)

    fetcher = BatchDataFetcher(max_workers=MAX_WORKERS)

    # 1. F&O stock list (Kite API + daily cache)
    log.info("[1/6] Fetching F&O stock list (Kite API, daily cache)...")
    symbols = override_symbols or get_fno_stocks(force_refresh=force_refresh)
    if dry_run:
        symbols = symbols[:10]
        log.info("Dry-run: %d symbols only", len(symbols))
    log.info("Stocks to screen: %d", len(symbols))

    # 2. Parallel OHLC fetch
    log.info("[2/6] Fetching weekly + daily OHLC in parallel...")
    fetch_start = time.time()
    batches  = [symbols[i:i + BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]
    all_data: Dict[str, Dict] = {}
    for i, batch in enumerate(batches, 1):
        all_data.update(fetcher.fetch_batch(batch, f"{i}/{len(batches)}"))
    fetch_elapsed = time.time() - fetch_start
    log.info("Data fetch done: %d / %d stocks in %.1fs",
             len(all_data), len(symbols), fetch_elapsed)

    if not all_data:
        log.error("No data retrieved — exiting.")
        sys.exit(0)

    # 3. Odd square screening
    log.info("[3/6] Odd Square matches (±%.1f%%)...", ODD_SQUARE_TOLERANCE)
    odd_matches = find_odd_square_matches(all_data, ODD_SQUARE_TOLERANCE)
    log.info("Odd Square matches: %d", len(odd_matches))

    # 4. VF level screening
    log.info("[4/6] VF Level matches (±%.0f%%)...", THRESHOLD * 100)
    level_matches = find_level_matches(all_data, THRESHOLD)
    log.info("Level matches: %d", len(level_matches))

    # 5. Cross-reference + scoring
    both_matches = set(odd_matches) & set(level_matches)
    log.info("[5/6] Both-criteria: %d → scoring...", len(both_matches))
    scored: Dict[str, Dict] = {}
    for sym in both_matches:
        od = odd_matches[sym]
        ld = level_matches[sym]
        scored[sym] = compute_signal_score(
            price          = od["price"],
            levels         = od.get("levels"),
            matched_levels = ld["matched_levels"],
            odd_deviation  = od["deviation_percent"],
            supertrend     = od.get("supertrend"),
            atr_rank       = od.get("atr_rank"),
            adx            = od.get("adx"),
            daily_rsi      = od.get("daily_rsi"),
            weekly_rsi     = od.get("weekly_rsi"),
        )
        sc = scored[sym]
        log.info("  SCORE  %-12s  %-4s  %3d/100  %-10s  %s",
                 sym, sc["signal"], sc["score"], sc["confidence"], sc["reason"])

    # 6. Write Excel files
    log.info("[6/6] Writing output files...")
    timestamp = run_start.strftime("%Y%m%d_%H%M%S")
    odd_file, level_file, both_file, odd_bytes, level_bytes, both_bytes = create_excel_files(
        odd_matches, level_matches, both_matches, scored, timestamp
    )

    run_end = datetime.now()

    # Console summary
    actionable = sorted(
        [(s, scored[s]) for s in both_matches
         if scored.get(s, {}).get("confidence") in ("STRONG", "MODERATE")],
        key=lambda x: x[1]["score"], reverse=True,
    )
    elapsed_total = (run_end - run_start).seconds

    print("\n" + "=" * 80)
    print("  SUMMARY")
    print("=" * 80)
    print(f"  Run mode            : {RUN_MODE.upper()}")
    print(f"  Run time            : {run_start.strftime('%H:%M:%S')} → "
          f"{run_end.strftime('%H:%M:%S')}  ({elapsed_total}s total)")
    print(f"  Data fetch          : {fetch_elapsed:.1f}s")
    print(f"  Stocks screened     : {len(symbols)}")
    print(f"  Data retrieved      : {len(all_data)}")
    print(f"  Odd Square matches  : {len(odd_matches)}")
    print(f"  Level matches       : {len(level_matches)}")
    print(f"  Both-criteria hits  : {len(both_matches)}")
    print(f"  Actionable signals  : {len(actionable)}")
    print(f"  Score bands         : >={SCORE_STRONG} STRONG  "
          f">={SCORE_MODERATE} MODERATE  >={SCORE_WEAK} WEAK")

    if actionable:
        rows = []
        for sym, sc in actionable:
            od = odd_matches[sym]
            rows.append({
                "Symbol":     sym,
                "Sector":     od.get("sector", "Other"),
                "Industry":   od.get("industry", "Other"),
                "Signal":     sc["signal"],
                "Score":      sc["score"],
                "Confidence": sc["confidence"],
                "Price":      od["price"],
                "ST":         "BULL" if od.get("supertrend") and od["supertrend"]["bullish"] else "BEAR",
                "ATR%":       _fmt(od.get("atr_rank"), ".0f"),
                "ADX":        _fmt(od["adx"]["adx"] if od.get("adx") else None, ".1f"),
                "D-RSI":      _fmt(od.get("daily_rsi")),
                "D-Status":   _rsi_status(od.get("daily_rsi")),
                "W-RSI":      _fmt(od.get("weekly_rsi")),
                "W-Status":   _rsi_status(od.get("weekly_rsi")),
            })
        df_sig = pd.DataFrame(rows)
        buys  = len(df_sig[df_sig["Signal"] == "BUY"])
        sells = len(df_sig[df_sig["Signal"] == "SELL"])
        print(f"\n  BUY: {buys}  |  SELL: {sells}\n")
        print(df_sig.to_string(index=False))
    else:
        print("\n  No actionable signals today.")

    # Telegram
    send_telegram_summary(
        odd_matches, level_matches, both_matches, scored,
        run_start, run_end, len(symbols), len(all_data),
        odd_file, level_file, both_file,
        odd_bytes, level_bytes, both_bytes,
    )

    print("\nOutput files:")
    if SAVE_LOCAL_FILES:
        for f in [odd_file, level_file, both_file]:
            if f: print(f"  ✓ {f}")
    else:
        print("  ✓ All files sent to Telegram (no local save)")
    print("=" * 80)


# ============= ENTRY POINT =============

if __name__ == "__main__":
    required = ["pandas", "numpy", "yfinance", "openpyxl", "requests"]
    missing  = [lib for lib in required
                if not __import__("importlib").util.find_spec(lib)]
    if missing:
        print(f"Missing: {', '.join(missing)}")
        print("Install: pip install " + " ".join(missing))
        sys.exit(1)

    parser = argparse.ArgumentParser(description="AIScan India v5 — NSE F&O Screener")
    parser.add_argument("--dry-run",       action="store_true",
                        help="Test with first 10 symbols only")
    parser.add_argument("--symbols",       nargs="+", metavar="SYM",
                        help="Override symbols  e.g. --symbols RELIANCE TCS")
    parser.add_argument("--local",         action="store_true",
                        help="Force local mode (save 3 Excel files to disk)")
    parser.add_argument("--cloud",         action="store_true",
                        help="Force cloud mode (Telegram only, no disk save)")
    parser.add_argument("--refresh-cache", action="store_true",
                        help="Force refresh of F&O symbol cache from Kite API")
    args = parser.parse_args()

    if args.local:
        os.environ["RUN_MODE"] = "local"
        IS_CLOUD         = False
        SAVE_LOCAL_FILES = True
    elif args.cloud:
        os.environ["RUN_MODE"] = "cloud"
        IS_CLOUD         = True
        SAVE_LOCAL_FILES = False

    main(
        dry_run         = args.dry_run,
        override_symbols= args.symbols,
        force_refresh   = args.refresh_cache,
    )

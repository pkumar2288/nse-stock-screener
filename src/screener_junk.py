"""
AIScan India v7.1 — Complete Rewrite with All Fixes + 15-min RSI + VWAP + Full Telegram
────────────────────────────────────────────────────────────────────────────────
FIXES from v6:
  ✓ self.mcx_symbols init bug fixed
  ✓ F&O detection O(n²) → pre-built set (O(n))
  ✓ RSI Wilder smoothing corrected
  ✓ Supertrend initial trend fixed (price vs midpoint)
  ✓ API calls halved via daily→weekly resampling
  ✓ Sector inference replaced with symbol-lookup dict
  ✓ ATR scoring made volatility-only (non-directional)
  ✓ JWT token expiry handling with re-auth on 401
  ✓ Retry logic with exponential backoff
  ✓ create_excel_files returns 3 values cleanly
  ✓ Config dataclass for all tunable parameters

NEW in v7:
  ✓ 15-minute RSI
  ✓ VWAP (intraday, 15-min bars)
  ✓ Price vs VWAP signal scoring (+10 buy / +10 sell)
  ✓ Volume confirmation in signal scoring
  ✓ Local SQLite cache for OHLCV (incremental updates)

NEW in v7.1:
  ✓ Rich Telegram messages — one card per actionable signal
  ✓ Every indicator included: RSI-D/W/15m, VWAP, SuperTrend,
    ADX/+DI/-DI, ATR Rank, Volume Ratio, VF Levels, Odd Square
  ✓ Session summary message (totals + top-5 table)
  ✓ Batched multi-chat dispatch with flood-control delay
  ✓ Graceful fallback if Telegram not configured
────────────────────────────────────────────────────────────────────────────────
"""

import sys
import time
import json
import logging
import argparse
import sqlite3
import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from typing import List, Dict, Tuple, Optional, Set
import os
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import re
import io as _io

import pyotp
import base64

# ─────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────
_stream_handler = logging.StreamHandler(
    stream=_io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    if hasattr(sys.stdout, "buffer") else sys.stdout
)
_stream_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logging.basicConfig(
    level=logging.INFO,
    handlers=[
        _stream_handler,
        logging.FileHandler("screener_v7.log", mode="a", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
#  CONFIG DATACLASS  (inject different sets for backtesting)
# ─────────────────────────────────────────────
@dataclass
class ScreenerConfig:
    # Precision
    round_preci: int = 2

    # VF / Volatility levels
    threshold: float = 0.01          # ±1 % price proximity
    odd_square_tolerance: float = 0.5

    # Indicator periods
    rsi_period: int = 14
    atr_period: int = 14
    atr_percentile_win: int = 52
    adx_period: int = 14
    supertrend_factor: int = 3
    supertrend_atr: int = 10

    # Weekly VF
    weekly_closes_needed: int = 11
    min_weekly_bars: int = 13

    # Intraday
    intraday_rsi_period: int = 14    # applied on 15-min bars
    vwap_bars: int = 26              # ~6.5 h of 15-min bars (one session)

    # Score thresholds
    score_strong: int = 75
    score_moderate: int = 55
    score_weak: int = 40

    # Parallel workers
    max_workers: int = 3
    request_delay: float = 1.5
    max_retries: int = 3

    # Cache
    symbol_cache_file: str = "angel_symbols_v7.json"
    ohlcv_db_file: str = "ohlcv_cache_v7.db"


CFG = ScreenerConfig()

ODD_SQUARES: List[Tuple[int, int]] = [(n, n * n) for n in range(1, 301, 2)]

# ─────────────────────────────────────────────
#  ENVIRONMENT / CREDENTIALS
# ─────────────────────────────────────────────
RUN_MODE         = os.environ.get("RUN_MODE", "local")
IS_CLOUD         = RUN_MODE in ("cloud", "github")
SAVE_LOCAL_FILES = not IS_CLOUD

BOT_TOKEN    = os.environ.get("TELEGRAM_BOT_TOKEN", "")
CHAT_ID      = os.environ.get("TELEGRAM_CHAT_ID", "")
CHAT_ID_LIST = [c.strip() for c in CHAT_ID.split(",") if CHAT_ID]

ANGEL_API_KEY     = os.environ.get("ANGEL_API_KEY", "JvgWluG4")
ANGEL_CLIENT_ID   = os.environ.get("ANGEL_CLIENT_ID", "")
ANGEL_PASSWORD    = os.environ.get("ANGEL_PASSWORD", "")
ANGEL_TOTP_SECRET = os.environ.get("ANGEL_TOTP_SECRET", "")

ANGEL_BASE_URL       = "https://apiconnect.angelone.in"
ANGEL_LOGIN_URL      = f"{ANGEL_BASE_URL}/rest/auth/angelbroking/user/v1/loginByPassword"
ANGEL_HISTORICAL_URL = f"{ANGEL_BASE_URL}/rest/secure/angelbroking/historical/v1/getCandleData"
ANGEL_MASTER_URL     = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"

# ─────────────────────────────────────────────
#  SECTOR LOOKUP  (symbol → (sector, industry))
#  Explicit mapping avoids substring false-positives
# ─────────────────────────────────────────────
SECTOR_MAP: Dict[str, Tuple[str, str]] = {
    # Banking
    "HDFCBANK":    ("Banking", "Private Sector Bank"),
    "ICICIBANK":   ("Banking", "Private Sector Bank"),
    "KOTAKBANK":   ("Banking", "Private Sector Bank"),
    "AXISBANK":    ("Banking", "Private Sector Bank"),
    "INDUSINDBK":  ("Banking", "Private Sector Bank"),
    "FEDERALBNK":  ("Banking", "Private Sector Bank"),
    "IDFCFIRSTB":  ("Banking", "Private Sector Bank"),
    "BANDHANBNK":  ("Banking", "Private Sector Bank"),
    "SBIN":        ("Banking", "Public Sector Bank"),
    "BANKBARODA":  ("Banking", "Public Sector Bank"),
    "PNB":         ("Banking", "Public Sector Bank"),
    "CANBK":       ("Banking", "Public Sector Bank"),
    "UNIONBANK":   ("Banking", "Public Sector Bank"),
    # Financial Services
    "BAJFINANCE":  ("Financial Services", "NBFC"),
    "BAJAJFINSV":  ("Financial Services", "Financial Holding"),
    "CHOLAFIN":    ("Financial Services", "NBFC"),
    "MUTHOOTFIN":  ("Financial Services", "Gold Finance"),
    "M&MFIN":      ("Financial Services", "NBFC"),
    "LICSGFIN":    ("Financial Services", "Housing Finance"),
    "HDFCLIFE":    ("Financial Services", "Life Insurance"),
    "SBILIFE":     ("Financial Services", "Life Insurance"),
    "ICICIPRULI":  ("Financial Services", "Life Insurance"),
    "ICICIGI":     ("Financial Services", "General Insurance"),
    "SBICARD":     ("Financial Services", "Credit Card"),
    # IT
    "TCS":         ("IT", "IT Services"),
    "INFY":        ("IT", "IT Services"),
    "WIPRO":       ("IT", "IT Services"),
    "HCLTECH":     ("IT", "IT Services"),
    "TECHM":       ("IT", "IT Services"),
    "LTIM":        ("IT", "IT Services"),
    "MPHASIS":     ("IT", "IT Services"),
    "PERSISTENT":  ("IT", "IT Services"),
    "COFORGE":     ("IT", "IT Services"),
    # Energy / Oil & Gas
    "RELIANCE":    ("Energy", "Integrated Oil & Gas"),
    "ONGC":        ("Energy", "Exploration & Production"),
    "IOC":         ("Energy", "Refining & Marketing"),
    "BPCL":        ("Energy", "Refining & Marketing"),
    "HINDPETRO":   ("Energy", "Refining & Marketing"),
    "GAIL":        ("Energy", "Gas Transmission"),
    "MGL":         ("Energy", "City Gas Distribution"),
    "IGL":         ("Energy", "City Gas Distribution"),
    "PETRONET":    ("Energy", "LNG"),
    # Power
    "NTPC":        ("Power", "Power Generation"),
    "POWERGRID":   ("Power", "Power Transmission"),
    "TATAPOWER":   ("Power", "Integrated Power"),
    "ADANIPOWER":  ("Power", "Power Generation"),
    "ADANIGREEN":  ("Power", "Renewable Energy"),
    "TORNTPOWER":  ("Power", "Integrated Power"),
    "CESC":        ("Power", "Integrated Power"),
    # Automobiles
    "MARUTI":      ("Automobile", "Passenger Cars"),
    "TATAMOTORS":  ("Automobile", "Commercial & Passenger Vehicles"),
    "M&M":         ("Automobile", "SUV & Tractors"),
    "BAJAJ-AUTO":  ("Automobile", "Two & Three Wheelers"),
    "HEROMOTOCO":  ("Automobile", "Two Wheelers"),
    "EICHERMOT":   ("Automobile", "Two Wheelers & Trucks"),
    "ASHOKLEY":    ("Automobile", "Commercial Vehicles"),
    "TVSMOTOR":    ("Automobile", "Two Wheelers"),
    "MOTHERSON":   ("Automobile", "Auto Ancillary"),
    "BOSCHLTD":    ("Automobile", "Auto Ancillary"),
    "APOLLOTYRE":  ("Automobile", "Tyres"),
    "MRF":         ("Automobile", "Tyres"),
    "CEATLTD":     ("Automobile", "Tyres"),
    # Metals & Mining
    "TATASTEEL":   ("Metals", "Steel"),
    "JSWSTEEL":    ("Metals", "Steel"),
    "SAIL":        ("Metals", "Steel"),
    "HINDALCO":    ("Metals", "Aluminium"),
    "NATIONALUM":  ("Metals", "Aluminium"),
    "VEDL":        ("Metals", "Diversified Metals"),
    "COALINDIA":   ("Metals", "Coal"),
    "NMDC":        ("Metals", "Iron Ore Mining"),
    "HINDCOPPER":  ("Metals", "Copper"),
    # FMCG
    "HINDUNILVR":  ("FMCG", "Personal & Home Care"),
    "ITC":         ("FMCG", "Cigarettes & FMCG"),
    "NESTLEIND":   ("FMCG", "Food & Beverages"),
    "BRITANNIA":   ("FMCG", "Food Products"),
    "DABUR":       ("FMCG", "Personal Care"),
    "MARICO":      ("FMCG", "Personal Care"),
    "GODREJCP":    ("FMCG", "Personal Care"),
    "COLPAL":      ("FMCG", "Personal Care"),
    "TATACONSUM":  ("FMCG", "Food & Beverages"),
    "EMAMILTD":    ("FMCG", "Personal Care"),
    # Pharma
    "SUNPHARMA":   ("Pharmaceuticals", "Pharma"),
    "DRREDDY":     ("Pharmaceuticals", "Pharma"),
    "CIPLA":       ("Pharmaceuticals", "Pharma"),
    "DIVISLAB":    ("Pharmaceuticals", "API & Pharma"),
    "LUPIN":       ("Pharmaceuticals", "Pharma"),
    "BIOCON":      ("Pharmaceuticals", "Biotech"),
    "AUROPHARMA":  ("Pharmaceuticals", "Pharma"),
    "TORNTPHARM":  ("Pharmaceuticals", "Pharma"),
    "ALKEM":       ("Pharmaceuticals", "Pharma"),
    # Telecom
    "BHARTIARTL":  ("Telecom", "Telecommunications"),
    "IDEA":        ("Telecom", "Telecommunications"),
    "TATACOMM":    ("Telecom", "Data Services"),
    # Construction / Infra
    "LT":          ("Construction", "Engineering & Construction"),
    "LTTS":        ("Construction", "Engineering Services"),
    "LXCHEM":      ("Construction", "Specialty Chemicals"),
    "ADANIPORTS":  ("Construction", "Ports & Logistics"),
    "IRB":         ("Construction", "Road Infrastructure"),
    "KNR":         ("Construction", "Road Construction"),
    # Cement
    "ULTRACEMCO":  ("Cement", "Cement"),
    "SHREECEM":    ("Cement", "Cement"),
    "AMBUJACEM":   ("Cement", "Cement"),
    "ACC":         ("Cement", "Cement"),
    "RAMCOCEM":    ("Cement", "Cement"),
    "JKCEMENT":    ("Cement", "Cement"),
    # Consumer Durables
    "HAVELLS":     ("Consumer Durables", "Electricals"),
    "TITAN":       ("Consumer Durables", "Jewellery & Watches"),
    "WHIRLPOOL":   ("Consumer Durables", "Home Appliances"),
    "VOLTAS":      ("Consumer Durables", "Air Conditioners"),
    "CROMPTON":    ("Consumer Durables", "Electricals"),
    "DIXON":       ("Consumer Durables", "Electronics"),
    # Paints & Chemicals
    "ASIANPAINT":  ("Paints", "Paints & Coatings"),
    "BERGEPAINT":  ("Paints", "Paints & Coatings"),
    "PIDILITIND":  ("Chemicals", "Adhesives & Sealants"),
    "SRF":         ("Chemicals", "Specialty Chemicals"),
    "DEEPAKNTR":   ("Chemicals", "Specialty Chemicals"),
    "AARTIIND":    ("Chemicals", "Specialty Chemicals"),
    "NAVINFLUOR":  ("Chemicals", "Specialty Chemicals"),
    "ALKYLAMINE":  ("Chemicals", "Specialty Chemicals"),
    # Conglomerate / Adani
    "ADANIENT":    ("Conglomerate", "Diversified"),
    "ADANITRANS":  ("Power", "Power Transmission"),
    # Indices
    "NIFTY":       ("Index", "Broad Market"),
    "BANKNIFTY":   ("Index", "Banking"),
    "FINNIFTY":    ("Index", "Financial Services"),
    "MIDCPNIFTY":  ("Index", "Mid Cap"),
    "NIFTYNEXT50": ("Index", "Large & Mid Cap"),
    # MCX Commodities
    "GOLD":        ("Commodity", "Precious Metals"),
    "SILVER":      ("Commodity", "Precious Metals"),
    "CRUDEOIL":    ("Commodity", "Energy"),
    "NATURALGAS":  ("Commodity", "Energy"),
    "COPPER":      ("Commodity", "Base Metals"),
    "ZINC":        ("Commodity", "Base Metals"),
    "LEAD":        ("Commodity", "Base Metals"),
    "NICKEL":      ("Commodity", "Base Metals"),
    "ALUMINIUM":   ("Commodity", "Base Metals"),
}

KNOWN_FNO_STOCKS: Set[str] = {
    "RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "KOTAKBANK", "SBIN",
    "BHARTIARTL", "ITC", "TATAMOTORS", "TATASTEEL", "HINDUNILVR", "BAJFINANCE",
    "MARUTI", "SUNPHARMA", "WIPRO", "HCLTECH", "POWERGRID", "NTPC", "ONGC",
    "AXISBANK", "INDUSINDBK", "ULTRACEMCO", "LT", "ASIANPAINT", "TITAN",
    "JSWSTEEL", "HINDALCO", "DIVISLAB", "DRREDDY", "CIPLA", "TECHM", "GRASIM",
    "NESTLEIND", "BRITANNIA", "TATACONSUM", "M&M", "BAJAJ-AUTO", "HEROMOTOCO",
    "COALINDIA", "ADANIPORTS", "ADANIENT", "UPL", "SHREECEM", "EICHERMOT",
    "PIDILITIND", "BERGEPAINT", "DABUR", "MARICO", "GODREJCP", "HAVELLS",
    "AMBUJACEM", "ACC", "BAJAJFINSV", "PAGEIND", "COLPAL", "BIOCON",
    "LUPIN", "AUROPHARMA", "TORNTPHARM", "ALKEM", "SAIL", "NMDC", "VEDL",
    "NATIONALUM", "HINDCOPPER", "IOC", "BPCL", "HINDPETRO", "GAIL",
    "TATAPOWER", "ADANIGREEN", "ADANIPOWER", "CESC", "TORNTPOWER",
    "ASHOKLEY", "TVSMOTOR", "MOTHERSON", "APOLLOTYRE", "MRF", "CEATLTD",
    "BANKBARODA", "PNB", "CANBK", "FEDERALBNK", "IDFCFIRSTB", "BANDHANBNK",
    "CHOLAFIN", "MUTHOOTFIN", "HDFCLIFE", "SBILIFE", "ICICIPRULI", "ICICIGI",
    "LTIM", "MPHASIS", "PERSISTENT", "COFORGE", "DEEPAKNTR", "SRF",
    "AARTIIND", "NAVINFLUOR", "RAMCOCEM", "JKCEMENT", "VOLTAS", "CROMPTON",
    "DIXON", "WHIRLPOOL", "IGL", "MGL", "PETRONET", "TATACOMM",
}

# ─────────────────────────────────────────────
#  OHLCV SQLITE CACHE
# ─────────────────────────────────────────────
class OHLCVCache:
    """Incremental SQLite cache for daily OHLCV data"""

    def __init__(self, db_path: str = CFG.ohlcv_db_file):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ohlcv (
                    symbol   TEXT,
                    interval TEXT,
                    dt       TEXT,
                    open     REAL,
                    high     REAL,
                    low      REAL,
                    close    REAL,
                    volume   REAL,
                    PRIMARY KEY (symbol, interval, dt)
                )
            """)
            conn.commit()

    def get(self, symbol: str, interval: str, since: datetime) -> Optional[pd.DataFrame]:
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql_query(
                "SELECT dt, open, high, low, close, volume FROM ohlcv "
                "WHERE symbol=? AND interval=? AND dt>=? ORDER BY dt",
                conn,
                params=(symbol, interval, since.isoformat()),
                parse_dates=["dt"],
            )
        if df.empty:
            return None
        df.set_index("dt", inplace=True)
        return df

    def latest_dt(self, symbol: str, interval: str) -> Optional[datetime]:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT MAX(dt) FROM ohlcv WHERE symbol=? AND interval=?",
                (symbol, interval),
            ).fetchone()
        if row and row[0]:
            return datetime.fromisoformat(row[0])
        return None

    def upsert(self, symbol: str, interval: str, df: pd.DataFrame):
        if df is None or df.empty:
            return
        rows = [
            (symbol, interval, str(idx), r.open, r.high, r.low, r.close, r.volume)
            for idx, r in df.iterrows()
        ]
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(
                "INSERT OR REPLACE INTO ohlcv "
                "(symbol, interval, dt, open, high, low, close, volume) "
                "VALUES (?,?,?,?,?,?,?,?)",
                rows,
            )
            conn.commit()

# ─────────────────────────────────────────────
#  SYMBOL MANAGER
# ─────────────────────────────────────────────
class AngelOneSymbolManager:

    def __init__(self):
        # FIX: all instance vars initialised here (v6 had mcx_symbols as local)
        self.all_symbols: Dict   = {}
        self.fno_symbols: Dict   = {}
        self.index_symbols: Dict = {}
        self.mcx_symbols: Dict   = {}
        self._load_cache()

    # ── cache helpers ──────────────────────────────────────────────────
    def _load_cache(self) -> bool:
        if not os.path.exists(CFG.symbol_cache_file):
            return False
        try:
            with open(CFG.symbol_cache_file, "r", encoding="utf-8") as f:
                cache = json.load(f)
            if cache.get("date") != date.today().isoformat():
                return False
            self.all_symbols   = cache.get("all_symbols",   {})
            self.fno_symbols   = cache.get("fno_symbols",   {})
            self.index_symbols = cache.get("index_symbols", {})
            self.mcx_symbols   = cache.get("mcx_symbols",   {})
            log.info(f"Cache hit: {len(self.fno_symbols)} F&O | "
                     f"{len(self.index_symbols)} indices | {len(self.mcx_symbols)} MCX")
            return True
        except Exception as e:
            log.warning(f"Cache load failed: {e}")
            return False

    def _save_cache(self):
        try:
            data = {
                "date":         date.today().isoformat(),
                "all_symbols":  self.all_symbols,
                "fno_symbols":  self.fno_symbols,
                "index_symbols":self.index_symbols,
                "mcx_symbols":  self.mcx_symbols,
            }
            with open(CFG.symbol_cache_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            log.warning(f"Cache save failed: {e}")

    # ── master fetch ───────────────────────────────────────────────────
    def fetch_master(self, force: bool = False) -> Dict:
        if not force and self._load_cache():
            return self.fno_symbols

        log.info("Fetching Angel One master file…")
        try:
            resp = requests.get(ANGEL_MASTER_URL, timeout=60,
                                headers={"User-Agent": "Mozilla/5.0"})
            resp.raise_for_status()
            data: List[Dict] = resp.json()
            log.info(f"Master: {len(data)} scrips")
        except Exception as e:
            log.error(f"Master fetch failed: {e}")
            return self._fallback()

        # ── FIX: build F&O set in ONE pass (O(n)) before main loop ────
        fo_base_symbols: Set[str] = set()
        for s in data:
            st = s.get("symboltype", "")
            if "FUT" in st or "OPT" in st:
                # base name = strip expiry/option suffix  e.g. RELIANCE23JANFUT → RELIANCE
                base = re.sub(
                    r"(\d{2}[A-Z]{3}\d{2,4}(FUT|CE|PE|FUTSTK|FUTIDX).*$)|"
                    r"(\d{2}[A-Z]{3}FUT.*$)|(FUT$)|(CE$)|(PE$)",
                    "", s.get("symbol", "")
                ).strip()
                if base:
                    fo_base_symbols.add(base)

        fo_base_symbols |= KNOWN_FNO_STOCKS  # always include known list

        self.all_symbols   = {}
        self.fno_symbols   = {}
        self.index_symbols = {}
        self.mcx_symbols   = {}

        for scrip in data:
            symbol   = scrip.get("symbol", "")
            exchange = scrip.get("exch", "")
            token    = str(scrip.get("token", ""))
            name     = scrip.get("name", symbol)
            series   = scrip.get("series", "")
            st       = scrip.get("symboltype", "")
            company  = scrip.get("companyname", name)

            if not all([symbol, exchange, token]):
                continue

            sym_clean = symbol.replace("-EQ", "").replace("-NSE", "").replace("-BSE", "")
            info = {
                "exchange":     exchange,
                "token":        token,
                "name":         name,
                "company_name": company,
                "series":       series,
                "symboltype":   st,
            }
            self.all_symbols[sym_clean] = info

            if exchange == "NSE":
                if sym_clean in ("NIFTY", "BANKNIFTY", "FINNIFTY",
                                  "NIFTYNEXT50", "MIDCPNIFTY"):
                    self.index_symbols[sym_clean] = info
                elif series == "EQ" and sym_clean in fo_base_symbols:
                    self.fno_symbols[sym_clean] = info

            elif exchange == "MCX":
                # Take the active nearest-expiry contract per commodity
                base_mcx = re.sub(r"\d.*$", "", sym_clean).strip()
                if base_mcx and base_mcx not in self.mcx_symbols:
                    self.mcx_symbols[base_mcx] = info

        log.info(f"Parsed: {len(self.fno_symbols)} F&O | "
                 f"{len(self.index_symbols)} indices | {len(self.mcx_symbols)} MCX")
        self._save_cache()
        return self.fno_symbols

    def _fallback(self) -> Dict:
        log.warning("Using fallback symbol list")
        for sym in KNOWN_FNO_STOCKS:
            info = {"exchange": "NSE", "token": "99999999", "name": sym}
            self.all_symbols[sym]  = info
            self.fno_symbols[sym]  = info
        for sym, tok, name in [
            ("NIFTY",     "99926000", "NIFTY 50"),
            ("BANKNIFTY", "99926001", "BANK NIFTY"),
            ("FINNIFTY",  "99926002", "FIN NIFTY"),
        ]:
            i = {"exchange": "NSE", "token": tok, "name": name}
            self.all_symbols[sym]   = i
            self.index_symbols[sym] = i
        for sym, tok, name in [
            ("GOLD",       "53726001", "Gold"),
            ("SILVER",     "53726002", "Silver"),
            ("CRUDEOIL",   "53726003", "Crude Oil"),
            ("NATURALGAS", "53726004", "Natural Gas"),
        ]:
            i = {"exchange": "MCX", "token": tok, "name": name}
            self.all_symbols[sym]  = i
            self.mcx_symbols[sym]  = i
        return self.fno_symbols

    def get_sector_industry(self, symbol: str) -> Tuple[str, str]:
        return SECTOR_MAP.get(symbol, ("Other", "Other"))

    def get_universe(self, fo=True, indices=True, mcx=True) -> Dict:
        out = {}
        if fo:      out.update(self.fno_symbols)
        if indices: out.update(self.index_symbols)
        if mcx:     out.update(self.mcx_symbols)
        return out

# ─────────────────────────────────────────────
#  ANGEL ONE REST CLIENT  (with retry + token expiry)
# ─────────────────────────────────────────────
class AngelOneRESTClient:

    def __init__(self):
        self._jwt: Optional[str]    = None
        self._jwt_expiry: Optional[datetime] = None
        self._last_req: float       = 0.0

    # ── connection ─────────────────────────────────────────────────────
    def _is_connected(self) -> bool:
        if not self._jwt:
            return False
        # FIX: treat token as expired 5 min before 6-hour mark
        if self._jwt_expiry and datetime.now() >= self._jwt_expiry:
            log.info("JWT token expired, re-authenticating…")
            self._jwt = None
            return False
        return True

    def connect(self) -> bool:
        if self._is_connected():
            return True
        try:
            try:
                secret_bytes = base64.b64decode(ANGEL_TOTP_SECRET)
                totp = pyotp.TOTP(secret_bytes).now()
            except Exception:
                totp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()

            payload = {
                "clientcode": ANGEL_CLIENT_ID,
                "password":   ANGEL_PASSWORD,
                "totp":       totp,
                "state":      "D",
                "apkversion": "1.0.0",
                "appsource":  "API",
                "userType":   "USER",
            }
            headers = self._base_headers()
            resp = requests.post(ANGEL_LOGIN_URL, json=payload,
                                  headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if data.get("status"):
                self._jwt        = data["data"]["jwtToken"]
                # Angel One JWT is valid for ~6 h; we refresh at 5h50m
                self._jwt_expiry = datetime.now() + timedelta(hours=5, minutes=50)
                log.info("Angel One: authenticated")
                return True
            log.error(f"Auth failed: {data.get('message')}")
            return False
        except Exception as e:
            log.error(f"Auth error: {e}")
            return False

    def _base_headers(self, auth: bool = False) -> Dict:
        h = {
            "Content-Type":       "application/json",
            "Accept":             "application/json",
            "X-UserType":         "USER",
            "X-SourceID":         "API",
            "X-ClientLocalIP":    "127.0.0.1",
            "X-ClientPublicIP":   "127.0.0.1",
            "X-MACAddress":       "00:00:00:00:00:00",
            "X-PrivateKey":       ANGEL_API_KEY,
        }
        if auth and self._jwt:
            h["Authorization"] = f"Bearer {self._jwt}"
        return h

    # ── rate limiter ────────────────────────────────────────────────────
    def _rate_limit(self):
        elapsed = time.time() - self._last_req
        if elapsed < CFG.request_delay:
            time.sleep(CFG.request_delay - elapsed)
        self._last_req = time.time()

    # ── fetch with retry ────────────────────────────────────────────────
    def _fetch(self, exchange: str, token: str,
               interval: str, fromdate: str, todate: str) -> Optional[List]:
        for attempt in range(1, CFG.max_retries + 1):
            if not self.connect():
                return None
            self._rate_limit()
            payload = {
                "exchange":    exchange,
                "symboltoken": token,
                "interval":    interval,
                "fromdate":    fromdate,
                "todate":      todate,
            }
            try:
                resp = requests.post(
                    ANGEL_HISTORICAL_URL,
                    json=payload,
                    headers=self._base_headers(auth=True),
                    timeout=60,
                )
                # FIX: re-auth on 401
                if resp.status_code == 401:
                    log.warning("401 received — forcing re-auth")
                    self._jwt = None
                    continue
                if resp.status_code == 429:
                    wait = 2 ** attempt
                    log.warning(f"Rate-limited, waiting {wait}s…")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                data = resp.json()
                if data.get("status") and data.get("data"):
                    return data["data"]
                return None
            except requests.exceptions.Timeout:
                log.warning(f"Timeout on attempt {attempt}/{CFG.max_retries}")
                time.sleep(2 ** attempt)
            except Exception as e:
                log.error(f"Fetch error (attempt {attempt}): {e}")
                time.sleep(2 ** attempt)
        return None

    def get_candles(self, symbol: str, exchange: str, token: str,
                    interval: str, days: int) -> Optional[pd.DataFrame]:
        end   = datetime.now()
        start = end - timedelta(days=days)
        raw   = self._fetch(
            exchange, token, interval,
            start.strftime("%Y-%m-%d 09:15"),
            end.strftime("%Y-%m-%d 15:30"),
        )
        if not raw:
            return None
        df = pd.DataFrame(raw, columns=["datetime", "open", "high", "low", "close", "volume"])
        df["datetime"] = pd.to_datetime(df["datetime"])
        df.set_index("datetime", inplace=True)
        for col in ("open", "high", "low", "close", "volume"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df.dropna(inplace=True)
        return df if not df.empty else None

# ─────────────────────────────────────────────
#  INDICATOR HELPERS
# ─────────────────────────────────────────────
def _s(val) -> float:
    """Safely convert any value to float scalar"""
    try:
        if val is None:
            return 0.0
        arr = np.asarray(val).flatten()
        return float(arr[0]) if arr.size else 0.0
    except Exception:
        return 0.0

def _col(df: pd.DataFrame, *names: str) -> Optional[pd.Series]:
    if df is None or df.empty:
        return None
    lmap = {c.lower(): c for c in df.columns}
    for n in names:
        real = lmap.get(n.lower())
        if real:
            s = df[real].dropna()
            return s.iloc[:, 0].astype(float) if isinstance(s, pd.DataFrame) else s.astype(float)
    return None

def _close(df): return _col(df, "close", "Close")
def _open(df):  return _col(df, "open",  "Open")
def _high(df):  return _col(df, "high",  "High")
def _low(df):   return _col(df, "low",   "Low")
def _vol(df):   return _col(df, "volume","Volume")


def calc_rsi(df: pd.DataFrame, period: int = CFG.rsi_period) -> Optional[float]:
    """
    Wilder RSI — FIX: seed from rolling mean of first `period` bars,
    then apply true Wilder smoothing for the rest.
    """
    c = _close(df)
    if c is None or len(c) < period * 2 + 1:
        return None
    delta = c.diff().dropna()
    gain  = delta.clip(lower=0).values
    loss  = (-delta.clip(upper=0)).values

    # Seed (Wilder initialisation)
    avg_g = gain[:period].mean()
    avg_l = loss[:period].mean()

    for i in range(period, len(gain)):
        avg_g = (avg_g * (period - 1) + gain[i]) / period
        avg_l = (avg_l * (period - 1) + loss[i]) / period

    if avg_l == 0:
        return 100.0
    return round(100 - 100 / (1 + avg_g / avg_l), 2)


def calc_vf_levels(df: pd.DataFrame, cfg: ScreenerConfig = CFG
                   ) -> Optional[Tuple[float, float, float, float]]:
    """Volatility-based price levels (weekly)"""
    c = _close(df)
    o = _open(df)
    if c is None or o is None or len(c) < cfg.min_weekly_bars:
        return None
    try:
        dc_vals = [_s(c.iloc[-(i + 2)]) for i in range(cfg.weekly_closes_needed)]
        dop     = _s(o.iloc[-1])
        if any(v <= 0 for v in dc_vals) or dop <= 0:
            return None
        dc   = dc_vals[0]
        lr0  = [np.log(dc_vals[i] / dc_vals[i + 1]) for i in range(10)]
        vol0 = np.sqrt(max(np.var(lr0), 0.0))
        r11  = round(dc * vol0, cfg.round_preci)
        dok  = dop if abs(dop - dc) > 0.382 * r11 else dc
        adj  = [dok] + dc_vals[1:]
        lr   = [np.log(adj[i] / adj[i + 1]) for i in range(10)]
        vol  = np.sqrt(max(np.var(lr), 0.0))
        r1   = round(dok * vol, cfg.round_preci)
        return (
            round(dok + r1 * 0.236, cfg.round_preci),
            round(dok + r1 * 0.382, cfg.round_preci),
            round(dok - r1 * 0.236, cfg.round_preci),
            round(dok - r1 * 0.382, cfg.round_preci),
        )
    except Exception:
        return None


def calc_supertrend(df: pd.DataFrame,
                    factor: int = CFG.supertrend_factor,
                    period: int = CFG.supertrend_atr) -> Optional[Dict]:
    """
    SuperTrend — FIX: initial trend determined by price vs midpoint,
    not hard-coded as bullish.
    """
    hi = _high(df); lo = _low(df); cl = _close(df)
    if hi is None or lo is None or cl is None or len(cl) < period * 2:
        return None
    try:
        tr   = pd.concat([hi - lo, (hi - cl.shift()).abs(), (lo - cl.shift()).abs()], axis=1).max(axis=1)
        atr  = tr.rolling(period).mean()
        hl2  = (hi + lo) / 2
        up   = hl2 + factor * atr
        dn   = hl2 - factor * atr

        st    = pd.Series(np.nan, index=df.index, dtype=float)
        trend = pd.Series(0,     index=df.index, dtype=int)

        for i in range(period, len(df)):
            mid = _s(hl2.iloc[i])
            if i == period:
                # FIX: initial direction from price vs midpoint
                trend.iloc[i] = 1 if _s(cl.iloc[i]) >= mid else -1
                st.iloc[i]    = _s(dn.iloc[i]) if trend.iloc[i] == 1 else _s(up.iloc[i])
            else:
                prev_st    = _s(st.iloc[i - 1])
                prev_trend = trend.iloc[i - 1]
                cur_cl     = _s(cl.iloc[i])
                cur_up     = _s(up.iloc[i])
                cur_dn     = _s(dn.iloc[i])

                if prev_trend == 1:
                    if cur_cl > _s(dn.iloc[i]):
                        trend.iloc[i] = 1
                        st.iloc[i]    = max(cur_dn, prev_st)
                    else:
                        trend.iloc[i] = -1
                        st.iloc[i]    = cur_up
                else:
                    if cur_cl < _s(up.iloc[i]):
                        trend.iloc[i] = -1
                        st.iloc[i]    = min(cur_up, prev_st)
                    else:
                        trend.iloc[i] = 1
                        st.iloc[i]    = cur_dn

        val = _s(st.iloc[-1])
        return {
            "value":   round(val, 2),
            "bullish": int(trend.iloc[-1]) == 1,
            "gap_pct": round(abs(_s(cl.iloc[-1]) - val) / val * 100, 2) if val else 0,
        }
    except Exception:
        return None


def calc_adx(df: pd.DataFrame, period: int = CFG.adx_period) -> Optional[Dict]:
    hi = _high(df); lo = _low(df); cl = _close(df)
    if hi is None or lo is None or cl is None or len(cl) < period * 2:
        return None
    try:
        up_move   = hi.diff()
        down_move = -lo.diff()
        pdm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
        ndm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
        tr  = pd.concat([hi - lo, (hi - cl.shift()).abs(), (lo - cl.shift()).abs()], axis=1).max(axis=1)
        atr = tr.rolling(period).mean()
        pdi = 100 * pd.Series(pdm, index=df.index).rolling(period).mean() / atr
        ndi = 100 * pd.Series(ndm, index=df.index).rolling(period).mean() / atr
        dx  = (100 * (pdi - ndi).abs() / (pdi + ndi)).replace([np.inf, -np.inf], np.nan)
        adx = dx.rolling(period).mean()
        return {
            "adx":      round(_s(adx.iloc[-1]),  2),
            "plus_di":  round(_s(pdi.iloc[-1]),  2),
            "minus_di": round(_s(ndi.iloc[-1]),  2),
        }
    except Exception:
        return None


def calc_atr_rank(df: pd.DataFrame,
                  period: int = CFG.atr_period,
                  window: int = CFG.atr_percentile_win) -> Optional[float]:
    hi = _high(df); lo = _low(df); cl = _close(df)
    if hi is None or lo is None or cl is None:
        return None
    try:
        tr  = pd.concat([hi - lo, (hi - cl.shift()).abs(), (lo - cl.shift()).abs()], axis=1).max(axis=1)
        atr = tr.rolling(period).mean().dropna()
        if len(atr) < window:
            return None
        cur = _s(atr.iloc[-1])
        return round((atr.iloc[-window:] < cur).sum() / window * 100, 1)
    except Exception:
        return None


def calc_vwap(df_15m: pd.DataFrame) -> Optional[float]:
    """
    Intraday VWAP from 15-min bars.
    Uses last `CFG.vwap_bars` bars (≈ 1 session).
    VWAP = Σ(typical_price × volume) / Σ(volume)
    """
    hi = _high(df_15m); lo = _low(df_15m); cl = _close(df_15m); vol = _vol(df_15m)
    if hi is None or lo is None or cl is None or vol is None or len(cl) < 5:
        return None
    try:
        bars = CFG.vwap_bars
        hi_w  = hi.iloc[-bars:]
        lo_w  = lo.iloc[-bars:]
        cl_w  = cl.iloc[-bars:]
        vol_w = vol.iloc[-bars:]
        tp    = (hi_w + lo_w + cl_w) / 3
        total_vol = vol_w.sum()
        if total_vol == 0:
            return None
        vwap = (tp * vol_w).sum() / total_vol
        return round(float(vwap), 2)
    except Exception:
        return None


def calc_volume_ratio(df: pd.DataFrame, short: int = 5, long: int = 20) -> Optional[float]:
    """Recent volume vs average — >1 means expanding"""
    vol = _vol(df)
    if vol is None or len(vol) < long:
        return None
    try:
        ratio = vol.iloc[-short:].mean() / vol.iloc[-long:].mean()
        return round(float(ratio), 2)
    except Exception:
        return None

# ─────────────────────────────────────────────
#  SCORING ENGINE
# ─────────────────────────────────────────────
def compute_signal_score(
    price:          float,
    matched_levels: List[Tuple[str, float]],
    odd_deviation:  Optional[float],
    supertrend:     Optional[Dict],
    atr_rank:       Optional[float],
    adx:            Optional[Dict],
    daily_rsi:      Optional[float],
    weekly_rsi:     Optional[float],
    rsi_15m:        Optional[float],
    vwap:           Optional[float],
    vol_ratio:      Optional[float],
) -> Dict:
    """
    Scoring breakdown (max 100):
      VF Level match       : 25 pts
      Odd Square proximity : 10 pts
      SuperTrend direction : 20 pts
      ADX strength         :  0–15 pts  (direction from DI)
      ATR expansion        :  0–10 pts  (non-directional — FIX from v6)
      VWAP position        : 10 pts     (NEW)
      15-min RSI confirm   :  5 pts     (NEW)
      Volume confirmation  :  5 pts     (NEW)
    Total                  : 100 pts
    """
    buy_score  = 0
    sell_score = 0
    reasons    = []

    # ── 1. VF Level (25 pts) ──────────────────────────────────────────
    vf_buy  = any("BUY"  in m[0] for m in matched_levels)
    vf_sell = any("SELL" in m[0] for m in matched_levels)
    vf_conf = any("CONFIRM" in m[0] for m in matched_levels)
    if vf_buy:
        pts = 25 if vf_conf else 18
        buy_score += pts
        reasons.append(f"VF {'confirmed' if vf_conf else 'initial'} buy")
    elif vf_sell:
        pts = 25 if vf_conf else 18
        sell_score += pts
        reasons.append(f"VF {'confirmed' if vf_conf else 'initial'} sell")

    # ── 2. Odd Square proximity (10 pts) ──────────────────────────────
    if odd_deviation is not None:
        pts = max(0, int(10 * (1 - odd_deviation / CFG.odd_square_tolerance)))
        # Neutral — adds to whichever side is ahead
        if buy_score >= sell_score:
            buy_score += pts
        else:
            sell_score += pts
        reasons.append(f"OddSq dev {odd_deviation:.1f}%")

    # ── 3. SuperTrend (20 pts) ────────────────────────────────────────
    if supertrend:
        pts = 20 if supertrend["gap_pct"] > 0.5 else 10
        if supertrend["bullish"]:
            buy_score  += pts
            reasons.append(f"ST bullish (gap {supertrend['gap_pct']:.1f}%)")
        else:
            sell_score += pts
            reasons.append(f"ST bearish (gap {supertrend['gap_pct']:.1f}%)")

    # ── 4. ADX (15 pts) — direction from DI crossover ────────────────
    if adx and adx["adx"] > 0:
        adx_val = adx["adx"]
        if   adx_val >= 40: pts = 15
        elif adx_val >= 25: pts = 10
        elif adx_val >= 20: pts = 5
        else:               pts = 0
        if adx["plus_di"] > adx["minus_di"]:
            buy_score  += pts
            reasons.append(f"ADX {adx_val:.0f} +DI>{'-DI'}")
        else:
            sell_score += pts
            reasons.append(f"ADX {adx_val:.0f} -DI>+DI")

    # ── 5. ATR expansion (10 pts, NON-DIRECTIONAL) — FIX from v6 ─────
    if atr_rank is not None:
        if   atr_rank >= 70: pts = 10
        elif atr_rank >= 50: pts = 7
        elif atr_rank >= 30: pts = 3
        else:                pts = 0
        # ATR is volatility only; boost whichever direction leads
        if buy_score >= sell_score:
            buy_score  += pts
        else:
            sell_score += pts
        reasons.append(f"ATR rank {atr_rank:.0f}th")

    # ── 6. VWAP position (10 pts) — NEW ──────────────────────────────
    if vwap and price > 0:
        if price > vwap:
            buy_score  += 10
            reasons.append(f"Above VWAP ₹{vwap:.2f}")
        else:
            sell_score += 10
            reasons.append(f"Below VWAP ₹{vwap:.2f}")

    # ── 7. 15-min RSI confirmation (5 pts) — NEW ─────────────────────
    if rsi_15m is not None:
        if rsi_15m >= 55:
            buy_score  += 5
            reasons.append(f"15m RSI {rsi_15m:.1f} bullish")
        elif rsi_15m <= 45:
            sell_score += 5
            reasons.append(f"15m RSI {rsi_15m:.1f} bearish")
        else:
            reasons.append(f"15m RSI {rsi_15m:.1f} neutral")

    # ── 8. Volume confirmation (5 pts) — NEW ─────────────────────────
    if vol_ratio is not None and vol_ratio > 1.2:
        pts = min(5, int(5 * (vol_ratio - 1.0) / 1.0))
        if buy_score >= sell_score:
            buy_score  += pts
        else:
            sell_score += pts
        reasons.append(f"Vol x{vol_ratio:.1f}")

    # ── Final ─────────────────────────────────────────────────────────
    if buy_score >= sell_score:
        signal = "BUY"
        score  = min(buy_score, 100)
    else:
        signal = "SELL"
        score  = min(sell_score, 100)

    if   score >= CFG.score_strong:   confidence = "STRONG"
    elif score >= CFG.score_moderate: confidence = "MODERATE"
    elif score >= CFG.score_weak:     confidence = "WEAK"
    else:
        confidence = "NO SIGNAL"
        signal     = "NEUTRAL"

    return {
        "signal":     signal,
        "score":      score,
        "confidence": confidence,
        "reason":     " | ".join(reasons) if reasons else "No clear signal",
        "buy_pts":    buy_score,
        "sell_pts":   sell_score,
        "vwap":       vwap,
        "rsi_15m":    rsi_15m,
        "vol_ratio":  vol_ratio,
    }

# ─────────────────────────────────────────────
#  PATTERN MATCHERS
# ─────────────────────────────────────────────
def _near(price: float, level: float, thr: float = CFG.threshold) -> bool:
    return level > 0 and abs(price - level) / level <= thr

def _within(price: float, target: float, pct: float = CFG.odd_square_tolerance) -> bool:
    return target > 0 > 0 and target * (1 - pct / 100) <= price <= target * (1 + pct / 100)


def find_odd_square_matches(stock_data: Dict) -> Dict:
    out = {}
    for sym, d in stock_data.items():
        price = d["price"]
        for odd, sq in ODD_SQUARES:
            if _within(price, sq, CFG.odd_square_tolerance):
                out[sym] = {
                    **d,
                    "odd_number":        odd,
                    "odd_square":        sq,
                    "deviation_percent": round(abs(price - sq) / sq * 100, 2),
                }
                break
    return out


def find_level_matches(stock_data: Dict) -> Dict:
    out = {}
    for sym, d in stock_data.items():
        if not d.get("has_levels"):
            continue
        price = d["price"]
        ba, bc, sb, sc = d["levels"]
        hits = []
        if _near(price, ba): hits.append(("BUY_ABOVE",    ba))
        if _near(price, bc): hits.append(("BUY_CONFIRM",  bc))
        if _near(price, sb): hits.append(("SELL_BELOW",   sb))
        if _near(price, sc): hits.append(("SELL_CONFIRM", sc))
        if hits:
            out[sym] = {
                **d,
                "matched_levels": hits,
                "deviation":      min(abs(price - lv[1]) / lv[1] * 100 for lv in hits),
            }
    return out

# ─────────────────────────────────────────────
#  DATA FETCHER
# ─────────────────────────────────────────────
class DataFetcher:
    def __init__(self, symbol_manager: AngelOneSymbolManager):
        self.client  = AngelOneRESTClient()
        self.sym_mgr = symbol_manager
        self.cache   = OHLCVCache()

    def _get_daily(self, symbol: str, info: Dict) -> Optional[pd.DataFrame]:
        """Fetch daily bars — incremental from SQLite cache"""
        exchange = info.get("exchange", "NSE")
        token    = info.get("token", "")
        if not token:
            return None

        latest = self.cache.latest_dt(symbol, "ONE_DAY")
        days   = 400
        if latest:
            days_needed = (datetime.now() - latest).days + 2
            if days_needed <= 0:
                return self.cache.get(symbol, "ONE_DAY",
                                      datetime.now() - timedelta(days=400))
            days = days_needed

        df_new = self.client.get_candles(symbol, exchange, token, "ONE_DAY", days)
        if df_new is not None:
            self.cache.upsert(symbol, "ONE_DAY", df_new)

        return self.cache.get(symbol, "ONE_DAY",
                              datetime.now() - timedelta(days=400))

    def _get_15m(self, symbol: str, info: Dict) -> Optional[pd.DataFrame]:
        """Fetch last 10 days of 15-min bars (intraday for VWAP + RSI)"""
        exchange = info.get("exchange", "NSE")
        token    = info.get("token", "")
        if not token:
            return None
        latest = self.cache.latest_dt(symbol, "FIFTEEN_MINUTE")
        days   = 10
        if latest:
            hours_ago = (datetime.now() - latest).total_seconds() / 3600
            if hours_ago < 0.5:
                return self.cache.get(symbol, "FIFTEEN_MINUTE",
                                      datetime.now() - timedelta(days=10))
        df_new = self.client.get_candles(symbol, exchange, token, "FIFTEEN_MINUTE", days)
        if df_new is not None:
            self.cache.upsert(symbol, "FIFTEEN_MINUTE", df_new)
        return self.cache.get(symbol, "FIFTEEN_MINUTE",
                              datetime.now() - timedelta(days=10))

    def fetch_one(self, symbol: str, info: Dict) -> Tuple[str, Optional[Dict]]:
        try:
            df_daily = self._get_daily(symbol, info)
            if df_daily is None or df_daily.empty or len(df_daily) < 30:
                return symbol, None

            # FIX: resample daily → weekly instead of a second API call
            df_weekly = df_daily.resample("W").agg(
                open="first", high="max", low="min", close="last", volume="sum"
            ).dropna()

            # 15-min data (best effort — don't abort if unavailable)
            df_15m = self._get_15m(symbol, info)

            price = _s(df_daily["close"].iloc[-1])
            if price <= 0:
                return symbol, None

            sector, industry = self.sym_mgr.get_sector_industry(symbol)

            return symbol, {
                "price":      price,
                "levels":     calc_vf_levels(df_weekly),
                "has_levels": calc_vf_levels(df_weekly) is not None,
                "daily_rsi":  calc_rsi(df_daily),
                "weekly_rsi": calc_rsi(df_weekly, CFG.rsi_period),
                "rsi_15m":    calc_rsi(df_15m, CFG.intraday_rsi_period) if df_15m is not None else None,
                "vwap":       calc_vwap(df_15m) if df_15m is not None else None,
                "supertrend": calc_supertrend(df_daily),
                "atr_rank":   calc_atr_rank(df_daily),
                "adx":        calc_adx(df_daily),
                "vol_ratio":  calc_volume_ratio(df_daily),
                "sector":     sector,
                "industry":   industry,
            }
        except Exception as e:
            log.error(f"fetch_one({symbol}): {e}")
            return symbol, None

    def fetch_all(self, universe: Dict) -> Dict[str, Dict]:
        total   = len(universe)
        results = {}
        log.info(f"Fetching {total} symbols with {CFG.max_workers} workers…")
        with ThreadPoolExecutor(max_workers=CFG.max_workers) as ex:
            futs = {ex.submit(self.fetch_one, sym, info): sym
                    for sym, info in universe.items()}
            done = 0
            for fut in as_completed(futs):
                done += 1
                sym  = futs[fut]
                try:
                    s, data = fut.result(timeout=120)
                    if data:
                        results[s] = data
                        log.info(f"  [{done:3}/{total}] ✓ {s:15} ₹{data['price']:>10.2f}"
                                 f"  RSI-D:{data['daily_rsi'] or '—':>6}"
                                 f"  RSI-15m:{data['rsi_15m'] or '—':>6}"
                                 f"  VWAP:{data['vwap'] or '—':>10}")
                    else:
                        log.info(f"  [{done:3}/{total}] ✗ {sym}: no data")
                except Exception as e:
                    log.error(f"  [{done:3}/{total}] ! {sym}: {e}")
        return results

# ─────────────────────────────────────────────
#  EXCEL OUTPUT  (FIX: returns 3 values cleanly)
# ─────────────────────────────────────────────
def _fmt(val, fmt=".2f") -> str:
    try:
        return format(float(val), fmt) if val is not None else "N/A"
    except Exception:
        return str(val)


def _vwap_position(price: float, vwap: Optional[float]) -> str:
    if vwap is None:
        return "N/A"
    return "ABOVE" if price > vwap else "BELOW"


def create_excel_output(
    odd_matches:   Dict,
    level_matches: Dict,
    both_matches:  Set,
    scored:        Dict,
    timestamp:     str,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Returns (odd_file, level_file, both_file).
    FIX from v6: no None padding in return tuple.
    """

    def _st_label(d):
        st = d.get("supertrend")
        if st is None:
            return "N/A"
        return "BULL" if st["bullish"] else "BEAR"

    # ── Odd Square file ───────────────────────────────────────────────
    odd_file = None
    if odd_matches:
        rows = []
        for sym, d in odd_matches.items():
            sc = scored.get(sym, {})
            rows.append({
                "Symbol":          sym,
                "Sector":          d.get("sector", "Other"),
                "Industry":        d.get("industry", "Other"),
                "Price":           d["price"],
                "Signal":          sc.get("signal", "—"),
                "Score":           sc.get("score", "—"),
                "Confidence":      sc.get("confidence", "—"),
                "Odd Number":      d["odd_number"],
                "Odd Square":      d["odd_square"],
                "Deviation %":     d["deviation_percent"],
                "Daily RSI":       _fmt(d.get("daily_rsi")),
                "Weekly RSI":      _fmt(d.get("weekly_rsi")),
                "15m RSI":         _fmt(d.get("rsi_15m")),
                "VWAP":            _fmt(d.get("vwap")),
                "vs VWAP":         _vwap_position(d["price"], d.get("vwap")),
                "Volume Ratio":    _fmt(d.get("vol_ratio")),
                "SuperTrend":      _st_label(d),
                "ATR Rank %":      _fmt(d.get("atr_rank")),
                "ADX":             _fmt(d["adx"]["adx"] if d.get("adx") else None),
            })
        odd_file = f"odd_square_matches_{timestamp}.xlsx"
        pd.DataFrame(rows).to_excel(odd_file, index=False)
        log.info(f"Saved {odd_file}")

    # ── VF Level file ─────────────────────────────────────────────────
    level_file = None
    if level_matches:
        rows = []
        for sym, d in level_matches.items():
            sc = scored.get(sym, {})
            rows.append({
                "Symbol":          sym,
                "Sector":          d.get("sector", "Other"),
                "Industry":        d.get("industry", "Other"),
                "Price":           d["price"],
                "Signal":          sc.get("signal", "—"),
                "Score":           sc.get("score", "—"),
                "Confidence":      sc.get("confidence", "—"),
                "Matched Levels":  ", ".join(m[0] for m in d["matched_levels"]),
                "Deviation %":     round(d["deviation"], 2),
                "Daily RSI":       _fmt(d.get("daily_rsi")),
                "Weekly RSI":      _fmt(d.get("weekly_rsi")),
                "15m RSI":         _fmt(d.get("rsi_15m")),
                "VWAP":            _fmt(d.get("vwap")),
                "vs VWAP":         _vwap_position(d["price"], d.get("vwap")),
                "Volume Ratio":    _fmt(d.get("vol_ratio")),
            })
        level_file = f"volatility_level_matches_{timestamp}.xlsx"
        pd.DataFrame(rows).to_excel(level_file, index=False)
        log.info(f"Saved {level_file}")

    # ── Both-criteria file ────────────────────────────────────────────
    both_file = None
    rows = []
    for sym in both_matches:
        sc = scored.get(sym, {})
        if sc.get("confidence") == "NO SIGNAL":
            continue
        od = odd_matches[sym]
        ld = level_matches[sym]
        rows.append({
            "Symbol":          sym,
            "Sector":          od.get("sector", "Other"),
            "Industry":        od.get("industry", "Other"),
            "Price":           od["price"],
            "Signal":          sc.get("signal", "—"),
            "Score":           sc.get("score", "—"),
            "Confidence":      sc.get("confidence", "—"),
            "Reason":          sc.get("reason", "—"),
            "Buy Pts":         sc.get("buy_pts", 0),
            "Sell Pts":        sc.get("sell_pts", 0),
            "Odd Square":      f"{od['odd_number']}²={od['odd_square']}",
            "Odd Sq Dev %":    od["deviation_percent"],
            "VF Levels Hit":   ", ".join(m[0] for m in ld["matched_levels"]),
            "VF Dev %":        round(ld["deviation"], 2),
            "Daily RSI":       _fmt(od.get("daily_rsi")),
            "Weekly RSI":      _fmt(od.get("weekly_rsi")),
            "15m RSI":         _fmt(od.get("rsi_15m")),
            "VWAP":            _fmt(od.get("vwap")),
            "vs VWAP":         _vwap_position(od["price"], od.get("vwap")),
            "Volume Ratio":    _fmt(od.get("vol_ratio")),
            "SuperTrend":      _st_label(od),
            "ST Gap %":        _fmt(od["supertrend"]["gap_pct"] if od.get("supertrend") else None),
            "ATR Rank":        _fmt(od.get("atr_rank")),
            "ADX":             _fmt(od["adx"]["adx"] if od.get("adx") else None),
            "+DI":             _fmt(od["adx"]["plus_di"] if od.get("adx") else None),
            "-DI":             _fmt(od["adx"]["minus_di"] if od.get("adx") else None),
        })
    if rows:
        df = pd.DataFrame(rows).sort_values("Score", ascending=False)
        both_file = f"both_criteria_signals_{timestamp}.xlsx"
        df.to_excel(both_file, index=False)
        log.info(f"Saved {both_file}")

    return odd_file, level_file, both_file

# ─────────────────────────────────────────────
#  TELEGRAM MESSENGER
# ─────────────────────────────────────────────
TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"

# Flood-control: Telegram allows ~30 msgs/sec per bot; we stay conservative
_TG_SEND_DELAY = 0.4   # seconds between sends
_TG_MAX_LEN    = 4096  # Telegram hard limit per message


def _tg_post(token: str, chat_id: str, text: str, retries: int = 3) -> bool:
    """Send one Telegram message with retry on 429 / 5xx."""
    url  = TELEGRAM_API.format(token=token)
    data = {"chat_id": chat_id, "text": text,
            "parse_mode": "HTML", "disable_web_page_preview": True}
    for attempt in range(1, retries + 1):
        try:
            resp = requests.post(url, json=data, timeout=15)
            if resp.status_code == 200:
                return True
            if resp.status_code == 429:
                wait = int(resp.json().get("parameters", {}).get("retry_after", 5))
                log.warning(f"Telegram 429 — waiting {wait}s")
                time.sleep(wait)
            else:
                log.warning(f"Telegram HTTP {resp.status_code}: {resp.text[:120]}")
        except Exception as e:
            log.warning(f"Telegram send error (attempt {attempt}): {e}")
        time.sleep(2 ** attempt)
    return False


def _tg_send_all(text: str) -> int:
    """Broadcast a message to all configured chat IDs. Returns success count."""
    if not BOT_TOKEN or not CHAT_ID_LIST:
        return 0
    sent = 0
    for cid in CHAT_ID_LIST:
        if _tg_post(BOT_TOKEN, cid, text):
            sent += 1
        time.sleep(_TG_SEND_DELAY)
    return sent


def _score_bar(score: int, width: int = 10) -> str:
    """Visual score bar  ████░░░░░░  62/100"""
    filled = round(score / 100 * width)
    return "█" * filled + "░" * (width - filled)


def _conf_emoji(confidence: str) -> str:
    return {"STRONG": "🟢", "MODERATE": "🟡", "WEAK": "🔴"}.get(confidence, "⚪")


def _sig_emoji(signal: str) -> str:
    return {"BUY": "📈", "SELL": "📉", "NEUTRAL": "➡️"}.get(signal, "➡️")


def _rsi_label(val: Optional[float]) -> str:
    if val is None:
        return "N/A"
    if val >= 70:
        return f"{val:.1f} 🔥OB"
    if val <= 30:
        return f"{val:.1f} 🧊OS"
    if val >= 55:
        return f"{val:.1f} ↑"
    if val <= 45:
        return f"{val:.1f} ↓"
    return f"{val:.1f} ─"


def _adx_label(adx_dict: Optional[Dict]) -> str:
    if not adx_dict:
        return "N/A"
    a = adx_dict["adx"]
    label = "Strong" if a >= 40 else "Trending" if a >= 25 else "Weak"
    arrow = "▲" if adx_dict["plus_di"] > adx_dict["minus_di"] else "▼"
    return f"{a:.1f} {label} {arrow}  (+DI {adx_dict['plus_di']:.1f} / -DI {adx_dict['minus_di']:.1f})"


def _st_label_tg(st: Optional[Dict]) -> str:
    if not st:
        return "N/A"
    icon = "🟢" if st["bullish"] else "🔴"
    dir_ = "Bullish" if st["bullish"] else "Bearish"
    return f"{icon} {dir_}  ₹{st['value']:.2f}  (gap {st['gap_pct']:.1f}%)"


def _vwap_label(price: float, vwap: Optional[float]) -> str:
    if vwap is None:
        return "N/A"
    diff_pct = (price - vwap) / vwap * 100
    icon     = "🔼" if price > vwap else "🔽"
    return f"₹{vwap:.2f}  {icon} {'Above' if price > vwap else 'Below'} by {abs(diff_pct):.2f}%"


def _vol_label(ratio: Optional[float]) -> str:
    if ratio is None:
        return "N/A"
    if ratio >= 2.0:
        return f"x{ratio:.2f} 🚀 Surge"
    if ratio >= 1.5:
        return f"x{ratio:.2f} ⬆ High"
    if ratio >= 1.2:
        return f"x{ratio:.2f} ↑ Above avg"
    if ratio < 0.8:
        return f"x{ratio:.2f} ↓ Low"
    return f"x{ratio:.2f} Normal"


def _atr_label(rank: Optional[float]) -> str:
    if rank is None:
        return "N/A"
    if rank >= 70:
        return f"{rank:.0f}th — Expanding 📊"
    if rank >= 40:
        return f"{rank:.0f}th — Normal"
    return f"{rank:.0f}th — Contracting 😴"


def build_signal_card(
    symbol:       str,
    od:           Dict,   # from odd_matches
    ld:           Dict,   # from level_matches
    sc:           Dict,   # from scored
    rank:         int,    # 1-based position in sorted list
) -> str:
    """
    Build a single rich Telegram HTML card for one signal.

    Structure:
    ┌─ Header (symbol, signal, score bar, confidence)
    ├─ Price block
    ├─ VWAP block
    ├─ RSI block (Daily / Weekly / 15-min)
    ├─ SuperTrend
    ├─ ADX
    ├─ ATR Rank
    ├─ Volume
    ├─ VF Levels
    ├─ Odd Square
    └─ Reason / score breakdown
    """
    price  = od["price"]
    signal = sc["signal"]
    conf   = sc["confidence"]
    score  = sc["score"]

    # VF levels
    ba, bc, sb, sc_lv = od.get("levels") or (None, None, None, None)
    vf_hits = ", ".join(m[0] for m in ld["matched_levels"])
    vf_dev  = f"{ld['deviation']:.2f}%"

    lines = [
        f"{'─'*38}",
        f"<b>#{rank}  {_sig_emoji(signal)}  {symbol}</b>   "
        f"<b>{signal}</b>  {_conf_emoji(conf)} {conf}",
        f"Score: <b>{score}/100</b>  {_score_bar(score)}",
        f"{'─'*38}",

        # ── Price
        f"💰 <b>Price</b>         ₹{price:,.2f}",

        # ── VWAP
        f"📊 <b>VWAP</b>          {_vwap_label(price, od.get('vwap'))}",

        # ── RSI
        f"📉 <b>RSI  Daily</b>    {_rsi_label(od.get('daily_rsi'))}",
        f"📉 <b>RSI  Weekly</b>   {_rsi_label(od.get('weekly_rsi'))}",
        f"📉 <b>RSI  15-min</b>   {_rsi_label(od.get('rsi_15m'))}",

        # ── SuperTrend
        f"🔀 <b>SuperTrend</b>    {_st_label_tg(od.get('supertrend'))}",

        # ── ADX
        f"📐 <b>ADX</b>           {_adx_label(od.get('adx'))}",

        # ── ATR
        f"📏 <b>ATR Rank</b>      {_atr_label(od.get('atr_rank'))}",

        # ── Volume
        f"🔊 <b>Volume Ratio</b>  {_vol_label(od.get('vol_ratio'))}",

        # ── VF Levels
        f"🎯 <b>VF Levels Hit</b> {vf_hits}  (dev {vf_dev})",
    ]

    # VF level values if available
    if ba:
        lines.append(
            f"   ↑BA ₹{ba:.2f}  ↑BC ₹{bc:.2f}  "
            f"↓SB ₹{sb:.2f}  ↓SC ₹{sc_lv:.2f}"
        )

    # ── Odd Square
    lines.append(
        f"🔢 <b>Odd Square</b>    {od['odd_number']}² = {od['odd_square']}"
        f"  (dev {od['deviation_percent']:.2f}%)"
    )

    # ── Sector
    lines.append(
        f"🏭 <b>Sector</b>        {od.get('sector','—')} › {od.get('industry','—')}"
    )

    # ── Score breakdown
    lines += [
        f"{'─'*38}",
        f"📋 <b>Reason</b>",
        f"   {sc.get('reason', '—')}",
        f"   Buy pts: {sc.get('buy_pts',0)}  |  Sell pts: {sc.get('sell_pts',0)}",
        f"{'─'*38}",
    ]

    return "\n".join(lines)


def build_summary_message(
    run_start:    datetime,
    elapsed:      int,
    total_uni:    int,
    total_fetched:int,
    odd_n:        int,
    level_n:      int,
    both_n:       int,
    actionable:   List[Tuple[str, Dict]],
    odd_matches:  Dict,
) -> str:
    """
    Session-level summary message sent BEFORE individual signal cards.
    Includes a compact top-5 table.
    """
    ts    = run_start.strftime("%d %b %Y  %H:%M IST")
    lines = [
        "╔══════════════════════════════════════╗",
        "║   🇮🇳  <b>AIScan India v7.1</b>  —  NSE/MCX  ║",
        "╚══════════════════════════════════════╝",
        f"🕐 <b>Run:</b> {ts}",
        f"⏱ <b>Elapsed:</b> {elapsed}s",
        "",
        "━━━━  SCAN STATS  ━━━━",
        f"🔭 Universe       : {total_uni}",
        f"✅ Data fetched   : {total_fetched}",
        f"🔢 Odd Sq hits    : {odd_n}",
        f"📈 VF Level hits  : {level_n}",
        f"🎯 Both-criteria  : {both_n}",
        f"🚦 Actionable     : {len(actionable)}",
    ]

    if actionable:
        lines += ["", "━━━━  TOP SIGNALS  ━━━━",
                  "<code>"
                  f"{'#':2} {'Sym':12} {'Sig':4} {'Scr':>4} {'Conf':8} {'Price':>9}"
                  "</code>"]
        for i, (sym, sc) in enumerate(actionable[:5], 1):
            price = odd_matches[sym]["price"]
            lines.append(
                f"<code>"
                f"{i:2} {sym:12} {sc['signal']:4} {sc['score']:>3}/100"
                f" {sc['confidence']:8} ₹{price:>8.2f}"
                f"</code>"
            )
        if len(actionable) > 5:
            lines.append(f"<i>…and {len(actionable)-5} more signals follow below</i>")

    lines += [
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "<i>Signals for educational purposes only.</i>",
        "<i>Not financial advice. Trade at your own risk.</i>",
    ]
    return "\n".join(lines)


def dispatch_telegram(
    run_start:    datetime,
    elapsed:      int,
    total_uni:    int,
    total_fetched:int,
    odd_matches:  Dict,
    level_matches:Dict,
    both_matches: Set,
    scored:       Dict,
) -> int:
    """
    Send full Telegram broadcast:
      1. Session summary message
      2. One card per actionable signal (STRONG + MODERATE), sorted by score
    Returns total messages sent.
    """
    if not BOT_TOKEN or not CHAT_ID_LIST:
        log.info("Telegram not configured — skipping.")
        return 0

    actionable = [
        (s, scored[s]) for s in both_matches
        if scored[s].get("confidence") in ("STRONG", "MODERATE")
    ]
    actionable.sort(key=lambda x: x[1]["score"], reverse=True)

    total_sent = 0

    # ── 1. Summary ────────────────────────────────────────────────────
    summary = build_summary_message(
        run_start, elapsed, total_uni, total_fetched,
        len(odd_matches), len(level_matches), len(both_matches),
        actionable, odd_matches,
    )
    n = _tg_send_all(summary)
    total_sent += n
    log.info(f"Telegram summary sent to {n}/{len(CHAT_ID_LIST)} chats")

    if not actionable:
        no_sig = (
            "⚠️  <b>No actionable signals found today.</b>\n"
            "All screened symbols are outside VF + Odd Square confluence zones."
        )
        _tg_send_all(no_sig)
        return total_sent + len(CHAT_ID_LIST)

    # ── 2. Individual signal cards ────────────────────────────────────
    for rank, (sym, sc) in enumerate(actionable, 1):
        od = odd_matches[sym]
        ld = level_matches[sym]
        card = build_signal_card(sym, od, ld, sc, rank)

        # Safety: Telegram 4096-char limit — truncate reason if needed
        if len(card) > _TG_MAX_LEN:
            card = card[:_TG_MAX_LEN - 20] + "\n… <i>(truncated)</i>"

        n = _tg_send_all(card)
        total_sent += n
        log.info(f"  Signal card [{rank}/{len(actionable)}] {sym} → {n} chats")

    return total_sent


# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────
def main(dry_run: bool = False,
         override_symbols: Optional[List[str]] = None,
         force_refresh: bool = False,
         no_telegram: bool = False):

    run_start = datetime.now()
    print("=" * 80)
    print("  AISCAN INDIA v7.1  —  15m RSI + VWAP + Volume + Full Telegram")
    print("=" * 80)

    if not all([ANGEL_CLIENT_ID, ANGEL_PASSWORD, ANGEL_TOTP_SECRET]):
        print("\n❌  Angel One credentials not set. Please export:")
        print("    ANGEL_CLIENT_ID / ANGEL_PASSWORD / ANGEL_TOTP_SECRET")
        sys.exit(1)

    # ── 1. Symbol universe ────────────────────────────────────────────
    print("\n[1/6] Building symbol universe…")
    sym_mgr = AngelOneSymbolManager()
    sym_mgr.fetch_master(force=force_refresh)

    if override_symbols:
        universe = {s: sym_mgr.all_symbols[s]
                    for s in override_symbols if s in sym_mgr.all_symbols}
    else:
        universe = sym_mgr.get_universe(fo=True, indices=True, mcx=True)

    if dry_run:
        universe = dict(list(universe.items())[:20])
        print(f"⚠️  DRY-RUN: limiting to {len(universe)} symbols")

    fo_n  = sum(1 for i in universe.values() if i.get("exchange") == "NSE")
    mcx_n = sum(1 for i in universe.values() if i.get("exchange") == "MCX")
    print(f"   Universe: {len(universe)} total  "
          f"({fo_n} NSE F&O/Indices | {mcx_n} MCX)")

    # ── 2. Connect ────────────────────────────────────────────────────
    print("\n[2/6] Connecting to Angel One…")
    fetcher = DataFetcher(sym_mgr)
    if not fetcher.client.connect():
        print("❌  Connection failed. Check credentials.")
        sys.exit(1)

    # ── 3. Fetch data ─────────────────────────────────────────────────
    print("\n[3/6] Fetching OHLCV + 15-min data…")
    t0       = time.time()
    all_data = fetcher.fetch_all(universe)
    fetch_s  = time.time() - t0
    print(f"   Fetched {len(all_data)}/{len(universe)} symbols in {fetch_s:.1f}s")

    if not all_data:
        log.error("No data retrieved.")
        sys.exit(0)

    # ── 4. Pattern screening ──────────────────────────────────────────
    print("\n[4/6] Screening for patterns…")
    odd_matches   = find_odd_square_matches(all_data)
    level_matches = find_level_matches(all_data)
    both_matches  = set(odd_matches) & set(level_matches)

    print(f"   Odd Square   : {len(odd_matches)}")
    print(f"   VF Levels    : {len(level_matches)}")
    print(f"   Both-criteria: {len(both_matches)}")

    # Score every both-hit
    scored = {}
    for sym in both_matches:
        od = odd_matches[sym]
        ld = level_matches[sym]
        scored[sym] = compute_signal_score(
            price          = od["price"],
            matched_levels = ld["matched_levels"],
            odd_deviation  = od["deviation_percent"],
            supertrend     = od.get("supertrend"),
            atr_rank       = od.get("atr_rank"),
            adx            = od.get("adx"),
            daily_rsi      = od.get("daily_rsi"),
            weekly_rsi     = od.get("weekly_rsi"),
            rsi_15m        = od.get("rsi_15m"),
            vwap           = od.get("vwap"),
            vol_ratio      = od.get("vol_ratio"),
        )

    # ── 5. Excel output ───────────────────────────────────────────────
    print("\n[5/6] Writing Excel files…")
    ts                     = run_start.strftime("%Y%m%d_%H%M%S")
    odd_f, level_f, both_f = create_excel_output(
        odd_matches, level_matches, both_matches, scored, ts)

    # ── 6. Telegram ───────────────────────────────────────────────────
    elapsed = (datetime.now() - run_start).seconds

    if no_telegram:
        print("\n[6/6] Telegram skipped (--no-telegram flag).")
        tg_sent = 0
    elif not BOT_TOKEN or not CHAT_ID_LIST:
        print("\n[6/6] Telegram not configured (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set).")
        tg_sent = 0
    else:
        print(f"\n[6/6] Sending Telegram messages to {len(CHAT_ID_LIST)} chat(s)…")
        tg_sent = dispatch_telegram(
            run_start     = run_start,
            elapsed       = elapsed,
            total_uni     = len(universe),
            total_fetched = len(all_data),
            odd_matches   = odd_matches,
            level_matches = level_matches,
            both_matches  = both_matches,
            scored        = scored,
        )
        print(f"   Sent {tg_sent} Telegram message(s)")

    # ── Console summary ───────────────────────────────────────────────
    actionable = [(s, scored[s]) for s in both_matches
                  if scored[s].get("confidence") in ("STRONG", "MODERATE")]
    actionable.sort(key=lambda x: x[1]["score"], reverse=True)

    print("\n" + "=" * 80)
    print("  EXECUTION SUMMARY")
    print("=" * 80)
    print(f"  Total time        : {elapsed}s  (fetch {fetch_s:.1f}s)")
    print(f"  Assets screened   : {len(all_data)}/{len(universe)}")
    print(f"  Odd Square hits   : {len(odd_matches)}")
    print(f"  VF Level hits     : {len(level_matches)}")
    print(f"  Both-criteria hits: {len(both_matches)}")
    print(f"  Actionable signals: {len(actionable)}")
    print(f"  Telegram sent     : {tg_sent} message(s)")

    if actionable:
        print(f"\n  🎯  ACTIONABLE SIGNALS  (top 20 by score):")
        print("  " + "─" * 90)
        hdr = (f"  {'#':>2}  {'Symbol':15} {'Sig':4}  {'Scr':>5}  {'Conf':8}  "
               f"{'Price':>9}  {'VWAP':>9}  {'vsVWAP':6}  "
               f"{'RSI-D':>6}  {'RSI-W':>6}  {'RSI-15':>6}  {'Vol':>6}")
        print(hdr)
        print("  " + "─" * 90)
        for i, (sym, sc) in enumerate(actionable[:20], 1):
            od   = odd_matches[sym]
            vwap = od.get("vwap")
            print(
                f"  {i:>2}  {sym:15} {sc['signal']:4}  {sc['score']:>4}/100"
                f"  {sc['confidence']:8}  ₹{od['price']:>8.2f}"
                f"  ₹{_fmt(vwap):>8}  {_vwap_position(od['price'],vwap):6}"
                f"  {_fmt(od.get('daily_rsi')):>6}"
                f"  {_fmt(od.get('weekly_rsi')):>6}"
                f"  {_fmt(od.get('rsi_15m')):>6}"
                f"  {_fmt(od.get('vol_ratio')):>6}"
            )
        print("  " + "─" * 90)
    else:
        print("\n  ⚠️  No actionable signals found today.")

    print("\n  📁 Output files:")
    for f in (odd_f, level_f, both_f):
        if f:
            print(f"     📎 {f}")
    print("=" * 80)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="AIScan India v7.1")
    ap.add_argument("--dry-run",         action="store_true",
                    help="Test with 20 symbols")
    ap.add_argument("--symbols",         nargs="+",
                    help="Specific symbols (e.g. RELIANCE TCS NIFTY)")
    ap.add_argument("--refresh-symbols", action="store_true",
                    help="Force re-download of symbol master")
    ap.add_argument("--no-telegram",     action="store_true",
                    help="Skip Telegram dispatch even if configured")
    args = ap.parse_args()

    main(
        dry_run          = args.dry_run,
        override_symbols = args.symbols,
        force_refresh    = args.refresh_symbols,
        no_telegram      = args.no_telegram,
    )

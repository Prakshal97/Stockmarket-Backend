"""
Market Data Agent — Fetches live CMP (Current Market Price) and Market Cap
for Indian stocks using yfinance (NSE: TICKER.NS / BSE: TICKER.BO).

No API key required. Uses Yahoo Finance as primary source and NSE as fallback.
"""
import re
import sys
import time
import requests
from typing import Optional, Tuple

# Force UTF-8 on Windows to prevent charmap encoding errors
if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass
if sys.stderr and hasattr(sys.stderr, 'reconfigure'):
    try:
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

# Simple in-memory cache: {ticker: (cmp, market_cap_cr, timestamp)}
_CACHE: dict = {}
CACHE_TTL_SECONDS = 300  # 5 minutes


def _clean_ticker(ticker: str) -> str:
    """Normalize a raw ticker string."""
    if not ticker:
        return ""
    # Remove exchange prefix like NSE: or BSE:
    ticker = re.sub(r'^(NSE|BSE)[:/\s]+', '', ticker, flags=re.IGNORECASE).strip()
    # Remove any non-alphanumeric except dash/ampersand
    ticker = re.sub(r'[^\w&-]', '', ticker).upper()
    return ticker


def _fetch_yfinance(ticker: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Fetch CMP and Market Cap via yfinance.
    Tries TICKER.NS (NSE) first, then TICKER.BO (BSE).
    Returns (cmp, market_cap_cr) or (None, None).
    """
    try:
        import yfinance as yf
    except ImportError:
        print("WARNING: yfinance not installed — run: pip install yfinance")
        return None, None

    for suffix in [".NS", ".BO"]:
        try:
            symbol = ticker + suffix
            stock = yf.Ticker(symbol)
            info = stock.info

            cmp = (
                info.get("currentPrice")
                or info.get("regularMarketPrice")
                or info.get("previousClose")
            )
            market_cap = info.get("marketCap")  # in INR (for Indian stocks)

            if cmp:
                cmp = round(float(cmp), 2)
                market_cap_cr = round(float(market_cap) / 1_00_00_000, 2) if market_cap else None
                print(f"INFO: yfinance {symbol} => CMP={cmp}, MktCap={market_cap_cr} Cr")
                return cmp, market_cap_cr
        except Exception as e:
            print(f"WARNING: yfinance {ticker}{suffix} failed: {e}")
            continue

    return None, None


def _fetch_nse_quote(ticker: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Fetch CMP from NSE's public quote API.
    Returns (cmp, None) — NSE API doesn't directly expose market cap.
    """
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Referer": "https://www.nseindia.com/",
            "Accept-Language": "en-US,en;q=0.9",
        }
        session = requests.Session()
        # Warm up session cookie
        session.get("https://www.nseindia.com/", headers=headers, timeout=5)
        time.sleep(0.5)

        url = f"https://www.nseindia.com/api/quote-equity?symbol={ticker}"
        resp = session.get(url, headers=headers, timeout=8)
        if resp.status_code == 200:
            data = resp.json()
            price_info = data.get("priceInfo", {})
            cmp = price_info.get("lastPrice") or price_info.get("close")
            if cmp:
                print(f"INFO: NSE API {ticker} => CMP={cmp}")
                return round(float(cmp), 2), None
    except Exception as e:
        print(f"WARNING: NSE quote API failed for {ticker}: {e}")

    return None, None


def get_market_data(ticker: str, exchange: str = "NSE") -> dict:
    """
    Main entry point: returns {'cmp': float|None, 'market_cap_cr': float|None}
    Uses cache → yfinance → NSE fallback.
    """
    clean = _clean_ticker(ticker)
    if not clean:
        return {"cmp": None, "market_cap_cr": None}

    # Check cache
    now = time.time()
    if clean in _CACHE:
        cached_cmp, cached_mcap, cached_ts = _CACHE[clean]
        if now - cached_ts < CACHE_TTL_SECONDS:
            return {"cmp": cached_cmp, "market_cap_cr": cached_mcap}

    # Try yfinance first (best for market cap)
    cmp, market_cap_cr = _fetch_yfinance(clean)

    # Fallback: NSE API (CMP only)
    if cmp is None:
        cmp, _ = _fetch_nse_quote(clean)

    # Store in cache
    _CACHE[clean] = (cmp, market_cap_cr, now)

    return {"cmp": cmp, "market_cap_cr": market_cap_cr}


def batch_get_market_data(ticker_list: list) -> dict:
    """
    Fetch market data for multiple tickers, deduplicated.
    Returns {ticker: {'cmp': ..., 'market_cap_cr': ...}}
    """
    unique = list(dict.fromkeys([_clean_ticker(t) for t in ticker_list if t]))
    results = {}
    for ticker in unique:
        if ticker:
            results[ticker] = get_market_data(ticker)
            time.sleep(0.2)  # Be polite to Yahoo Finance
    return results

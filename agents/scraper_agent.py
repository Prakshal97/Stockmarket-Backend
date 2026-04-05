"""
NSE & BSE Scraper Agent
Fetches latest corporate announcements from both exchanges.
Handles deduplication, PDF links, and anti-bot measures.
"""
import os
import time
import hashlib
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from dotenv import load_dotenv

load_dotenv()

NSE_USER_AGENT = os.getenv("NSE_USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

NSE_SESSION = requests.Session()
NSE_SESSION.headers.update({
    "User-Agent": NSE_USER_AGENT,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.nseindia.com/companies-listing/corporate-filings-announcements",
    "Connection": "keep-alive",
})

BSE_SESSION = requests.Session()
BSE_SESSION.headers.update({
    "User-Agent": NSE_USER_AGENT,
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.bseindia.com",
    "Referer": "https://www.bseindia.com/corporates/ann.html",
})


def _make_announcement_id(exchange: str, raw_id: str) -> str:
    """Generate a unique deduplication ID."""
    return hashlib.md5(f"{exchange}:{raw_id}".encode()).hexdigest()


def fetch_nse_announcements(from_date: Optional[str] = None, to_date: Optional[str] = None) -> List[Dict]:
    """
    Fetch corporate announcements from NSE.
    Returns list of normalized announcement dicts.
    """
    announcements = []

    if not from_date:
        from_date = (datetime.now() - timedelta(days=1)).strftime("%d-%m-%Y")
    if not to_date:
        to_date = datetime.now().strftime("%d-%m-%Y")

    try:
        # First hit the main page to get cookies
        NSE_SESSION.get("https://www.nseindia.com", timeout=10)
        time.sleep(1)

        url = "https://www.nseindia.com/api/corporate-announcements"
        params = {
            "index": "equities",
            "from_date": from_date,
            "to_date": to_date,
        }

        response = NSE_SESSION.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()

        items = data if isinstance(data, list) else data.get("data", [])

        for item in items:
            try:
                # Extract PDF attachment URL if available
                pdf_url = None
                attachments = item.get("attchmntFile", "") or item.get("attachments", "")
                if attachments:
                    if not attachments.startswith("http"):
                        pdf_url = f"https://nsearchives.nseindia.com/corporate/{attachments}"
                    else:
                        pdf_url = attachments

                # Parse announcement date
                raw_date = item.get("an_dt") or item.get("date") or item.get("exchdisstime", "")
                try:
                    ann_date = datetime.strptime(raw_date[:10], "%d-%b-%Y") if raw_date else datetime.now()
                except:
                    try:
                        ann_date = datetime.strptime(raw_date[:10], "%Y-%m-%d")
                    except:
                        ann_date = datetime.now()

                symbol = item.get("symbol", "") or item.get("nsesymbol", "")
                company = item.get("comp", "") or item.get("companyName", symbol)
                subject = item.get("desc", "") or item.get("subject", "")
                seq_no = item.get("seqnum", "") or item.get("seq_id", str(item.get("id", "")))

                if not subject:
                    continue

                announcements.append({
                    "exchange": "NSE",
                    "company_name": company,
                    "ticker": symbol,
                    "raw_subject": subject,
                    "raw_body": item.get("body", ""),
                    "pdf_url": pdf_url,
                    "source_url": f"https://www.nseindia.com/companies-listing/corporate-filings-announcements",
                    "announcement_date": ann_date.isoformat(),
                    "fetched_at": datetime.utcnow().isoformat(),
                    "processed": False,
                    "announcement_id": _make_announcement_id("NSE", str(seq_no) + subject[:20]),
                })
            except Exception as e:
                print(f"⚠️ NSE item parse error: {e}")
                continue

        print(f"✅ NSE: Fetched {len(announcements)} announcements")

    except Exception as e:
        print(f"❌ NSE fetch failed: {e}")
        # Return mock data for development/testing
        announcements = _get_mock_nse_announcements()

    return announcements


def fetch_bse_announcements(from_date: Optional[str] = None, to_date: Optional[str] = None) -> List[Dict]:
    """
    Fetch corporate announcements from BSE.
    Returns list of normalized announcement dicts.
    """
    announcements = []

    if not from_date:
        from_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    if not to_date:
        to_date = datetime.now().strftime("%Y%m%d")

    try:
        url = "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"
        params = {
            "strCat": "-1",
            "strPrevDate": from_date,
            "strScrip": "",
            "strSearch": "P",
            "strToDate": to_date,
            "strType": "C",
            "subcategory": "-1",
        }

        response = BSE_SESSION.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()

        items = data.get("Table", []) if isinstance(data, dict) else []

        for item in items:
            try:
                # BSE PDF link
                pdf_url = None
                scrip_cd = item.get("SCRIP_CD", "")
                news_id = item.get("NEWSID", "")
                attach_file = item.get("ATTACHMENTNAME", "")
                if attach_file:
                    pdf_url = f"https://www.bseindia.com/xml-data/corpfiling/AttachHis/{attach_file}"

                # Parse BSE date format
                raw_date = item.get("News_submission_dt", "") or item.get("DT_TM", "")
                try:
                    ann_date = datetime.strptime(raw_date[:10], "%Y-%m-%d")
                except:
                    ann_date = datetime.now()

                subject = item.get("NEWSSUB", "") or item.get("HEADLINE", "")
                company = item.get("SLONGNAME", "") or item.get("COMPANYNAME", str(scrip_cd))

                if not subject:
                    continue

                announcements.append({
                    "exchange": "BSE",
                    "company_name": company,
                    "ticker": item.get("NSE_SYMBOL", str(scrip_cd)),
                    "raw_subject": subject,
                    "raw_body": item.get("NEWSSUB", ""),
                    "pdf_url": pdf_url,
                    "source_url": f"https://www.bseindia.com/corporates/ann.html",
                    "announcement_date": ann_date.isoformat(),
                    "fetched_at": datetime.utcnow().isoformat(),
                    "processed": False,
                    "announcement_id": _make_announcement_id("BSE", str(news_id) + subject[:20]),
                })
            except Exception as e:
                print(f"⚠️ BSE item parse error: {e}")
                continue

        print(f"✅ BSE: Fetched {len(announcements)} announcements")

    except Exception as e:
        print(f"❌ BSE fetch failed: {e}")
        announcements = _get_mock_bse_announcements()

    return announcements


def extract_pdf_text(pdf_url: str) -> Optional[str]:
    """Download and extract text from a PDF announcement."""
    try:
        import pdfplumber
        import io

        response = requests.get(pdf_url, timeout=20, headers={"User-Agent": NSE_USER_AGENT})
        response.raise_for_status()

        with pdfplumber.open(io.BytesIO(response.content)) as pdf:
            text = ""
            for page in pdf.pages[:5]:  # Max 5 pages
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"
        return text[:5000]  # Limit for AI context
    except Exception as e:
        print(f"⚠️ PDF extraction failed for {pdf_url}: {e}")
        return None


def fetch_all_announcements() -> List[Dict]:
    """Fetch from both NSE and BSE, combine results."""
    print("🔄 Starting announcement fetch cycle...")
    nse = fetch_nse_announcements()
    time.sleep(2)  # Polite delay
    bse = fetch_bse_announcements()
    all_announcements = nse + bse
    print(f"📊 Total fetched: {len(all_announcements)} announcements")
    return all_announcements


# ─── Mock Data for Development/Testing ─────────────────────────────────────

def _get_mock_nse_announcements() -> List[Dict]:
    """Fallback mock data when NSE API is unavailable."""
    now = datetime.now()
    return [
        {
            "exchange": "NSE",
            "company_name": "Reliance Industries Ltd",
            "ticker": "RELIANCE",
            "raw_subject": "Board Meeting - Financial Results for Q4 FY2024",
            "raw_body": "Reliance Industries Limited has declared financial results for Q4 FY2024. Revenue increased by 12% YoY to Rs 2,23,000 Cr. Net Profit stands at Rs 18,951 Cr, up 6% YoY. EBITDA margin improved to 17.2%. Board has recommended final dividend of Rs 9 per share.",
            "pdf_url": None,
            "source_url": "https://www.nseindia.com/companies-listing/corporate-filings-announcements",
            "announcement_date": now.isoformat(),
            "fetched_at": now.isoformat(),
            "processed": False,
            "announcement_id": _make_announcement_id("NSE", "RELIANCE_Q4FY24_RESULTS"),
        },
        {
            "exchange": "NSE",
            "company_name": "Tata Consultancy Services Ltd",
            "ticker": "TCS",
            "raw_subject": "Increase in Authorized Share Capital",
            "raw_body": "TCS Board approves increase in Authorized Share Capital from Rs 375 Crore to Rs 500 Crore. Board Meeting held on 05-Apr-2024. Resolution passed with requisite majority.",
            "pdf_url": None,
            "source_url": "https://www.nseindia.com/companies-listing/corporate-filings-announcements",
            "announcement_date": now.isoformat(),
            "fetched_at": now.isoformat(),
            "processed": False,
            "announcement_id": _make_announcement_id("NSE", "TCS_AUTH_CAP_2024"),
        },
        {
            "exchange": "NSE",
            "company_name": "HDFC Bank Ltd",
            "ticker": "HDFCBANK",
            "raw_subject": "Dividend Announcement - Final Dividend FY2024",
            "raw_body": "HDFC Bank announces final dividend of Rs 19.50 per equity share of face value Rs 1 each for FY2024. Record date is 12-April-2024. Total outflow approximately Rs 14,800 Cr.",
            "pdf_url": None,
            "source_url": "https://www.nseindia.com/companies-listing/corporate-filings-announcements",
            "announcement_date": now.isoformat(),
            "fetched_at": now.isoformat(),
            "processed": False,
            "announcement_id": _make_announcement_id("NSE", "HDFCBANK_DIV_FY24"),
        },
        {
            "exchange": "NSE",
            "company_name": "Infosys Ltd",
            "ticker": "INFY",
            "raw_subject": "Major Order Win - $1.5 Billion Deal with Global Retailer",
            "raw_body": "Infosys has signed a $1.5 billion multi-year deal with a leading global retail chain for end-to-end IT transformation. The deal covers cloud migration, ERP modernization, and AI-driven analytics over 7 years.",
            "pdf_url": None,
            "source_url": "https://www.nseindia.com/companies-listing/corporate-filings-announcements",
            "announcement_date": now.isoformat(),
            "fetched_at": now.isoformat(),
            "processed": False,
            "announcement_id": _make_announcement_id("NSE", "INFY_ORDER_WIN_2024"),
        },
        {
            "exchange": "NSE",
            "company_name": "Adani Enterprises Ltd",
            "ticker": "ADANIENT",
            "raw_subject": "Preferential Allotment of Shares",
            "raw_body": "Adani Enterprises Board approves preferential issue of 1,00,00,000 equity shares at Rs 2850 per share to certain identified investors. Total fund raise of Rs 2850 Crore subject to shareholder approval.",
            "pdf_url": None,
            "source_url": "https://www.nseindia.com/companies-listing/corporate-filings-announcements",
            "announcement_date": now.isoformat(),
            "fetched_at": now.isoformat(),
            "processed": False,
            "announcement_id": _make_announcement_id("NSE", "ADANIENT_PREF_2024"),
        },
    ]


def _get_mock_bse_announcements() -> List[Dict]:
    """Fallback mock data when BSE API is unavailable."""
    now = datetime.now()
    return [
        {
            "exchange": "BSE",
            "company_name": "ONGC Ltd",
            "ticker": "ONGC",
            "raw_subject": "Increase in Authorized Capital from Rs 12,850 Cr to Rs 15,000 Cr",
            "raw_body": "Board of Directors approved increase in Authorized Share Capital from Rs 12,850 Crore to Rs 15,000 Crore. Board Meeting date: 03-Apr-2024. Subject to shareholders approval via postal ballot.",
            "pdf_url": None,
            "source_url": "https://www.bseindia.com/corporates/ann.html",
            "announcement_date": now.isoformat(),
            "fetched_at": now.isoformat(),
            "processed": False,
            "announcement_id": _make_announcement_id("BSE", "ONGC_AUTH_CAP_2024"),
        },
        {
            "exchange": "BSE",
            "company_name": "Wipro Ltd",
            "ticker": "WIPRO",
            "raw_subject": "Buyback of Equity Shares",
            "raw_body": "Wipro Board approves buyback of up to 26,96,08,470 equity shares at Rs 445 per share. Total buyback size Rs 12,000 Crore (approx 3.15% of total paid-up equity). Open Market route via Stock Exchange.",
            "pdf_url": None,
            "source_url": "https://www.bseindia.com/corporates/ann.html",
            "announcement_date": now.isoformat(),
            "fetched_at": now.isoformat(),
            "processed": False,
            "announcement_id": _make_announcement_id("BSE", "WIPRO_BUYBACK_2024"),
        },
        {
            "exchange": "BSE",
            "company_name": "Maruti Suzuki India Ltd",
            "ticker": "MARUTI",
            "raw_subject": "Q4 FY2024 Production & Sales Data",
            "raw_body": "Maruti Suzuki reports total sales of 1,85,899 units in March 2024, up 3.5% YoY. Exports stood at 23,956 units. Total FY2024 domestic sales cross 20 lakh units for first time in company history.",
            "pdf_url": None,
            "source_url": "https://www.bseindia.com/corporates/ann.html",
            "announcement_date": now.isoformat(),
            "fetched_at": now.isoformat(),
            "processed": False,
            "announcement_id": _make_announcement_id("BSE", "MARUTI_SALES_Q4FY24"),
        },
    ]

"""
NSE & BSE Scraper Agent
Fetches latest corporate announcements from both exchanges.

KEY INSIGHT (2026-05-06):
- NSE auth capital announcements are hidden inside "Outcome of Board Meeting" entries.
  The real content is ONLY in the PDF attachment. We must fetch PDFs for suspect subjects.
- BSE's primary JSON API (api.bseindia.com) returns empty {} — we use their alternative
  search endpoint which still works.

Field names from live NSE API:
  - company  → sm_name
  - body     → attchmntText (often empty; real info is in PDF)
  - subject  → desc
  - date     → an_dt (format: "06-May-2026 16:11:43")
  - pdf      → attchmntFile (relative path, prefix with nsearchives URL)
  - id       → seq_id
  - ticker   → symbol
"""
import os
import io
import time
import hashlib
import requests
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional
from dotenv import load_dotenv

load_dotenv()

NSE_USER_AGENT = os.getenv(
    "NSE_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# Subjects that often hide auth-capital content in their PDFs (NSE-specific)
SUSPECT_SUBJECTS = [
    "outcome of board meeting",
    "outcome of the board meeting",
    "board meeting outcome",
    "outcome of meeting",
    "board meeting held",
    "outcome of the meeting",
    "result of board meeting",
    "update on board meeting",
    "intimation of board meeting",
    "general updates",
    "updates",
    "corporate action",
    "other",
]

IST = timezone(timedelta(hours=5, minutes=30))
FETCH_WINDOW_HOURS = int(os.getenv("FETCH_WINDOW_HOURS", "48"))

# ── NSE Session ──────────────────────────────────────────────────────────────
NSE_SESSION = requests.Session()
NSE_SESSION.headers.update({
    "User-Agent": NSE_USER_AGENT,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    # Keep this to encodings requests can always decode. NSE may return Brotli
    # bytes when "br" is advertised, which breaks response.json() without brotli.
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
})

# ── BSE Session ──────────────────────────────────────────────────────────────
BSE_SESSION = requests.Session()
BSE_SESSION.headers.update({
    "User-Agent": NSE_USER_AGENT,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Referer": "https://www.bseindia.com/corporates/ann.html",
    "Origin": "https://www.bseindia.com",
})

_nse_session_warmed = False

def _make_announcement_id(exchange: str, raw_id: str) -> str:
    """Generate a unique deduplication ID."""
    return hashlib.md5(f"{exchange}:{raw_id}".encode()).hexdigest()


def _window_start_ist(hours: int = FETCH_WINDOW_HOURS) -> datetime:
    return datetime.now(IST) - timedelta(hours=hours)


def _to_utc_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=IST)
    return dt.astimezone(timezone.utc).replace(tzinfo=None).isoformat() + "Z"


def _parse_nse_datetime(raw_date: str) -> datetime:
    raw_date = (raw_date or "").strip()
    for fmt, value in [
        ("%d-%b-%Y %H:%M:%S", raw_date[:20].strip()),
        ("%d-%b-%Y", raw_date[:11].strip()),
        ("%d%m%Y%H%M%S", raw_date[:14].strip()),
    ]:
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=IST)
        except Exception:
            pass
    return datetime.now(IST)


def _parse_bse_datetime(item: dict) -> datetime:
    raw_values = [
        item.get("DissemDT"),
        item.get("News_submission_dt"),
        item.get("DT_TM"),
        item.get("NEWS_DT"),
        item.get("DateTime"),
    ]
    formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%Y-%m-%d",
        "%d/%m/%Y",
    ]
    for raw in raw_values:
        raw = str(raw or "").strip()
        if not raw:
            continue
        normalized = raw.replace("Z", "").split(".")[0]
        for fmt in formats:
            try:
                return datetime.strptime(normalized[:len(datetime.now().strftime(fmt))], fmt).replace(tzinfo=IST)
            except Exception:
                pass
        try:
            return datetime.fromisoformat(normalized).replace(tzinfo=IST)
        except Exception:
            pass
    return datetime.now(IST)

def _warm_nse_session(force: bool = False):
    """Warm up NSE session to get valid cookies."""
    global _nse_session_warmed
    if _nse_session_warmed and not force:
        return
    for attempt in range(2):
        try:
            NSE_SESSION.get("https://www.nseindia.com", timeout=20)
            time.sleep(2)
            # Also hit the announcements page to get the right cookie context
            NSE_SESSION.get(
                "https://www.nseindia.com/companies-listing/corporate-filings-announcements",
                timeout=20
            )
            time.sleep(1)
            NSE_SESSION.headers.update({
                "Referer": "https://www.nseindia.com/companies-listing/corporate-filings-announcements",
                "X-Requested-With": "XMLHttpRequest",
            })
            _nse_session_warmed = True
            print("INFO: NSE session warmed up OK")
            return
        except Exception as e:
            print(f"WARNING: NSE warmup attempt {attempt+1} failed: {e}")
            time.sleep(3)
    print("WARNING: NSE warmup failed after 2 attempts — will try anyway")


def is_suspect_subject(subject: str) -> bool:
    """Return True if this subject often hides auth-capital content in the PDF."""
    s = subject.lower().strip()
    return any(sus in s for sus in SUSPECT_SUBJECTS)


def _should_scan_bse_pdf(subject: str, body: str) -> bool:
    """Keep BSE PDF scans focused; full-feed PDF scans are too slow."""
    text = f"{subject} {body}".lower()
    strong_terms = [
        "authorized capital",
        "authorised capital",
        "authorized share capital",
        "authorised share capital",
        "increase in authorized",
        "increase in authorised",
        "increase in the authorized",
        "increase in the authorised",
        "increase in share capital",
        "alteration of capital",
        "capital clause",
        "memorandum of association",
        "clause v of the memorandum",
    ]
    return any(term in text for term in strong_terms)


def extract_pdf_text(pdf_url: str, max_pages: int = 10) -> Dict:
    """
    Download and extract text from a PDF announcement attachment.
    Implements OCR fallback if native text extraction fails or yields too little text.
    Returns: dict with keys: text, method, success, error
    """
    result = {"text": None, "method": "none", "success": False, "error": ""}
    try:
        import pdfplumber

        headers = {"User-Agent": NSE_USER_AGENT}
        if "nsearchives" in pdf_url or "nseindia" in pdf_url:
            resp = NSE_SESSION.get(pdf_url, timeout=30)
        else:
            resp = requests.get(pdf_url, timeout=30, headers=headers)
        
        resp.raise_for_status()
        if len(resp.content) < 100:
            result["error"] = "PDF too small or empty"
            return result

        text_parts = []
        has_images = False
        
        with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
            for page in pdf.pages[:max_pages]:
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)
                
                # Check if page might be a scanned image
                if len(page.images) > 0:
                    has_images = True
                    
                tables = page.extract_tables()
                for table in tables:
                    for row in table:
                        if row:
                            text_parts.append(" | ".join(str(c) for c in row if c))

        extracted_text = "\n".join(text_parts)[:10000]
        
        # Check if we got meaningful text. If not, try OCR
        if len(extracted_text.strip()) < 50 and has_images:
            try:
                import pytesseract
                from pdf2image import convert_from_bytes
                
                images = convert_from_bytes(resp.content, first_page=1, last_page=max_pages)
                ocr_text = []
                for img in images:
                    # Basic preprocessing could be added here
                    ocr_text.append(pytesseract.image_to_string(img))
                    
                ocr_final = "\n".join(ocr_text)[:10000]
                if len(ocr_final.strip()) > 10:
                    result["text"] = ocr_final
                    result["method"] = "ocr"
                    result["success"] = True
                    return result
            except Exception as ocr_e:
                print(f"INFO: OCR fallback failed for {pdf_url}: {ocr_e}")
                result["error"] += f" | OCR Failed: {ocr_e}"
        
        if extracted_text.strip():
            result["text"] = extracted_text
            result["method"] = "pdf_text"
            result["success"] = True
        else:
            result["error"] = "No extractable text found"
            
        return result
    except Exception as e:
        print(f"WARNING: PDF extraction failed for {pdf_url}: {e}")
        result["error"] = str(e)
        return result


def fetch_nse_announcements(from_date: Optional[str] = None, to_date: Optional[str] = None) -> List[Dict]:
    """
    Fetch corporate announcements from NSE.
    
    Dates in DD-MM-YYYY format.
    Automatically pre-scans PDFs for 'Outcome of Board Meeting' items so
    the classifier can detect auth-capital content hidden in PDFs.
    """
    announcements = []

    if not from_date:
        from_date = _window_start_ist().strftime("%d-%m-%Y")
    if not to_date:
        to_date = datetime.now(IST).strftime("%d-%m-%Y")

    _warm_nse_session()

    try:
        url = "https://www.nseindia.com/api/corporate-announcements"
        params = {
            "index": "equities",
            "from_date": from_date,
            "to_date": to_date,
        }

        try:
            response = NSE_SESSION.get(url, params=params, timeout=25)
            response.raise_for_status()
            # Verify JSON content type
            if "application/json" in response.headers.get("Content-Type", ""):
                try:
                    data = response.json()
                except Exception as json_err:
                    raw = response.text[:500]
                    print(f"WARNING: NSE JSON decode failed: {json_err}. Raw response snippet: {raw}")
                    data = []
            else:
                print(f"WARNING: NSE response non-JSON content type: {response.headers.get('Content-Type')}, status {response.status_code}")
                data = []
        except Exception as http_err:
            print(f"WARNING: NSE request failed: {http_err}")
            data = []
        items = data if isinstance(data, list) else data.get("data", [])
        print(f"INFO: NSE raw API returned {len(items)} items")
        # If primary endpoint returned no items, try alternative endpoint
        if not items:
            alt_url = "https://www.nseindia.com/api/company-announcements"
            alt_params = {"symbol": "*", "fromdate": from_date, "todate": to_date}
            try:
                alt_resp = NSE_SESSION.get(alt_url, params=alt_params, timeout=25)
                alt_resp.raise_for_status()
                # Verify JSON content type
                if "application/json" in alt_resp.headers.get("Content-Type", ""):
                    alt_data = alt_resp.json()
                    alt_items = alt_data if isinstance(alt_data, list) else alt_data.get("data", [])
                    if alt_items:
                        print(f"INFO: NSE alternative endpoint returned {len(alt_items)} items")
                        items = alt_items
                else:
                    print(f"WARNING: NSE alternative endpoint returned non-JSON content type: {alt_resp.headers.get('Content-Type')}")
            except Exception as alt_err:
                print(f"WARNING: NSE alternative fetch failed: {alt_err}")

        for item in items:
            try:
                # ── Field mapping (from live NSE API inspection) ──────────────
                pdf_file = item.get("attchmntFile", "")
                if pdf_file:
                    if pdf_file.startswith("http"):
                        pdf_url = pdf_file
                    else:
                        # Remove any leading slash
                        pdf_file = pdf_file.lstrip("/")
                        pdf_url = f"https://nsearchives.nseindia.com/corporate/{pdf_file}"
                else:
                    pdf_url = None

                # Date examples: "06-May-2026 16:11:43" or "06052026165911"
                raw_date = (item.get("an_dt") or item.get("dt") or "").strip()
                ann_date = _parse_nse_datetime(raw_date)
                if ann_date < _window_start_ist():
                    continue

                symbol = (item.get("symbol") or "").strip()
                company = (item.get("sm_name") or item.get("companyName") or symbol).strip()
                subject = (item.get("desc") or "").strip()
                body = (item.get("attchmntText") or "").strip()
                seq_no = item.get("seq_id") or item.get("seqId") or str(hash(symbol + subject))

                if not subject:
                    continue

                ann = {
                    "exchange": "NSE",
                    "company_name": company,
                    "ticker": symbol,
                    "raw_subject": subject,
                    "raw_body": body,
                    "pdf_url": pdf_url,
                    "source_url": f"https://www.nseindia.com/companies-listing/corporate-filings-announcements?symbol={symbol}",
                    "announcement_date": _to_utc_iso(ann_date),
                    "fetched_at": datetime.utcnow().isoformat() + "Z",
                    "processed": False,
                    "announcement_id": _make_announcement_id("NSE", str(seq_no) + subject[:20]),
                }

                ann["extraction_method"] = "none"
                ann["extraction_success"] = False
                ann["extraction_error"] = ""

                # ── KEY FIX: Pre-scan PDF for suspect subjects ───────────────
                # "Outcome of Board Meeting" items hide auth-capital content in PDFs.
                # We fetch the PDF text NOW so classifier can detect it.
                if is_suspect_subject(subject) and pdf_url and not body:
                    print(f"INFO: [NSE] Pre-scanning PDF for '{company}' ({subject[:50]})...")
                    pdf_res = extract_pdf_text(pdf_url)
                    if pdf_res["success"] and pdf_res["text"]:
                        ann["raw_body"] = pdf_res["text"]
                        ann["extraction_method"] = pdf_res["method"]
                        ann["extraction_success"] = True
                        print(f"INFO: [NSE] PDF pre-scan OK ({pdf_res['method']}) for {company} ({len(pdf_res['text'])} chars)")
                    else:
                        ann["extraction_error"] = pdf_res["error"]

                announcements.append(ann)

            except Exception as e:
                print(f"WARNING: NSE item parse error: {e}")
                continue

        print(f"OK NSE: Fetched {len(announcements)} announcements (from {from_date} to {to_date})")

    except Exception as e:
        print(f"ERROR: NSE fetch failed: {e}")
        # Session may be stale — reset it for next attempt
        global _nse_session_warmed
        _nse_session_warmed = False

    return announcements


def fetch_bse_announcements(from_date: Optional[str] = None, to_date: Optional[str] = None) -> List[Dict]:
    """
    Fetch corporate announcements from BSE.
    
    Dates in YYYYMMDD format.
    Uses multiple fallback endpoints since BSE's primary API returns empty {}.
    """
    announcements = []

    if not from_date:
        from_date = _window_start_ist().strftime("%Y%m%d")
    if not to_date:
        to_date = datetime.now(IST).strftime("%Y%m%d")

    # Convert YYYYMMDD to DD/MM/YYYY for some endpoints
    try:
        dt_from = datetime.strptime(from_date, "%Y%m%d")
        dt_to = datetime.strptime(to_date, "%Y%m%d")
        from_dmy = dt_from.strftime("%d/%m/%Y")
        to_dmy = dt_to.strftime("%d/%m/%Y")
        from_ymd = dt_from.strftime("%Y-%m-%d")
        to_ymd = dt_to.strftime("%Y-%m-%d")
    except Exception:
        from_dmy = to_dmy = ""
        from_ymd = to_ymd = ""

    # Endpoint 1: paginated API used by the public BSE announcements page.
    items = _fetch_bse_getannouncements(from_dmy, to_dmy)

    # Endpoint 2: AnnSubCategoryGetData fallback.
    if not items:
        print("INFO: BSE paginated endpoint returned nothing, trying subcategory API...")
        items = _fetch_bse_primary(from_date, to_date)

    # Endpoint 3: alternate getanndata fallback.
    if not items:
        print("INFO: BSE subcategory API returned nothing, trying getanndata...")
        items = _fetch_bse_fallback_search(from_ymd, to_ymd)

    if not items:
        print("⚠️ BSE: All endpoints returned empty. BSE API may be down.")
        return []

    for item in items:
        try:
            scrip_cd = item.get("SCRIP_CD") or item.get("scrip_cd") or item.get("ScripCode", "")
            news_id = item.get("NEWSID") or item.get("NewsId") or item.get("news_id") or ""
            attach_file = (
                item.get("ATTACHMENTNAME") or 
                item.get("AttachmentName") or 
                item.get("attachmentname") or ""
            )

            ann_date = _parse_bse_datetime(item)
            if ann_date < _window_start_ist():
                continue
            
            pdf_url = None
            if attach_file:
                pdf_url = f"https://www.bseindia.com/xml-data/corpfiling/AttachLive/{attach_file}"

            subject = (
                item.get("NEWSSUB") or 
                item.get("NewsSub") or 
                item.get("HEADLINE") or 
                item.get("Headline") or ""
            ).strip()
            company = (
                item.get("SLONGNAME") or 
                item.get("CompanyName") or 
                item.get("COMPANYNAME") or 
                str(scrip_cd)
            ).strip()
            ticker = (
                item.get("NSE_SYMBOL") or 
                item.get("NseSymbol") or 
                str(scrip_cd)
            ).strip()

            if not subject:
                continue

            body = " ".join(
                str(item.get(key) or "").strip()
                for key in ["HEADLINE", "MORE", "CATEGORYNAME", "ANNOUNCEMENT_TYPE"]
                if item.get(key)
            ) or subject

            ann = {
                "exchange": "BSE",
                "company_name": company,
                "ticker": ticker,
                "raw_subject": subject,
                "raw_body": body,
                "pdf_url": pdf_url,
                "source_url": f"https://www.bseindia.com/corporates/ann.html?scripcd={scrip_cd}&newsid={news_id}",
                "announcement_date": _to_utc_iso(ann_date),
                "fetched_at": datetime.utcnow().isoformat() + "Z",
                "processed": False,
                "announcement_id": _make_announcement_id("BSE", str(news_id) + subject[:20]),
            }

            ann["extraction_method"] = "none"
            ann["extraction_success"] = False
            ann["extraction_error"] = ""

            # Pre-scan PDFs for suspect subjects (same logic as NSE)
            if _should_scan_bse_pdf(subject, body) and pdf_url:
                print(f"INFO: [BSE] Pre-scanning PDF for '{company}' ({subject[:50]})...")
                pdf_res = extract_pdf_text(pdf_url)
                if pdf_res["success"] and pdf_res["text"]:
                    ann["raw_body"] = pdf_res["text"]
                    ann["extraction_method"] = pdf_res["method"]
                    ann["extraction_success"] = True
                    print(f"INFO: [BSE] PDF pre-scan OK ({pdf_res['method']}) for {company} ({len(pdf_res['text'])} chars)")
                else:
                    ann["extraction_error"] = pdf_res["error"]

            announcements.append(ann)

        except Exception as e:
            print(f"WARNING: BSE item parse error: {e}")
            continue

    print(f"OK BSE: Fetched {len(announcements)} announcements (from {from_date} to {to_date})")
    return announcements


def _fetch_bse_primary(from_date: str, to_date: str) -> list:
    """BSE primary endpoint: AnnSubCategoryGetData."""
    try:
        url = "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"
        for search_type in ["P", "A", "C"]:
            params = {
                "strCat": "-1",
                "strPrevDate": from_date,
                "strScrip": "",
                "strSearch": search_type,
                "strToDate": to_date,
                "strType": "C",
                "subcategory": "-1",
            }
            resp = BSE_SESSION.get(url, params=params, timeout=20)
            if resp.status_code == 200:
                data = resp.json()
                items = data.get("Table", []) if isinstance(data, dict) else []
                if items:
                    print(f"INFO: BSE primary endpoint returned {len(items)} items (search={search_type})")
                    return items
        return []
    except Exception as e:
        print(f"WARNING: BSE primary endpoint failed: {e}")
        return []


def _fetch_bse_fallback_search(from_ymd: str, to_ymd: str) -> list:
    """BSE fallback: getanndata endpoint (different API path)."""
    try:
        url = "https://api.bseindia.com/BseIndiaAPI/api/getanndata/w"
        params = {
            "scripcode": "",
            "strCat": "-1",
            "strPrevDate": from_ymd,
            "strToDate": to_ymd,
            "strType": "C",
            "subcategory": "-1",
        }
        resp = BSE_SESSION.get(url, params=params, timeout=20)
        if resp.status_code == 200:
            # Ensure response is JSON
            if "application/json" in resp.headers.get("Content-Type", ""):
                try:
                    data = resp.json()
                except Exception as json_err:
                    raw = resp.text[:500]
                    print(f"WARNING: BSE fallback JSON decode failed: {json_err}. Raw response snippet: {raw}")
                    data = {}
                items = data.get("Table", []) if isinstance(data, dict) else []
                if items:
                    print(f"INFO: BSE fallback (getanndata) returned {len(items)} items")
                    return items
            else:
                print(f"WARNING: BSE fallback returned non-JSON content type: {resp.headers.get('Content-Type')}")
        return []
    except Exception as e:
        print(f"WARNING: BSE fallback getanndata failed: {e}")
        return []


def _fetch_bse_getannouncements(from_dmy: str, to_dmy: str) -> list:
    """BSE third fallback: announcements search via corp filing."""
    try:
        BSE_SESSION.get("https://www.bseindia.com/corporates/ann.html", timeout=20)
        url = "https://api.bseindia.com/BseIndiaAPI/api/AnnGetData/w"
        all_items = []
        total_pages = None
        from_ymd = datetime.strptime(from_dmy, "%d/%m/%Y").strftime("%Y%m%d")
        to_ymd = datetime.strptime(to_dmy, "%d/%m/%Y").strftime("%Y%m%d")

        for page in range(1, 101):
            params = {
                "pageno": page,
                "strCat": "-1",
                "strPrevDate": from_ymd,
                "strScrip": "",
                "strSearch": "P",
                "strToDate": to_ymd,
                "strType": "C",
            }
            resp = BSE_SESSION.get(url, params=params, timeout=20)
            if resp.status_code != 200:
                break
            if "application/json" not in resp.headers.get("Content-Type", ""):
                print(f"WARNING: BSE AnnGetData returned non-JSON content type: {resp.headers.get('Content-Type')}")
                break
            data = resp.json()
            items = data.get("Table", []) if isinstance(data, dict) else []
            if not items:
                break
            all_items.extend(items)
            if total_pages is None:
                try:
                    total_pages = int(items[0].get("TotalPageCnt") or 0)
                except Exception:
                    total_pages = 0
            if total_pages and page >= total_pages:
                break
            time.sleep(0.2)

        if all_items:
            print(f"INFO: BSE AnnGetData returned {len(all_items)} items")
            return all_items

        # Try the BSE's public announcement search
        url = "https://www.bseindia.com/corporates/Comp_Annoucements.aspx"
        # This is HTML, not JSON — skip unless we add HTML parsing
        # Instead try the JSON API that the BSE announcements page uses
        url2 = "https://api.bseindia.com/BseIndiaAPI/api/AnnGetData/w"
        params = {
            "strCat": "-1",
            "strPrevDate": from_dmy,
            "strScrip": "",
            "strSearch": "P",
            "strToDate": to_dmy,
            "strType": "C",
            "subcategory": "-1",
        }
        resp = BSE_SESSION.get(url2, params=params, timeout=20)
        if resp.status_code == 200:
            data = resp.json()
            items = data.get("Table", []) if isinstance(data, dict) else []
            if items:
                print(f"INFO: BSE AnnGetData returned {len(items)} items")
                return items

        # Last resort: try the Authorised Capital category directly (category 30 = Capital)
        url3 = "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"
        for cat in ["30", "29", "31", "32"]:
            params3 = {
                "strCat": cat,
                "strPrevDate": from_dmy.replace("/", ""),
                "strScrip": "",
                "strSearch": "P",
                "strToDate": to_dmy.replace("/", ""),
                "strType": "C",
                "subcategory": "-1",
            }
            resp3 = BSE_SESSION.get(url3, params=params3, timeout=20)
            if resp3.status_code == 200:
                data3 = resp3.json()
                items3 = data3.get("Table", []) if isinstance(data3, dict) else []
                if items3:
                    print(f"INFO: BSE Category {cat} returned {len(items3)} items")
                    return items3

        return []
    except Exception as e:
        print(f"WARNING: BSE third fallback failed: {e}")
        return []


def fetch_all_announcements() -> List[Dict]:
    """Fetch from both NSE and BSE."""
    nse = fetch_nse_announcements()
    time.sleep(2)
    bse = fetch_bse_announcements()
    all_ann = nse + bse
    print(f"INFO: Total fetched: {len(all_ann)} ({len(nse)} NSE + {len(bse)} BSE)")
    return all_ann

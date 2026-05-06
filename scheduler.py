"""
APScheduler — runs the full fetch → classify → route → process pipeline
every N minutes automatically.

Architecture:
  Step 1: Fetch all announcements from NSE/BSE
  Step 2: Classify each as 'authorized_capital' or 'general'
  Step 3: Route to correct collection
  Step 4: Process unprocessed items in each collection separately
  Step 5: Promotion logic: If a general announcement's PDF contains Auth Capital info, move it.
  Step 6: Cleanup old data from both collections
"""
import asyncio
import os
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

load_dotenv()

FETCH_INTERVAL = int(os.getenv("FETCH_INTERVAL_MINUTES", "10"))
MAX_PROCESS = int(os.getenv("MAX_PROCESS_PER_CYCLE", "100"))

scheduler = AsyncIOScheduler()
last_run: dict = {"time": None, "count": 0, "auth_new": 0, "general_new": 0}
_auth_live_cache: dict = {"time": None, "data": []}

def get_cached_recent_authorized_capital() -> list:
    """Return the most recently built live authorized-capital cache."""
    return _auth_live_cache.get("data", [])

async def run_pipeline():
    """Full pipeline: Scrape → Classify → Route → Process → Save."""
    from agents.scraper_agent import fetch_all_announcements
    from agents.classifier import is_pure_authorized_capital
    from database import (
        upsert_authorized_capital, upsert_general_announcement,
        cleanup_old_announcements
    )

    print(f"\n{'='*60}")
    print(f"PIPELINE: Started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")

    # ── Step 1: Fetch ─────────────────────────────────────────────────────────
    raw_announcements = await asyncio.to_thread(fetch_all_announcements)

    # ── Step 2 & 3: Classify and Route ────────────────────────────────────────
    auth_new = 0
    general_new = 0

    for ann in raw_announcements:
        if is_pure_authorized_capital(ann):
            is_new = await upsert_authorized_capital(ann)
            if is_new:
                auth_new += 1
                print(f"NEW AUTH: {ann.get('company_name')} — {ann.get('raw_subject', '')[:60]}")
        else:
            is_new = await upsert_general_announcement(ann)
            if is_new:
                general_new += 1

    print(f"CLASSIFY: {auth_new} new auth capital + {general_new} new general (from {len(raw_announcements)} fetched)")

    # ── Step 4: Process Collections ───────────────────────────────────────────
    auth_processed = await _process_authorized_capital(MAX_PROCESS)
    general_processed = await _process_general_announcements(MAX_PROCESS)

    total_processed = auth_processed + general_processed
    last_run["time"] = datetime.utcnow().isoformat() + "Z"
    last_run["count"] = total_processed
    last_run["auth_new"] = auth_new
    last_run["general_new"] = general_new

    print(f"SUCCESS: Pipeline complete: {auth_processed} auth + {general_processed} general processed")

    await cleanup_old_announcements()
    print(f"{'='*60}\n")

async def run_pipeline_extended(days: int = 2):
    """Extended pipeline for force-fetching historical data."""
    from agents.scraper_agent import fetch_nse_announcements, fetch_bse_announcements
    from agents.classifier import is_pure_authorized_capital
    from database import (
        upsert_authorized_capital, upsert_general_announcement,
        cleanup_old_announcements
    )
    import time

    print(f"\n{'='*60}")
    print(f"EXTENDED PIPELINE: Fetching last {days} days")
    print(f"{'='*60}")

    hours = min(days * 24, int(os.getenv("FETCH_WINDOW_HOURS", "48")))
    from_date_nse = (datetime.now() - timedelta(hours=hours)).strftime("%d-%m-%Y")
    to_date_nse = datetime.now().strftime("%d-%m-%Y")
    from_date_bse = (datetime.now() - timedelta(hours=hours)).strftime("%Y%m%d")
    to_date_bse = datetime.now().strftime("%Y%m%d")

    nse = await asyncio.to_thread(fetch_nse_announcements, from_date_nse, to_date_nse)
    time.sleep(2)
    bse = await asyncio.to_thread(fetch_bse_announcements, from_date_bse, to_date_bse)
    raw_announcements = nse + bse

    print(f"EXTENDED: Fetched {len(raw_announcements)} total ({len(nse)} NSE + {len(bse)} BSE)")

    auth_new = general_new = 0
    for ann in raw_announcements:
        if is_pure_authorized_capital(ann):
            is_new = await upsert_authorized_capital(ann)
            if is_new:
                auth_new += 1
        else:
            is_new = await upsert_general_announcement(ann)
            if is_new:
                general_new += 1

    print(f"EXTENDED CLASSIFY: {auth_new} new auth capital + {general_new} new general")

    auth_processed = await _process_authorized_capital(MAX_PROCESS * 2)
    general_processed = await _process_general_announcements(MAX_PROCESS * 2)

    last_run["time"] = datetime.utcnow().isoformat() + "Z"
    last_run["count"] = auth_processed + general_processed
    last_run["auth_new"] = auth_new
    last_run["general_new"] = general_new

    await cleanup_old_announcements()
    print(f"{'='*60}\n")


async def refresh_recent_authorized_capital(
    hours: int = 48,
    force: bool = False,
    persist: bool = True,
    lightweight: bool = False,
) -> list:
    """
    Live-sync authorized capital announcements from NSE/BSE and return the
    latest 48-hour records ready for the client.

    The live result is cached briefly so dashboard refreshes do not hammer
    the exchanges on every poll.
    """
    cache_ttl_seconds = int(os.getenv("AUTH_LIVE_CACHE_SECONDS", "180"))
    now = datetime.now(timezone.utc)
    cache_time = _auth_live_cache.get("time")
    if (
        not force
        and cache_time
        and isinstance(cache_time, datetime)
        and (now - cache_time).total_seconds() < cache_ttl_seconds
    ):
        return _auth_live_cache.get("data", [])

    from agents.scraper_agent import fetch_nse_announcements, fetch_bse_announcements
    from agents.classifier import is_pure_authorized_capital, extract_auth_capital_deterministic
    if not lightweight:
        from agents.analyst_agent import enrich_announcement
    if persist:
        from database import upsert_authorized_capital, update_authorized_capital_ai

    nse_from = (datetime.now() - timedelta(hours=hours)).strftime("%d-%m-%Y")
    nse_to = datetime.now().strftime("%d-%m-%Y")
    bse_from = (datetime.now() - timedelta(hours=hours)).strftime("%Y%m%d")
    bse_to = datetime.now().strftime("%Y%m%d")

    nse = await asyncio.to_thread(fetch_nse_announcements, nse_from, nse_to)
    bse = await asyncio.to_thread(fetch_bse_announcements, bse_from, bse_to)
    raw_announcements = nse + bse

    rows = []
    for ann in raw_announcements:
        try:
            ann_date_str = ann.get("announcement_date", "")
            ann_date = datetime.fromisoformat(ann_date_str.replace("Z", "+00:00")) if ann_date_str else None
            if not ann_date or (now - ann_date).total_seconds() > hours * 3600:
                continue
        except Exception:
            continue

        if not is_pure_authorized_capital(ann):
            continue

        full_text = f"{ann.get('raw_subject', '')} {ann.get('raw_body', '')}".strip()
        auth_data = extract_auth_capital_deterministic(full_text)
        ai_data = {
            "company_name": ann.get("company_name"),
            "ticker": ann.get("ticker"),
            "announcement_type": "Increase in Authorized Capital",
            "title": "AUTH CAPITAL",
            "description": ann.get("raw_subject", ""),
            "key_details": ann.get("raw_subject", ""),
            "sentiment": "Neutral",
            "impact_level": "Medium",
            "impact": "Medium",
            "sector": ann.get("sector") or "General",
            "ai_insight": "Deterministic extraction from the official announcement text and PDF.",
            "trading_signal": "⚖️ Neutral",
            "authorized_capital": auth_data,
        }

        if not lightweight:
            ai_data = enrich_announcement(ann, ai_data)

        ticker = (ai_data.get("ticker") or ann.get("ticker") or "").strip()
        if ticker and not lightweight:
            try:
                from agents.market_data import get_market_data
                mkt = await asyncio.to_thread(get_market_data, ticker, ann.get("exchange", "NSE"))
                if mkt.get("cmp") is not None:
                    ai_data["cmp"] = mkt["cmp"]
                if mkt.get("market_cap_cr") is not None:
                    ai_data["market_cap_cr"] = mkt["market_cap_cr"]
            except Exception:
                pass

        excel_row = _build_auth_excel_row(ann, ai_data, auth_data)
        if persist:
            try:
                await upsert_authorized_capital(ann)
                await update_authorized_capital_ai(ann["announcement_id"], ai_data, auth_data, excel_row)
            except Exception as db_err:
                print(f"WARNING: Live auth-capital persistence failed for {ann.get('company_name')}: {db_err}")

        old_cap = auth_data.get("existing_auth_eq_cap_inr")
        new_cap = auth_data.get("new_auth_eq_cap_inr")
        increase_amt = None
        pct_increase = auth_data.get("percentage_increase")
        if isinstance(old_cap, (int, float)) and old_cap < 1_00_000:
            old_cap = None
        if isinstance(new_cap, (int, float)) and new_cap < 1_00_000:
            new_cap = None
        if old_cap and new_cap and new_cap > old_cap:
            increase_amt = round(float(new_cap) - float(old_cap), 2)
        else:
            increase_amt = auth_data.get("proposed_increase_inr")
            if isinstance(increase_amt, (int, float)) and increase_amt < 1_00_000:
                increase_amt = None
        if old_cap and new_cap and new_cap <= old_cap:
            increase_amt = None
            pct_increase = None
        if pct_increase is None and old_cap and new_cap and new_cap > old_cap:
            try:
                pct_increase = round(((float(new_cap) - float(old_cap)) / float(old_cap)) * 100, 2)
            except Exception:
                pct_increase = None

        rows.append({
            "id": ann["announcement_id"],
            "category": "authorized_capital",
            "exchange": ann.get("exchange", "NSE"),
            "company_name": ai_data.get("company_name") or ann.get("company_name"),
            "symbol": ai_data.get("ticker") or ann.get("ticker"),
            "announcement_type": "Increase in Authorized Capital",
            "title": ai_data.get("title", "AUTH CAPITAL"),
            "announcement_title": ai_data.get("title", "AUTH CAPITAL"),
            "description": ai_data.get("description") or ai_data.get("key_details") or ann.get("raw_subject"),
            "announcement_date": ann.get("announcement_date", ""),
            "board_approval": ai_data.get("authorized_capital", {}).get("board_approval") or auth_data.get("board_approval"),
            "date_of_board_meeting": ai_data.get("authorized_capital", {}).get("date_of_board_meeting") or auth_data.get("date_of_board_meeting"),
            "old_capital_inr": old_cap,
            "new_capital_inr": new_cap,
            "increase_amount_inr": increase_amt,
            "percentage_increase": pct_increase,
            "face_value_inr": auth_data.get("face_value_inr"),
            "cmp": ai_data.get("cmp"),
            "market_cap_cr": ai_data.get("market_cap_cr"),
            "sector": ai_data.get("sector"),
            "sentiment": ai_data.get("sentiment", "Neutral"),
            "impact": ai_data.get("impact") or ai_data.get("impact_level", "Low"),
            "ai_insight": ai_data.get("ai_insight"),
            "trading_signal": ai_data.get("trading_signal"),
            "source_url": ann.get("source_url", "#"),
            "pdf_url": ann.get("pdf_url"),
            "created_at": ann.get("fetched_at") or ann.get("announcement_date", ""),
        })

    rows.sort(key=lambda x: x.get("announcement_date", ""), reverse=True)
    _auth_live_cache["time"] = now
    _auth_live_cache["data"] = rows
    return rows

async def _process_authorized_capital(limit: int) -> int:
    """Process unprocessed authorized capital announcements."""
    from agents.classifier import extract_auth_capital_deterministic
    from agents.analyst_agent import enrich_announcement
    from database import get_unprocessed_auth_capital, update_authorized_capital_ai

    unprocessed = await get_unprocessed_auth_capital(limit=limit)
    processed_count = 0

    for ann in unprocessed:
        try:
            # Extract PDF text for auth capital
            if ann.get("pdf_url"):
                from agents.scraper_agent import extract_pdf_text
                print(f"INFO: [AUTH] Extracting PDF for {ann.get('company_name')}...")
                pdf_text = await asyncio.to_thread(extract_pdf_text, ann["pdf_url"])
                if pdf_text:
                    ann["raw_body"] = (ann.get("raw_body", "") + "\n\n" + pdf_text)[:8000]

            full_text = (ann.get("raw_subject", "") + " " + ann.get("raw_body", "")).strip()
            auth_data = extract_auth_capital_deterministic(full_text)

            ai_data = {
                "company_name": ann.get("company_name"),
                "ticker": ann.get("ticker"),
                "announcement_type": "Increase in Authorized Capital",
                "title": "AUTH CAPITAL",
                "description": ann.get("raw_subject", ""),
                "key_details": ann.get("raw_subject", ""),
                "sentiment": "Neutral",
                "impact_level": "Medium",
                "impact": "Medium",
                "sector": ann.get("sector") or "General",
                "ai_insight": "Deterministic extraction from the official announcement text and PDF.",
                "trading_signal": "⚖️ Neutral",
                "authorized_capital": auth_data,
            }

            ai_data = enrich_announcement(ann, ai_data)

            # Fetch Market Data
            ticker = (ai_data.get("ticker") or ann.get("ticker") or "").strip()
            if ticker:
                try:
                    from agents.market_data import get_market_data
                    mkt = await asyncio.to_thread(get_market_data, ticker, ann.get("exchange", "NSE"))
                    if mkt.get("cmp"): ai_data["cmp"] = mkt["cmp"]
                    if mkt.get("market_cap_cr"): ai_data["market_cap_cr"] = mkt["market_cap_cr"]
                except: pass

            excel_row = _build_auth_excel_row(ann, ai_data, auth_data)
            await update_authorized_capital_ai(ann["announcement_id"], ai_data, auth_data, excel_row)
            processed_count += 1
            print(f"SUCCESS: [AUTH] {ann.get('company_name')} processed ✓")
        except Exception as e:
            print(f"ERROR: [AUTH] Processing failed for {ann.get('company_name')}: {e}")

    return processed_count

async def _process_general_announcements(limit: int) -> int:
    """Process general announcements and promote to Auth Capital if found in PDF."""
    from agents.extractor_agent import extract_announcement
    from agents.analyst_agent import enrich_announcement
    from agents.classifier import is_pure_authorized_capital
    from database import (
        get_unprocessed_general, update_general_announcement_ai, 
        upsert_authorized_capital, db
    )

    unprocessed = await get_unprocessed_general(limit=limit)
    processed_count = 0

    for ann in unprocessed:
        try:
            # ── Deep Check: Should we scan the PDF? ──
            # Suspect subjects that often contain capital changes
            suspect_subjects = [
                "outcome of board meeting", "general updates", "updates", 
                "intimation", "corporate action", "board meeting", "other"
            ]
            subject_lower = (ann.get("raw_subject") or "").lower()
            
            needs_pdf_scan = any(s in subject_lower for s in suspect_subjects) or len(ann.get("raw_body", "")) < 200
            
            pdf_text = None
            if needs_pdf_scan and ann.get("pdf_url"):
                from agents.scraper_agent import extract_pdf_text
                print(f"INFO: [GEN] Deep scanning PDF for {ann.get('company_name')}...")
                pdf_text = await asyncio.to_thread(extract_pdf_text, ann["pdf_url"])
                if pdf_text:
                    ann["raw_body"] = (ann.get("raw_body", "") + "\n\n" + pdf_text)[:8000]

            # ── Check for Promotion ──
            is_actually_auth_capital = is_pure_authorized_capital(ann)

            if is_actually_auth_capital:
                print(f"PROMOTION: {ann.get('company_name')} promoted to Authorized Capital! 🚀")
                # Remove from general
                await db.general_announcements.delete_one({"announcement_id": ann["announcement_id"]})
                # Upsert to auth capital (it will be processed in next auth cycle or right away if we add logic)
                await upsert_authorized_capital(ann)
                continue

            # Standard AI extraction for general items
            ai_data = await asyncio.to_thread(extract_announcement, ann)
            if not ai_data: continue

            ai_data = enrich_announcement(ann, ai_data)
            
            # Market Data
            ticker = (ai_data.get("ticker") or ann.get("ticker") or "").strip()
            if ticker:
                try:
                    from agents.market_data import get_market_data
                    mkt = await asyncio.to_thread(get_market_data, ticker, ann.get("exchange", "NSE"))
                    if mkt.get("cmp"): ai_data["cmp"] = mkt["cmp"]
                    if mkt.get("market_cap_cr"): ai_data["market_cap_cr"] = mkt["market_cap_cr"]
                except: pass

            excel_row = _build_general_excel_row(ann, ai_data)
            await update_general_announcement_ai(ann["announcement_id"], ai_data, excel_row)
            processed_count += 1
        except Exception as e:
            print(f"ERROR: [GEN] Processing failed for {ann.get('company_name')}: {e}")

    return processed_count

def _build_auth_excel_row(ann: dict, ai_data: dict, auth_data: dict) -> dict:
    auth_cap = ai_data.get("authorized_capital", {}) or {}
    for key in ["board_approval", "date_of_board_meeting", "existing_auth_eq_cap_inr", "new_auth_eq_cap_inr", "proposed_increase_inr", "face_value_inr", "percentage_increase"]:
        if auth_data.get(key) not in [None, "Not Available"]:
            auth_cap[key] = auth_data[key]
    sentiment = ai_data.get("sentiment", "Neutral")
    insight = ai_data.get("ai_insight") or ai_data.get("key_details", "")
    return {
        "sr_no": None,
        "date_of_entry": ann.get("announcement_date", ""),
        "company_name": ai_data.get("company_name", ann.get("company_name", "")),
        "board_approval": auth_cap.get("board_approval", "Not Available"),
        "date_of_board_meeting": auth_cap.get("date_of_board_meeting", "Not Available"),
        "existing_auth_eq_cap_inr": auth_cap.get("existing_auth_eq_cap_inr"),
        "new_auth_eq_cap_inr": auth_cap.get("new_auth_eq_cap_inr"),
        "proposed_increase_inr": auth_cap.get("proposed_increase_inr"),
        "face_value_inr": auth_cap.get("face_value_inr"),
        "percentage_increase": auth_cap.get("percentage_increase"),
        "cmp": ai_data.get("cmp"),
        "market_cap_cr": ai_data.get("market_cap_cr"),
        "sector": ai_data.get("sector", ""),
        "remark_positive": insight if sentiment in ["Positive", "Neutral"] else "",
        "remark_negative": insight if sentiment == "Negative" else "",
        "action": ai_data.get("trading_signal", ""),
        "link": ann.get("source_url", ""),
    }

def _build_general_excel_row(ann: dict, ai_data: dict) -> dict:
    return {
        "company_name": ai_data.get("company_name", ann.get("company_name", "")),
        "date": ann.get("announcement_date", ""),
        "category": ai_data.get("announcement_type", "Other"),
        "sentiment": ai_data.get("sentiment", "Neutral"),
        "impact": ai_data.get("impact_level", "Low"),
        "summary": ai_data.get("description") or ai_data.get("key_details", ""),
        "source": ann.get("source_url", ""),
    }

def start_scheduler():
    scheduler.add_job(run_pipeline, trigger=IntervalTrigger(minutes=FETCH_INTERVAL), id="pipeline_job", replace_existing=True)
    scheduler.start()
    print(f"Scheduler started — fetching every {FETCH_INTERVAL} minutes")

def stop_scheduler():
    if scheduler.running: scheduler.shutdown()

"""
APScheduler — runs the full fetch → classify → route → process pipeline
every N minutes automatically.

Architecture:
  Step 1: Fetch all announcements from NSE/BSE
  Step 2: Classify each as 'authorized_capital' or 'general'
  Step 3: Route to correct collection
  Step 4: Process unprocessed items in each collection separately
  Step 5: Cleanup old data from both collections
"""
import asyncio
import os
from datetime import datetime
from dotenv import load_dotenv
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

load_dotenv()

FETCH_INTERVAL = int(os.getenv("FETCH_INTERVAL_MINUTES", "10"))
MAX_PROCESS = int(os.getenv("MAX_PROCESS_PER_CYCLE", "20"))

scheduler = AsyncIOScheduler()
last_run: dict = {"time": None, "count": 0}


async def run_pipeline():
    """
    Full pipeline: Scrape → Classify → Route → Process → Save.
    """
    from agents.scraper_agent import fetch_all_announcements
    from agents.classifier import classify_announcement
    from database import (
        upsert_authorized_capital, upsert_general_announcement,
        get_unprocessed_auth_capital, get_unprocessed_general,
        update_authorized_capital_ai, update_general_announcement_ai,
        cleanup_old_announcements
    )

    print(f"\n{'='*60}")
    print(f"PIPELINE: Started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")

    # ── Step 1: Fetch ─────────────────────────────────────────────
    raw_announcements = await asyncio.to_thread(fetch_all_announcements)

    # ── Step 2 & 3: Classify and Route ────────────────────────────
    auth_new = 0
    general_new = 0

    for ann in raw_announcements:
        category = classify_announcement(ann)

        if category == "authorized_capital":
            is_new = await upsert_authorized_capital(ann)
            if is_new:
                auth_new += 1
        else:
            is_new = await upsert_general_announcement(ann)
            if is_new:
                general_new += 1

    print(f"CLASSIFY: {auth_new} new auth capital + {general_new} new general (from {len(raw_announcements)} fetched)")

    # ── Step 4a: Process Authorized Capital ───────────────────────
    auth_processed = await _process_authorized_capital(MAX_PROCESS)

    # ── Step 4b: Process General Announcements ────────────────────
    general_processed = await _process_general_announcements(MAX_PROCESS)

    total_processed = auth_processed + general_processed
    last_run["time"] = datetime.utcnow().isoformat() + "Z"
    last_run["count"] = total_processed

    print(f"SUCCESS: Pipeline complete: {auth_processed} auth + {general_processed} general processed")

    # ── Step 5: Cleanup ───────────────────────────────────────────
    await cleanup_old_announcements(hours=24)

    print(f"{'='*60}\n")


async def _process_authorized_capital(limit: int) -> int:
    """Process unprocessed authorized capital announcements."""
    from agents.classifier import extract_auth_capital_deterministic
    from agents.extractor_agent import extract_announcement
    from agents.analyst_agent import enrich_announcement
    from database import get_unprocessed_auth_capital, update_authorized_capital_ai

    unprocessed = await get_unprocessed_auth_capital(limit=limit)
    processed_count = 0

    for ann in unprocessed:
        try:
            # Always extract PDF text for auth capital (critical for numbers)
            if ann.get("pdf_url"):
                from agents.scraper_agent import extract_pdf_text
                print(f"INFO: [AUTH] Extracting PDF for {ann.get('company_name')}...")
                pdf_text = await asyncio.to_thread(extract_pdf_text, ann["pdf_url"])
                if pdf_text:
                    ann["raw_body"] = (ann.get("raw_body", "") + "\n\n" + pdf_text)[:6000]

            full_text = (ann.get("raw_subject", "") + " " + ann.get("raw_body", "")).strip()

            # ── Deterministic extraction FIRST ────────────────────
            auth_data = extract_auth_capital_deterministic(full_text)

            # ── Master Data fallback for existing capital ─────────
            ticker = ann.get("ticker", "").upper()
            if not auth_data.get("existing_auth_eq_cap_inr"):
                try:
                    import json
                    master_path = os.path.join(os.path.dirname(__file__), "agents", "master_data.json")
                    if os.path.exists(master_path):
                        with open(master_path, "r") as f:
                            master = json.load(f)
                        if ticker in master and master[ticker].get("existing_auth_cap"):
                            auth_data["existing_auth_eq_cap_inr"] = master[ticker]["existing_auth_cap"]
                            print(f"INFO: [AUTH] Master data resolved existing capital for {ticker}")
                except Exception as me:
                    print(f"WARNING: Master data lookup failed: {me}")

            # ── AI extraction for classification + insight only ───
            ai_data = await asyncio.to_thread(extract_announcement, ann)
            if not ai_data:
                ai_data = {}

            # Force the correct type
            ai_data["announcement_type"] = "Increase in Authorized Capital"

            # Merge deterministic auth_data INTO ai_data (regex wins for numbers)
            if not ai_data.get("authorized_capital"):
                ai_data["authorized_capital"] = {}
            for key, val in auth_data.items():
                if val is not None and val != "Not Available":
                    ai_data["authorized_capital"][key] = val

            # Rule-based enrichment
            ai_data = enrich_announcement(ann, ai_data)

            # ── Fetch live CMP & Market Cap ───────────────────────
            t = ai_data.get("ticker") or ticker
            if t:
                try:
                    from agents.market_data import get_market_data
                    mkt = await asyncio.to_thread(get_market_data, t, ann.get("exchange", "NSE"))
                    if mkt.get("cmp") is not None:
                        ai_data["cmp"] = mkt["cmp"]
                    if mkt.get("market_cap_cr") is not None:
                        ai_data["market_cap_cr"] = mkt["market_cap_cr"]
                except Exception as me:
                    print(f"WARNING: Market data failed for {t}: {me}")

            # ── Build Excel row ───────────────────────────────────
            excel_row = _build_auth_excel_row(ann, ai_data, auth_data)

            # ── Save ──────────────────────────────────────────────
            await update_authorized_capital_ai(ann["announcement_id"], ai_data, auth_data, excel_row)
            processed_count += 1
            print(f"SUCCESS: [AUTH] {ai_data.get('company_name', ann.get('company_name'))} processed")

        except Exception as e:
            print(f"ERROR: [AUTH] Error processing {ann.get('company_name')}: {e}")

    return processed_count


async def _process_general_announcements(limit: int) -> int:
    """Process unprocessed general announcements with full AI pipeline."""
    from agents.extractor_agent import extract_announcement
    from agents.analyst_agent import enrich_announcement
    from database import get_unprocessed_general, update_general_announcement_ai

    unprocessed = await get_unprocessed_general(limit=limit)
    processed_count = 0

    for ann in unprocessed:
        try:
            # PDF extraction for short bodies
            if ann.get("pdf_url") and len(ann.get("raw_body", "")) < 500:
                from agents.scraper_agent import extract_pdf_text
                print(f"INFO: [GEN] Extracting PDF for {ann.get('company_name')}...")
                pdf_text = await asyncio.to_thread(extract_pdf_text, ann["pdf_url"])
                if pdf_text:
                    ann["raw_body"] = (ann.get("raw_body", "") + "\n\n" + pdf_text)[:6000]

            # Full AI extraction
            ai_data = await asyncio.to_thread(extract_announcement, ann)
            if not ai_data:
                continue

            # Rule-based enrichment
            ai_data = enrich_announcement(ann, ai_data)

            # Fetch live CMP & Market Cap
            ticker = ai_data.get("ticker") or ann.get("ticker", "")
            if ticker and (ai_data.get("cmp") is None or ai_data.get("market_cap_cr") is None):
                try:
                    from agents.market_data import get_market_data
                    mkt = await asyncio.to_thread(get_market_data, ticker, ann.get("exchange", "NSE"))
                    if mkt.get("cmp") is not None:
                        ai_data["cmp"] = mkt["cmp"]
                    if mkt.get("market_cap_cr") is not None:
                        ai_data["market_cap_cr"] = mkt["market_cap_cr"]
                except Exception as me:
                    print(f"WARNING: Market data failed for {ticker}: {me}")

            # Build Excel row
            excel_row = _build_general_excel_row(ann, ai_data)

            # Save
            await update_general_announcement_ai(ann["announcement_id"], ai_data, excel_row)
            processed_count += 1

        except Exception as e:
            print(f"ERROR: [GEN] Error processing {ann.get('company_name')}: {e}")

    return processed_count


def _build_auth_excel_row(ann: dict, ai_data: dict, auth_data: dict) -> dict:
    """Build the Excel row for an authorized capital announcement."""
    auth_cap = ai_data.get("authorized_capital", {}) or {}
    # Prefer deterministic auth_data over AI-extracted
    for key in ["board_approval", "date_of_board_meeting",
                 "existing_auth_eq_cap_inr", "new_auth_eq_cap_inr", "proposed_increase_inr"]:
        if auth_data.get(key) is not None and auth_data[key] != "Not Available":
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
        "cmp": ai_data.get("cmp"),
        "market_cap_cr": ai_data.get("market_cap_cr"),
        "sector": ai_data.get("sector", ""),
        "remark_positive": insight if sentiment in ["Positive", "Neutral"] else "",
        "remark_negative": insight if sentiment == "Negative" else "",
        "action": ai_data.get("trading_signal", ""),
        "link": ann.get("source_url", ""),
    }


def _build_general_excel_row(ann: dict, ai_data: dict) -> dict:
    """Build the Excel row for a general announcement."""
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
    """Start the background scheduler."""
    scheduler.add_job(
        run_pipeline,
        trigger=IntervalTrigger(minutes=FETCH_INTERVAL),
        id="pipeline_job",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    scheduler.start()
    print(f"⏰ Scheduler started — fetching every {FETCH_INTERVAL} minutes")


def stop_scheduler():
    """Stop the background scheduler."""
    if scheduler.running:
        scheduler.shutdown()
        print("⏹️ Scheduler stopped")

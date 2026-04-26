"""
APScheduler — runs the full fetch → extract → analyze → store pipeline
every N minutes automatically.
"""
import asyncio
import os
from datetime import datetime, timedelta
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
    Full pipeline: Scrape → Store → Extract (AI) → Save enriched data.
    """
    from agents.scraper_agent import fetch_all_announcements
    from agents.extractor_agent import extract_announcement
    from agents.analyst_agent import enrich_announcement
    from database import upsert_announcement, get_unprocessed_announcements, update_announcement_ai

    print(f"\n{'='*60}")
    print(f"PIPELINE: Pipeline started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")

    # Step 1: Fetch
    raw_announcements = await asyncio.to_thread(fetch_all_announcements)
    new_count = 0

    # Step 2: Store raw announcements (dedup via MongoDB)
    for ann in raw_announcements:
        is_new = await upsert_announcement(ann)
        if is_new:
            new_count += 1

    print(f"INFO: Stored {new_count} new announcements (out of {len(raw_announcements)} fetched)")

    # Step 3: Process unprocessed announcements with AI
    unprocessed = await get_unprocessed_announcements(limit=MAX_PROCESS)
    processed_count = 0

    for ann in unprocessed:
        try:
            # If PDF exists, extract its text to give the AI proper context
            if ann.get("pdf_url") and len(ann.get("raw_body", "")) < 500:
                from agents.scraper_agent import extract_pdf_text
                print(f"INFO: Extracting PDF text for {ann.get('company_name')}...")
                pdf_text = await asyncio.to_thread(extract_pdf_text, ann["pdf_url"])
                if pdf_text:
                    ann["raw_body"] = (ann.get("raw_body", "") + "\n\n" + pdf_text)[:5000]

            # AI Extraction
            ai_data = await asyncio.to_thread(extract_announcement, ann)
            if not ai_data:
                continue

            # Rule-based enrichment
            ai_data = enrich_announcement(ann, ai_data)

            # Fetch live CMP & Market Cap (yfinance / NSE API)
            ticker = ai_data.get("ticker") or ann.get("ticker", "")
            if ticker and (ai_data.get("cmp") is None or ai_data.get("market_cap_cr") is None):
                try:
                    from agents.market_data import get_market_data
                    mkt = await asyncio.to_thread(get_market_data, ticker, ann.get("exchange", "NSE"))
                    if mkt.get("cmp") is not None:
                        ai_data["cmp"] = mkt["cmp"]
                    if mkt.get("market_cap_cr") is not None:
                        ai_data["market_cap_cr"] = mkt["market_cap_cr"]
                    if mkt.get("cmp"):
                        print(f"INFO: Market data for {ticker}: CMP={mkt['cmp']}, MktCap={mkt['market_cap_cr']} Cr")
                except Exception as me:
                    print(f"WARNING: Market data fetch failed for {ticker}: {me}")

            # Build Excel row
            excel_row = _build_excel_row(ann, ai_data)

            # Save to MongoDB
            await update_announcement_ai(ann["announcement_id"], ai_data, excel_row)
            processed_count += 1

        except Exception as e:
            print(f"ERROR: Error processing {ann.get('company_name')}: {e}")

    last_run["time"] = datetime.utcnow().isoformat() + "Z"
    last_run["count"] = processed_count

    print(f"SUCCESS: Pipeline complete: {processed_count} announcements AI-processed")
    
    # Step 4: Cleanup announcements older than 24 hours to enforce strict 24h limit
    try:
        from database import db
        cutoff = (datetime.utcnow() - timedelta(hours=24)).isoformat() + "Z"
        del_result = await db.announcements.delete_many({"announcement_date": {"$lt": cutoff}})
        print(f"CLEANUP: Deleted {del_result.deleted_count} announcements older than 24 hours.")
    except Exception as e:
        print(f"⚠️ Cleanup failed: {e}")

    print(f"{'='*60}\n")


def _build_excel_row(ann: dict, ai_data: dict) -> dict:
    """Build the Excel row dict for quick retrieval."""
    auth_cap = ai_data.get("authorized_capital", {}) or {}
    return {
        "sr_no": None,  # Will be assigned at export time
        "date_of_entry": ann.get("announcement_date", ""),
        "company_name": ai_data.get("company_name", ann.get("company_name", "")),
        "exchange": ann.get("exchange", ""),
        "ticker": ai_data.get("ticker", ann.get("ticker", "")),
        "announcement_type": ai_data.get("announcement_type", "Other"),
        "key_details": ai_data.get("key_details", ""),
        "revenue_profit_impact": ai_data.get("revenue_profit_impact", ""),
        "sentiment": ai_data.get("sentiment", "Neutral"),
        "impact_level": ai_data.get("impact_level", "Low"),
        "ai_insight": ai_data.get("ai_insight", ""),
        "trading_signal": ai_data.get("trading_signal", ""),
        "sector": ai_data.get("sector", ""),
        "board_approval": auth_cap.get("board_approval", ""),
        "date_of_board_meeting": auth_cap.get("date_of_board_meeting", ""),
        "existing_auth_eq_cap_inr": auth_cap.get("existing_auth_eq_cap_inr"),
        "new_auth_eq_cap_inr": auth_cap.get("new_auth_eq_cap_inr"),
        "proposed_increase_inr": auth_cap.get("proposed_increase_inr"),
        "cmp": ai_data.get("cmp"),
        "market_cap_cr": ai_data.get("market_cap_cr"),
        "source_url": ann.get("source_url", ""),
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

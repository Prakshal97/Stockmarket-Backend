"""
APScheduler — runs the full fetch → extract → analyze → store pipeline
every N minutes automatically.
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
    Full pipeline: Scrape → Store → Extract (AI) → Save enriched data.
    """
    from agents.scraper_agent import fetch_all_announcements
    from agents.extractor_agent import extract_announcement
    from agents.analyst_agent import enrich_announcement
    from database import upsert_announcement, get_unprocessed_announcements, update_announcement_ai

    print(f"\n{'='*60}")
    print(f"🤖 Pipeline started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")

    # Step 1: Fetch
    raw_announcements = fetch_all_announcements()
    new_count = 0

    # Step 2: Store raw announcements (dedup via MongoDB)
    for ann in raw_announcements:
        is_new = await upsert_announcement(ann)
        if is_new:
            new_count += 1

    print(f"📥 Stored {new_count} new announcements (out of {len(raw_announcements)} fetched)")

    # Step 3: Process unprocessed announcements with AI
    unprocessed = await get_unprocessed_announcements(limit=MAX_PROCESS)
    processed_count = 0

    for ann in unprocessed:
        try:
            # AI Extraction
            ai_data = extract_announcement(ann)
            if not ai_data:
                continue

            # Rule-based enrichment
            ai_data = enrich_announcement(ann, ai_data)

            # Build Excel row
            excel_row = _build_excel_row(ann, ai_data)

            # Save to MongoDB
            await update_announcement_ai(ann["announcement_id"], ai_data, excel_row)
            processed_count += 1

        except Exception as e:
            print(f"❌ Error processing {ann.get('company_name')}: {e}")

    last_run["time"] = datetime.utcnow().isoformat()
    last_run["count"] = processed_count

    print(f"✅ Pipeline complete: {processed_count} announcements AI-processed")
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

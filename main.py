"""
FastAPI Backend — Main application entry point.
Routes:
  /api/authorized-capital   — Auth capital announcements (PRIMARY)
  /api/announcements        — General announcements (SECONDARY)
  /api/stats                — Segregated statistics
  /api/excel/*              — Excel exports
  /api/trigger              — Manual pipeline trigger
  /api/debug                — Database diagnostic info
  /api/reprocess            — Reset + re-run AI on all stored items
  /api/force-fetch          — Force fetch the latest 48-hour NSE/BSE window
"""
import os
import sys

# Force UTF-8 output on Windows
os.environ.setdefault("PYTHONIOENCODING", "utf-8")
if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

import io
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, Query, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(
    title="AlphaIntel — NSE/BSE Financial Intelligence Agent",
    description="AI-powered corporate announcement analyzer with segregated Authorized Capital tracking",
    version="3.1.0"
)

# CORS for Next.js frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _safe(value, default="Not Available"):
    """Ensure no field is ever empty, null, or blank in API responses."""
    if value is None:
        return default
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped or stripped.lower() in ("null", "none", "n/a", "na"):
            return default
        return stripped
    return value


def _pick_numeric(*values):
    """Return the first usable numeric value from a list of candidates."""
    for value in values:
        if value is None:
            continue
        if isinstance(value, (int, float)):
            return value
        if isinstance(value, str):
            cleaned = value.replace("₹", "").replace(",", "").strip()
            try:
                if cleaned and cleaned.lower() not in ("na", "n/a", "none", "not available"):
                    return float(cleaned)
            except Exception:
                continue
    return None


# ─── Startup / Shutdown ──────────────────────────────────────────────────────

@app.on_event("startup")
async def startup_event():
    from database import connect_db
    from scheduler import start_scheduler, refresh_recent_authorized_capital
    await connect_db()
    start_scheduler()
    import asyncio
    asyncio.create_task(refresh_recent_authorized_capital(48, True, False, True))


@app.on_event("shutdown")
async def shutdown_event():
    from database import close_db
    from scheduler import stop_scheduler
    stop_scheduler()
    await close_db()


# ─── Health Check ─────────────────────────────────────────────────────────────

@app.get("/")
async def home():
    return {"status": "AlphaIntel v3.1 — Segregated Pipeline is LIVE 🚀", "version": "3.1.0"}

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "service": "AlphaIntel — NSE/BSE Financial Intelligence Agent",
        "ai_engine": "Groq (LLaMA 3.1)",
        "pipeline": "segregated (auth_capital + general)",
        "display_window": "48 hours"
    }


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1: AUTHORIZED CAPITAL API (PRIMARY PRIORITY)
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/api/authorized-capital")
async def get_authorized_capital(
    background_tasks: BackgroundTasks,
    limit: int = Query(100, ge=1, le=500),
    skip: int = Query(0, ge=0),
    refresh: bool = Query(False, description="Kick off a background live refresh, then return cached 48-hour data"),
):
    """Fetch ONLY authorized capital announcements. Top priority data."""
    import asyncio
    from database import get_authorized_capital_list
    from scheduler import refresh_recent_authorized_capital, get_cached_recent_authorized_capital

    if refresh and background_tasks is not None:
        background_tasks.add_task(refresh_recent_authorized_capital, 48, True, False, True)

    try:
        announcements = await asyncio.wait_for(
            get_authorized_capital_list(limit=limit + skip, skip=0),
            timeout=12,
        )
    except Exception:
        announcements = get_cached_recent_authorized_capital()

    total = len(announcements)
    announcements = announcements[skip: skip + limit]

    results = []
    for ann in announcements:
        ai_data = ann.get("ai_data", {}) or {}
        auth_data = ann.get("auth_data", {}) or {}
        excel_row = ann.get("excel_row", {}) or {}
        auth_cap = ai_data.get("authorized_capital", {}) or {}

        old_cap = _pick_numeric(
            ann.get("old_capital_inr"),
            ann.get("existing_auth_eq_cap_inr"),
            auth_data.get("existing_auth_eq_cap_inr"),
            excel_row.get("existing_auth_eq_cap_inr"),
            auth_cap.get("existing_auth_eq_cap_inr"),
        )
        new_cap = _pick_numeric(
            ann.get("new_capital_inr"),
            ann.get("new_auth_eq_cap_inr"),
            auth_data.get("new_auth_eq_cap_inr"),
            excel_row.get("new_auth_eq_cap_inr"),
            auth_cap.get("new_auth_eq_cap_inr"),
        )
        increase_amt = None
        pct_increase = _pick_numeric(
            ann.get("percentage_increase"),
            auth_data.get("percentage_increase"),
            excel_row.get("percentage_increase"),
            auth_cap.get("percentage_increase"),
        )
        face_value = _pick_numeric(
            ann.get("face_value_inr"),
            auth_data.get("face_value_inr"),
            excel_row.get("face_value_inr"),
            auth_cap.get("face_value_inr"),
        )
        if old_cap and new_cap and new_cap > old_cap:
            increase_amt = round(float(new_cap) - float(old_cap), 2)
        else:
            increase_amt = _pick_numeric(
                ann.get("increase_amount_inr"),
                ann.get("proposed_increase_inr"),
                auth_data.get("proposed_increase_inr"),
                excel_row.get("proposed_increase_inr"),
                auth_cap.get("proposed_increase_inr"),
            )
        if pct_increase is None and old_cap and new_cap and new_cap > old_cap:
            try:
                pct_increase = round(((float(new_cap) - float(old_cap)) / float(old_cap)) * 100, 2)
            except Exception:
                pct_increase = None
        results.append({
            "id": ann.get("id") or ann.get("announcement_id") or ann.get("_id"),
            "category": ann.get("category", "verified"),
            "exchange": _safe(ann.get("exchange"), "NSE"),
            "company_name": _safe(ann.get("company_name")),
            "symbol": _safe(ann.get("symbol") or ann.get("ticker")),
            "announcement_type": _safe(ann.get("announcement_type"), "Increase in Authorized Capital"),
            "title": _safe(ann.get("title"), "AUTH CAPITAL"),
            "announcement_title": _safe(ann.get("announcement_title") or ann.get("title"), "AUTH CAPITAL"),
            "description": _safe(ann.get("description") or ann.get("key_details") or ann.get("raw_subject")),
            "announcement_date": _safe(ann.get("announcement_date"), ""),
            "board_approval": _safe(ann.get("board_approval")),
            "date_of_board_meeting": _safe(ann.get("date_of_board_meeting")),
            "old_capital_inr": old_cap,
            "new_capital_inr": new_cap,
            "increase_amount_inr": increase_amt,
            "percentage_increase": pct_increase,
            "face_value_inr": face_value,
            "cmp": ann.get("cmp"),
            "market_cap_cr": ann.get("market_cap_cr"),
            "sector": _safe(ann.get("sector")),
            "sentiment": _safe(ann.get("sentiment"), "Neutral"),
            "impact": _safe(ann.get("impact"), "Medium"),
            "ai_insight": _safe(ann.get("ai_insight")),
            "trading_signal": _safe(ann.get("trading_signal")),
            "source_url": _safe(ann.get("source_url"), "#"),
            "pdf_url": ann.get("pdf_url"),
            "created_at": _safe(ann.get("created_at")),
            # Enhanced Production Metadata
            "confidence": _safe(ann.get("confidence_level") or ai_data.get("confidence")),
            "evidence_snippet": _safe(ann.get("evidence_snippet") or ai_data.get("evidence_snippet")),
            "extraction_method": _safe(ann.get("extraction_method"), "Title Scan"),
        })

    return {"announcements": results, "total": total, "skip": skip, "limit": limit, "refreshed": refresh}


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1B: POSSIBLE CAPITAL RELATED API
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/api/possible-capital")
async def get_possible_capital(limit: int = Query(50, ge=1, le=200)):
    """Fetch filings with context signals (EGM, Postal Ballot) but no verified proof."""
    from database import get_possible_capital_list
    announcements = await get_possible_capital_list(limit=limit)
    
    results = []
    for ann in announcements:
        results.append({
            "id": ann.get("announcement_id"),
            "company_name": _safe(ann.get("company_name")),
            "symbol": _safe(ann.get("ticker")),
            "raw_subject": _safe(ann.get("raw_subject")),
            "announcement_date": _safe(ann.get("announcement_date")),
            "exchange": _safe(ann.get("exchange")),
            "pdf_url": ann.get("pdf_url"),
            "category": "possible",
            "confidence": "MEDIUM",
            "matched_keywords": ann.get("matched_keywords", [])
        })
    return {"announcements": results, "count": len(results)}

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2: GENERAL ANNOUNCEMENTS API (SECONDARY)
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/api/announcements")
async def get_announcements(
    limit: int = Query(100, ge=1, le=500),
    skip: int = Query(0, ge=0),
    exchange: Optional[str] = Query(None, description="NSE | BSE"),
    announcement_type: Optional[str] = Query(None),
    sentiment: Optional[str] = Query(None, description="Positive | Neutral | Negative"),
    impact: Optional[str] = Query(None, description="High | Medium | Low"),
    ticker: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
):
    """Fetch general (non-auth-capital) announcements with optional filters."""
    from database import get_general_announcements, get_general_count

    announcements = await get_general_announcements(
        limit=limit, skip=skip,
        exchange=exchange, announcement_type=announcement_type,
        sentiment=sentiment, impact=impact,
        ticker=ticker, search=search
    )

    results = []
    for ann in announcements:
        ann["_id"] = str(ann.get("_id", ""))
        ai_data = ann.get("ai_data", {}) or {}

        try:
            ann_date = ann.get("announcement_date", "")
            if isinstance(ann_date, datetime):
                ann_date = ann_date.isoformat()
        except Exception:
            ann_date = ""

        created_at = ann.get("fetched_at") or ann.get("announcement_date", "")
        if isinstance(created_at, datetime):
            created_at = created_at.isoformat()

        results.append({
            "id": ann["_id"],
            "category": "general",
            "exchange": _safe(ann.get("exchange"), "NSE"),
            "company_name": _safe(ai_data.get("company_name") or ann.get("company_name")),
            "ticker": _safe(ai_data.get("ticker") or ann.get("ticker")),
            "announcement_type": _safe(ai_data.get("announcement_type"), "Other"),
            "title": _safe(ai_data.get("title"), "ANNOUNCEMENT"),
            "description": _safe(
                ai_data.get("description") or ai_data.get("key_details") or ann.get("raw_subject")
            ),
            "announcement_date": ann_date,
            "key_details": _safe(ai_data.get("key_details") or ann.get("raw_subject")),
            "revenue_profit_impact": _safe(ai_data.get("revenue_profit_impact")),
            "sentiment": _safe(ai_data.get("sentiment"), "Neutral"),
            "impact_level": _safe(ai_data.get("impact_level") or ai_data.get("impact"), "Low"),
            "impact": _safe(ai_data.get("impact") or ai_data.get("impact_level"), "Low"),
            "board_approval": _safe(ai_data.get("board_approval")),
            "meeting_date": _safe(ai_data.get("meeting_date")),
            "ai_insight": _safe(ai_data.get("ai_insight")),
            "trading_signal": _safe(ai_data.get("trading_signal")),
            "sector": _safe(ai_data.get("sector")),
            "cmp": ai_data.get("cmp"),
            "market_cap_cr": ai_data.get("market_cap_cr"),
            "source_url": _safe(ann.get("source_url"), "#"),
            "pdf_url": ann.get("pdf_url"),
            "processed": ann.get("processed", False),
            "created_at": _safe(created_at),
        })

    total = await get_general_count()
    return {"announcements": results, "total": total, "skip": skip, "limit": limit}


# ─── Stats API ────────────────────────────────────────────────────────────────

@app.get("/api/stats")
async def get_stats_endpoint():
    """Dashboard statistics with segregated counts."""
    from database import get_stats, get_last_fetch_time
    from scheduler import last_run

    stats = await get_stats()
    stats["last_fetched"] = last_run.get("time") or await get_last_fetch_time()
    return stats


# ─── Debug API ────────────────────────────────────────────────────────────────

@app.get("/api/debug")
async def debug_info():
    """Database diagnostic endpoint — see exactly what is stored."""
    from database import get_db_summary
    from scheduler import last_run
    summary = await get_db_summary()
    summary["last_pipeline_run"] = last_run
    return summary


# ─── Force Fetch API ──────────────────────────────────────────────────────────

@app.post("/api/force-fetch")
async def force_fetch(
    background_tasks: BackgroundTasks,
    days: int = Query(2, ge=1, le=2, description="Window in days; capped to the configured 48-hour limit")
):
    """Force fetch from NSE/BSE for the latest 48-hour window and process everything."""
    from scheduler import run_pipeline_extended
    background_tasks.add_task(run_pipeline_extended, days)
    return {
        "message": "Force-fetch triggered for the latest 48-hour window!",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "note": "Check /api/debug in 60s to see results"
    }


# ─── Reprocess API ────────────────────────────────────────────────────────────

@app.post("/api/reprocess")
async def reprocess_all(background_tasks: BackgroundTasks):
    """Reset all items to unprocessed and re-run AI extraction."""
    from database import reset_unprocessed
    from scheduler import run_pipeline
    count = await reset_unprocessed()
    background_tasks.add_task(run_pipeline)
    return {
        "message": f"Reset {count} items to unprocessed. Pipeline triggered.",
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }


# ─── Excel Export APIs ────────────────────────────────────────────────────────

@app.get("/api/excel/segregated-report")
async def download_segregated_excel(
    limit: int = Query(500, ge=1, le=2000),
):
    """Download the dual-section Excel: Sheet 1 = Auth Capital, Sheet 2 = Others."""
    from database import get_general_announcements, get_authorized_capital_list
    from agents.reporter_agent import generate_segregated_excel

    auth_anns = await get_authorized_capital_list(limit=limit, skip=0)
    general_anns = await get_general_announcements(limit=limit)

    import asyncio
    excel_bytes = await asyncio.to_thread(generate_segregated_excel, auth_anns, general_anns)
    filename = f"AlphaIntel_Segregated_{datetime.now().strftime('%d%m%Y_%H%M')}.xlsx"

    return StreamingResponse(
        io.BytesIO(excel_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@app.get("/api/excel/authorized-capital")
async def download_authorized_capital_excel(
    limit: int = Query(500, ge=1, le=2000),
):
    """Download Excel with ONLY authorized capital announcements."""
    from database import get_authorized_capital_list
    from agents.reporter_agent import generate_authorized_capital_excel

    announcements = await get_authorized_capital_list(limit=limit, skip=0)
    import asyncio
    excel_bytes = await asyncio.to_thread(generate_authorized_capital_excel, announcements)
    filename = f"Authorized_Capital_{datetime.now().strftime('%d%m%Y')}.xlsx"

    return StreamingResponse(
        io.BytesIO(excel_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@app.get("/api/excel/full-report")
async def download_full_report(
    limit: int = Query(200, ge=1, le=1000),
    exchange: Optional[str] = Query(None),
    sentiment: Optional[str] = Query(None),
    impact: Optional[str] = Query(None),
):
    """Download general announcements Excel report."""
    from database import get_general_announcements
    from agents.reporter_agent import generate_full_report_excel

    announcements = await get_general_announcements(
        limit=limit, exchange=exchange,
        sentiment=sentiment, impact=impact
    )
    import asyncio
    excel_bytes = await asyncio.to_thread(generate_full_report_excel, announcements)
    filename = f"NSE_BSE_Report_{datetime.now().strftime('%d%m%Y_%H%M')}.xlsx"

    return StreamingResponse(
        io.BytesIO(excel_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


# ─── Company Detail API ───────────────────────────────────────────────────────

@app.get("/api/company/{ticker}")
async def get_company_profile(ticker: str, limit: int = Query(20, ge=1, le=100)):
    """Get all announcements for a company by ticker (from both collections)."""
    from database import get_company_announcements

    announcements = await get_company_announcements(ticker, limit)
    results = []
    for ann in announcements:
        ann["_id"] = str(ann.get("_id", ""))
        ai_data = ann.get("ai_data", {}) or {}
        results.append({
            "id": ann["_id"],
            "category": ann.get("_category", "general"),
            "exchange": _safe(ann.get("exchange")),
            "company_name": _safe(ai_data.get("company_name") or ann.get("company_name")),
            "ticker": _safe(ann.get("ticker")),
            "announcement_type": _safe(ai_data.get("announcement_type"), "Other"),
            "title": _safe(ai_data.get("title"), "ANNOUNCEMENT"),
            "description": _safe(ai_data.get("description") or ai_data.get("key_details")),
            "announcement_date": ann.get("announcement_date", ""),
            "sentiment": _safe(ai_data.get("sentiment"), "Neutral"),
            "impact": _safe(ai_data.get("impact") or ai_data.get("impact_level"), "Low"),
            "ai_insight": _safe(ai_data.get("ai_insight")),
            "trading_signal": _safe(ai_data.get("trading_signal")),
            "sector": _safe(ai_data.get("sector")),
        })

    return {"ticker": ticker, "count": len(results), "announcements": results}


# ─── Manual Trigger ───────────────────────────────────────────────────────────

@app.post("/api/trigger")
async def trigger_pipeline(background_tasks: BackgroundTasks):
    """Manually trigger the fetch pipeline."""
    from scheduler import run_pipeline
    background_tasks.add_task(run_pipeline)
    return {"message": "Segregated pipeline triggered!", "timestamp": datetime.utcnow().isoformat() + "Z"}


# ─── Announcement Types List ──────────────────────────────────────────────────

@app.get("/api/types")
async def get_announcement_types():
    """Get all available announcement types."""
    return {
        "types": [
            "Financial Results", "Dividend", "Merger & Acquisition",
            "Board Meeting", "Order Win", "Rights Issue", "Buyback",
            "Insider Trading", "AGM/EGM", "Share Allotment",
            "Regulatory Filing", "Other"
        ],
        "primary_type": "Increase in Authorized Capital"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)

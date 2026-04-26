"""
FastAPI Backend — Main application entry point.
Routes:
  /api/authorized-capital   — Auth capital announcements (PRIMARY)
  /api/announcements        — General announcements (SECONDARY)
  /api/stats                — Segregated statistics
  /api/excel/*              — Excel exports
  /api/trigger              — Manual pipeline trigger
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
    version="3.0.0"
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


# ─── Startup / Shutdown ────────────────────────────────────────────────────

@app.on_event("startup")
async def startup_event():
    from database import connect_db
    from scheduler import start_scheduler, run_pipeline
    await connect_db()
    start_scheduler()
    import asyncio
    asyncio.create_task(run_pipeline())


@app.on_event("shutdown")
async def shutdown_event():
    from database import close_db
    from scheduler import stop_scheduler
    stop_scheduler()
    await close_db()


# ─── Health Check ──────────────────────────────────────────────────────────

@app.get("/")
async def home():
    return {"status": "AlphaIntel v3.0 — Segregated Pipeline is LIVE 🚀"}

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "service": "AlphaIntel — NSE/BSE Financial Intelligence Agent",
        "ai_engine": "Groq (LLaMA 3.1)",
        "pipeline": "segregated (auth_capital + general)"
    }


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 1: AUTHORIZED CAPITAL API (PRIMARY PRIORITY)
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/api/authorized-capital")
async def get_authorized_capital(
    limit: int = Query(50, ge=1, le=200),
    skip: int = Query(0, ge=0),
):
    """Fetch ONLY authorized capital announcements. Top priority data."""
    from database import get_authorized_capital_list, get_authorized_capital_count

    announcements = await get_authorized_capital_list(limit=limit, skip=skip)

    results = []
    for ann in announcements:
        ann["_id"] = str(ann.get("_id", ""))
        ai_data = ann.get("ai_data", {}) or {}
        auth_data = ann.get("auth_data", {}) or {}
        auth_cap = ai_data.get("authorized_capital", {}) or {}

        # Merge deterministic auth_data (priority) with AI auth_cap
        merged_auth = {**auth_cap}
        for key, val in auth_data.items():
            if val is not None and val != "Not Available":
                merged_auth[key] = val

        try:
            ann_date = ann.get("announcement_date", "")
            if isinstance(ann_date, datetime):
                ann_date = ann_date.isoformat()
        except:
            ann_date = ""

        created_at = ann.get("fetched_at") or ann.get("announcement_date", "")
        if isinstance(created_at, datetime):
            created_at = created_at.isoformat()

        results.append({
            "id": ann["_id"],
            "category": "authorized_capital",
            "exchange": _safe(ann.get("exchange"), "NSE"),
            "company_name": _safe(ai_data.get("company_name") or ann.get("company_name")),
            "ticker": _safe(ai_data.get("ticker") or ann.get("ticker")),
            "announcement_type": "Increase in Authorized Capital",
            "title": _safe(ai_data.get("title"), "AUTH CAPITAL"),
            "description": _safe(ai_data.get("description") or ai_data.get("key_details") or ann.get("raw_subject")),
            "announcement_date": ann_date,
            "board_approval": _safe(merged_auth.get("board_approval")),
            "date_of_board_meeting": _safe(merged_auth.get("date_of_board_meeting")),
            "existing_auth_eq_cap_inr": merged_auth.get("existing_auth_eq_cap_inr"),
            "new_auth_eq_cap_inr": merged_auth.get("new_auth_eq_cap_inr"),
            "proposed_increase_inr": merged_auth.get("proposed_increase_inr"),
            "cmp": ai_data.get("cmp"),
            "market_cap_cr": ai_data.get("market_cap_cr"),
            "sector": _safe(ai_data.get("sector")),
            "sentiment": _safe(ai_data.get("sentiment"), "Neutral"),
            "impact": _safe(ai_data.get("impact") or ai_data.get("impact_level"), "Medium"),
            "ai_insight": _safe(ai_data.get("ai_insight")),
            "trading_signal": _safe(ai_data.get("trading_signal")),
            "source_url": _safe(ann.get("source_url"), "#"),
            "pdf_url": ann.get("pdf_url"),
            "created_at": _safe(created_at),
        })

    total = await get_authorized_capital_count()
    return {"announcements": results, "total": total, "skip": skip, "limit": limit}


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 2: GENERAL ANNOUNCEMENTS API (SECONDARY)
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/api/announcements")
async def get_announcements(
    limit: int = Query(50, ge=1, le=200),
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
        except:
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
            "description": _safe(ai_data.get("description") or ai_data.get("key_details") or ann.get("raw_subject")),
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


# ─── Stats API ───────────────────────────────────────────────────────────

@app.get("/api/stats")
async def get_stats_endpoint():
    """Dashboard statistics with segregated counts."""
    from database import get_stats, get_last_fetch_time
    from scheduler import last_run

    stats = await get_stats()
    stats["last_fetched"] = last_run.get("time") or await get_last_fetch_time()
    return stats


# ─── Excel Export APIs ───────────────────────────────────────────────────

@app.get("/api/excel/segregated-report")
async def download_segregated_excel(
    limit: int = Query(500, ge=1, le=2000),
):
    """Download the dual-section Excel: Sheet 1 = Auth Capital, Sheet 2 = Others."""
    from database import get_authorized_capital_list, get_general_announcements
    from agents.reporter_agent import generate_segregated_excel

    auth_anns = await get_authorized_capital_list(limit=limit)
    general_anns = await get_general_announcements(limit=limit)

    excel_bytes = generate_segregated_excel(auth_anns, general_anns)
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

    announcements = await get_authorized_capital_list(limit=limit)

    excel_bytes = generate_authorized_capital_excel(announcements)
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

    excel_bytes = generate_full_report_excel(announcements)
    filename = f"NSE_BSE_Report_{datetime.now().strftime('%d%m%Y_%H%M')}.xlsx"

    return StreamingResponse(
        io.BytesIO(excel_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


# ─── Company Detail API ──────────────────────────────────────────────────

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


# ─── Manual Trigger ──────────────────────────────────────────────────────

@app.post("/api/trigger")
async def trigger_pipeline(background_tasks: BackgroundTasks):
    """Manually trigger the fetch pipeline."""
    from scheduler import run_pipeline
    background_tasks.add_task(run_pipeline)
    return {"message": "Segregated pipeline triggered!", "timestamp": datetime.utcnow().isoformat() + "Z"}


# ─── Announcement Types List ─────────────────────────────────────────────

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

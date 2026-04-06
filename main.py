"""
FastAPI Backend — Main application entry point.
Routes: /announcements, /excel, /analyze, /stats, /trigger
"""
import os
import io
from datetime import datetime
from typing import Optional, List
from fastapi import FastAPI, Query, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(
    title="NSE/BSE Financial Intelligence Agent",
    description="AI-powered corporate announcement analyzer for Indian stock markets",
    version="1.0.0"
)

# CORS for Next.js frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        os.getenv("FRONTEND_URL", "http://localhost:3000"),
        "http://localhost:3000",
        "http://localhost:3001",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── Startup / Shutdown ────────────────────────────────────────────────────

@app.on_event("startup")
async def startup_event():
    from database import connect_db
    from scheduler import start_scheduler, run_pipeline
    await connect_db()
    start_scheduler()
    # Run pipeline immediately on start to populate data
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
    return {"status": "NSE/BSE Financial Intelligence Agent Backend is live 🚀"}

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "NSE/BSE Financial Intelligence Agent"
    }


# ─── Announcements API ────────────────────────────────────────────────────

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
    """Fetch processed announcements with optional filters."""
    from database import get_announcements, get_total_count

    announcements = await get_announcements(
        limit=limit,
        skip=skip,
        exchange=exchange,
        announcement_type=announcement_type,
        sentiment=sentiment,
        impact=impact,
        ticker=ticker,
        search=search
    )

    # Serialize MongoDB docs
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

        results.append({
            "id": ann["_id"],
            "exchange": ann.get("exchange", ""),
            "company_name": ai_data.get("company_name") or ann.get("company_name", ""),
            "ticker": ai_data.get("ticker") or ann.get("ticker", ""),
            "announcement_type": ai_data.get("announcement_type", "Other"),
            "announcement_date": ann_date,
            "key_details": ai_data.get("key_details", ann.get("raw_subject", "")),
            "revenue_profit_impact": ai_data.get("revenue_profit_impact"),
            "sentiment": ai_data.get("sentiment", "Neutral"),
            "impact_level": ai_data.get("impact_level", "Low"),
            "ai_insight": ai_data.get("ai_insight", ""),
            "trading_signal": ai_data.get("trading_signal", ""),
            "sector": ai_data.get("sector", ""),
            "cmp": ai_data.get("cmp"),
            "market_cap_cr": ai_data.get("market_cap_cr"),
            "source_url": ann.get("source_url", ""),
            "pdf_url": ann.get("pdf_url"),
            "processed": ann.get("processed", False),
            "authorized_capital": ai_data.get("authorized_capital"),
        })

    total = await get_total_count()
    return {"data": results, "total": total, "skip": skip, "limit": limit}


@app.get("/api/announcements/{announcement_id}")
async def get_announcement_detail(announcement_id: str):
    """Get a single announcement by MongoDB ID."""
    from database import db
    from bson import ObjectId

    try:
        doc = await db.announcements.find_one({"_id": ObjectId(announcement_id)})
        if not doc:
            raise HTTPException(status_code=404, detail="Announcement not found")
        doc["_id"] = str(doc["_id"])
        return doc
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# ─── Stats API ───────────────────────────────────────────────────────────

@app.get("/api/stats")
async def get_stats():
    """Dashboard statistics."""
    from database import get_stats, get_last_fetch_time
    from scheduler import last_run

    stats = await get_stats()
    stats["last_fetched"] = last_run.get("time") or await get_last_fetch_time()
    return stats


# ─── Excel Export API ────────────────────────────────────────────────────

@app.get("/api/excel/authorized-capital")
async def download_authorized_capital_excel(
    limit: int = Query(500, ge=1, le=2000),
    from_date: Optional[str] = Query(None),
    to_date: Optional[str] = Query(None),
):
    """Download Excel in the user's exact 'Increase in Authorized Capital' format."""
    from database import get_announcements
    from agents.reporter_agent import generate_authorized_capital_excel

    announcements = await get_announcements(
        announcement_type="Increase in Authorized Capital",
        limit=limit
    )

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
    """Download comprehensive multi-sheet Excel report."""
    from database import get_announcements
    from agents.reporter_agent import generate_full_report_excel

    announcements = await get_announcements(
        limit=limit,
        exchange=exchange,
        sentiment=sentiment,
        impact=impact
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
    """Get all announcements for a company by ticker."""
    from database import get_company_announcements

    announcements = await get_company_announcements(ticker, limit)
    results = []
    for ann in announcements:
        ann["_id"] = str(ann.get("_id", ""))
        ai_data = ann.get("ai_data", {}) or {}
        results.append({
            "id": ann["_id"],
            "exchange": ann.get("exchange", ""),
            "company_name": ai_data.get("company_name") or ann.get("company_name", ""),
            "ticker": ann.get("ticker", ""),
            "announcement_type": ai_data.get("announcement_type", "Other"),
            "announcement_date": ann.get("announcement_date", ""),
            "key_details": ai_data.get("key_details", ""),
            "sentiment": ai_data.get("sentiment", "Neutral"),
            "impact_level": ai_data.get("impact_level", "Low"),
            "ai_insight": ai_data.get("ai_insight", ""),
            "trading_signal": ai_data.get("trading_signal", ""),
            "sector": ai_data.get("sector", ""),
        })

    return {"ticker": ticker, "count": len(results), "data": results}


# ─── Manual Trigger ──────────────────────────────────────────────────────

@app.post("/api/trigger")
async def trigger_pipeline(background_tasks: BackgroundTasks):
    """Manually trigger the fetch pipeline (for testing)."""
    from scheduler import run_pipeline
    background_tasks.add_task(run_pipeline)
    return {"message": "Pipeline triggered!", "timestamp": datetime.utcnow().isoformat()}


# ─── Announcement Types List ─────────────────────────────────────────────

@app.get("/api/types")
async def get_announcement_types():
    """Get all available announcement types."""
    return {
        "types": [
            "Financial Results", "Dividend", "Merger & Acquisition",
            "Increase in Authorized Capital", "Board Meeting", "Order Win",
            "Rights Issue", "Buyback", "Insider Trading", "AGM/EGM",
            "Share Allotment", "Regulatory Filing", "Other"
        ]
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)

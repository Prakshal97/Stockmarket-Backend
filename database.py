"""
MongoDB Atlas connection and CRUD operations using motor (async).
Dual-collection architecture:
  - authorized_capital: Only auth capital announcements
  - general_announcements: Everything else

Date window: 48 hours for client-facing authorized-capital freshness.
"""
import os
from datetime import datetime, timedelta
from typing import Optional, List
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", os.getenv("MONGODB_DB_NAME", "stockmarket_agent"))

DISPLAY_HOURS = int(os.getenv("DISPLAY_HOURS", "48"))
CLEANUP_HOURS = int(os.getenv("CLEANUP_HOURS", str(DISPLAY_HOURS)))

client: Optional[AsyncIOMotorClient] = None
db = None


async def connect_db():
    """Connect to MongoDB Atlas and create indexes for both collections."""
    global client, db
    client = AsyncIOMotorClient(MONGODB_URI)
    db = client[DB_NAME]

    # ── Authorized Capital Collection ─────────────────────────────────────────
    await db.authorized_capital.create_index("announcement_id", unique=True)
    await db.authorized_capital.create_index("announcement_date")
    await db.authorized_capital.create_index("ticker")
    await db.authorized_capital.create_index([("processed", 1), ("announcement_date", -1)])

    # ── General Announcements Collection ──────────────────────────────────────
    await db.general_announcements.create_index("announcement_id", unique=True)
    await db.general_announcements.create_index("announcement_date")
    await db.general_announcements.create_index("ticker")
    await db.general_announcements.create_index("processed")
    await db.general_announcements.create_index([("processed", 1), ("announcement_date", -1)])

    print(f"SUCCESS: Connected to MongoDB Atlas (DB: {DB_NAME}, dual-collection mode)")


async def close_db():
    """Close MongoDB connection."""
    global client
    if client:
        client.close()
        print("INFO: MongoDB connection closed")


def _get_cutoff_str(hours: int = DISPLAY_HOURS) -> str:
    """Return ISO cutoff string in UTC for MongoDB string comparison."""
    return (datetime.utcnow() - timedelta(hours=hours)).isoformat() + "Z"


# ── Authorized Capital CRUD ───────────────────────────────────────────────────

async def upsert_authorized_capital(announcement: dict) -> bool:
    """Insert or update an authorized capital announcement. Returns True if new."""
    try:
        result = await db.authorized_capital.update_one(
            {"announcement_id": announcement["announcement_id"]},
            {"$setOnInsert": announcement},
            upsert=True
        )
        return result.upserted_id is not None
    except Exception as e:
        print(f"ERROR: Auth capital upsert error: {e}")
        return False


async def update_authorized_capital_ai(announcement_id: str, ai_data: dict, auth_data: dict, excel_row: dict):
    """Save enriched data back to the authorized_capital collection."""
    await db.authorized_capital.update_one(
        {"announcement_id": announcement_id},
        {
            "$set": {
                "processed": True,
                "ai_data": ai_data,
                "auth_data": auth_data,
                "excel_row": excel_row,
                "processed_at": datetime.utcnow().isoformat() + "Z"
            }
        }
    )


async def get_authorized_capital_list(
    limit: int = 50,
    skip: int = 0,
) -> List[dict]:
    """Fetch processed authorized capital announcements within DISPLAY_HOURS."""
    cutoff = _get_cutoff_str(DISPLAY_HOURS)
    query = {"processed": True, "announcement_date": {"$gte": cutoff}}
    projection = {
        "_id": 1,
        "announcement_id": 1,
        "exchange": 1,
        "company_name": 1,
        "ticker": 1,
        "symbol": 1,
        "raw_subject": 1,
        "raw_body": 1,
        "title": 1,
        "announcement_title": 1,
        "announcement_date": 1,
        "source_url": 1,
        "pdf_url": 1,
        "processed": 1,
        "ai_data": 1,
        "auth_data": 1,
        "excel_row": 1,
        "old_capital_inr": 1,
        "new_capital_inr": 1,
        "increase_amount_inr": 1,
        "percentage_increase": 1,
        "face_value_inr": 1,
        "created_at": 1,
        "board_approval": 1,
        "date_of_board_meeting": 1,
        "sector": 1,
        "sentiment": 1,
        "impact": 1,
        "cmp": 1,
        "market_cap_cr": 1,
    }
    cursor = db.authorized_capital.find(
        query,
        projection=projection,
        sort=[("announcement_date", -1)]
    )
    rows = await cursor.to_list(length=limit + skip + 50)

    from agents.classifier import is_pure_authorized_capital

    filtered = []
    for row in rows:
        row.setdefault("announcement_id", str(row.get("announcement_id") or row.get("_id") or ""))
        if is_pure_authorized_capital(row):
            filtered.append(row)

    return filtered[skip: skip + limit]


async def get_authorized_capital_count() -> int:
    """Count authorized capital announcements within DISPLAY_HOURS."""
    cutoff = _get_cutoff_str(DISPLAY_HOURS)
    cursor = db.authorized_capital.find(
        {"processed": True, "announcement_date": {"$gte": cutoff}},
        projection={"_id": 0, "announcement_id": 1, "raw_subject": 1, "raw_body": 1, "title": 1, "ai_data": 1},
    )
    rows = await cursor.to_list(length=1000)
    from agents.classifier import is_pure_authorized_capital
    return sum(1 for row in rows if is_pure_authorized_capital(row))


async def get_unprocessed_auth_capital(limit: int = 50) -> List[dict]:
    """Fetch unprocessed authorized capital announcements (no date filter)."""
    cursor = db.authorized_capital.find(
        {"processed": False},
        sort=[("announcement_date", -1)]
    ).limit(limit)
    return await cursor.to_list(length=limit)


# ── General Announcements CRUD ────────────────────────────────────────────────

async def upsert_general_announcement(announcement: dict) -> bool:
    """Insert or update a general announcement. Returns True if new."""
    try:
        result = await db.general_announcements.update_one(
            {"announcement_id": announcement["announcement_id"]},
            {"$setOnInsert": announcement},
            upsert=True
        )
        return result.upserted_id is not None
    except Exception as e:
        print(f"ERROR: General announcement upsert error: {e}")
        return False


async def update_general_announcement_ai(announcement_id: str, ai_data: dict, excel_row: dict):
    """Save AI extraction results to the general_announcements collection."""
    await db.general_announcements.update_one(
        {"announcement_id": announcement_id},
        {
            "$set": {
                "processed": True,
                "ai_data": ai_data,
                "excel_row": excel_row,
                "processed_at": datetime.utcnow().isoformat() + "Z"
            }
        }
    )


async def get_unprocessed_general(limit: int = 50) -> List[dict]:
    """Fetch general announcements not yet processed by AI."""
    cursor = db.general_announcements.find(
        {"processed": False},
        sort=[("announcement_date", -1)]
    ).limit(limit)
    return await cursor.to_list(length=limit)


async def get_general_announcements(
    limit: int = 50,
    skip: int = 0,
    exchange: Optional[str] = None,
    announcement_type: Optional[str] = None,
    sentiment: Optional[str] = None,
    impact: Optional[str] = None,
    ticker: Optional[str] = None,
    search: Optional[str] = None
) -> List[dict]:
    """Fetch processed general announcements with filters within DISPLAY_HOURS."""
    cutoff = _get_cutoff_str(DISPLAY_HOURS)
    query = {"processed": True, "announcement_date": {"$gte": cutoff}}

    if exchange:
        query["exchange"] = exchange.upper()
    if announcement_type:
        query["ai_data.announcement_type"] = announcement_type
    if sentiment:
        query["ai_data.sentiment"] = {"$regex": sentiment, "$options": "i"}
    if impact:
        query["$or"] = [
            {"ai_data.impact_level": {"$regex": impact, "$options": "i"}},
            {"ai_data.impact": {"$regex": impact, "$options": "i"}},
        ]
    if ticker:
        query["ticker"] = {"$regex": ticker, "$options": "i"}
    if search:
        query["$or"] = [
            {"company_name": {"$regex": search, "$options": "i"}},
            {"ticker": {"$regex": search, "$options": "i"}},
            {"ai_data.key_details": {"$regex": search, "$options": "i"}},
            {"raw_subject": {"$regex": search, "$options": "i"}},
        ]

    cursor = db.general_announcements.find(
        query,
        sort=[("announcement_date", -1)]
    ).skip(skip).limit(limit)
    return await cursor.to_list(length=limit)


async def get_general_count(query: dict = None) -> int:
    """Count general announcements matching query."""
    if query is None:
        cutoff = _get_cutoff_str(DISPLAY_HOURS)
        query = {"processed": True, "announcement_date": {"$gte": cutoff}}
    return await db.general_announcements.count_documents(query)


# ── Unified Stats ─────────────────────────────────────────────────────────────

async def get_stats() -> dict:
    """Aggregate stats for dashboard across both collections."""
    cutoff = _get_cutoff_str(DISPLAY_HOURS)
    auth_count = await get_authorized_capital_count()

    pipeline = [
        {"$match": {"processed": True, "announcement_date": {"$gte": cutoff}}},
        {"$group": {
            "_id": None,
            "total": {"$sum": 1},
            "by_exchange": {"$push": "$exchange"},
            "by_type": {"$push": "$ai_data.announcement_type"},
            "by_sentiment": {"$push": "$ai_data.sentiment"},
            "by_impact": {"$push": "$ai_data.impact_level"}
        }}
    ]
    result = await db.general_announcements.aggregate(pipeline).to_list(1)

    def count_values(lst):
        counts = {}
        for v in lst:
            if v:
                counts[v] = counts.get(v, 0) + 1
        return counts

    if result:
        row = result[0]
        general_total = row["total"]
        by_exchange = count_values(row["by_exchange"])
        by_type = count_values(row["by_type"])
        by_sentiment = count_values(row["by_sentiment"])
        by_impact = count_values(row["by_impact"])
    else:
        general_total = 0
        by_exchange = {}
        by_type = {}
        by_sentiment = {}
        by_impact = {}

    if auth_count > 0:
        by_type["Increase in Authorized Capital"] = auth_count
        # Also count NSE/BSE auth capital in exchange stats
        auth_by_exchange = await db.authorized_capital.aggregate([
            {"$match": {"processed": True, "announcement_date": {"$gte": cutoff}}},
            {"$group": {"_id": "$exchange", "count": {"$sum": 1}}}
        ]).to_list(10)
        for row in auth_by_exchange:
            ex = row["_id"] or "NSE"
            by_exchange[ex] = by_exchange.get(ex, 0) + row["count"]

    return {
        "total_announcements": general_total + auth_count,
        "auth_capital_count": auth_count,
        "general_count": general_total,
        "by_exchange": by_exchange,
        "by_type": by_type,
        "by_sentiment": by_sentiment,
        "by_impact": by_impact,
        "display_hours": DISPLAY_HOURS,
    }


async def get_last_fetch_time() -> Optional[str]:
    """Get the most recent fetched_at timestamp from either collection."""
    docs = []
    for coll_name in ["authorized_capital", "general_announcements"]:
        doc = await db[coll_name].find_one(
            {},
            sort=[("fetched_at", -1)],
            projection={"fetched_at": 1}
        )
        if doc and "fetched_at" in doc:
            docs.append(doc["fetched_at"])
    return max(docs) if docs else None


async def get_company_announcements(ticker: str, limit: int = 20) -> List[dict]:
    """Get all announcements for a specific company from both collections."""
    cutoff = _get_cutoff_str(DISPLAY_HOURS)
    base_query = {
        "ticker": {"$regex": ticker, "$options": "i"},
        "processed": True,
        "announcement_date": {"$gte": cutoff}
    }

    auth = await db.authorized_capital.find(
        base_query, sort=[("announcement_date", -1)]
    ).limit(limit).to_list(length=limit)

    general = await db.general_announcements.find(
        base_query, sort=[("announcement_date", -1)]
    ).limit(limit).to_list(length=limit)

    for a in auth:
        a["_category"] = "authorized_capital"
    for g in general:
        g["_category"] = "general"

    combined = auth + general
    combined.sort(key=lambda x: x.get("announcement_date", ""), reverse=True)
    return combined[:limit]


# ── Debug Helpers ─────────────────────────────────────────────────────────────

async def get_db_summary() -> dict:
    """Return a full summary of DB state for debugging."""
    auth_total = await db.authorized_capital.count_documents({})
    auth_processed = await db.authorized_capital.count_documents({"processed": True})
    auth_unprocessed = await db.authorized_capital.count_documents({"processed": False})

    gen_total = await db.general_announcements.count_documents({})
    gen_processed = await db.general_announcements.count_documents({"processed": True})
    gen_unprocessed = await db.general_announcements.count_documents({"processed": False})

    # Get 5 most recent auth capital
    recent_auth = await db.authorized_capital.find(
        {}, sort=[("announcement_date", -1)]
    ).limit(5).to_list(5)
    for r in recent_auth:
        r["_id"] = str(r["_id"])
        r.pop("ai_data", None)
        r.pop("auth_data", None)
        r.pop("excel_row", None)

    return {
        "authorized_capital": {
            "total": auth_total,
            "processed": auth_processed,
            "unprocessed": auth_unprocessed,
            "recent_5": recent_auth,
        },
        "general_announcements": {
            "total": gen_total,
            "processed": gen_processed,
            "unprocessed": gen_unprocessed,
        },
        "display_window_hours": DISPLAY_HOURS,
        "cutoff_utc": _get_cutoff_str(DISPLAY_HOURS),
    }


# ── Cleanup ───────────────────────────────────────────────────────────────────

async def cleanup_old_announcements(hours: int = CLEANUP_HOURS):
    """Delete announcements older than `hours` from both collections."""
    cutoff = _get_cutoff_str(hours)

    r1 = await db.authorized_capital.delete_many({"announcement_date": {"$lt": cutoff}})
    r2 = await db.general_announcements.delete_many({"announcement_date": {"$lt": cutoff}})

    total_deleted = r1.deleted_count + r2.deleted_count
    if total_deleted > 0:
        print(f"CLEANUP: Deleted {r1.deleted_count} auth + {r2.deleted_count} general older than {hours}h")
    return total_deleted


async def reset_unprocessed():
    """Reset ALL items to unprocessed=False so they can be re-extracted by AI."""
    r1 = await db.authorized_capital.update_many(
        {}, {"$set": {"processed": False}}
    )
    r2 = await db.general_announcements.update_many(
        {}, {"$set": {"processed": False}}
    )
    print(f"RESET: {r1.modified_count} auth + {r2.modified_count} general reset to unprocessed")
    return r1.modified_count + r2.modified_count

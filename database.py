"""
MongoDB Atlas connection and CRUD operations using motor (async).
Dual-collection architecture:
  - authorized_capital: Only auth capital announcements
  - general_announcements: Everything else
"""
import os
from datetime import datetime, timedelta
from typing import Optional, List
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "stockmarket_agent")

client: Optional[AsyncIOMotorClient] = None
db = None


async def connect_db():
    """Connect to MongoDB Atlas and create indexes for both collections."""
    global client, db
    client = AsyncIOMotorClient(MONGODB_URI)
    db = client[DB_NAME]

    # ── Authorized Capital Collection ─────────────────────────────
    await db.authorized_capital.create_index("announcement_id", unique=True)
    await db.authorized_capital.create_index("announcement_date")
    await db.authorized_capital.create_index("ticker")

    # ── General Announcements Collection ──────────────────────────
    await db.general_announcements.create_index("announcement_id", unique=True)
    await db.general_announcements.create_index("announcement_date")
    await db.general_announcements.create_index("ticker")
    await db.general_announcements.create_index("processed")
    await db.general_announcements.create_index([("processed", 1), ("announcement_date", -1)])

    print("SUCCESS: Connected to MongoDB Atlas (dual-collection mode)")


async def close_db():
    """Close MongoDB connection."""
    global client
    if client:
        client.close()
        print("INFO: MongoDB connection closed")


# ── Authorized Capital CRUD ───────────────────────────────────────────────

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
    """Fetch processed authorized capital announcements."""
    cutoff = (datetime.utcnow() - timedelta(hours=24)).isoformat() + "Z"
    query = {"processed": True, "announcement_date": {"$gte": cutoff}}
    cursor = db.authorized_capital.find(
        query,
        sort=[("announcement_date", -1)]
    ).skip(skip).limit(limit)
    return await cursor.to_list(length=limit)


async def get_authorized_capital_count() -> int:
    """Count authorized capital announcements in the last 24h."""
    cutoff = (datetime.utcnow() - timedelta(hours=24)).isoformat() + "Z"
    return await db.authorized_capital.count_documents(
        {"processed": True, "announcement_date": {"$gte": cutoff}}
    )


async def get_unprocessed_auth_capital(limit: int = 20) -> List[dict]:
    """Fetch unprocessed authorized capital announcements."""
    cursor = db.authorized_capital.find(
        {"processed": False},
        sort=[("announcement_date", -1)]
    ).limit(limit)
    return await cursor.to_list(length=limit)


# ── General Announcements CRUD ────────────────────────────────────────────

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


async def get_unprocessed_general(limit: int = 20) -> List[dict]:
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
    """Fetch processed general announcements with filters."""
    cutoff = (datetime.utcnow() - timedelta(hours=24)).isoformat() + "Z"
    query = {"processed": True, "announcement_date": {"$gte": cutoff}}

    if exchange:
        query["exchange"] = exchange
    if announcement_type:
        query["ai_data.announcement_type"] = announcement_type
    if sentiment:
        query["ai_data.sentiment"] = sentiment
    if impact:
        query["ai_data.impact_level"] = impact
    if ticker:
        query["ticker"] = {"$regex": ticker, "$options": "i"}
    if search:
        query["$or"] = [
            {"company_name": {"$regex": search, "$options": "i"}},
            {"ticker": {"$regex": search, "$options": "i"}},
            {"ai_data.key_details": {"$regex": search, "$options": "i"}}
        ]

    cursor = db.general_announcements.find(
        query,
        sort=[("announcement_date", -1)]
    ).skip(skip).limit(limit)
    return await cursor.to_list(length=limit)


async def get_general_count(query: dict = None) -> int:
    """Count general announcements matching query."""
    if query is None:
        cutoff = (datetime.utcnow() - timedelta(hours=24)).isoformat() + "Z"
        query = {"processed": True, "announcement_date": {"$gte": cutoff}}
    return await db.general_announcements.count_documents(query)


# ── Unified Stats ─────────────────────────────────────────────────────────

async def get_stats() -> dict:
    """Aggregate stats for dashboard across both collections."""
    cutoff = (datetime.utcnow() - timedelta(hours=24)).isoformat() + "Z"

    # Auth capital count
    auth_count = await db.authorized_capital.count_documents(
        {"processed": True, "announcement_date": {"$gte": cutoff}}
    )

    # General stats via aggregation
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

    # Add auth capital as a type
    if auth_count > 0:
        by_type["Increase in Authorized Capital"] = auth_count

    return {
        "total_announcements": general_total + auth_count,
        "auth_capital_count": auth_count,
        "general_count": general_total,
        "by_exchange": by_exchange,
        "by_type": by_type,
        "by_sentiment": by_sentiment,
        "by_impact": by_impact,
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
    cutoff = (datetime.utcnow() - timedelta(hours=24)).isoformat() + "Z"
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

    # Tag each doc with its category
    for a in auth:
        a["_category"] = "authorized_capital"
    for g in general:
        g["_category"] = "general"

    # Merge and sort by date
    combined = auth + general
    combined.sort(key=lambda x: x.get("announcement_date", ""), reverse=True)
    return combined[:limit]


# ── Cleanup ───────────────────────────────────────────────────────────────

async def cleanup_old_announcements(hours: int = 24):
    """Delete announcements older than `hours` from both collections."""
    cutoff = (datetime.utcnow() - timedelta(hours=hours)).isoformat() + "Z"

    r1 = await db.authorized_capital.delete_many({"announcement_date": {"$lt": cutoff}})
    r2 = await db.general_announcements.delete_many({"announcement_date": {"$lt": cutoff}})

    total_deleted = r1.deleted_count + r2.deleted_count
    if total_deleted > 0:
        print(f"CLEANUP: Deleted {r1.deleted_count} auth + {r2.deleted_count} general older than {hours}h")
    return total_deleted

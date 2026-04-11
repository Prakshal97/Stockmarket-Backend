"""
MongoDB Atlas connection and CRUD operations using motor (async).
"""
import os
from datetime import datetime
from typing import Optional, List
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "stockmarket_agent")

client: Optional[AsyncIOMotorClient] = None
db = None


async def connect_db():
    """Connect to MongoDB Atlas."""
    global client, db
    client = AsyncIOMotorClient(MONGODB_URI)
    db = client[DB_NAME]
    # Create indexes for performance
    await db.announcements.create_index("announcement_id", unique=True)
    await db.announcements.create_index("announcement_date")
    await db.announcements.create_index("ticker")
    await db.announcements.create_index("processed")
    # Compound index for faster feed queries (processed + date sort)
    await db.announcements.create_index([("processed", 1), ("announcement_date", -1)])
    print("✅ Connected to MongoDB Atlas")


async def close_db():
    """Close MongoDB connection."""
    global client
    if client:
        client.close()
        print("🔴 MongoDB connection closed")


async def upsert_announcement(announcement: dict) -> bool:
    """Insert or update announcement. Returns True if it was new."""
    try:
        result = await db.announcements.update_one(
            {"announcement_id": announcement["announcement_id"]},
            {"$setOnInsert": announcement},
            upsert=True
        )
        return result.upserted_id is not None  # True if new
    except Exception as e:
        print(f"❌ DB upsert error: {e}")
        return False


async def get_unprocessed_announcements(limit: int = 20) -> List[dict]:
    """Fetch announcements not yet processed by AI."""
    cursor = db.announcements.find(
        {"processed": False},
        sort=[("announcement_date", -1)]
    ).limit(limit)
    return await cursor.to_list(length=limit)


async def update_announcement_ai(announcement_id: str, ai_data: dict, excel_row: dict):
    """Save AI extraction results back to MongoDB."""
    await db.announcements.update_one(
        {"announcement_id": announcement_id},
        {
            "$set": {
                "processed": True,
                "ai_data": ai_data,
                "excel_row": excel_row,
                "processed_at": datetime.utcnow().isoformat()
            }
        }
    )


async def get_announcements(
    limit: int = 50,
    skip: int = 0,
    exchange: Optional[str] = None,
    announcement_type: Optional[str] = None,
    sentiment: Optional[str] = None,
    impact: Optional[str] = None,
    ticker: Optional[str] = None,
    search: Optional[str] = None
) -> List[dict]:
    """Fetch processed announcements with filters."""
    query = {"processed": True}

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

    cursor = db.announcements.find(
        query,
        sort=[("announcement_date", -1)]
    ).skip(skip).limit(limit)
    return await cursor.to_list(length=limit)


async def get_stats() -> dict:
    """Aggregate stats for dashboard."""
    pipeline = [
        {"$match": {"processed": True}},
        {"$group": {
            "_id": None,
            "total": {"$sum": 1},
            "by_exchange": {"$push": "$exchange"},
            "by_type": {"$push": "$ai_data.announcement_type"},
            "by_sentiment": {"$push": "$ai_data.sentiment"},
            "by_impact": {"$push": "$ai_data.impact_level"}
        }}
    ]
    result = await db.announcements.aggregate(pipeline).to_list(1)
    if not result:
        return {}

    def count_values(lst):
        counts = {}
        for v in lst:
            if v:
                counts[v] = counts.get(v, 0) + 1
        return counts

    row = result[0]
    return {
        "total_announcements": row["total"],
        "by_exchange": count_values(row["by_exchange"]),
        "by_type": count_values(row["by_type"]),
        "by_sentiment": count_values(row["by_sentiment"]),
        "by_impact": count_values(row["by_impact"]),
    }


async def get_total_count(query: dict = None) -> int:
    """Count total documents matching query."""
    if query is None:
        query = {"processed": True}
    return await db.announcements.count_documents(query)


async def get_last_fetch_time() -> Optional[str]:
    """Get the most recent fetched_at timestamp."""
    doc = await db.announcements.find_one(
        {},
        sort=[("fetched_at", -1)],
        projection={"fetched_at": 1}
    )
    if doc and "fetched_at" in doc:
        return doc["fetched_at"]
    return None


async def get_company_announcements(ticker: str, limit: int = 20) -> List[dict]:
    """Get all announcements for a specific company."""
    cursor = db.announcements.find(
        {"ticker": {"$regex": ticker, "$options": "i"}, "processed": True},
        sort=[("announcement_date", -1)]
    ).limit(limit)
    return await cursor.to_list(length=limit)

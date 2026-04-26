"""
Diagnostic script — inspect what's actually stored in MongoDB
for authorized capital announcements.
"""
import asyncio
import os
os.environ['PYTHONIOENCODING'] = 'utf-8'

import motor.motor_asyncio
from dotenv import load_dotenv

load_dotenv()


async def diagnose():
    uri = os.environ.get('MONGODB_URI')
    dbname = os.environ.get('DB_NAME', 'stockmarket_agent')
    client = motor.motor_asyncio.AsyncIOMotorClient(uri)
    database = client[dbname]
    col = database.announcements

    # ── 1. Totals ────────────────────────────────────────────────
    total = await col.count_documents({})
    processed = await col.count_documents({'processed': True})
    print(f"Total docs: {total}  |  Processed: {processed}")

    # ── 2. Announcement type distribution ───────────────────────
    print("\n--- Announcement Types in DB ---")
    cursor = col.aggregate([
        {"$match": {"processed": True}},
        {"$group": {"_id": "$ai_data.announcement_type", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ])
    async for row in cursor:
        print(f"  {row['_id']}: {row['count']}")

    # ── 3. Find auth capital announcements (any match) ──────────
    print("\n--- Auth Capital Announcements ---")
    auth_cursor = col.find({
        "$or": [
            {"ai_data.announcement_type": "Increase in Authorized Capital"},
            {"raw_subject": {"$regex": "authorized capital", "$options": "i"}},
            {"raw_subject": {"$regex": "authorised capital", "$options": "i"}},
        ]
    }).limit(5)

    docs = await auth_cursor.to_list(length=5)
    print(f"Found: {len(docs)} matching docs")

    for doc in docs:
        ai = doc.get("ai_data", {}) or {}
        auth = ai.get("authorized_capital", {}) or {}
        body = doc.get("raw_body", "")
        print(f"\n  Company : {doc.get('company_name')}")
        print(f"  Subject : {doc.get('raw_subject', '')[:120]}")
        print(f"  Type    : {ai.get('announcement_type')}")
        print(f"  Body len: {len(body)}")
        print(f"  Body[0:500]: {repr(body[:500])}")
        print(f"  authorized_capital stored: {auth}")
        print(f"  PDF URL : {doc.get('pdf_url', 'none')}")
        print("  " + "-"*60)

    # ── 4. Sample ANY processed doc to see structure ─────────────
    print("\n--- Sample Processed Doc (any type) ---")
    sample = await col.find_one({"processed": True})
    if sample:
        ai = sample.get("ai_data", {}) or {}
        body = sample.get("raw_body", "")
        print(f"  Company          : {sample.get('company_name')}")
        print(f"  Type             : {ai.get('announcement_type')}")
        print(f"  ai_data keys     : {list(ai.keys())}")
        print(f"  authorized_capital: {ai.get('authorized_capital')}")
        print(f"  raw_body length  : {len(body)}")
        print(f"  raw_body[:400]   : {repr(body[:400])}")

asyncio.run(diagnose())

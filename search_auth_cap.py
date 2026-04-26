import asyncio
import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "stockmarket_agent")

async def search_other():
    client = AsyncIOMotorClient(MONGODB_URI)
    db = client[DB_NAME]
    
    cursor = db.announcements.find({
        "$or": [
            {"raw_subject": {"$regex": "capital", "$options": "i"}},
            {"raw_subject": {"$regex": "authorized", "$options": "i"}},
            {"raw_subject": {"$regex": "authorised", "$options": "i"}}
        ]
    })
    
    results = await cursor.to_list(None)
    print(f"Found {len(results)} potential candidates:")
    for r in results:
        print(f"- [{r.get('exchange')}] {r.get('company_name')}: {r.get('raw_subject')}")
        print(f"  Processed: {r.get('processed')}, Type: {r.get('ai_data', {}).get('announcement_type')}")

    client.close()

if __name__ == "__main__":
    asyncio.run(search_other())

import asyncio
import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv()

async def main():
    uri = os.getenv('MONGODB_URI')
    db_name = os.getenv('DB_NAME', 'stockmarketagent')
    client = AsyncIOMotorClient(uri)
    db = client[db_name]
    
    count = await db.announcements.count_documents({})
    print(f"Total documents: {count}")
    
    result = await db.announcements.update_many({}, {"$set": {"processed": False, "ai_data": None}})
    print(f"Reset {result.modified_count} announcements to unprocessed state.")
    
if __name__ == "__main__":
    asyncio.run(main())

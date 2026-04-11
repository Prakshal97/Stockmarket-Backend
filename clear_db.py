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
    
    deleted = await db.announcements.delete_many({})
    print(f"✅ Cleared {deleted.deleted_count} old garbage documents from DB.")
    
if __name__ == "__main__":
    asyncio.run(main())

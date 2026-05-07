import asyncio
from scheduler import refresh_recent_authorized_capital
import database

async def test_live_pipeline():
    print("Connecting to DB...")
    await database.connect_db()
    
    print("Running refresh_recent_authorized_capital (force=True)...")
    await refresh_recent_authorized_capital(hours=48, force=True, persist=False, lightweight=True)

if __name__ == "__main__":
    asyncio.run(test_live_pipeline())

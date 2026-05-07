import asyncio
import database
from agents.classifier import is_pure_authorized_capital

async def main():
    await database.connect_db()
    cursor = database.db.authorized_capital.find({})
    items = await cursor.to_list(None)
    
    removed = 0
    for ann in items:
        if not is_pure_authorized_capital(ann):
            print(f"Removing invalid item from auth DB: {ann.get('company_name')} - {ann.get('raw_subject')}")
            await database.db.authorized_capital.delete_one({"_id": ann["_id"]})
            # Upsert it into general_announcements just in case it's not there
            await database.upsert_general_announcement(ann)
            removed += 1
            
    print(f"Total invalid items removed from authorized_capital: {removed}")

asyncio.run(main())

import asyncio
import database

async def main():
    await database.connect_db()
    announcements = await database.get_authorized_capital_list(limit=100, skip=0)
    print(f"Total returned: {len(announcements)}")
    for ann in announcements:
        print(f"Company: {ann.get('company_name')} - Date: {ann.get('announcement_date')} - ID: {ann.get('announcement_id')} - Subject: {ann.get('raw_subject')}")
        
asyncio.run(main())

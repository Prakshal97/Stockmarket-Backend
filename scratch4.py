import asyncio
import database

async def main():
    await database.connect_db()
    res = await database.db.authorized_capital.delete_many({'company_name': {'$regex': 'ONGC|Oil and Natural Gas|TCS|Tata Consultancy', '$options': 'i'}})
    print(f'Deleted {res.deleted_count} documents from authorized_capital')

asyncio.run(main())

import asyncio
import database

async def main():
    await database.connect_db()
    ongc = await database.db.authorized_capital.find_one({'company_name': {'$regex': 'ONGC|Oil and Natural Gas', '$options': 'i'}})
    tcs = await database.db.authorized_capital.find_one({'company_name': {'$regex': 'TCS|Tata Consultancy', '$options': 'i'}})
    
    if ongc:
        print('ONGC full body:')
        print(ongc.get('raw_body', ''))
    if tcs:
        print('\nTCS full body:')
        print(tcs.get('raw_body', ''))
        
asyncio.run(main())

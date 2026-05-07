import asyncio
import database

async def main():
    await database.connect_db()
    ongc = await database.db.authorized_capital.find_one({'company_name': {'$regex': 'ONGC|Oil and Natural Gas', '$options': 'i'}})
    tcs = await database.db.authorized_capital.find_one({'company_name': {'$regex': 'TCS|Tata Consultancy', '$options': 'i'}})
    
    print('ONGC subject:', ongc.get('raw_subject', '') if ongc else 'Not found')
    if ongc:
        print('ONGC text snippet:', (ongc.get('raw_subject', '') + ' ' + ongc.get('raw_body', ''))[:500])
        print('ONGC classification passed logic:', [term for term in ["dividend", "financial results", "outcome of board meeting"] if term in ongc.get('raw_body', '').lower()])
        print('Is pure:', ongc.get('company_name'))

    print('TCS subject:', tcs.get('raw_subject', '') if tcs else 'Not found')
    if tcs:
        print('TCS text snippet:', (tcs.get('raw_subject', '') + ' ' + tcs.get('raw_body', ''))[:500])
        
asyncio.run(main())

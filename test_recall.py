import asyncio
from datetime import datetime, timedelta, timezone
import os

import database
from agents.scraper_agent import fetch_all_announcements

IST = timezone(timedelta(hours=5, minutes=30))

async def validate_recall(hours: int = 48):
    await database.connect_db()
    
    print(f"\n{'='*60}")
    print(f"RECALL VALIDATION MODE ({hours} hours)")
    print(f"{'='*60}")
    
    # 1. Simulate what the scraper sees
    print("\nFetching raw exchange data (this may take a minute)...")
    raw = await asyncio.to_thread(fetch_all_announcements)
    total_fetched = len(raw)
    
    cutoff = datetime.utcnow() - timedelta(hours=hours)
    cutoff_iso = cutoff.isoformat() + "Z"
    
    # 2. Query DB
    auth_count = await database.db.authorized_capital.count_documents({"announcement_date": {"$gte": cutoff_iso}})
    gen_count = await database.db.general_announcements.count_documents({"announcement_date": {"$gte": cutoff_iso}})
    total_inserted = auth_count + gen_count
    
    # 3. Extraction stats
    ocr_count = await database.db.general_announcements.count_documents({
        "announcement_date": {"$gte": cutoff_iso},
        "extraction_method": "ocr"
    }) + await database.db.authorized_capital.count_documents({
        "announcement_date": {"$gte": cutoff_iso},
        "extraction_method": "ocr"
    })
    
    failed_extraction = await database.db.general_announcements.count_documents({
        "announcement_date": {"$gte": cutoff_iso},
        "extraction_success": False
    })
    
    pdf_count = await database.db.general_announcements.count_documents({
        "announcement_date": {"$gte": cutoff_iso},
        "extraction_method": "pdf_text"
    }) + await database.db.authorized_capital.count_documents({
        "announcement_date": {"$gte": cutoff_iso},
        "extraction_method": "pdf_text"
    })

    print(f"\n[ RECALL METRICS ]")
    print(f"  Total Fetched from Exchanges : {total_fetched}")
    print(f"  Total Inserted in DB         : {total_inserted}")
    if total_fetched > 0:
        print(f"  Recall Percentage            : {min((total_inserted / total_fetched) * 100, 100.0):.2f}%")
    else:
        print(f"  Recall Percentage            : N/A")
        
    print(f"\n[ ROUTING METRICS ]")
    print(f"  Authorized Capital Route     : {auth_count}")
    print(f"  General Announcements Route  : {gen_count}")
    
    print(f"\n[ EXTRACTION METRICS ]")
    print(f"  Successful Native PDF Reads  : {pdf_count}")
    print(f"  Successful OCR Fallbacks     : {ocr_count}")
    print(f"  Failed / No Extraction       : {failed_extraction}")
    
    if total_inserted < total_fetched * 0.9:
        print("\nWARNING: Significant gap between fetched and inserted.")
        print("This may be due to deduplication (if run across overlapping windows) or failures.")
    else:
        print("\nHEALTHY: Insertion count matches or exceeds recent fetched count (due to duplicates/overlaps).")

if __name__ == "__main__":
    asyncio.run(validate_recall())

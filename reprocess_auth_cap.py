"""
Reprocess Script — Re-extracts authorized capital data for existing records.
Useful for fixing missing/empty columns in the Excel export.
"""
import asyncio
import os
import sys
from datetime import datetime

# Add the current directory to sys.path to import local modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import database
from agents.scraper_agent import extract_pdf_text
from agents.extractor_agent import extract_announcement
from agents.analyst_agent import enrich_announcement
from scheduler import _build_excel_row

async def reprocess_auth_cap():
    print("Connecting to database...")
    await database.connect_db()
    
    # Query for announcements that are likely Authorized Capital but have empty fields
    # or just all that are marked as Increase in Authorized Capital
    query = {
        "$or": [
            {"ai_data.announcement_type": "Increase in Authorized Capital"},
            {"raw_subject": {"$regex": "authorized capital", "$options": "i"}},
            {"raw_subject": {"$regex": "authorised capital", "$options": "i"}}
        ]
    }
    
    cursor = database.db.announcements.find(query)
    announcements = await cursor.to_list(length=100)
    
    print(f"Found {len(announcements)} announcements to reprocess.")
    
    count = 0
    for ann in announcements:
        try:
            print(f"\nProcessing {ann.get('company_name')} ({ann.get('ticker')})...")
            
            # 1. Force PDF extraction
            if ann.get("pdf_url"):
                print(f"  Extracting PDF text...")
                pdf_text = await asyncio.to_thread(extract_pdf_text, ann["pdf_url"])
                if pdf_text:
                    ann["raw_body"] = (ann.get("raw_body", "") + "\n\n" + pdf_text)[:6000]
                    print(f"  Extracted {len(pdf_text)} chars from PDF.")
                else:
                    print(f"  Failed to extract PDF text.")

            # 2. Re-run AI extraction
            print(f"  Running AI extraction...")
            ai_data = await asyncio.to_thread(extract_announcement, ann)
            if not ai_data:
                print(f"  AI extraction failed.")
                continue

            # 3. Rule-based enrichment (this has the new regex logic)
            print(f"  Enriching data...")
            ai_data = enrich_announcement(ann, ai_data)
            
            # 4. Fetch live market data
            ticker = ai_data.get("ticker") or ann.get("ticker", "")
            if ticker:
                try:
                    from agents.market_data import get_market_data
                    print(f"  Fetching market data for {ticker}...")
                    mkt = await asyncio.to_thread(get_market_data, ticker, ann.get("exchange", "NSE"))
                    if mkt.get("cmp") is not None:
                        ai_data["cmp"] = mkt["cmp"]
                    if mkt.get("market_cap_cr") is not None:
                        ai_data["market_cap_cr"] = mkt["market_cap_cr"]
                except Exception as me:
                    print(f"  Market data fetch failed: {me}")

            # 5. Build Excel row
            excel_row = _build_excel_row(ann, ai_data)

            # 6. Save back to DB
            await database.update_announcement_ai(ann["announcement_id"], ai_data, excel_row)
            print(f"  Successfully updated DB.")
            count += 1
            
        except Exception as e:
            print(f"  Error processing {ann.get('company_name')}: {e}")

    print(f"\nReprocessing complete. Updated {count} announcements.")

if __name__ == "__main__":
    asyncio.run(reprocess_auth_cap())

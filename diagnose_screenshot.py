import pymongo
import json

def check_companies():
    # Use the URI from database.py logic
    import os
    from dotenv import load_dotenv
    load_dotenv()
    uri = os.getenv("MONGODB_URI")
    db_name = os.getenv("DB_NAME", "stockmarket_agent")
    
    client = pymongo.MongoClient(uri)
    db = client[db_name]
    coll = db['announcements']

    companies = ['JAYKAY', 'SEJALLTD', 'REFEX', 'DSFCL', 'WEWORK', 'JKCEMENT', 'BUTTERFLY']
    print(f"{'Company':<20} | {'Type':<25} | {'Auth Cap Data'}")
    print("-" * 80)
    
    for comp in companies:
        doc = coll.find_one({'company_name': {'$regex': comp, '$options': 'i'}})
        if doc:
            ai_data = doc.get("ai_data", {})
            ann_type = ai_data.get("announcement_type", "Other")
            auth_cap = ai_data.get("authorized_capital", {})
            
            cap_str = "None"
            if auth_cap:
                existing = auth_cap.get("existing_auth_eq_cap_inr")
                new = auth_cap.get("new_auth_eq_cap_inr")
                if existing or new:
                    cap_str = f"E:{existing}, N:{new}"
                else:
                    cap_str = "Keys Exist but Null"
            
            print(f"{doc.get('company_name', comp)[:20]:<20} | {ann_type:<25} | {cap_str}")
            print(f"  Subject: {doc.get('raw_subject')}")
            print(f"  Ticker: {doc.get('ticker')}")
            print("-" * 40)
        else:
            print(f"{comp:<20} | {'NOT FOUND':<25} | N/A")

if __name__ == "__main__":
    check_companies()

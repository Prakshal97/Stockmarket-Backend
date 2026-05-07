"""
DEEP BODY TEXT INSPECTOR — show exactly what NSE/BSE sends in attchmntText
for the near-miss postal ballot / EGM / shareholders meeting filings.

Run:  python inspect_body_text.py
"""
import sys, os, time
sys.path.insert(0, os.path.dirname(__file__))
from datetime import datetime, timedelta

def fetch_nse_raw(days=2):
    import requests
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36',
        'Accept': 'application/json',
        'Referer': 'https://www.nseindia.com/',
    })
    session.get('https://www.nseindia.com', timeout=15)
    time.sleep(2)
    from_date = (datetime.now() - timedelta(days=days)).strftime('%d-%m-%Y')
    to_date   = datetime.now().strftime('%d-%m-%Y')
    r = session.get(
        f'https://www.nseindia.com/api/corporate-announcements?index=equities&from_date={from_date}&to_date={to_date}',
        timeout=30
    )
    return r.json() if r.status_code == 200 else []

print("Fetching NSE raw items...")
items = fetch_nse_raw(days=2)
print(f"Got {len(items)} items\n")

# ── Focus: postal ballot, shareholders meeting, EGM, capital structure ─────────
focus_descs = ["postal ballot", "shareholders meeting", "egm", "capital structure",
                "capital", "alteration", "increase", "agm", "extraordinary"]

print("="*70)
print("ALL DISTINCT NSE 'desc' (category) VALUES IN THIS FETCH")
print("="*70)
from collections import Counter
all_descs = Counter(item.get('desc','').lower() for item in items)
for desc, count in all_descs.most_common(50):
    print(f"  {count:5d}x  {desc}")

print("\n" + "="*70)
print("FULL BODY TEXT for 'Shareholders meeting' filings (first 30)")
print("="*70)
sm_items = [i for i in items if 'shareholders meeting' in (i.get('desc','') or '').lower()]
print(f"Total 'Shareholders meeting' filings: {len(sm_items)}\n")
for item in sm_items[:30]:
    body = item.get('attchmntText', '') or ''
    print(f"  Company : {item.get('sm_name','')[:50]}")
    print(f"  Date    : {item.get('an_dt','')[:10]}")
    print(f"  body    : {body[:400]}")
    print(f"  PDF     : {item.get('attchmntFile','')[:80]}")
    print()

print("\n" + "="*70)
print("FULL BODY TEXT for 'Postal Ballot' filings (first 20)")
print("="*70)
pb_items = [i for i in items if 'postal ballot' in (i.get('desc','') or '').lower()]
print(f"Total 'Postal Ballot' desc filings: {len(pb_items)}\n")
for item in pb_items[:20]:
    body = item.get('attchmntText', '') or ''
    print(f"  Company : {item.get('sm_name','')[:50]}")
    print(f"  Date    : {item.get('an_dt','')[:10]}")
    print(f"  body    : {body[:400]}")
    print()

print("\n" + "="*70)
print("FULL BODY TEXT for 'Capital Structure' filings (first 20)")
print("="*70)
cs_items = [i for i in items if 'capital structure' in (i.get('desc','') or '').lower()]
print(f"Total 'Capital Structure' desc filings: {len(cs_items)}\n")
for item in cs_items[:20]:
    body = item.get('attchmntText', '') or ''
    print(f"  Company : {item.get('sm_name','')[:50]}")
    print(f"  Date    : {item.get('an_dt','')[:10]}")
    print(f"  body    : {body[:400]}")
    print(f"  PDF     : {item.get('attchmntFile','')[:80]}")
    print()

print("\n" + "="*70)
print("FILINGS WHERE 'capital' APPEARS IN BODY TEXT (attchmntText)")
print("="*70)
capital_in_body = [
    i for i in items
    if 'capital' in (i.get('attchmntText','') or '').lower()
    or 'authoris' in (i.get('attchmntText','') or '').lower()
    or 'authorized' in (i.get('attchmntText','') or '').lower()
]
print(f"Total filings with 'capital'/'authoris*' in body: {len(capital_in_body)}\n")
for item in capital_in_body[:30]:
    body = item.get('attchmntText', '') or ''
    print(f"  [{item.get('desc','')}] {item.get('sm_name','')[:40]}")
    print(f"  body: {body[:300]}")
    print()

print("\n" + "="*70)
print("ALL FIELDS for Tata Capital entries (to see exact data)")
print("="*70)
for item in items:
    if 'tata capital' in (item.get('sm_name','') or '').lower():
        print(f"  sm_name      : {item.get('sm_name')}")
        print(f"  desc         : {item.get('desc')}")
        print(f"  an_dt        : {item.get('an_dt')}")
        print(f"  attchmntText : {(item.get('attchmntText') or '')[:600]}")
        print(f"  attchmntFile : {item.get('attchmntFile','')[:100]}")
        print()

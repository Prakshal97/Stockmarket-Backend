"""Test BSE API and fix NSE field mapping."""
import requests, time, json
from datetime import datetime, timedelta

# ── BSE Test ──────────────────────────────────────────────────────────────────
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Origin': 'https://www.bseindia.com',
    'Referer': 'https://www.bseindia.com/corporates/ann.html',
})

try:
    session.get('https://www.bseindia.com', timeout=10)
    time.sleep(1)
except Exception as e:
    print(f"BSE warmup: {e}")

from_d = (datetime.now() - timedelta(days=5)).strftime('%Y%m%d')
to_d = datetime.now().strftime('%Y%m%d')
print(f"Date range: {from_d} -> {to_d}")

url = 'https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w'
params = {
    'strCat': '-1',
    'strPrevDate': from_d,
    'strScrip': '',
    'strSearch': 'P',
    'strToDate': to_d,
    'strType': 'C',
    'subcategory': '-1',
}

r = session.get(url, params=params, timeout=20)
print(f"BSE Status: {r.status_code}")
print(f"BSE Response length: {len(r.text)}")
print(f"BSE Response preview: {r.text[:500]}")

try:
    data = r.json()
    print(f"BSE JSON keys: {list(data.keys()) if isinstance(data, dict) else 'list'}")
    items = data.get('Table', []) if isinstance(data, dict) else data
    print(f"BSE items count: {len(items)}")
    if items:
        print(f"First item keys: {list(items[0].keys())}")
        print(f"First item: {json.dumps(items[0], indent=2)}")
except Exception as e:
    print(f"JSON parse error: {e}")

# ── NSE field mapping check ───────────────────────────────────────────────────
print("\n--- NSE FIELD MAPPING ---")
nse = requests.Session()
nse.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': 'application/json',
    'Referer': 'https://www.nseindia.com/companies-listing/corporate-filings-announcements',
})
nse.get('https://www.nseindia.com', timeout=10)
time.sleep(2)
from_n = (datetime.now() - timedelta(days=1)).strftime('%d-%m-%Y')
to_n = datetime.now().strftime('%d-%m-%Y')
r2 = nse.get(
    f'https://www.nseindia.com/api/corporate-announcements?index=equities&from_date={from_n}&to_date={to_n}',
    timeout=20
)
print(f"NSE status: {r2.status_code}, items: {len(r2.json()) if r2.status_code == 200 else 'N/A'}")
if r2.status_code == 200:
    items2 = r2.json()
    if items2:
        item = items2[0]
        print(f"NSE Fields: {list(item.keys())}")
        print(f"sm_name (company): {item.get('sm_name')}")
        print(f"symbol (ticker): {item.get('symbol')}")
        print(f"desc (subject): {item.get('desc')}")
        print(f"attchmntText (body): {item.get('attchmntText', '')[:100]}")
        print(f"an_dt (date): {item.get('an_dt')}  len={len(item.get('an_dt',''))}")
        print(f"seq_id: {item.get('seq_id')}")
        print(f"smIndustry (sector): {item.get('smIndustry')}")
        # Check auth capital items
        auth_items = [i for i in items2 if any(
            kw in (i.get('desc','') or '').lower()
            for kw in ['authorized capital','authorised capital','authorized share','authorised share','alteration','capital clause']
        )]
        print(f"\nAuth capital items (today): {len(auth_items)}")
        for ai in auth_items:
            print(f"  - {ai.get('sm_name')} | {ai.get('desc')}")

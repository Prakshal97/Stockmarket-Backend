"""Test alternative BSE API endpoints."""
import requests, time, json
from datetime import datetime, timedelta

session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
})

# Warmup with cookies
print("Warming BSE session...")
r0 = session.get('https://www.bseindia.com', timeout=15)
print(f"BSE homepage: {r0.status_code}, cookies: {dict(session.cookies)}")
time.sleep(1.5)

session.headers.update({
    'Origin': 'https://www.bseindia.com',
    'Referer': 'https://www.bseindia.com/corporates/ann.html',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
    'Sec-Fetch-Dest': 'empty',
})

from_d = (datetime.now() - timedelta(days=2)).strftime('%Y%m%d')
to_d = datetime.now().strftime('%Y%m%d')

# Method 1: Standard endpoint
print(f"\n=== Method 1: Standard BSE API ({from_d}->{to_d}) ===")
r1 = session.get(
    'https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w',
    params={'strCat': '-1', 'strPrevDate': from_d, 'strScrip': '', 'strSearch': 'P', 'strToDate': to_d, 'strType': 'C', 'subcategory': '-1'},
    timeout=20
)
print(f"Status: {r1.status_code}, Len: {len(r1.text)}, Body: {r1.text[:300]}")

# Method 2: Alternative BSE endpoint
print("\n=== Method 2: Alternative BSE AnnGetData ===")
r2 = session.get(
    'https://api.bseindia.com/BseIndiaAPI/api/AnnGetData/w',
    params={'Segments': 'Equity', 'Period': 'custom', 'frmdt': from_d, 'todt': to_d, 'Category': '-1', 'subcategory': '-1'},
    timeout=20
)
print(f"Status: {r2.status_code}, Len: {len(r2.text)}, Body: {r2.text[:300]}")

# Method 3: BSE Bulk query API
print("\n=== Method 3: BSE BulkQuery ===")
r3 = session.get(
    'https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w',
    params={'strCat': '13', 'strPrevDate': from_d, 'strScrip': '', 'strSearch': 'P', 'strToDate': to_d, 'strType': 'C', 'subcategory': '-1'},
    timeout=20
)
print(f"Status: {r3.status_code}, Len: {len(r3.text)}, Body: {r3.text[:300]}")

# Method 4: NSE search for auth capital
print("\n=== Method 4: NSE auth capital search (last 7 days) ===")
nse = requests.Session()
nse.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json',
    'Referer': 'https://www.nseindia.com/',
})
nse.get('https://www.nseindia.com', timeout=10)
time.sleep(2)
from_n = (datetime.now() - timedelta(days=7)).strftime('%d-%m-%Y')
to_n = datetime.now().strftime('%d-%m-%Y')
r4 = nse.get(f'https://www.nseindia.com/api/corporate-announcements?index=equities&from_date={from_n}&to_date={to_n}', timeout=20)
items4 = r4.json() if r4.status_code == 200 else []
print(f"NSE items (7 days): {len(items4)}")
auth_items = [i for i in items4 if any(
    kw in (i.get('desc','') or '').lower()
    for kw in ['authorized capital','authorised capital','authorized share','authorised share','alteration of capital','capital clause','increase in capital']
)]
print(f"Auth capital items found: {len(auth_items)}")
for ai in auth_items:
    print(f"  - {ai.get('sm_name')} | {ai.get('an_dt')} | {ai.get('desc')}")

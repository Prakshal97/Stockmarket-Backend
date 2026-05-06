"""Check NSE subjects related to capital and test BSE fixes."""
import requests, time, json
from datetime import datetime, timedelta
from collections import Counter

# ── NSE: Check what categories might be auth capital in disguise ──────────────
nse = requests.Session()
nse.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': 'application/json',
    'Referer': 'https://www.nseindia.com/',
})
nse.get('https://www.nseindia.com', timeout=10)
time.sleep(2)

from_n = (datetime.now() - timedelta(days=7)).strftime('%d-%m-%Y')
to_n = datetime.now().strftime('%d-%m-%Y')
r = nse.get(
    f'https://www.nseindia.com/api/corporate-announcements?index=equities&from_date={from_n}&to_date={to_n}',
    timeout=20
)
items = r.json() if r.status_code == 200 else []
print(f"NSE total items (7 days): {len(items)}")

# Print top 30 most frequent subjects
descs = Counter(item.get('desc','') for item in items)
print("\nTop 30 subjects:")
for desc, cnt in descs.most_common(30):
    print(f"  {cnt:4d}x  {desc}")

# Check for capital-related subjects
print("\nCapital-related subjects:")
capital_kws = ['capital', 'alteration', 'memorandum', 'increase', 'share', 'allot', 'ipo', 'egm', 'agm', 'right']
for item in items:
    desc = (item.get('desc') or '').lower()
    body = (item.get('attchmntText') or '').lower()
    if any(kw in desc for kw in ['capital', 'alteration', 'memorandum', 'increase in authorized', 'increase in authorised']):
        print(f"  [{item.get('an_dt','')[:10]}] {item.get('sm_name','')[:30]:30s} | {item.get('desc','')} ")

# ── BSE: Try with different headers ──────────────────────────────────────────
print("\n\n=== BSE Alternative Approaches ===")
bse = requests.Session()
bse.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9',
})
# Full browser warmup
r0 = bse.get('https://www.bseindia.com/corporates/ann.html', timeout=15)
print(f"BSE ann page: {r0.status_code}, cookies: {list(bse.cookies.keys())}")
time.sleep(2)

bse.headers.update({
    'Accept': 'application/json, text/plain, */*',
    'Origin': 'https://www.bseindia.com',
    'Referer': 'https://www.bseindia.com/corporates/ann.html',
    'X-Requested-With': 'XMLHttpRequest',
})

from_d = (datetime.now() - timedelta(days=2)).strftime('%Y%m%d')
to_d = datetime.now().strftime('%Y%m%d')

# Try strSearch=Q (quarterly?)
r1 = bse.get(
    'https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w',
    params={'strCat': '-1', 'strPrevDate': from_d, 'strScrip': '', 'strSearch': 'Q', 'strToDate': to_d, 'strType': 'C', 'subcategory': '-1'},
    timeout=20
)
print(f"BSE strSearch=Q: {r1.status_code}, len={len(r1.text)}, body={r1.text[:200]}")

# Try without strSearch
r2 = bse.get(
    'https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w',
    params={'strCat': '-1', 'strPrevDate': from_d, 'strScrip': '', 'strSearch': '', 'strToDate': to_d, 'strType': 'C', 'subcategory': '-1'},
    timeout=20
)
print(f"BSE no strSearch: {r2.status_code}, len={len(r2.text)}, body={r2.text[:200]}")

# Try DD/MM/YYYY date format
from_d2 = (datetime.now() - timedelta(days=2)).strftime('%d/%m/%Y')
to_d2 = datetime.now().strftime('%d/%m/%Y')
r3 = bse.get(
    'https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w',
    params={'strCat': '-1', 'strPrevDate': from_d2, 'strScrip': '', 'strSearch': 'P', 'strToDate': to_d2, 'strType': 'C', 'subcategory': '-1'},
    timeout=20
)
print(f"BSE DD/MM/YYYY: {r3.status_code}, len={len(r3.text)}, body={r3.text[:200]}")

# Try category 13 (Capital Structure) with strSearch=Q
r4 = bse.get(
    'https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w',
    params={'strCat': '13', 'strPrevDate': from_d, 'strScrip': '', 'strSearch': 'Q', 'strToDate': to_d, 'strType': 'C', 'subcategory': '-1'},
    timeout=20
)
print(f"BSE cat=13 strSearch=Q: {r4.status_code}, len={len(r4.text)}, body={r4.text[:300]}")

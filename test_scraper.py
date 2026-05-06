"""
Diagnostic test for NSE + BSE scraper.
Run from backend directory: .\\venv\\Scripts\\python.exe test_scraper.py
"""
import sys
import io
# Force UTF-8 output to avoid Windows cp1252 crashes
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

sys.path.insert(0, ".")

from agents.scraper_agent import (
    fetch_nse_announcements,
    fetch_bse_announcements,
    _fetch_bse_primary,
    _fetch_bse_fallback_search,
    _fetch_bse_getannouncements,
    _warm_nse_session,
)
from datetime import datetime, timedelta
from collections import Counter

from_date_nse = (datetime.now() - timedelta(days=3)).strftime("%d-%m-%Y")
to_date_nse = datetime.now().strftime("%d-%m-%Y")
from_date_bse = (datetime.now() - timedelta(days=3)).strftime("%Y%m%d")
to_date_bse = datetime.now().strftime("%Y%m%d")
from_ymd = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
to_ymd = datetime.now().strftime("%Y-%m-%d")
from_dmy = (datetime.now() - timedelta(days=3)).strftime("%d/%m/%Y")
to_dmy = datetime.now().strftime("%d/%m/%Y")

print(f"=== NSE Test ({from_date_nse} to {to_date_nse}) ===")
nse_items = fetch_nse_announcements(from_date_nse, to_date_nse)
print(f"NSE total fetched: {len(nse_items)}")

if nse_items:
    sample = nse_items[0]
    print(f"  Sample: company={sample.get('company_name')}")
    print(f"  subject={sample.get('raw_subject', '')[:70]}")
    print(f"  body_len={len(sample.get('raw_body', ''))}, pdf_url={str(sample.get('pdf_url', ''))[:80]}")

# Count by subject category
subject_counts = Counter(a["raw_subject"] for a in nse_items)
print("\nTop 10 subjects:")
for subj, cnt in subject_counts.most_common(10):
    print(f"  {cnt:4d}x  {subj[:70]}")

# Auth capital hits (items that have substantial body text from PDF pre-scan)
auth_hits = [a for a in nse_items if len(a.get("raw_body", "")) > 200]
print(f"\nItems with PDF body content (>200 chars): {len(auth_hits)}")
for a in auth_hits[:5]:
    print(f"  {a['company_name']}: {a['raw_subject'][:60]} | body={len(a['raw_body'])} chars")

# Check items mentioning authorized capital
auth_kw_hits = [a for a in nse_items if any(
    kw in (a.get("raw_body","") + a.get("raw_subject","")).lower()
    for kw in ["authorized capital", "authorised capital", "alteration of capital"]
)]
print(f"\nItems matching auth-capital keywords: {len(auth_kw_hits)}")
for a in auth_kw_hits:
    print(f"  >> {a['company_name']}: {a['raw_subject'][:60]}")

print(f"\n=== BSE Endpoint Diagnostics ===")
print(f"  [1] Testing primary endpoint (AnnSubCategoryGetData)...")
primary = _fetch_bse_primary(from_date_bse, to_date_bse)
print(f"      Result: {len(primary)} items")
if primary:
    print(f"      Sample keys: {list(primary[0].keys())[:8]}")
    print(f"      Sample subject: {primary[0].get('NEWSSUB','')[:70]}")

print(f"  [2] Testing fallback (getanndata, dates: {from_ymd} to {to_ymd})...")
fallback = _fetch_bse_fallback_search(from_ymd, to_ymd)
print(f"      Result: {len(fallback)} items")
if fallback:
    print(f"      Sample keys: {list(fallback[0].keys())[:8]}")

print(f"  [3] Testing third fallback (AnnGetData / category search)...")
third = _fetch_bse_getannouncements(from_dmy, to_dmy)
print(f"      Result: {len(third)} items")
if third:
    print(f"      Sample keys: {list(third[0].keys())[:8]}")

print(f"\n  Running full BSE fetch (with fallback chain)...")
bse_items = fetch_bse_announcements(from_date_bse, to_date_bse)
print(f"  BSE total fetched: {len(bse_items)}")
if bse_items:
    sample = bse_items[0]
    print(f"  Sample: company={sample.get('company_name')}")
    print(f"  subject={sample.get('raw_subject', '')[:70]}")

print("\n=== DONE ===")

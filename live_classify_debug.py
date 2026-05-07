"""
LIVE CLASSIFIER DIAGNOSTICS — Real NSE/BSE filings only.

Fetches the last 48h of live announcements from NSE and BSE,
scores every single one through the classifier, and produces:

  1. Score distribution (>40, >30, >20, >10, <=0)
  2. MAX score achieved
  3. Top-50 highest-scoring rejected candidates (score < threshold)
  4. All filings that PASSED threshold
  5. Raw text snippets of real filings to explain EXACTLY why scoring is failing

Run:  python live_classify_debug.py
"""
import sys
import os
import time
from datetime import datetime, timedelta
from collections import Counter

# ── Bootstrap path ────────────────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(__file__))

from agents.classifier import evaluate_authorized_capital, CONFIDENCE_THRESHOLD

# ── Fetch Live NSE Data ───────────────────────────────────────────────────────

def fetch_nse_live(days=2):
    import requests
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.nseindia.com/',
        'Origin': 'https://www.nseindia.com',
    })
    try:
        session.get('https://www.nseindia.com', timeout=15)
        time.sleep(2)
        session.get('https://www.nseindia.com/companies-listing/corporate-filings-announcements', timeout=15)
        time.sleep(1)
    except Exception as e:
        print(f"  [NSE warmup failed: {e}]")

    from_date = (datetime.now() - timedelta(days=days)).strftime('%d-%m-%Y')
    to_date   = datetime.now().strftime('%d-%m-%Y')
    url = f'https://www.nseindia.com/api/corporate-announcements?index=equities&from_date={from_date}&to_date={to_date}'
    print(f"  NSE URL: {url}")

    try:
        r = session.get(url, timeout=30)
        print(f"  NSE response: HTTP {r.status_code}, {len(r.text)} bytes")
        if r.status_code != 200:
            print(f"  NSE body: {r.text[:300]}")
            return []
        items = r.json()
        print(f"  NSE raw items: {len(items)}")
    except Exception as e:
        print(f"  NSE fetch error: {e}")
        return []

    announcements = []
    for item in items:
        ann = {
            "announcement_id": item.get("an_dt", "") + "_" + item.get("sm_isin", "") + "_NSE",
            "company_name":    item.get("sm_name", item.get("corp", "Unknown")),
            "ticker":          item.get("symbol", item.get("sm_symbol", "")),
            "exchange":        "NSE",
            "raw_subject":     item.get("desc", item.get("subject", "")),
            "raw_body":        item.get("attchmntText", item.get("body", "")),
            "title":           item.get("desc", item.get("subject", "")),
            "announcement_date": item.get("an_dt", item.get("date", "")),
            "pdf_url":         item.get("attchmntFile", ""),
        }
        announcements.append(ann)
    return announcements


def fetch_bse_live(days=2):
    import requests
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.bseindia.com/corporates/ann.html',
        'Origin': 'https://www.bseindia.com',
        'X-Requested-With': 'XMLHttpRequest',
    })
    try:
        session.get('https://www.bseindia.com', timeout=15)
        time.sleep(2)
        session.get('https://www.bseindia.com/corporates/ann.html', timeout=15)
        time.sleep(1)
    except Exception as e:
        print(f"  [BSE warmup failed: {e}]")

    from_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
    to_date   = datetime.now().strftime('%Y%m%d')
    url = 'https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w'
    params = {
        'strCat': '-1', 'strPrevDate': from_date, 'strScrip': '',
        'strSearch': 'P', 'strToDate': to_date, 'strType': 'C', 'subcategory': '-1'
    }
    print(f"  BSE URL: {url}  params={params}")

    try:
        r = session.get(url, params=params, timeout=30)
        print(f"  BSE response: HTTP {r.status_code}, {len(r.text)} bytes")
        if r.status_code != 200:
            print(f"  BSE body: {r.text[:300]}")
            return []
        data = r.json()
        items = data.get('Table', data) if isinstance(data, dict) else data
        print(f"  BSE raw items: {len(items) if isinstance(items, list) else 'not a list'}")
        if not isinstance(items, list):
            print(f"  BSE data type: {type(data)}, keys: {list(data.keys()) if isinstance(data, dict) else 'N/A'}")
            return []
    except Exception as e:
        print(f"  BSE fetch error: {e}")
        return []

    announcements = []
    for item in items:
        subject = (item.get('NEWSSUB', '') or item.get('Headline', '') or
                   item.get('NEWS_SUB', '') or item.get('HEADLINE', '') or
                   item.get('subject', '') or '')
        body    = (item.get('ATTACHMENTNAME', '') or item.get('body', '') or '')
        company = (item.get('SLONGNAME', '') or item.get('COMPANYNAME', '') or
                   item.get('COMPANY_NAME', '') or item.get('company', 'Unknown'))
        ann = {
            "announcement_id": str(item.get('NEWSID', item.get('id', ''))) + "_BSE",
            "company_name":    company,
            "ticker":          item.get('SCRIP_CD', item.get('scrip', '')),
            "exchange":        "BSE",
            "raw_subject":     subject,
            "raw_body":        body,
            "title":           subject,
            "announcement_date": item.get('NEWS_DT', item.get('date', '')),
            "pdf_url":         item.get('ATTACHMENTNAME', ''),
        }
        announcements.append(ann)
    return announcements


# ── Main diagnostic ───────────────────────────────────────────────────────────

def main():
    print("\n" + "="*70)
    print("LIVE CLASSIFIER DIAGNOSTICS — Real NSE/BSE Filings (Last 48h)")
    print("="*70)
    print(f"  Threshold: {CONFIDENCE_THRESHOLD}")
    print(f"  Run time : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # ── Fetch ─────────────────────────────────────────────────────────────────
    print("[1/4] Fetching NSE live filings...")
    nse = fetch_nse_live(days=2)
    print(f"      -> {len(nse)} NSE filings loaded\n")

    time.sleep(2)

    print("[2/4] Fetching BSE live filings...")
    bse = fetch_bse_live(days=2)
    print(f"      -> {len(bse)} BSE filings loaded\n")

    all_filings = nse + bse
    print(f"[3/4] Scoring {len(all_filings)} total real filings...\n")

    if not all_filings:
        print("ERROR: 0 filings fetched. NSE/BSE auth may be failing.")
        print("       Check internet connectivity and session warmup.")
        return

    # ── Score every filing ────────────────────────────────────────────────────
    scored = []
    passed = []
    for ann in all_filings:
        # Suppress per-filing print during bulk scoring
        import agents.classifier as clf_module
        old_print = __builtins__.__dict__.get('print') if hasattr(__builtins__, '__dict__') else None

        result = evaluate_authorized_capital(ann)
        entry = {
            "company":   ann.get("company_name", "Unknown"),
            "exchange":  ann.get("exchange", "?"),
            "subject":   ann.get("raw_subject", ""),
            "body_snippet": (ann.get("raw_body", "") or "")[:300],
            "score":     result["score"],
            "positives": result["matched_kws"],
            "negatives": result["rejected_kws"],
            "passed":    result["passed"],
            "reason":    result["reason"],
        }
        scored.append(entry)
        if result["passed"]:
            passed.append(entry)

    # ── Score distribution ─────────────────────────────────────────────────────
    all_scores = [e["score"] for e in scored]
    max_score  = max(all_scores) if all_scores else 0
    min_score  = min(all_scores) if all_scores else 0
    avg_score  = sum(all_scores) / len(all_scores) if all_scores else 0

    bucket_gt40 = sum(1 for s in all_scores if s > 40)
    bucket_gt30 = sum(1 for s in all_scores if s > 30)
    bucket_gt20 = sum(1 for s in all_scores if s > 20)
    bucket_gt10 = sum(1 for s in all_scores if s > 10)
    bucket_gt0  = sum(1 for s in all_scores if s > 0)
    bucket_eq0  = sum(1 for s in all_scores if s == 0)
    bucket_neg  = sum(1 for s in all_scores if s < 0)

    print("\n" + "="*70)
    print("SCORE DISTRIBUTION (Real Live Filings)")
    print("="*70)
    print(f"  Total filings scored   : {len(scored)}")
    print(f"  PASSED (score >= {CONFIDENCE_THRESHOLD})   : {len(passed)}")
    print(f"  FAILED (score <  {CONFIDENCE_THRESHOLD})   : {len(scored) - len(passed)}")
    print()
    print(f"  MAX score achieved     : {max_score}")
    print(f"  MIN score              : {min_score}")
    print(f"  AVG score              : {avg_score:.1f}")
    print()
    print(f"  Filings with score >40 : {bucket_gt40}  {'<-- these should be PASSING' if bucket_gt40 > 0 else '<-- NONE EXIST'}")
    print(f"  Filings with score >30 : {bucket_gt30}")
    print(f"  Filings with score >20 : {bucket_gt20}")
    print(f"  Filings with score >10 : {bucket_gt10}")
    print(f"  Filings with score  >0 : {bucket_gt0}")
    print(f"  Filings with score  =0 : {bucket_eq0}  (no keywords matched at all)")
    print(f"  Filings with score  <0 : {bucket_neg}  (penalties outweigh positives)")

    # ── Keyword appearance audit ───────────────────────────────────────────────
    print("\n" + "="*70)
    print("KEYWORD APPEARANCE IN REAL FILINGS (Raw Subject + Body)")
    print("="*70)
    key_phrases = [
        "increase in authorized capital",
        "increase in authorised capital",
        "increase in authorized share capital",
        "increase in authorised share capital",
        "alteration of capital clause",
        "alteration in capital clause",
        "alteration of capital",
        "capital clause",
        "memorandum of association",
        "postal ballot",
        "authorized capital",
        "authorised capital",
        "authorized share capital",
        "authorised share capital",
        "increase in share capital",
    ]
    for kw in key_phrases:
        count = sum(
            1 for e in scored
            if kw in (e["subject"] + " " + e["body_snippet"]).lower()
        )
        flag = " <<< FOUND IN REAL DATA" if count > 0 else ""
        print(f"  {count:4d}x  '{kw}'{flag}")

    # ── PASSED filings ─────────────────────────────────────────────────────────
    print("\n" + "="*70)
    print(f"PASSED FILINGS ({len(passed)} total)")
    print("="*70)
    if not passed:
        print("  !!! ZERO REAL FILINGS CROSSED THE THRESHOLD !!!")
        print("  This confirms the scoring model is miscalibrated for real exchange data.")
        print("  Investigate: real titles may use different phrasing than expected.")
    else:
        for i, e in enumerate(passed[:20], 1):
            print(f"\n  #{i:02d} [{e['exchange']}] {e['company']}")
            print(f"       Score   : {e['score']}")
            print(f"       Subject : {e['subject'][:100]}")
            print(f"       Positive: {e['positives']}")
            print(f"       Negative: {e['negatives']}")

    # ── Top-50 rejected candidates ─────────────────────────────────────────────
    rejected = [e for e in scored if not e["passed"]]
    rejected_sorted = sorted(rejected, key=lambda x: x["score"], reverse=True)
    top50 = rejected_sorted[:50]

    print("\n" + "="*70)
    print("TOP 50 HIGHEST-SCORING REJECTED REAL FILINGS")
    print("(Most likely over-rejected genuine candidates)")
    print("="*70)
    for i, e in enumerate(top50, 1):
        print(f"\n  #{i:02d} [{e['exchange']}] {e['company']}")
        print(f"       Score    : {e['score']}  (need >= {CONFIDENCE_THRESHOLD} to pass)")
        print(f"       Subject  : {e['subject'][:110]}")
        print(f"       Positives: {e['positives']}")
        print(f"       Negatives: {e['negatives']}")
        snippet = e["body_snippet"].replace("\n", " ").strip()
        if snippet:
            print(f"       Snippet  : {snippet[:150]}")
        print(f"       Reason   : {e['reason']}")

    # ── Score=0 sample — filings with NO keyword match at all ─────────────────
    zero_scored = [e for e in scored if e["score"] == 0]
    print("\n" + "="*70)
    print(f"SAMPLE OF SCORE=0 FILINGS (no keywords matched, first 15)")
    print("="*70)
    for e in zero_scored[:15]:
        print(f"  [{e['exchange']}] {e['company']:40s} | {e['subject'][:80]}")

    # ── Positive score sample — things that scored > 0 but < threshold ─────────
    non_zero_failed = [e for e in rejected if e["score"] > 0]
    non_zero_failed.sort(key=lambda x: x["score"], reverse=True)
    print("\n" + "="*70)
    print(f"FILINGS THAT SCORED >0 BUT FAILED THRESHOLD ({len(non_zero_failed)} total)")
    print("="*70)
    for e in non_zero_failed[:20]:
        print(f"\n  score={e['score']:+3d} [{e['exchange']}] {e['company']}")
        print(f"          Subject  : {e['subject'][:100]}")
        print(f"          Positives: {e['positives']}")
        print(f"          Negatives: {e['negatives']}")

    # ── Final verdict ──────────────────────────────────────────────────────────
    print("\n" + "="*70)
    print("DIAGNOSTIC VERDICT")
    print("="*70)
    if len(passed) == 0 and max_score == 0:
        print("  VERDICT: Zero filings scored above 0.")
        print("  CAUSE  : Real exchange titles/subjects do NOT contain the expected")
        print("           keyword phrases. The classifier keywords need to be")
        print("           calibrated to match the ACTUAL NSE/BSE subject format.")
        print("  ACTION : Inspect the 'score=0' samples above to see what real titles look like.")
    elif len(passed) == 0 and max_score > 0:
        print(f"  VERDICT: Real filings ARE scoring (max={max_score}) but not reaching threshold={CONFIDENCE_THRESHOLD}.")
        print(f"  CAUSE  : Threshold ({CONFIDENCE_THRESHOLD}) is too high OR penalty values are too aggressive.")
        print(f"  ACTION : Lower threshold or reduce penalty weights.")
    else:
        print(f"  VERDICT: {len(passed)} real filings are passing. Pipeline should be inserting them.")
    print("="*70 + "\n")


if __name__ == "__main__":
    main()

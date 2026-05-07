"""
Quick smoke test for the weighted scoring classifier (v2).
Run:  python test_scoring.py
"""
from agents.classifier import evaluate_authorized_capital, CONFIDENCE_THRESHOLD, get_top_rejected_candidates

print(f"=== Classifier v2 - Threshold: {CONFIDENCE_THRESHOLD} ===\n")

tests = [
    # Should PASS - pure title match (+50)
    {"company_name": "ABC Ltd", "exchange": "NSE",
     "raw_subject": "Increase in Authorized Capital", "raw_body": ""},

    # Should PASS - postal ballot + capital + MOA (+20+30)
    {"company_name": "XYZ Ltd", "exchange": "BSE",
     "raw_subject": "Postal ballot - Increase in Authorised Share Capital and MOA amendment",
     "raw_body": "postal ballot memorandum of association"},

    # Should PASS - board meeting outcome wrapper BUT capital is material (+50 -20)
    {"company_name": "PQR Ltd", "exchange": "NSE",
     "raw_subject": "Outcome of Board Meeting - Increase in Authorised Capital",
     "raw_body": "alteration of capital clause memorandum of association"},

    # Should PASS - body has strong capital language even though subject is generic (+30+40-20)
    {"company_name": "DEF Ltd", "exchange": "BSE",
     "raw_subject": "Outcome of Board Meeting",
     "raw_body": "increase in authorized share capital from 100 crore to 200 crore alteration of capital clause"},

    # Should FAIL - only financial results
    {"company_name": "GHI Ltd", "exchange": "NSE",
     "raw_subject": "Financial Results for Q3",
     "raw_body": "quarterly results revenue profit"},

    # Should FAIL - explicit negation
    {"company_name": "JKL Ltd", "exchange": "BSE",
     "raw_subject": "Board Meeting Outcome",
     "raw_body": "no change in authorized capital no alteration"},

    # Should PASS - postal ballot + capital clause (with dividend as noise)
    {"company_name": "MNO Ltd", "exchange": "NSE",
     "raw_subject": "Postal Ballot Notice - Alteration of Capital Clause",
     "raw_body": "increase in authorised share capital dividend bonus shares"},

    # Should PASS - MOA + capital (previously killed by board meeting penalty)
    {"company_name": "STU Ltd", "exchange": "BSE",
     "raw_subject": "Board Meeting: Increase in Authorised Share Capital",
     "raw_body": "memorandum of association alteration board of directors"},

    # Should FAIL - dividend-only mention, no capital language
    {"company_name": "VWX Ltd", "exchange": "NSE",
     "raw_subject": "Declaration of Interim Dividend",
     "raw_body": "dividend record date payment date"},
]

results = []
for t in tests:
    r = evaluate_authorized_capital(t)
    results.append((t, r))

print("\n=== SUMMARY ===")
for t, r in results:
    status = "PASS" if r["passed"] else "FAIL"
    print(f"  [{status}] score={r['score']:+3d}  {t['company_name']:8s} | {t['raw_subject'][:70]}")

print(f"\nRejected buffer size: {len(get_top_rejected_candidates())}")
print("\nTop rejected candidates:")
for c in get_top_rejected_candidates(5):
    print(f"  score={c['score']:+d}  {c['company']}  |  {c['subject'][:60]}")
    print(f"         pos={c['positives']}  neg={c['negatives']}")

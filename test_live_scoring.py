import asyncio
import statistics
from database import connect_db
from agents.scraper_agent import fetch_all_announcements
from agents.classifier import evaluate_authorized_capital

async def analyze_live_feed():
    print("Fetching LIVE NSE/BSE Announcements...")
    raw = await asyncio.to_thread(fetch_all_announcements)
    print(f"Fetched {len(raw)} announcements. Analyzing...")
    
    scored_announcements = []
    
    for ann in raw:
        res = evaluate_authorized_capital(ann)
        scored_announcements.append({
            "company": ann.get("company_name", "Unknown"),
            "subject": ann.get("raw_subject", ""),
            "score": res["score"],
            "matched_pos": res.get("matched_kws", []),
            "matched_neg": res.get("rejected_kws", []),
            "body_snippet": ann.get("raw_body", "")[:200].replace('\n', ' '),
            "reason": res.get("reason", "N/A"),
            "passed": res["passed"]
        })
        
    rejected = [s for s in scored_announcements if not s["passed"]]
    rejected.sort(key=lambda x: x["score"], reverse=True)
    
    top_50 = rejected[:50]
    
    print("\n" + "="*80)
    print("TOP 50 HIGHEST-SCORING REJECTED CANDIDATES")
    print("="*80)
    for i, c in enumerate(top_50, 1):
        print(f"\n{i}. {c['company']}")
        print(f"   Subject: {c['subject']}")
        print(f"   Score: {c['score']}")
        print(f"   Matched Positive: {c['matched_pos']}")
        print(f"   Matched Rejection: {c['matched_neg']}")
        print(f"   Reason: {c['reason']}")
        print(f"   Snippet: {c['body_snippet']}...")
        
    scores = [s["score"] for s in scored_announcements]
    max_score = max(scores) if scores else 0
    avg_score = statistics.mean(scores) if scores else 0
    over_40 = sum(1 for s in scores if s > 40)
    over_30 = sum(1 for s in scores if s > 30)
    over_20 = sum(1 for s in scores if s > 20)
    
    print("\n" + "="*80)
    print("SCORE DISTRIBUTION")
    print("="*80)
    print(f"Maximum Score: {max_score}")
    print(f"Average Score: {avg_score:.2f}")
    print(f"Scores > 40: {over_40}")
    print(f"Scores > 30: {over_30}")
    print(f"Scores > 20: {over_20}")

if __name__ == "__main__":
    asyncio.run(analyze_live_feed())

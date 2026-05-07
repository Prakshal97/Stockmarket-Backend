import asyncio
from agents.classifier import evaluate_authorized_capital

def test():
    dummy = {
        "raw_subject": "Increase in authorized capital from Rs 100 Cr to Rs 200 Cr",
        "raw_body": "The board has approved the alteration of capital clause of MOA.",
        "company_name": "TEST CORP",
        "exchange": "NSE",
        "announcement_date": "2026-05-07T10:00:00Z"
    }
    res = evaluate_authorized_capital(dummy)
    print("VERIFIED TEST:", res["category"] == "verified")
    
    dummy2 = {
        "raw_subject": "Notice of EGM",
        "raw_body": "Meeting to discuss various items.",
        "company_name": "TEST CORP 2",
        "exchange": "BSE",
        "announcement_date": "2026-05-07T10:00:00Z"
    }
    res2 = evaluate_authorized_capital(dummy2)
    print("GENERAL TEST:", res2["category"] == "general")
    
    dummy3 = {
        "raw_subject": "Notice of Postal Ballot",
        "raw_body": "Regarding alteration of capital clause.",
        "company_name": "TEST CORP 3",
        "exchange": "NSE",
        "announcement_date": "2026-05-07T10:00:00Z"
    }
    res3 = evaluate_authorized_capital(dummy3)
    print("VERIFIED TEST (Postal + Phrase):", res3["category"] == "verified")

    dummy4 = {
        "raw_subject": "Postal Ballot Notice",
        "raw_body": "General updates.",
        "company_name": "TEST CORP 4",
        "exchange": "BSE",
        "announcement_date": "2026-05-07T10:00:00Z"
    }
    res4 = evaluate_authorized_capital(dummy4)
    print("POSSIBLE TEST (Postal only):", res4["category"] == "possible")

if __name__ == "__main__":
    test()

import asyncio
from agents.classifier import is_pure_authorized_capital

def run_tests():
    test_cases = [
        {
            "company_name": "Aarvee Denims",
            "raw_subject": "Explicit MOA capital clause amendment",
            "raw_body": "The company proposes to alter the capital clause of the memorandum of association to increase authorized share capital.",
            "title": "MOA Amendment",
            "exchange": "NSE"
        },
        {
            "company_name": "Camlin Fine Sciences",
            "raw_subject": "Postal ballot / MOA amendment related filing",
            "raw_body": "Notice of postal ballot for seeking shareholder approval for amendment to moa regarding authorised share capital.",
            "title": "Postal Ballot",
            "exchange": "NSE"
        },
        {
            "company_name": "Trent",
            "raw_subject": "Postal ballot related to corporate approvals",
            "raw_body": "We are submitting the notice of postal ballot for the increase in authorised share capital of the company.",
            "title": "Postal Ballot Notice",
            "exchange": "NSE"
        },
        {
            "company_name": "R M Drip and Sprinklers Systems",
            "raw_subject": "Postal ballot with capital restructuring resolutions",
            "raw_body": "Approval for alteration in capital clause and increase in authorized capital via postal ballot.",
            "title": "Capital Restructuring",
            "exchange": "NSE"
        },
        {
            "company_name": "Brigade Enterprises",
            "raw_subject": "Amendment to MOA / increase in authorized capital",
            "raw_body": "Outcome of board meeting: financial results declared and increase in authorized capital.",
            "title": "Outcome of Board Meeting",
            "exchange": "NSE"
        }
    ]

    print("\n" + "="*50)
    print("FINAL VALIDATION REPORT")
    print("="*50)

    for i, test in enumerate(test_cases):
        print(f"\n--- Test Case {i+1}: {test['company_name']} ---")
        passed = is_pure_authorized_capital(test)
        # the debug logs will print automatically

if __name__ == "__main__":
    run_tests()

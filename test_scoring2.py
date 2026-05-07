from agents.classifier import evaluate_authorized_capital, CONFIDENCE_THRESHOLD

print(f'\nThreshold: {CONFIDENCE_THRESHOLD}')

# Simulate Tata Capital 'Notice of Postal Ballot' (Shareholders meeting)
r1 = evaluate_authorized_capital({
    'company_name': 'Tata Capital Limited',
    'exchange': 'NSE',
    'raw_subject': 'Shareholders meeting',
    'raw_body': 'Tata Capital Limited has informed the Exchange regarding Notice of Postal Ballot',
    'title': 'Shareholders meeting',
})
print(f'[Tata Capital - Shareholders meeting]')
print(f'  Score={r1["score"]}  PASS={r1["passed"]}  Positives={r1["matched_kws"]}')

# Simulate Tata Capital 'Copy of Newspaper Publication for Postal Ballot Notice'
r2 = evaluate_authorized_capital({
    'company_name': 'Tata Capital Limited',
    'exchange': 'NSE',
    'raw_subject': 'Copy of Newspaper Publication',
    'raw_body': 'Tata Capital Limited has informed the Exchange about Copy of Newspaper Publication for Postal Ballot Notice.',
    'title': 'Copy of Newspaper Publication',
})
print(f'[Tata Capital - Newspaper Publication]')
print(f'  Score={r2["score"]}  PASS={r2["passed"]}  Positives={r2["matched_kws"]}')

# Simulate Adani Total Gas scrutinizer report (should still fail - no PDF yet)
r3 = evaluate_authorized_capital({
    'company_name': 'Adani Total Gas Limited',
    'exchange': 'NSE',
    'raw_subject': 'Shareholders meeting',
    'raw_body': 'Adani Total Gas Limited has submitted the Exchange a copy Srutinizers report of Postal Ballot. Further, the company has informed the Exchange regarding voting results of Postal Ballot',
    'title': 'Shareholders meeting',
})
print(f'[Adani Total Gas - Scrutinizer report]')
print(f'  Score={r3["score"]}  PASS={r3["passed"]}  Positives={r3["matched_kws"]}')

# Simulate same Adani filing AFTER PDF fetch adds capital content
r4 = evaluate_authorized_capital({
    'company_name': 'Adani Total Gas Limited',
    'exchange': 'NSE',
    'raw_subject': 'Shareholders meeting',
    'raw_body': 'Adani Total Gas Limited has submitted the Exchange a copy Srutinizers report of Postal Ballot. Further, the company has informed the Exchange regarding voting results of Postal Ballot. RESOLUTION: Increase in Authorised Share Capital from Rs. 200 Crore to Rs. 500 Crore and consequent alteration of Memorandum of Association',
    'title': 'Shareholders meeting',
})
print(f'[Adani Total Gas - AFTER PDF content injection]')
print(f'  Score={r4["score"]}  PASS={r4["passed"]}  Positives={r4["matched_kws"]}')

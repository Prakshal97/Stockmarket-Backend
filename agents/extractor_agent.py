"""
AI Extractor Agent — Uses Google Gemini to extract structured financial data
from raw NSE/BSE announcement text.
"""
import os
import json
import re
from typing import Optional
import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)

# Use Gemini 1.5 Flash for speed and cost efficiency
model = genai.GenerativeModel("gemini-1.5-flash") if GEMINI_API_KEY else None

EXTRACTION_PROMPT = """
You are a financial analyst AI specializing in Indian stock markets (NSE/BSE).
Analyze the following corporate announcement and extract structured financial intelligence.

ANNOUNCEMENT:
Company: {company}
Ticker: {ticker}
Exchange: {exchange}
Subject: {subject}
Body: {body}

Extract ALL of the following and return a VALID JSON object (no markdown, no backticks, just raw JSON):

{{
  "company_name": "exact company name",
  "ticker": "NSE/BSE ticker symbol",
  "sector": "sector name (e.g., IT & Software, Banking, FMCG, Pharma, Auto, Energy, Metals, Textiles, Packaging, etc.)",
  "announcement_type": "one of: Financial Results | Dividend | Merger & Acquisition | Increase in Authorized Capital | Board Meeting | Order Win | Rights Issue | Buyback | Insider Trading | AGM/EGM | Share Allotment | Regulatory Filing | Other",
  "key_details": "2-3 sentence summary of what this announcement says in plain English",
  "revenue_profit_impact": "mention specific numbers if available: revenue, profit, deal value, dividend amount etc. or null",
  "sentiment": "Positive | Neutral | Negative",
  "impact_level": "High | Medium | Low",
  "ai_insight": "2-3 sentences of actionable insight for a retail investor. Should mention likely stock reaction, what to watch, and any risks.",
  "cmp": null,
  "market_cap_cr": null,
  "key_numbers": {{
    "value1_name": "value1",
    "value2_name": "value2"
  }},
  "authorized_capital": {{
    "board_approval": "Yes | No | null",
    "date_of_board_meeting": "DD-MM-YYYY or null",
    "existing_auth_eq_cap_inr": number_or_null,
    "new_auth_eq_cap_inr": number_or_null,
    "proposed_increase_inr": number_or_null
  }}
}}

RULES:
- For "Increase in Authorized Capital" type: ALWAYS fill the authorized_capital object carefully
- authorized_capital figures should be in INR (raw rupees, not crores)
- Return ONLY valid JSON, no explanation
- If data is unavailable, use null
- Impact: High = major financial event, deal >500Cr, quarterly results; Medium = moderate events; Low = routine filings
- Sentiment: Positive = good news for stock, Negative = bad news, Neutral = informational
"""


def extract_announcement(announcement: dict) -> Optional[dict]:
    """
    Use Gemini to extract structured data from a raw announcement.
    Returns the ai_data dict or None on failure.
    """
    if not model:
        print("⚠️ Gemini not configured — using mock extraction")
        return _mock_extraction(announcement)

    try:
        body_text = announcement.get("raw_body", "") or ""
        subject_text = announcement.get("raw_subject", "")

        # Combine subject and body, limit length
        full_text = f"{subject_text}\n\n{body_text}"[:3000]

        prompt = EXTRACTION_PROMPT.format(
            company=announcement.get("company_name", "Unknown"),
            ticker=announcement.get("ticker", ""),
            exchange=announcement.get("exchange", "NSE"),
            subject=subject_text,
            body=body_text[:2000],
        )

        response = model.generate_content(
            prompt,
            generation_config=genai.types.GenerationConfig(
                temperature=0.1,
                max_output_tokens=1024,
            )
        )

        raw_text = response.text.strip()

        # Clean up any markdown code blocks
        raw_text = re.sub(r"```json\s*", "", raw_text)
        raw_text = re.sub(r"```\s*", "", raw_text)
        raw_text = raw_text.strip()

        # Extract JSON if wrapped in extra text
        json_match = re.search(r'\{.*\}', raw_text, re.DOTALL)
        if json_match:
            raw_text = json_match.group(0)

        ai_data = json.loads(raw_text)

        # Validate required fields
        if not ai_data.get("announcement_type"):
            ai_data["announcement_type"] = "Other"
        if not ai_data.get("sentiment"):
            ai_data["sentiment"] = "Neutral"
        if not ai_data.get("impact_level"):
            ai_data["impact_level"] = "Low"

        print(f"✅ AI extracted: {ai_data.get('company_name')} — {ai_data.get('announcement_type')} ({ai_data.get('sentiment')})")
        return ai_data

    except json.JSONDecodeError as e:
        print(f"❌ JSON parse error for {announcement.get('company_name')}: {e}")
        return _mock_extraction(announcement)
    except Exception as e:
        print(f"❌ Gemini extraction error for {announcement.get('company_name')}: {e}")
        return _mock_extraction(announcement)


def _mock_extraction(announcement: dict) -> dict:
    """
    Fallback extraction when Gemini is not available.
    Uses simple keyword matching for basic categorization.
    """
    subject = announcement.get("raw_subject", "").lower()
    body = announcement.get("raw_body", "").lower()
    text = subject + " " + body

    # Determine type
    if any(k in text for k in ["authorized capital", "authorised capital", "increase in capital"]):
        ann_type = "Increase in Authorized Capital"
    elif any(k in text for k in ["financial results", "quarterly results", "q4", "q3", "q2", "q1", "annual results"]):
        ann_type = "Financial Results"
    elif any(k in text for k in ["dividend", "final dividend", "interim dividend"]):
        ann_type = "Dividend"
    elif any(k in text for k in ["merger", "acquisition", "amalgamation", "takeover"]):
        ann_type = "Merger & Acquisition"
    elif any(k in text for k in ["order", "contract", "deal", "award"]):
        ann_type = "Order Win"
    elif any(k in text for k in ["buyback", "buy-back", "repurchase"]):
        ann_type = "Buyback"
    elif any(k in text for k in ["rights issue", "rights shares"]):
        ann_type = "Rights Issue"
    elif any(k in text for k in ["board meeting", "board of directors"]):
        ann_type = "Board Meeting"
    elif any(k in text for k in ["agm", "egm", "annual general", "extraordinary general"]):
        ann_type = "AGM/EGM"
    else:
        ann_type = "Other"

    # Determine sentiment
    if any(k in text for k in ["profit", "revenue up", "increase", "growth", "win", "order", "dividend", "buyback"]):
        sentiment = "Positive"
    elif any(k in text for k in ["loss", "decline", "below expectations", "penalty", "regulatory"]):
        sentiment = "Negative"
    else:
        sentiment = "Neutral"

    # Impact
    if any(k in text for k in ["q4", "q3", "q2", "q1", "annual", "merger", "acquisition", "1000 cr", "billion"]):
        impact = "High"
    elif any(k in text for k in ["dividend", "order", "buyback", "rights"]):
        impact = "Medium"
    else:
        impact = "Low"

    return {
        "company_name": announcement.get("company_name", "Unknown"),
        "ticker": announcement.get("ticker", ""),
        "sector": "General",
        "announcement_type": ann_type,
        "key_details": announcement.get("raw_subject", ""),
        "revenue_profit_impact": None,
        "sentiment": sentiment,
        "impact_level": impact,
        "ai_insight": f"This is a {ann_type.lower()} announcement from {announcement.get('company_name', 'the company')}. Monitor closely for any price movement. Review full details before making any investment decision.",
        "cmp": None,
        "market_cap_cr": None,
        "key_numbers": {},
        "authorized_capital": {
            "board_approval": None,
            "date_of_board_meeting": None,
            "existing_auth_eq_cap_inr": None,
            "new_auth_eq_cap_inr": None,
            "proposed_increase_inr": None,
        }
    }

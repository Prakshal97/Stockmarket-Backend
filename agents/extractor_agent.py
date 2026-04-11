"""
AI Extractor Agent — Uses Groq (LLaMA 3 70B) to extract structured financial data
from raw NSE/BSE announcement text.
"""
import os
import json
import re
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

GROQ_API_KEY = os.getenv("GROQ_API_KEY")

# Initialize Groq client
groq_client = None
if GROQ_API_KEY:
    try:
        from groq import Groq
        groq_client = Groq(api_key=GROQ_API_KEY)
        print("✅ Groq AI client initialized (llama3-70b-8192)")
    except ImportError:
        print("⚠️ Groq package not installed — run: pip install groq")
    except Exception as e:
        print(f"⚠️ Groq init error: {e}")

GROQ_MODEL = "llama-3.3-70b-versatile"

EXTRACTION_PROMPT = """You are a financial analyst AI specializing in Indian stock markets (NSE/BSE).
Analyze the following corporate announcement and extract structured financial intelligence.

ANNOUNCEMENT:
Company: {company}
Ticker: {ticker}
Exchange: {exchange}
Subject: {subject}
Body: {body}

You MUST return ONLY a valid JSON object with these exact keys. No markdown, no backticks, no explanation — just raw JSON:

{{
  "company_name": "exact company name",
  "ticker": "NSE/BSE ticker symbol",
  "sector": "sector name (e.g., IT & Software, Banking, FMCG, Pharma, Auto, Energy, Metals, Textiles, Packaging, etc.)",
  "announcement_type": "one of: Financial Results | Dividend | Merger & Acquisition | Increase in Authorized Capital | Board Meeting | Order Win | Rights Issue | Buyback | Insider Trading | AGM/EGM | Share Allotment | Regulatory Filing | Other",
  "title": "short 1-3 word heading like BUYBACK, FINANCIAL RESULTS, DIVIDEND, ORDER WIN, BOARD MEETING, etc.",
  "description": "clean 2-3 sentence summary of the announcement in plain English",
  "key_details": "2-3 sentence summary for investors",
  "revenue_profit_impact": "mention specific numbers if available: revenue, profit, deal value, dividend amount etc. or Not Available",
  "sentiment": "POSITIVE | NEGATIVE | NEUTRAL",
  "impact": "HIGH | MEDIUM | LOW",
  "board_approval": "Yes | No | Not Available",
  "meeting_date": "DD Month YYYY format or Not Available",
  "ai_insight": "2-3 sentences of actionable insight for a retail investor. Should mention likely stock reaction, what to watch, and any risks.",
  "trading_signal": "emoji + signal like 🚀 Strong Bullish, 📈 Bullish, 🟢 Mildly Positive, 🔴 Strong Bearish, 📉 Bearish, ⚖️ Neutral",
  "cmp": null,
  "market_cap_cr": null,
  "authorized_capital": {{
    "board_approval": "Yes | No | Not Available",
    "date_of_board_meeting": "DD-MM-YYYY or Not Available",
    "existing_auth_eq_cap_inr": null,
    "new_auth_eq_cap_inr": null,
    "proposed_increase_inr": null
  }}
}}

RULES:
- For "Increase in Authorized Capital" type: ALWAYS fill the authorized_capital object carefully
- authorized_capital figures should be in INR (raw rupees, not crores)
- Return ONLY valid JSON, no explanation, no markdown
- If data is unavailable, use "Not Available" for strings and null for numbers
- Impact: HIGH = major financial event, deal >500Cr, quarterly results; MEDIUM = moderate events; LOW = routine filings
- Sentiment: POSITIVE = good news for stock, NEGATIVE = bad news, NEUTRAL = informational
- title should be SHORT: e.g. "BUYBACK", "Q4 RESULTS", "DIVIDEND", "ORDER WIN"
- board_approval and meeting_date MUST always be present (use "Not Available" if unknown)
"""


def _sanitize_field(value, default="Not Available"):
    """Ensure no field is ever empty, null, or blank."""
    if value is None:
        return default
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped or stripped.lower() in ("null", "none", "n/a", "na", ""):
            return default
        return stripped
    return value


def _sanitize_ai_data(ai_data: dict) -> dict:
    """Ensure ALL string fields have values — never empty."""
    string_fields = [
        "company_name", "ticker", "sector", "announcement_type",
        "title", "description", "key_details", "revenue_profit_impact",
        "sentiment", "impact", "board_approval", "meeting_date",
        "ai_insight", "trading_signal"
    ]
    for field in string_fields:
        ai_data[field] = _sanitize_field(ai_data.get(field))

    # Normalize sentiment/impact casing
    sentiment = ai_data.get("sentiment", "NEUTRAL").upper()
    if sentiment in ("POSITIVE", "NEGATIVE", "NEUTRAL"):
        ai_data["sentiment"] = sentiment.capitalize()
    else:
        ai_data["sentiment"] = "Neutral"

    impact = ai_data.get("impact", "LOW").upper()
    if impact in ("HIGH", "MEDIUM", "LOW"):
        ai_data["impact"] = impact.capitalize()
    else:
        ai_data["impact"] = "Low"

    # Map impact to impact_level for backward compatibility
    ai_data["impact_level"] = ai_data["impact"]

    return ai_data


def extract_announcement(announcement: dict) -> Optional[dict]:
    """
    Use Groq (LLaMA 3 70B) to extract structured data from a raw announcement.
    Returns the ai_data dict or None on failure.
    """
    if not groq_client:
        print("⚠️ Groq not configured — using mock extraction")
        return _mock_extraction(announcement)

    try:
        body_text = announcement.get("raw_body", "") or ""
        subject_text = announcement.get("raw_subject", "")

        prompt = EXTRACTION_PROMPT.format(
            company=announcement.get("company_name", "Unknown"),
            ticker=announcement.get("ticker", ""),
            exchange=announcement.get("exchange", "NSE"),
            subject=subject_text,
            body=body_text[:2000],
        )

        response = groq_client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": "You are a financial analyst AI. You MUST respond with ONLY valid JSON. No markdown, no explanation, no backticks."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            temperature=0.3,
            max_tokens=1024,
        )

        raw_text = response.choices[0].message.content.strip()

        # Clean up any markdown code blocks
        raw_text = re.sub(r"```json\s*", "", raw_text)
        raw_text = re.sub(r"```\s*", "", raw_text)
        raw_text = raw_text.strip()

        # Extract JSON if wrapped in extra text
        json_match = re.search(r'\{.*\}', raw_text, re.DOTALL)
        if json_match:
            raw_text = json_match.group(0)

        ai_data = json.loads(raw_text)

        # Sanitize all fields
        ai_data = _sanitize_ai_data(ai_data)

        # Validate required fields
        if not ai_data.get("announcement_type") or ai_data["announcement_type"] == "Not Available":
            ai_data["announcement_type"] = "Other"

        print(f"✅ Groq extracted: {ai_data.get('company_name')} — {ai_data.get('title')} ({ai_data.get('sentiment')})")
        return ai_data

    except json.JSONDecodeError as e:
        print(f"❌ JSON parse error for {announcement.get('company_name')}: {e}")
        return _mock_extraction(announcement)
    except Exception as e:
        if "decommissioned" in str(e).lower() or "not found" in str(e).lower():
            print(f"⚠️ Groq model {GROQ_MODEL} failed. Switching to llama-3.1-8b-instant...")
            # Retry with fallback model
            response = groq_client.chat.completions.create(
                model="llama-3.1-8b-instant",
                messages=[
                    {"role": "system", "content": "You are a financial analyst AI. You MUST respond with ONLY valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=1024,
            )
            raw_text = response.choices[0].message.content.strip()
            raw_text = re.sub(r"```json\s*", "", raw_text)
            raw_text = re.sub(r"```\s*", "", raw_text)
            raw_text = raw_text.strip()
            json_match = re.search(r'\{.*\}', raw_text, re.DOTALL)
            if json_match:
                raw_text = json_match.group(0)
            ai_data = json.loads(raw_text)
            ai_data = _sanitize_ai_data(ai_data)
            if not ai_data.get("announcement_type") or ai_data["announcement_type"] == "Not Available":
                ai_data["announcement_type"] = "Other"
            print(f"✅ Groq fallback extracted: {ai_data.get('company_name')} — {ai_data.get('title')} ({ai_data.get('sentiment')})")
            return ai_data
            
        print(f"❌ Groq extraction error for {announcement.get('company_name')}: {e}")
        return _mock_extraction(announcement)


def _mock_extraction(announcement: dict) -> dict:
    """
    Fallback extraction when Groq is not available.
    Uses simple keyword matching for basic categorization.
    """
    subject = announcement.get("raw_subject", "").lower()
    body = announcement.get("raw_body", "").lower()
    text = subject + " " + body
    company_name = announcement.get("company_name", "Unknown Company")

    # Determine type & title
    if any(k in text for k in ["authorized capital", "authorised capital", "increase in capital"]):
        ann_type = "Increase in Authorized Capital"
        title = "AUTHORIZED CAPITAL"
    elif any(k in text for k in ["financial results", "quarterly results", "q4", "q3", "q2", "q1", "annual results"]):
        ann_type = "Financial Results"
        title = "FINANCIAL RESULTS"
    elif any(k in text for k in ["dividend", "final dividend", "interim dividend"]):
        ann_type = "Dividend"
        title = "DIVIDEND"
    elif any(k in text for k in ["merger", "acquisition", "amalgamation", "takeover"]):
        ann_type = "Merger & Acquisition"
        title = "M&A"
    elif any(k in text for k in ["order", "contract", "deal", "award"]):
        ann_type = "Order Win"
        title = "ORDER WIN"
    elif any(k in text for k in ["buyback", "buy-back", "repurchase"]):
        ann_type = "Buyback"
        title = "BUYBACK"
    elif any(k in text for k in ["rights issue", "rights shares"]):
        ann_type = "Rights Issue"
        title = "RIGHTS ISSUE"
    elif any(k in text for k in ["board meeting", "board of directors"]):
        ann_type = "Board Meeting"
        title = "BOARD MEETING"
    elif any(k in text for k in ["agm", "egm", "annual general", "extraordinary general"]):
        ann_type = "AGM/EGM"
        title = "AGM/EGM"
    elif any(k in text for k in ["allotment", "share allotment", "preferential"]):
        ann_type = "Share Allotment"
        title = "SHARE ALLOTMENT"
    else:
        ann_type = "Other"
        title = "ANNOUNCEMENT"

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

    # Extract board approval
    board_approval = "Not Available"
    if "board approved" in text or "board approves" in text or "approved by the board" in text:
        board_approval = "Yes"

    # Extract meeting date
    meeting_date = "Not Available"
    import re
    date_match = re.search(
        r'(\d{1,2}(?:st|nd|rd|th)?\s*[-/]?\s*(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*[-/,\s]*\d{4})',
        text, re.IGNORECASE
    )
    if date_match:
        meeting_date = date_match.group(1).strip()
    else:
        date_match2 = re.search(r'(\d{2}[-/]\d{2}[-/]\d{4})', text)
        if date_match2:
            meeting_date = date_match2.group(1)

    # Build description
    raw_subject = announcement.get("raw_subject", "Corporate announcement")
    description = f"{company_name} has made a {ann_type.lower()} announcement. {raw_subject}"

    return {
        "company_name": company_name,
        "ticker": announcement.get("ticker", ""),
        "sector": "General",
        "announcement_type": ann_type,
        "title": title,
        "description": description,
        "key_details": raw_subject,
        "revenue_profit_impact": "Not Available",
        "sentiment": sentiment,
        "impact": impact,
        "impact_level": impact,
        "board_approval": board_approval,
        "meeting_date": meeting_date,
        "ai_insight": f"This is a {ann_type.lower()} announcement from {company_name}. Monitor closely for any price movement. Review full details before making any investment decision.",
        "trading_signal": "⚖️ Neutral",
        "cmp": None,
        "market_cap_cr": None,
        "key_numbers": {},
        "authorized_capital": {
            "board_approval": board_approval,
            "date_of_board_meeting": meeting_date if meeting_date != "Not Available" else "Not Available",
            "existing_auth_eq_cap_inr": None,
            "new_auth_eq_cap_inr": None,
            "proposed_increase_inr": None,
        }
    }

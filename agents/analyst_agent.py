"""
Analyst Agent — Scores, classifies and enriches announcement data.
Combines rule-based scoring with AI insights for final analysis.
"""
from typing import Dict, Optional
from datetime import datetime


# Impact scoring rules
IMPACT_KEYWORDS = {
    "High": [
        "quarterly results", "annual results", "q4", "q3", "q2", "q1",
        "merger", "acquisition", "amalgamation", "billion", "1000 cr",
        "2000 cr", "3000 cr", "major order", "strategic partnership",
        "demerger", "delisting", "open offer", "block deal"
    ],
    "Medium": [
        "dividend", "buyback", "rights issue", "order win", "contract",
        "authorized capital", "preferential allotment", "qip", "ipo",
        "board meeting", "debt restructuring", "rating upgrade", "rating downgrade"
    ],
    "Low": [
        "agm", "egm", "compliance", "regulatory", "disclosure",
        "change in director", "share transfer", "insider trading disclosure"
    ]
}

SENTIMENT_BOOST = {
    "Positive": [
        "profit up", "revenue up", "revenue growth", "profit growth", "order win",
        "new contract", "dividend declared", "buyback", "upgrade rating",
        "record revenue", "record profit", "beat estimates", "strong results",
        "expansion", "new plant", "capacity addition"
    ],
    "Negative": [
        "loss", "net loss", "decline", "revenue down", "profit down",
        "below estimates", "downgrade", "penalty", "sebi notice", "regulatory action",
        "fraud", "investigation", "write-off", "impairment", "default"
    ]
}

SECTOR_MAP = {
    "IT & Software": ["tcs", "infosys", "wipro", "hcl", "tech mahindra", "mphasis", "ltimindtree"],
    "Banking": ["hdfc bank", "icici bank", "sbi", "axis bank", "kotak", "indusind", "pnb", "bank of baroda"],
    "FMCG": ["hindustan unilever", "hul", "itc", "nestle", "dabur", "marico", "britannia", "godrej consumer"],
    "Pharma": ["sun pharma", "dr reddy", "cipla", "divi's", "lupin", "aurobindo", "biocon"],
    "Energy": ["reliance", "ongc", "bpcl", "iocl", "hpcl", "coal india", "ntpc", "power grid", "adani green"],
    "Auto": ["maruti", "tata motors", "m&m", "mahindra", "hero motocorp", "bajaj auto", "tvs motor", "ashok leyland"],
    "Metals": ["tata steel", "jsw steel", "hindalco", "vedanta", "coal india", "nmdc", "sail"],
    "Real Estate": ["dlf", "godrej properties", "prestige", "brigade", "oberoi realty"],
    "Telecom": ["airtel", "jio", "vodafone", "vi", "indus towers"],
    "Finance": ["bajaj finance", "muthoot", "l&t finance", "cholamandalam", "shriram finance"],
}


def enrich_announcement(announcement: dict, ai_data: dict) -> dict:
    """
    Enrich AI-extracted data with rule-based scoring and additional insight.
    Returns the final merged analysis dict.
    """
    subject = announcement.get("raw_subject", "").lower()
    body = announcement.get("raw_body", "").lower()
    text = subject + " " + body
    company = announcement.get("company_name", "").lower()

    # Override sector if we can detect it
    detected_sector = _detect_sector(company, text)
    if detected_sector and not ai_data.get("sector"):
        ai_data["sector"] = detected_sector

    # Revalidate impact level using rules (AI sometimes gets this wrong)
    rule_impact = _rule_based_impact(text)
    ai_impact = ai_data.get("impact_level", "Low")

    # Take the higher of the two (AI or rule)
    impact_order = ["Low", "Medium", "High"]
    final_impact = impact_order[max(
        impact_order.index(rule_impact) if rule_impact in impact_order else 0,
        impact_order.index(ai_impact) if ai_impact in impact_order else 0
    )]
    ai_data["impact_level"] = final_impact

    # Revalidate sentiment
    rule_sentiment = _rule_based_sentiment(text)
    ai_sentiment = ai_data.get("sentiment", "Neutral")
    # Trust AI more for sentiment, but flag if very mismatched
    if rule_sentiment == "Negative" and ai_sentiment == "Positive":
        ai_data["sentiment"] = "Neutral"  # Conservative approach
    elif rule_sentiment != ai_sentiment and rule_sentiment != "Neutral":
        pass  # Let AI decide

    # Ensure authorized_capital is correct if keywords are present
    if "authorized capital" in text or "authorised capital" in text or "authorized share capital" in text or "authorised share capital" in text:
        extracted_auth = _extract_auth_capital_from_text(text)
        existing_auth = ai_data.get("authorized_capital") or {}
        
        merged_auth = {}
        for key in ["board_approval", "date_of_board_meeting", "existing_auth_eq_cap_inr", "new_auth_eq_cap_inr", "proposed_increase_inr"]:
            val = extracted_auth.get(key)
            if val is None or val == "Not Available":
                val = existing_auth.get(key)
            merged_auth[key] = val
            
        # If we found actual amounts or if it's already this type, ensure the type is set correctly
        if merged_auth.get("new_auth_eq_cap_inr") or ai_data.get("announcement_type") == "Increase in Authorized Capital":
            ai_data["announcement_type"] = "Increase in Authorized Capital"
            ai_data["authorized_capital"] = merged_auth

    # Add trading signals
    ai_data["trading_signal"] = _generate_trading_signal(ai_data)

    return ai_data


def _detect_sector(company: str, text: str) -> Optional[str]:
    """Simple keyword-based sector detection."""
    for sector, keywords in SECTOR_MAP.items():
        for kw in keywords:
            if kw in company or kw in text:
                return sector
    return None


def _rule_based_impact(text: str) -> str:
    """Determine impact level using keyword rules."""
    for level in ["High", "Medium", "Low"]:
        for kw in IMPACT_KEYWORDS[level]:
            if kw in text:
                return level
    return "Low"


def _rule_based_sentiment(text: str) -> str:
    """Determine sentiment using keyword rules."""
    positive_score = sum(1 for kw in SENTIMENT_BOOST["Positive"] if kw in text)
    negative_score = sum(1 for kw in SENTIMENT_BOOST["Negative"] if kw in text)

    if positive_score > negative_score:
        return "Positive"
    elif negative_score > positive_score:
        return "Negative"
    return "Neutral"


def _extract_auth_capital_from_text(text: str) -> dict:
    """
    Extract authorized capital figures from raw text using context-aware regex.
    Handles patterns like:
      - "from Rs 100 Crore to Rs 200 Crore"
      - "Existing: Rs 5,75,00,000  New: Rs 10,00,00,000"
      - "increase of Rs 50 Crore"
    """
    import re

    auth_cap = {
        "board_approval": None,
        "date_of_board_meeting": None,
        "existing_auth_eq_cap_inr": None,
        "new_auth_eq_cap_inr": None,
        "proposed_increase_inr": None,
    }

    t = text.lower()

    # ── Board approval ───────────────────────────────────────────
    if any(p in t for p in ["board approved", "board approves", "approved by the board", "board of directors approved"]):
        auth_cap["board_approval"] = "Yes"
    elif any(p in t for p in ["shareholders approval", "postal ballot", "subject to approval"]):
        auth_cap["board_approval"] = "Pending Shareholder Approval"

    # ── Date of board meeting ────────────────────────────────────
    date_patterns = [
        r'\b(\d{2}[-/]\d{2}[-/]\d{4})\b',
        r'\b(\d{1,2}(?:st|nd|rd|th)?\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*,?\s+\d{4})\b',
        r'\b(\d{1,2}[-/]\d{1,2}[-/]\d{4})\b',
    ]
    for pat in date_patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            auth_cap["date_of_board_meeting"] = m.group(1)
            break

    # ── Helper: parse a number string (crore or raw INR) ─────────
    def parse_amount(s: str) -> float:
        """Convert text like '100 Crore', '1,00,00,000', '5.75 Cr' → INR float."""
        s = s.strip().replace(",", "")
        crore_match = re.search(r'([\d.]+)\s*(?:crore|cr\.?)\b', s, re.IGNORECASE)
        if crore_match:
            return float(crore_match.group(1)) * 1_00_00_000
        lakh_match = re.search(r'([\d.]+)\s*(?:lakh|lac)\b', s, re.IGNORECASE)
        if lakh_match:
            return float(lakh_match.group(1)) * 1_00_000
        # Raw number (could be INR directly, e.g. 57,50,00,000 → already stripped commas)
        num_match = re.search(r'([\d.]+)', s)
        if num_match:
            return float(num_match.group(1))
        return 0.0

    # ── Pattern 1: "from Rs X to Rs Y" ──────────────────────────
    from_to = re.search(
        r'from\s+(?:rs\.?|inr\.?)?\s*([\d,]+(?:\.\d+)?(?:\s*(?:crore|cr\.?|lakh|lac))?)'
        r'\s+to\s+(?:rs\.?|inr\.?)?\s*([\d,]+(?:\.\d+)?(?:\s*(?:crore|cr\.?|lakh|lac))?)',
        text, re.IGNORECASE
    )
    if from_to:
        existing = parse_amount(from_to.group(1))
        new = parse_amount(from_to.group(2))
        if existing > 0 and new > 0 and new != existing:
            auth_cap["existing_auth_eq_cap_inr"] = existing
            auth_cap["new_auth_eq_cap_inr"] = new
            auth_cap["proposed_increase_inr"] = new - existing

    # ── Pattern 2: "increase of Rs X" (when we only have delta) ──
    if auth_cap["proposed_increase_inr"] is None:
        increase_match = re.search(
            r'(?:increase\s+of|by\s+(?:rs\.?|inr\.?))\s*([\d,]+(?:\.\d+)?(?:\s*(?:crore|cr\.?|lakh|lac))?)',
            text, re.IGNORECASE
        )
        if increase_match:
            auth_cap["proposed_increase_inr"] = parse_amount(increase_match.group(1))

    # ── Pattern 3: "existing capital: X" / "new/revised capital: Y" ──
    if auth_cap["existing_auth_eq_cap_inr"] is None:
        existing_patterns = [
            r'(?:existing|present|current)\s+(?:authorized|authorised)?\s*(?:share\s+)?capital[:\s]+(?:rs\.?|inr\.?)?\s*([\d,]+(?:\.\d+)?(?:\s*(?:crore|cr\.?|lakh|lac))?)',
            r'(?:from)\s+(?:rs\.?|inr\.?)?\s*([\d,]+(?:\.\d+)?(?:\s*(?:crore|cr\.?|lakh|lac))?)',
        ]
        for pat in existing_patterns:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                val = parse_amount(m.group(1))
                if val > 0:
                    auth_cap["existing_auth_eq_cap_inr"] = val
                    break

    if auth_cap["new_auth_eq_cap_inr"] is None:
        new_patterns = [
            r'(?:new|revised|increased|proposed)\s+(?:authorized|authorised)?\s*(?:share\s+)?capital[:\s]+(?:rs\.?|inr\.?)?\s*([\d,]+(?:\.\d+)?(?:\s*(?:crore|cr\.?|lakh|lac))?)',
            r'(?:to)\s+(?:rs\.?|inr\.?)?\s*([\d,]+(?:\.\d+)?(?:\s*(?:crore|cr\.?|lakh|lac))?)',
        ]
        for pat in new_patterns:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                val = parse_amount(m.group(1))
                if val > 0:
                    auth_cap["new_auth_eq_cap_inr"] = val
                    break

    # ── Pattern 4: Table-style rows like "57,50,00,000 | 1,00,00,00,000" ──
    # (pdfplumber table rows joined with " | ")
    if auth_cap["existing_auth_eq_cap_inr"] is None:
        # Look for two large raw INR numbers side by side
        big_nums = re.findall(r'\b(\d{1,3}(?:,\d{2,3}){3,})\b', text)
        if len(big_nums) >= 2:
            vals = []
            for n in big_nums:
                try:
                    vals.append(float(n.replace(",", "")))
                except:
                    pass
            vals = sorted(set(vals))
            if len(vals) >= 2 and vals[-1] != vals[0]:
                auth_cap["existing_auth_eq_cap_inr"] = vals[0]
                auth_cap["new_auth_eq_cap_inr"] = vals[-1]
                auth_cap["proposed_increase_inr"] = vals[-1] - vals[0]

    # ── Compute proposed increase if we have both ─────────────────
    if (auth_cap["proposed_increase_inr"] is None
            and auth_cap["existing_auth_eq_cap_inr"] is not None
            and auth_cap["new_auth_eq_cap_inr"] is not None
            and auth_cap["new_auth_eq_cap_inr"] > auth_cap["existing_auth_eq_cap_inr"]):
        auth_cap["proposed_increase_inr"] = (
            auth_cap["new_auth_eq_cap_inr"] - auth_cap["existing_auth_eq_cap_inr"]
        )

    return auth_cap


def _generate_trading_signal(ai_data: dict) -> str:
    """Generate a simple trading signal based on sentiment and impact."""
    sentiment = ai_data.get("sentiment", "Neutral")
    impact = ai_data.get("impact_level", "Low")
    ann_type = ai_data.get("announcement_type", "Other")

    if sentiment == "Positive" and impact == "High":
        return "🚀 Strong Bullish"
    elif sentiment == "Positive" and impact == "Medium":
        return "📈 Bullish"
    elif sentiment == "Positive" and impact == "Low":
        return "🟢 Mildly Positive"
    elif sentiment == "Negative" and impact == "High":
        return "🔴 Strong Bearish"
    elif sentiment == "Negative" and impact == "Medium":
        return "📉 Bearish"
    elif sentiment == "Negative" and impact == "Low":
        return "🟠 Mildly Negative"
    elif ann_type == "Increase in Authorized Capital":
        return "⚠️ Watch — Dilution Risk"
    elif ann_type == "Buyback":
        return "📈 Bullish (Buyback)"
    elif ann_type == "Dividend":
        return "🟢 Positive (Income)"
    else:
        return "⚖️ Neutral"

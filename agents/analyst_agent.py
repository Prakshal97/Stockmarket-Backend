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

    # Ensure authorized_capital is correct
    if ai_data.get("announcement_type") == "Increase in Authorized Capital":
        if not ai_data.get("authorized_capital"):
            ai_data["authorized_capital"] = _extract_auth_capital_from_text(text)

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
    Try to extract authorized capital figures from raw text using regex.
    Matches patterns like 'Rs 100 Crore', '100,00,00,000', etc.
    """
    import re

    auth_cap = {
        "board_approval": None,
        "date_of_board_meeting": None,
        "existing_auth_eq_cap_inr": None,
        "new_auth_eq_cap_inr": None,
        "proposed_increase_inr": None,
    }

    # Board approval
    if "board approved" in text or "board approves" in text or "approved by the board" in text:
        auth_cap["board_approval"] = "Yes"
    elif "no board approval" in text or "shareholders approval" in text:
        auth_cap["board_approval"] = "No"

    # Date patterns: DD-MM-YYYY, DD/MM/YYYY, DDth Month YYYY
    date_patterns = [
        r'\b(\d{2}[-/]\d{2}[-/]\d{4})\b',
        r'\b(\d{1,2}(?:st|nd|rd|th)?\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+\d{4})\b',
    ]
    for pattern in date_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            auth_cap["date_of_board_meeting"] = match.group(1)
            break

    # Crore amounts
    crore_matches = re.findall(r'rs[.\s]*(\d+(?:,\d+)*(?:\.\d+)?)\s*(?:crore|cr)', text, re.IGNORECASE)
    if len(crore_matches) >= 2:
        try:
            amounts = [float(m.replace(",", "")) * 1_00_00_000 for m in crore_matches[:3]]
            amounts.sort()
            auth_cap["existing_auth_eq_cap_inr"] = amounts[0]
            auth_cap["new_auth_eq_cap_inr"] = amounts[-1]
            auth_cap["proposed_increase_inr"] = amounts[-1] - amounts[0]
        except:
            pass

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

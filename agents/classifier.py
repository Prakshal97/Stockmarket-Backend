"""
Deterministic Classifier & Extractor for Authorized Capital Announcements.

This module uses ONLY keyword matching and regex to:
1. Classify announcements as 'authorized_capital' or 'general'
2. Extract financial values (existing/new capital) deterministically

AI (Groq) is NEVER called from this module.
"""
import re
from typing import Optional


# ── Keywords that indicate Authorized Capital ─────────────────────────────

AUTH_CAPITAL_KEYWORDS = [
    "authorized capital",
    "authorised capital",
    "authorized share capital",
    "authorised share capital",
    "authorized equity capital",
    "authorised equity capital",
    "increase in authorized",
    "increase in authorised",
    "capital structure change",
    "increase in capital",
    "alteration of capital",
    "increase of authorized",
    "increase of authorised",
]

# Secondary confirmation keywords (must appear alongside primary keywords)
AUTH_CAPITAL_CONFIRM = [
    "increase", "enhanced", "augmented", "raised", "altered",
    "from", "to", "crore", "cr", "lakh", "lac",
    "existing", "proposed", "new", "revised",
]


def classify_announcement(announcement: dict) -> str:
    """
    Classify an announcement as 'authorized_capital' or 'general'.

    Uses keyword matching on BOTH subject AND body/PDF text.
    Returns 'authorized_capital' only when primary keywords are found
    AND content validation passes (not just a passing mention).
    """
    subject = (announcement.get("raw_subject", "") or "").lower()
    body = (announcement.get("raw_body", "") or "").lower()
    full_text = subject + " " + body

    # Step 1: Check for primary keywords
    has_primary_keyword = any(kw in full_text for kw in AUTH_CAPITAL_KEYWORDS)

    if not has_primary_keyword:
        return "general"

    # Step 2: Content validation — make sure it's actually ABOUT capital change
    # (not just mentioning it in passing, e.g. "no change in authorized capital")
    negation_patterns = [
        "no change in authorized",
        "no change in authorised",
        "no alteration",
        "not proposed",
        "unchanged authorized",
        "unchanged authorised",
    ]
    has_negation = any(neg in full_text for neg in negation_patterns)
    if has_negation:
        return "general"

    # Step 3: Confirm via secondary keywords (at least one must appear)
    has_confirmation = any(kw in full_text for kw in AUTH_CAPITAL_CONFIRM)

    # If primary keyword is in the subject line, it's very strong signal
    subject_has_keyword = any(kw in subject for kw in AUTH_CAPITAL_KEYWORDS)

    if subject_has_keyword or has_confirmation:
        return "authorized_capital"

    return "general"


# ── Deterministic Financial Value Extraction ──────────────────────────────

def _parse_amount(s: str) -> Optional[float]:
    """
    Convert text like '100 Crore', '1,00,00,000', '5.75 Cr' → INR float.
    Returns None if no valid number found.
    """
    if not s:
        return None
    s = s.strip().replace(",", "")

    crore_match = re.search(r'([\d.]+)\s*(?:crore|cr\.?)\b', s, re.IGNORECASE)
    if crore_match:
        try:
            return float(crore_match.group(1)) * 1_00_00_000
        except ValueError:
            return None

    lakh_match = re.search(r'([\d.]+)\s*(?:lakh|lac)\b', s, re.IGNORECASE)
    if lakh_match:
        try:
            return float(lakh_match.group(1)) * 1_00_000
        except ValueError:
            return None

    # Raw number (could be INR directly)
    num_match = re.search(r'([\d.]+)', s)
    if num_match:
        try:
            val = float(num_match.group(1))
            return val if val > 0 else None
        except ValueError:
            return None

    return None


def extract_auth_capital_deterministic(text: str) -> dict:
    """
    Extract Authorized Capital financial figures from raw text using
    context-aware regex. NO LLM involved.

    Returns dict with keys:
        board_approval, date_of_board_meeting,
        existing_auth_eq_cap_inr, new_auth_eq_cap_inr, proposed_increase_inr
    """
    result = {
        "board_approval": "Not Available",
        "date_of_board_meeting": "Not Available",
        "existing_auth_eq_cap_inr": None,
        "new_auth_eq_cap_inr": None,
        "proposed_increase_inr": None,
    }

    t = text.lower()

    # ── Board Approval ────────────────────────────────────────────
    if any(p in t for p in [
        "board approved", "board approves", "approved by the board",
        "board of directors approved", "board has approved",
        "board of directors has approved"
    ]):
        result["board_approval"] = "Yes"
    elif any(p in t for p in [
        "shareholders approval", "postal ballot",
        "subject to approval", "subject to shareholders",
        "pending approval"
    ]):
        result["board_approval"] = "Pending Shareholder Approval"

    # ── Date of Board Meeting ─────────────────────────────────────
    date_patterns = [
        # DD-MM-YYYY or DD/MM/YYYY
        r'\b(\d{2}[-/]\d{2}[-/]\d{4})\b',
        # 5th April 2024, 05 Apr 2024, etc.
        r'\b(\d{1,2}(?:st|nd|rd|th)?\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*[,\s]+\d{4})\b',
        # YYYY-MM-DD
        r'\b(\d{4}[-/]\d{2}[-/]\d{2})\b',
    ]
    for pat in date_patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            result["date_of_board_meeting"] = m.group(1).strip()
            break

    # ── Pattern 1: "from Rs X to Rs Y" (strongest signal) ────────
    from_to = re.search(
        r'from\s+(?:rs\.?|inr\.?|₹)?\s*([\d,]+(?:\.\d+)?(?:\s*(?:crore|cr\.?|lakh|lac))?)'
        r'\s+to\s+(?:rs\.?|inr\.?|₹)?\s*([\d,]+(?:\.\d+)?(?:\s*(?:crore|cr\.?|lakh|lac))?)',
        text, re.IGNORECASE
    )
    if from_to:
        existing = _parse_amount(from_to.group(1))
        new = _parse_amount(from_to.group(2))
        if existing and new and new != existing:
            result["existing_auth_eq_cap_inr"] = existing
            result["new_auth_eq_cap_inr"] = new
            result["proposed_increase_inr"] = abs(new - existing)

    # ── Pattern 2: "existing capital: X" / "new/revised capital: Y" ──
    if result["existing_auth_eq_cap_inr"] is None:
        existing_patterns = [
            r'(?:existing|present|current)\s+(?:authorized|authorised)?\s*(?:share\s+)?(?:equity\s+)?capital\s*(?:of)?\s*(?:is|was|:)?\s*(?:rs\.?|inr\.?|₹)?\s*([\d,]+(?:\.\d+)?(?:\s*(?:crore|cr\.?|lakh|lac))?)',
            r'(?:existing|present|current)\s+(?:authorized|authorised)?\s*(?:share\s+)?(?:equity\s+)?capital\s*[:\s]+(?:rs\.?|inr\.?|₹)?\s*([\d,]+(?:\.\d+)?(?:\s*(?:crore|cr\.?|lakh|lac))?)',
        ]
        for pat in existing_patterns:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                val = _parse_amount(m.group(1))
                if val and val > 1_00_000:  # Must be > 1 Lakh to be capital
                    result["existing_auth_eq_cap_inr"] = val
                    break

    if result["new_auth_eq_cap_inr"] is None:
        new_patterns = [
            r'(?:new|revised|increased|proposed|enhanced)\s+(?:authorized|authorised)?\s*(?:share\s+)?(?:equity\s+)?capital\s*(?:of)?\s*(?:is|to|:)?\s*(?:rs\.?|inr\.?|₹)?\s*([\d,]+(?:\.\d+)?(?:\s*(?:crore|cr\.?|lakh|lac))?)',
            r'(?:new|revised|increased|proposed|enhanced)\s+(?:authorized|authorised)?\s*(?:share\s+)?(?:equity\s+)?capital\s*[:\s]+(?:rs\.?|inr\.?|₹)?\s*([\d,]+(?:\.\d+)?(?:\s*(?:crore|cr\.?|lakh|lac))?)',
        ]
        for pat in new_patterns:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                val = _parse_amount(m.group(1))
                if val and val > 1_00_000:
                    result["new_auth_eq_cap_inr"] = val
                    break

    # ── Pattern 3: "increase of Rs X" (delta only) ───────────────
    if result["proposed_increase_inr"] is None:
        increase_patterns = [
            r'(?:increase\s+of|increased?\s+by|by\s+(?:rs\.?|inr\.?|₹))\s*([\d,]+(?:\.\d+)?(?:\s*(?:crore|cr\.?|lakh|lac))?)',
            r'(?:proposed\s+increase)\s*(?:of)?\s*(?:rs\.?|inr\.?|₹)?\s*([\d,]+(?:\.\d+)?(?:\s*(?:crore|cr\.?|lakh|lac))?)',
        ]
        for pat in increase_patterns:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                val = _parse_amount(m.group(1))
                if val and val > 1_00_000:
                    result["proposed_increase_inr"] = val
                    break

    # ── Pattern 4: Table-style rows (PDF extracted) ──────────────
    # pdfplumber tables joined with " | " — look for two large INR numbers
    if result["existing_auth_eq_cap_inr"] is None:
        big_nums = re.findall(r'\b(\d{1,3}(?:,\d{2,3}){3,})\b', text)
        if len(big_nums) >= 2:
            vals = []
            for n in big_nums:
                try:
                    v = float(n.replace(",", ""))
                    if v > 1_00_000:  # Ignore small numbers
                        vals.append(v)
                except ValueError:
                    pass
            vals = sorted(set(vals))
            if len(vals) >= 2 and vals[-1] != vals[0]:
                result["existing_auth_eq_cap_inr"] = vals[0]
                result["new_auth_eq_cap_inr"] = vals[-1]
                result["proposed_increase_inr"] = vals[-1] - vals[0]

    # ── Compute proposed increase if we have both ─────────────────
    if (result["proposed_increase_inr"] is None
            and result["existing_auth_eq_cap_inr"] is not None
            and result["new_auth_eq_cap_inr"] is not None):
        diff = result["new_auth_eq_cap_inr"] - result["existing_auth_eq_cap_inr"]
        if diff > 0:
            result["proposed_increase_inr"] = diff

    # ── Sanitize: reject face values picked up as capital ─────────
    for key in ["existing_auth_eq_cap_inr", "new_auth_eq_cap_inr"]:
        val = result.get(key)
        if val is not None and val < 10_00_000:  # < 10 Lakh is likely face value
            result[key] = None

    return result

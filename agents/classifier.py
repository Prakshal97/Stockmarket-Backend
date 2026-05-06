"""
Deterministic Classifier & Extractor for Authorized Capital Announcements.

Uses ONLY keyword matching and regex to:
1. Classify announcements as 'authorized_capital' or 'general'
2. Extract financial values (existing/new capital) deterministically

AI (Groq) is NEVER called from this module.
"""
import re
from typing import Optional


# ── Primary keywords that STRONGLY indicate Authorized Capital change ─────────

AUTH_CAPITAL_KEYWORDS = [
    # English spellings
    "authorized capital",
    "authorised capital",
    "authorized share capital",
    "authorised share capital",
    "authorized equity capital",
    "authorised equity capital",
    # Increase phrases
    "increase in authorized",
    "increase in authorised",
    "increase of authorized",
    "increase of authorised",
    "increase in the authorized",
    "increase in the authorised",
    # Alteration phrases (used by NSE/BSE official filings)
    "alteration of capital",
    "alteration in capital",
    "alteration of memorandum",
    "alteration in memorandum",
    "capital clause",
    "clause v of the memorandum",
    "memorandum of association",
    # General capital structure
    "capital structure change",
    "increase in capital",
    "reclassification of capital",
    "reclassification of authorized",
    "reclassification of authorised",
    "enhancement of authorized",
    "enhancement of authorised",
    # Hindi transliteration sometimes used
    "paidup capital",
    "paid-up capital increase",
    "increase in share capital",
    "raise in authorized",
    "raise in authorised",
]

# Strong phrases that indicate a dedicated authorized-capital filing.
AUTH_CAPITAL_STRONG_PHRASES = [
    "increase in authorized capital",
    "increase in authorised capital",
    "increase in authorized share capital",
    "increase in authorised share capital",
    "authorized share capital",
    "authorised share capital",
    "authorized capital",
    "authorised capital",
    "alteration of capital clause",
    "alteration in capital clause",
    "amendment to memorandum",
    "amendment to moa",
    "capital clause",
]

# Terms that usually indicate the filing is a mixed announcement and should not
# be surfaced as a pure authorized-capital event.
AUTH_CAPITAL_REJECT_TERMS = [
    "financial results",
    "audited results",
    "quarterly results",
    "annual results",
    "dividend",
    "bonus issue",
    "bonus shares",
    "preferential issue",
    "preferential basis",
    "private placement",
    "qualified institutions placement",
    "qip",
    "fund raising",
    "fund-raising",
    "issue of equity shares",
    "issue of shares",
    "warrants",
    "convertible instruments",
    "earnings call",
    "board meeting outcome",
    "board meeting intimation",
    "meeting of the board",
    "outcome of board meeting",
    "appointment of director",
    "appointment of a director",
    "change of name",
    "alteration of main object",
    "main object clause",
    "voluntary surrender",
    "resignation",
    "results and",
    "results, dividend",
    "results, bonus",
    "rights issue",
    "buyback",
    "merger",
    "amalgamation",
    "acquisition",
]

# Phrases that NEGATE — it's talking about capital but NOT changing it
NEGATION_PATTERNS = [
    "no change in authorized",
    "no change in authorised",
    "no alteration",
    "not proposed",
    "unchanged authorized",
    "unchanged authorised",
    "no increase in authorized",
    "no increase in authorised",
    "there is no change",
    "does not affect authorized",
    "does not affect authorised",
]


def is_pure_authorized_capital(announcement: dict) -> bool:
    """
    Return True only for dedicated authorized-capital announcements.

    This is intentionally strict: a filing must be primarily about capital
    alteration / authorized share capital, and must not be a mixed results,
    dividend, bonus, or board-outcome disclosure that merely mentions capital.
    """
    subject = (announcement.get("raw_subject", "") or "").lower().strip()
    body = (announcement.get("raw_body", "") or "").lower().strip()
    title = (announcement.get("title", "") or "").lower().strip()
    ai_title = (announcement.get("ai_data", {}) or {}).get("title", "")
    ai_title = (ai_title or "").lower().strip()

    full_text = " ".join(filter(None, [subject, body, title, ai_title]))

    has_strong_phrase = any(kw in subject or kw in title or kw in full_text for kw in AUTH_CAPITAL_STRONG_PHRASES)
    has_reject_term = any(term in full_text for term in AUTH_CAPITAL_REJECT_TERMS)
    has_auth_keyword = any(kw in full_text for kw in AUTH_CAPITAL_KEYWORDS)

    if not has_auth_keyword:
        return False

    if has_reject_term:
        return False

    # Strong phrase in the subject/title is the cleanest signal.
    if has_strong_phrase and (any(kw in subject for kw in AUTH_CAPITAL_STRONG_PHRASES) or any(kw in title for kw in AUTH_CAPITAL_STRONG_PHRASES)):
        return True

    # Allow a body-only match only when it is clearly a capital-clause filing
    # and not a mixed disclosure.
    capital_clause_context = any(term in full_text for term in [
        "capital clause",
        "memorandum of association",
        "moa",
        "alteration of capital",
        "authorized share capital",
        "authorised share capital",
    ])
    return has_auth_keyword and capital_clause_context and not has_reject_term


def classify_announcement(announcement: dict) -> str:
    """
    Classify an announcement as 'authorized_capital' or 'general'.

    Uses strict intent-based matching on BOTH subject AND body/PDF text.
    Returns 'authorized_capital' only for dedicated capital filings.
    """
    subject = (announcement.get("raw_subject", "") or "").lower().strip()
    body = (announcement.get("raw_body", "") or "").lower().strip()
    full_text = subject + " " + body

    if any(neg in full_text for neg in NEGATION_PATTERNS):
        return "general"

    if is_pure_authorized_capital(announcement):
        return "authorized_capital"

    # Fallback: if the wording is very explicit and still non-mixed, allow it.
    if any(kw in subject for kw in AUTH_CAPITAL_STRONG_PHRASES) and not any(term in full_text for term in AUTH_CAPITAL_REJECT_TERMS):
        return "authorized_capital"

    return "general"


# ── Deterministic Financial Value Extraction ──────────────────────────────────

def _parse_amount(s: str) -> Optional[float]:
    """
    Convert text like '100 Crore', '1,00,00,000', '5.75 Cr' → INR float.
    Returns None if no valid number found.
    """
    if not s:
        return None
    s = s.strip().replace(",", "")

    crore_match = re.search(r'([\d.]+)\s*(?:crores?|crs?|cr\.?)\b', s, re.IGNORECASE)
    if crore_match:
        try:
            return float(crore_match.group(1)) * 1_00_00_000
        except ValueError:
            return None

    lakh_match = re.search(r'([\d.]+)\s*(?:lakhs?|lacs?|lac)\b', s, re.IGNORECASE)
    if lakh_match:
        try:
            return float(lakh_match.group(1)) * 1_00_000
        except ValueError:
            return None

    # Raw number
    num_match = re.search(r'([\d.]+)', s)
    if num_match:
        try:
            val = float(num_match.group(1))
            return val if val > 0 else None
        except ValueError:
            return None

    return None


def _compute_capital_diff(existing: Optional[float], new: Optional[float]) -> Optional[float]:
    """Deterministically compute the increase from old/new capital when both exist."""
    if existing is None or new is None:
        return None
    try:
        existing_val = float(existing)
        new_val = float(new)
        if new_val <= existing_val:
            return None
        return new_val - existing_val
    except Exception:
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
        "face_value_inr": None,
        "percentage_increase": None,
    }

    t = text.lower()

    # ── Board Approval ────────────────────────────────────────────────────────
    if any(p in t for p in [
        "board approved", "board approves", "approved by the board",
        "board of directors approved", "board has approved",
        "board of directors has approved", "the board has approved",
        "directors have approved", "resolution has been passed",
    ]):
        result["board_approval"] = "Yes"
    elif any(p in t for p in [
        "shareholders approval", "postal ballot",
        "subject to approval", "subject to shareholders",
        "pending approval", "awaiting approval",
        "egm", "agm", "extra-ordinary general",
    ]):
        result["board_approval"] = "Pending Shareholder Approval"

    # ── Date of Board Meeting ──────────────────────────────────────────────────
    date_patterns = [
        r'\b(\d{2}[-/]\d{2}[-/]\d{4})\b',
        r'\b(\d{1,2}(?:st|nd|rd|th)?\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*[,\s]+\d{4})\b',
        r'\b(\d{4}[-/]\d{2}[-/]\d{2})\b',
        r'\b(\d{1,2}[-/]\d{1,2}[-/]\d{4})\b',
    ]
    for pat in date_patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            result["date_of_board_meeting"] = m.group(1).strip()
            break

    # ── Pattern 1: "from Rs X to Rs Y" (strongest signal) ────────────────────
    from_to = re.search(
        r'from\s+(?:rs\.?|inr\.?|₹)?\s*([\d,]+(?:\.\d+)?(?:\s*(?:crores?|crs?|cr\.?|lakhs?|lacs?|lac))?)'
        r'\s+to\s+(?:rs\.?|inr\.?|₹)?\s*([\d,]+(?:\.\d+)?(?:\s*(?:crores?|crs?|cr\.?|lakhs?|lacs?|lac))?)',
        text, re.IGNORECASE | re.DOTALL
    )
    if from_to:
        existing = _parse_amount(from_to.group(1))
        new = _parse_amount(from_to.group(2))
        if existing and new and new != existing:
            result["existing_auth_eq_cap_inr"] = existing
            result["new_auth_eq_cap_inr"] = new
            result["proposed_increase_inr"] = _compute_capital_diff(existing, new)

    # ── Pattern 2: "existing capital: X" / "new/revised capital: Y" ──────────
    if result["existing_auth_eq_cap_inr"] is None:
        existing_patterns = [
            r'(?:existing|present|current)\s+(?:authorized|authorised)?\s*(?:share\s+)?(?:equity\s+)?capital\s*(?:of)?\s*(?:is|was|:)?\s*(?:rs\.?|inr\.?|₹)?\s*([\d,]+(?:\.\d+)?(?:\s*(?:crores?|crs?|cr\.?|lakhs?|lacs?|lac))?)',
            r'(?:existing|present|current)\s+(?:authorized|authorised)?\s*(?:share\s+)?(?:equity\s+)?capital\s*[:\s]+(?:rs\.?|inr\.?|₹)?\s*([\d,]+(?:\.\d+)?(?:\s*(?:crores?|crs?|cr\.?|lakhs?|lacs?|lac))?)',
            r'(?:from)\s+(?:rs\.?|inr\.?|₹)?\s*([\d,]+(?:\.\d+)?)\s*(?:crores?|crs?|cr\.?)',
        ]
        for pat in existing_patterns:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                val = _parse_amount(m.group(1))
                if val and val > 1_00_000:
                    result["existing_auth_eq_cap_inr"] = val
                    break

    if result["new_auth_eq_cap_inr"] is None:
        new_patterns = [
            r'(?:new|revised|increased|proposed|enhanced)\s+(?:authorized|authorised)?\s*(?:share\s+)?(?:equity\s+)?capital\s*(?:of)?\s*(?:is|to|:)?\s*(?:rs\.?|inr\.?|₹)?\s*([\d,]+(?:\.\d+)?(?:\s*(?:crores?|crs?|cr\.?|lakhs?|lacs?|lac))?)',
            r'(?:new|revised|increased|proposed|enhanced)\s+(?:authorized|authorised)?\s*(?:share\s+)?(?:equity\s+)?capital\s*[:\s]+(?:rs\.?|inr\.?|₹)?\s*([\d,]+(?:\.\d+)?(?:\s*(?:crores?|crs?|cr\.?|lakhs?|lacs?|lac))?)',
            r'(?:to)\s+(?:rs\.?|inr\.?|₹)?\s*([\d,]+(?:\.\d+)?)\s*(?:crores?|crs?|cr\.?)',
        ]
        for pat in new_patterns:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                val = _parse_amount(m.group(1))
                if val and val > 1_00_000:
                    result["new_auth_eq_cap_inr"] = val
                    break

    # ── Pattern 3: "increase of Rs X" (delta only) ───────────────────────────
    if result["proposed_increase_inr"] is None:
        increase_patterns = [
            r'(?:increase\s+of|increased?\s+by|by\s+(?:rs\.?|inr\.?|₹))\s*([\d,]+(?:\.\d+)?(?:\s*(?:crores?|crs?|cr\.?|lakhs?|lacs?|lac))?)',
            r'(?:proposed\s+increase)\s*(?:of)?\s*(?:rs\.?|inr\.?|₹)?\s*([\d,]+(?:\.\d+)?(?:\s*(?:crores?|crs?|cr\.?|lakhs?|lacs?|lac))?)',
        ]
        for pat in increase_patterns:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                val = _parse_amount(m.group(1))
                if val and val > 1_00_000:
                    result["proposed_increase_inr"] = val
                    break

    # ── Pattern 4: Table-style rows from PDF (two large INR numbers) ──────────
    if result["existing_auth_eq_cap_inr"] is None:
        big_nums = re.findall(r'\b(\d{1,3}(?:,\d{2,3}){3,})\b', text)
        if len(big_nums) >= 2:
            vals = []
            for n in big_nums:
                try:
                    v = float(n.replace(",", ""))
                    if v > 1_00_000:
                        vals.append(v)
                except ValueError:
                    pass
            vals = sorted(set(vals))
            if len(vals) >= 2 and vals[-1] != vals[0]:
                result["existing_auth_eq_cap_inr"] = vals[0]
                result["new_auth_eq_cap_inr"] = vals[-1]
                result["proposed_increase_inr"] = _compute_capital_diff(vals[0], vals[-1])

    # ── Face value extraction ─────────────────────────────────────────────────
    face_patterns = [
        r'(?:face\s*value|fv|nominal\s*value)\s*(?:of|at|:|is|per\s+share)?\s*(?:rs\.?|inr\.?|₹)?\s*([\d,]+(?:\.\d+)?)',
        r'(?:equity\s+shares?\s+of\s+)?(?:rs\.?|inr\.?|₹)?\s*([\d,]+(?:\.\d+)?)\s*(?:each|per\s+share)\b',
    ]
    for pat in face_patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            try:
                face_val = float(m.group(1).replace(",", ""))
                if 0 < face_val <= 1000:
                    result["face_value_inr"] = face_val
                    break
            except ValueError:
                pass

    # ── Compute proposed increase if we have both ─────────────────────────────
    if result["existing_auth_eq_cap_inr"] is not None and result["new_auth_eq_cap_inr"] is not None:
        diff = _compute_capital_diff(result["existing_auth_eq_cap_inr"], result["new_auth_eq_cap_inr"])
        if diff is not None:
            result["proposed_increase_inr"] = diff

    if (result["percentage_increase"] is None
            and result["existing_auth_eq_cap_inr"]
            and result["new_auth_eq_cap_inr"]
            and result["new_auth_eq_cap_inr"] > result["existing_auth_eq_cap_inr"]):
        try:
            result["percentage_increase"] = round(
                ((result["new_auth_eq_cap_inr"] - result["existing_auth_eq_cap_inr"]) / result["existing_auth_eq_cap_inr"]) * 100,
                2,
            )
        except Exception:
            pass

    # ── Sanitize: reject face values picked up as capital ─────────────────────
    for key in ["existing_auth_eq_cap_inr", "new_auth_eq_cap_inr"]:
        val = result.get(key)
        if val is not None and val < 10_00_000:  # < 10 Lakh is likely face value
            result[key] = None

    return result

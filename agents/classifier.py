"""
Deterministic Classifier & Extractor for Authorized Capital Announcements.

Uses ONLY keyword matching and regex to:
1. Classify announcements as 'authorized_capital' or 'general'
2. Extract financial values (existing/new capital) deterministically

AI (Groq) is NEVER called from this module.

10: 3-TIER CLASSIFICATION MODEL (Production Grade):
11:   - VERIFIED: Explicit restructuring proof (Phrases or Capital Values)
12:   - POSSIBLE: Context signals (EGM, Postal Ballot) without verified proof
13:   - GENERAL: Everything else
14: 
15:   Decision logic:
16:   1. If old/new capital extracted OR Rule 2/3 matched -> VERIFIED
17:   2. If EGM/Postal Ballot/etc. matched -> POSSIBLE
18:   3. Otherwise -> GENERAL
"""
import re
from typing import Optional

# ── Confidence threshold for pass/fail decision ───────────────────────────────
# Threshold calibrated from live NSE/BSE diagnostic:
# Real filings max out at score=20 (Tata Capital postal ballot case).
# The previous threshold of 40 was unreachable for real exchange data.
CONFIDENCE_THRESHOLD = 20

# ── Phrases that NEGATE — it's talking about capital but NOT changing it ──────
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

# ─────────────────────────────────────────────────────────────────────────────
# In-memory store for tracking rejected candidates (for diagnostic reporting)
# ─────────────────────────────────────────────────────────────────────────────
_rejected_candidates: list = []   # list of dicts with score + metadata
MAX_REJECTED_STORE = 500          # rolling buffer size


def _record_rejected(company: str, exchange: str, subject: str, score: int,
                     positives: list, negatives: list) -> None:
    """Keep a rolling buffer of rejected candidates for audit reporting."""
    global _rejected_candidates
    _rejected_candidates.append({
        "company": company,
        "exchange": exchange,
        "subject": subject[:120],
        "score": score,
        "positives": positives,
        "negatives": negatives,
    })
    # Keep only the last MAX_REJECTED_STORE entries
    if len(_rejected_candidates) > MAX_REJECTED_STORE:
        _rejected_candidates = _rejected_candidates[-MAX_REJECTED_STORE:]


def get_top_rejected_candidates(n: int = 20) -> list:
    """
    Return the top-N rejected candidates sorted by score descending.
    These are the filings most likely to be genuine but incorrectly filtered.
    """
    return sorted(_rejected_candidates, key=lambda x: x["score"], reverse=True)[:n]


def extract_evidence_snippet(text: str, keywords: list) -> str:
    """Extract a 150-char snippet containing the first matched keyword."""
    if not text: return ""
    text_clean = text.replace("\n", " ").replace("\r", " ")
    for kw in keywords:
        idx = text_clean.lower().find(kw.lower())
        if idx != -1:
            start = max(0, idx - 40)
            end = min(len(text_clean), idx + 110)
            snippet = text_clean[start:end].strip()
            if start > 0: snippet = "..." + snippet
            if end < len(text_clean): snippet = snippet + "..."
            return snippet
    return ""


# ─────────────────────────────────────────────────────────────────────────────
# Core scoring engine
# ─────────────────────────────────────────────────────────────────────────────

def evaluate_authorized_capital(announcement: dict) -> dict:
    """
    Evaluate an announcement using the 3-tier Production Accuracy model.
    
    Tiers:
      - verified: Explicit restructuring proof found.
      - possible: Context signals present, but no hard proof.
      - general: No capital-restructuring indicators.
    """
    subject = (announcement.get("raw_subject", "") or "").lower().strip()
    body    = (announcement.get("raw_body",    "") or "").lower().strip()
    title   = (announcement.get("title",       "") or "").lower().strip()
    exchange = announcement.get("exchange", "Unknown")
    company  = announcement.get("company_name", "Unknown")

    full_text = " ".join(filter(None, [subject, body, title]))
    
    matched_verified = []
    matched_possible = []
    
    # ── VERIFIED RULE 1: Capital Transformation (from Rs X to Rs Y) ───────────
    if "from rs" in full_text and " to rs" in full_text:
        matched_verified.append("transformation regex (from Rs X to Rs Y)")
    
    # ── VERIFIED RULE 2: Explicit Restructuring Phrases ───────────────────────
    strong_phrases = [
        "increase in authorized capital",
        "increase in authorised capital",
        "increase in authorized share capital",
        "increase in authorised share capital",
        "alteration of capital clause",
        "alteration in capital clause",
        "amendment to memorandum of association",
        "amendment to moa",
        "authorised equity share capital",
        "raising of capital",
        "increase the capital",
        "alteration of clause v",
    ]
    for kw in strong_phrases:
        if kw in full_text:
            matched_verified.append(f"explicit phrase: {kw}")

    # ── POSSIBLE TIER: Context Signals ────────────────────────────────────────
    possible_signals = [
        "postal ballot",
        "extraordinary general meeting",
        " egm",
        "agm",
        "shareholders meeting",
        "newspaper publication",
        "scrutinizer report",
        "voting result",
        "board meeting outcome",
        "outcome of board meeting",
    ]
    for kw in possible_signals:
        if kw in full_text:
            matched_possible.append(f"context signal: {kw}")

    # ── Final Verdict ─────────────────────────────────────────────────────────
    # A filing is VERIFIED only if matched_verified is not empty.
    # Otherwise, it is POSSIBLE if matched_possible is not empty.
    # Otherwise, it is GENERAL.
    
    category = "general"
    confidence = "LOW"
    evidence = ""
    
    if matched_verified:
        category = "verified"
        confidence = "HIGH"
        evidence = extract_evidence_snippet(full_text, strong_phrases + ["from rs"])
    elif matched_possible:
        # Check if there is AT LEAST a bare mention of 'capital' or 'authorized'
        # to qualify as 'possible'
        if any(kw in full_text for kw in ["capital", "authorized", "authorised"]):
            category = "possible"
            confidence = "MEDIUM"
            evidence = extract_evidence_snippet(full_text, possible_signals)
        else:
            category = "general"
    
    # Audit Logging
    if category != "general":
        verdict = f"[{category.upper()}]"
        print(f"\n[CLASSIFIER] {verdict} {company}")
        print(f"  Subject: {subject[:80]}")
        print(f"  Evidence: {evidence}")
        print(f"  Matches: {matched_verified if matched_verified else matched_possible}")

    return {
        "passed": category == "verified", # Backward compat
        "category": category,
        "confidence": confidence,
        "reason": f"Matches: {matched_verified if matched_verified else matched_possible}",
        "evidence": evidence,
        "score": 100 if category == "verified" else (50 if category == "possible" else 0),
        "threshold": 100,
        "source": "Title" if any(kw in subject for kw in strong_phrases) else "Body",
        "title": subject,
        "exchange": exchange,
        "matched_kws": matched_verified if matched_verified else matched_possible,
        "rejected_kws": []
    }


def classify_announcement(announcement: dict) -> str:
    """
    Route announcement to its designated tier.
    """
    res = evaluate_authorized_capital(announcement)
    return res["category"]


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
        new_val      = float(new)
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
        "board_approval":          "Not Available",
        "date_of_board_meeting":   "Not Available",
        "existing_auth_eq_cap_inr": None,
        "new_auth_eq_cap_inr":      None,
        "proposed_increase_inr":    None,
        "face_value_inr":           None,
        "percentage_increase":      None,
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
        new      = _parse_amount(from_to.group(2))
        if existing and new and new != existing:
            result["existing_auth_eq_cap_inr"] = existing
            result["new_auth_eq_cap_inr"]      = new
            result["proposed_increase_inr"]    = _compute_capital_diff(existing, new)

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
                result["new_auth_eq_cap_inr"]      = vals[-1]
                result["proposed_increase_inr"]    = _compute_capital_diff(vals[0], vals[-1])

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

    if (
        result["percentage_increase"] is None
        and result["existing_auth_eq_cap_inr"]
        and result["new_auth_eq_cap_inr"]
        and result["new_auth_eq_cap_inr"] > result["existing_auth_eq_cap_inr"]
    ):
        try:
            result["percentage_increase"] = round(
                ((result["new_auth_eq_cap_inr"] - result["existing_auth_eq_cap_inr"])
                 / result["existing_auth_eq_cap_inr"]) * 100,
                2,
            )
        except Exception:
            pass

    # ── Sanitize: reject face values picked up as capital ─────────────────────
    for key in ["existing_auth_eq_cap_inr", "new_auth_eq_cap_inr"]:
        val = result.get(key)
        if val is not None and val < 10_00_000:   # < 10 Lakh is likely face value
            result[key] = None

    return result

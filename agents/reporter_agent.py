"""
Reporter Agent — Generates styled Excel reports from processed announcements.
Matches the exact format shown in the user's screenshot.
"""
import io
from datetime import datetime
from typing import List, Dict, Optional
import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import (
    PatternFill, Font, Alignment, Border, Side, GradientFill
)
from openpyxl.utils import get_column_letter
from openpyxl.chart import BarChart, Reference


# Color palette
COLORS = {
    "header_bg": "1E3A5F",       # Deep navy
    "header_font": "FFFFFF",
    "title_bg": "0D1B2A",        # Darker navy for title
    "positive_bg": "D5F5E3",     # Light green
    "negative_bg": "FADBD8",     # Light red
    "neutral_bg": "FEF9E7",      # Light yellow
    "high_impact": "E74C3C",     # Red
    "medium_impact": "F39C12",   # Orange
    "low_impact": "27AE60",      # Green
    "alt_row": "EBF5FB",         # Light blue alternate row
    "border": "BDC3C7",          # Light gray border
}

THIN_BORDER = Border(
    left=Side(style='thin', color=COLORS["border"]),
    right=Side(style='thin', color=COLORS["border"]),
    top=Side(style='thin', color=COLORS["border"]),
    bottom=Side(style='thin', color=COLORS["border"])
)


def _fmt_currency(val) -> str:
    """Format INR value: 1,00,00,000 → '₹1 Crore' or '₹1,000 Cr'."""
    if val is None:
        return "Unavailable"
    try:
        val = float(val)
        crores = val / 1_00_00_000
        if crores >= 1:
            return f"₹{crores:,.2f} Cr"
        lakhs = val / 1_00_000
        if lakhs >= 1:
            return f"₹{lakhs:,.2f} L"
        return f"₹{val:,.0f}"
    except:
        return str(val) if val else "Unavailable"


def generate_authorized_capital_excel(announcements: List[Dict]) -> bytes:
    """
    Generate Excel matching the user's 'Increase in Authorized Capital' format.

    Columns:
    Sr.no | Date of Entry | Name of the Company | Board Approval |
    D O B M | Exist Auth Eq Cap (INR) | New Auth Eq Cap (INR) |
    Proposed Increase (INR) | CMP | M cap (In Cr) | Sector
    """
    wb = Workbook()
    ws = wb.active
    ws.title = "Authorized Capital"

    # ── Title Row ──────────────────────────────────────────────────────
    ws.merge_cells("A1:K1")
    title_cell = ws["A1"]
    title_cell.value = "Increase in Authorized Capital"
    title_cell.font = Font(name="Calibri", bold=True, size=14, color=COLORS["header_font"])
    title_cell.fill = PatternFill("solid", fgColor=COLORS["title_bg"])
    title_cell.alignment = Alignment(horizontal="center", vertical="center")
    ws.row_dimensions[1].height = 28

    # ── Header Row ─────────────────────────────────────────────────────
    headers = [
        "Sr.no", "Date of Entry", "Name of the Company", "Board Approval",
        "D O B M", "Exist Auth Eq Cap ( INR )", "New Auth Eq Cap (INR)",
        "Proposed Increase ( INR )", "CMP", "M cap (In Cr)", "Sector"
    ]
    col_widths = [7, 15, 30, 15, 15, 25, 22, 22, 10, 16, 20]

    for col_idx, (header, width) in enumerate(zip(headers, col_widths), 1):
        cell = ws.cell(row=2, column=col_idx, value=header)
        cell.font = Font(name="Calibri", bold=True, size=10, color=COLORS["header_font"])
        cell.fill = PatternFill("solid", fgColor=COLORS["header_bg"])
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = THIN_BORDER
        ws.column_dimensions[get_column_letter(col_idx)].width = width

    ws.row_dimensions[2].height = 30

    # ── Data Rows ──────────────────────────────────────────────────────
    row_num = 3
    sr_no = 1

    for ann in announcements:
        ai_data = ann.get("ai_data", {}) or {}
        auth_cap = ai_data.get("authorized_capital", {}) or {}

        # Parse date
        try:
            ann_date = datetime.fromisoformat(ann.get("announcement_date", "")).strftime("%d.%m.%Y")
        except:
            ann_date = ""

        row_data = [
            sr_no,
            ann_date,
            ai_data.get("company_name") or ann.get("company_name", ""),
            auth_cap.get("board_approval", ""),
            auth_cap.get("date_of_board_meeting", ""),
            _fmt_currency(auth_cap.get("existing_auth_eq_cap_inr")),
            _fmt_currency(auth_cap.get("new_auth_eq_cap_inr")),
            _fmt_currency(auth_cap.get("proposed_increase_inr")),
            ai_data.get("cmp") or "Unavailable",
            ai_data.get("market_cap_cr") or "Unavailable",
            ai_data.get("sector", ""),
        ]

        is_alt = (sr_no % 2 == 0)
        alt_fill = PatternFill("solid", fgColor=COLORS["alt_row"]) if is_alt else None

        for col_idx, value in enumerate(row_data, 1):
            cell = ws.cell(row=row_num, column=col_idx, value=value)
            cell.font = Font(name="Calibri", size=10)
            cell.border = THIN_BORDER
            cell.alignment = Alignment(horizontal="center" if col_idx != 3 else "left",
                                       vertical="center", wrap_text=True)
            if alt_fill:
                cell.fill = alt_fill

        ws.row_dimensions[row_num].height = 20
        row_num += 1
        sr_no += 1

    # Freeze header rows
    ws.freeze_panes = "A3"

    return _save_workbook_bytes(wb)


def generate_full_report_excel(announcements: List[Dict]) -> bytes:
    """
    Generate a comprehensive multi-sheet Excel with all announcement types.
    Sheet 1: All Announcements
    Sheet 2: Authorized Capital (user's format)
    Sheet 3: High Impact Only
    Sheet 4: Summary Stats
    """
    wb = Workbook()

    # ─── Sheet 1: All Announcements ────────────────────────────────────
    ws_all = wb.active
    ws_all.title = "All Announcements"
    _create_all_announcements_sheet(ws_all, announcements)

    # ─── Sheet 2: Authorized Capital ───────────────────────────────────
    auth_cap_anns = [a for a in announcements
                     if a.get("ai_data", {}) and
                     a["ai_data"].get("announcement_type") == "Increase in Authorized Capital"]
    if auth_cap_anns:
        ws_auth = wb.create_sheet("Authorized Capital")
        _create_authorized_capital_sheet(ws_auth, auth_cap_anns)

    # ─── Sheet 3: High Impact ──────────────────────────────────────────
    high_impact = [a for a in announcements
                   if a.get("ai_data", {}) and
                   a["ai_data"].get("impact_level") == "High"]
    if high_impact:
        ws_high = wb.create_sheet("🔥 High Impact")
        _create_all_announcements_sheet(ws_high, high_impact)

    # ─── Sheet 4: Summary ─────────────────────────────────────────────
    ws_summary = wb.create_sheet("Summary")
    _create_summary_sheet(ws_summary, announcements)

    return _save_workbook_bytes(wb)


def _create_all_announcements_sheet(ws, announcements: List[Dict]):
    """Create the main announcements sheet."""
    # Title
    ws.merge_cells("A1:L1")
    title = ws["A1"]
    title.value = f"NSE/BSE Corporate Announcements — {datetime.now().strftime('%d %B %Y')}"
    title.font = Font(name="Calibri", bold=True, size=13, color="FFFFFF")
    title.fill = PatternFill("solid", fgColor=COLORS["title_bg"])
    title.alignment = Alignment(horizontal="center", vertical="center")
    ws.row_dimensions[1].height = 26

    headers = [
        "Sr.no", "Date", "Exchange", "Company", "Type",
        "Key Details", "Revenue/Profit Impact",
        "Sentiment", "Impact", "AI Insight", "Trading Signal", "Source"
    ]
    col_widths = [7, 14, 10, 28, 22, 40, 25, 12, 10, 45, 20, 15]

    for col_idx, (h, w) in enumerate(zip(headers, col_widths), 1):
        cell = ws.cell(row=2, column=col_idx, value=h)
        cell.font = Font(name="Calibri", bold=True, size=10, color="FFFFFF")
        cell.fill = PatternFill("solid", fgColor=COLORS["header_bg"])
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = THIN_BORDER
        ws.column_dimensions[get_column_letter(col_idx)].width = w

    ws.row_dimensions[2].height = 30

    for sr_no, ann in enumerate(announcements, 1):
        ai_data = ann.get("ai_data", {}) or {}
        row = sr_no + 2

        try:
            ann_date = datetime.fromisoformat(ann.get("announcement_date", "")).strftime("%d-%m-%Y")
        except:
            ann_date = ""

        sentiment = ai_data.get("sentiment", "Neutral")
        impact = ai_data.get("impact_level", "Low")

        # Row fill based on sentiment
        if sentiment == "Positive":
            row_fill = PatternFill("solid", fgColor=COLORS["positive_bg"])
        elif sentiment == "Negative":
            row_fill = PatternFill("solid", fgColor=COLORS["negative_bg"])
        else:
            row_fill = PatternFill("solid", fgColor=COLORS["neutral_bg"]) if sr_no % 2 == 0 else None

        row_data = [
            sr_no,
            ann_date,
            ann.get("exchange", ""),
            ai_data.get("company_name") or ann.get("company_name", ""),
            ai_data.get("announcement_type", "Other"),
            ai_data.get("key_details", ann.get("raw_subject", "")),
            ai_data.get("revenue_profit_impact", ""),
            sentiment,
            impact,
            ai_data.get("ai_insight", ""),
            ai_data.get("trading_signal", ""),
            ann.get("source_url", ""),
        ]

        for col_idx, value in enumerate(row_data, 1):
            cell = ws.cell(row=row, column=col_idx, value=value)
            cell.font = Font(name="Calibri", size=9)
            cell.border = THIN_BORDER
            cell.alignment = Alignment(
                horizontal="left" if col_idx in [4, 6, 7, 10] else "center",
                vertical="center",
                wrap_text=True
            )
            if row_fill:
                cell.fill = row_fill

            # Color-code sentiment cell
            if col_idx == 8:  # Sentiment column
                if sentiment == "Positive":
                    cell.font = Font(name="Calibri", size=9, bold=True, color="1E8449")
                elif sentiment == "Negative":
                    cell.font = Font(name="Calibri", size=9, bold=True, color="C0392B")
                else:
                    cell.font = Font(name="Calibri", size=9, bold=True, color="D68910")

            # Color-code impact cell
            if col_idx == 9:  # Impact column
                if impact == "High":
                    cell.font = Font(name="Calibri", size=9, bold=True, color=COLORS["high_impact"])
                elif impact == "Medium":
                    cell.font = Font(name="Calibri", size=9, bold=True, color=COLORS["medium_impact"])
                else:
                    cell.font = Font(name="Calibri", size=9, bold=True, color=COLORS["low_impact"])

        ws.row_dimensions[row].height = 45

    ws.freeze_panes = "A3"


def _create_authorized_capital_sheet(ws, announcements: List[Dict]):
    """Mirror of the user's exact Excel format."""
    ws.merge_cells("A1:K1")
    title_cell = ws["A1"]
    title_cell.value = "Increase in Authorized Capital"
    title_cell.font = Font(name="Calibri", bold=True, size=14, color=COLORS["header_font"])
    title_cell.fill = PatternFill("solid", fgColor=COLORS["title_bg"])
    title_cell.alignment = Alignment(horizontal="center", vertical="center")
    ws.row_dimensions[1].height = 28

    headers = [
        "Sr.no", "Date of Entry", "Name of the Company", "Board Approval",
        "D O B M", "Exist Auth Eq Cap ( INR )", "New Auth Eq Cap (INR)",
        "Proposed Increase ( INR )", "CMP", "M cap (In Cr)", "Sector"
    ]
    col_widths = [7, 15, 30, 15, 15, 25, 22, 22, 10, 16, 20]

    for col_idx, (h, w) in enumerate(zip(headers, col_widths), 1):
        cell = ws.cell(row=2, column=col_idx, value=h)
        cell.font = Font(name="Calibri", bold=True, size=10, color="FFFFFF")
        cell.fill = PatternFill("solid", fgColor=COLORS["header_bg"])
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = THIN_BORDER
        ws.column_dimensions[get_column_letter(col_idx)].width = w

    ws.row_dimensions[2].height = 30

    for sr_no, ann in enumerate(announcements, 1):
        ai_data = ann.get("ai_data", {}) or {}
        auth_cap = ai_data.get("authorized_capital", {}) or {}

        try:
            ann_date = datetime.fromisoformat(ann.get("announcement_date", "")).strftime("%d.%m.%Y")
        except:
            ann_date = ""

        row_data = [
            sr_no,
            ann_date,
            ai_data.get("company_name") or ann.get("company_name", ""),
            auth_cap.get("board_approval", ""),
            auth_cap.get("date_of_board_meeting", ""),
            _fmt_currency(auth_cap.get("existing_auth_eq_cap_inr")),
            _fmt_currency(auth_cap.get("new_auth_eq_cap_inr")),
            _fmt_currency(auth_cap.get("proposed_increase_inr")),
            ai_data.get("cmp", "Unavailable") or "Unavailable",
            ai_data.get("market_cap_cr", "Unavailable") or "Unavailable",
            ai_data.get("sector", ""),
        ]

        is_alt = (sr_no % 2 == 0)
        alt_fill = PatternFill("solid", fgColor=COLORS["alt_row"]) if is_alt else None
        row = sr_no + 2

        for col_idx, value in enumerate(row_data, 1):
            cell = ws.cell(row=row, column=col_idx, value=value)
            cell.font = Font(name="Calibri", size=10)
            cell.border = THIN_BORDER
            cell.alignment = Alignment(
                horizontal="left" if col_idx == 3 else "center",
                vertical="center"
            )
            if alt_fill:
                cell.fill = alt_fill

        ws.row_dimensions[row].height = 20

    ws.freeze_panes = "A3"


def _create_summary_sheet(ws, announcements: List[Dict]):
    """Create a statistics summary sheet."""
    ws.merge_cells("A1:D1")
    ws["A1"].value = "📊 Announcement Intelligence Summary"
    ws["A1"].font = Font(name="Calibri", bold=True, size=14, color="FFFFFF")
    ws["A1"].fill = PatternFill("solid", fgColor=COLORS["title_bg"])
    ws["A1"].alignment = Alignment(horizontal="center", vertical="center")
    ws.row_dimensions[1].height = 30

    total = len(announcements)
    processed = [a for a in announcements if a.get("ai_data")]

    # Count by type
    type_counts = {}
    sentiment_counts = {"Positive": 0, "Neutral": 0, "Negative": 0}
    impact_counts = {"High": 0, "Medium": 0, "Low": 0}
    exchange_counts = {"NSE": 0, "BSE": 0}

    for ann in processed:
        ai = ann.get("ai_data", {})
        t = ai.get("announcement_type", "Other")
        type_counts[t] = type_counts.get(t, 0) + 1
        s = ai.get("sentiment", "Neutral")
        if s in sentiment_counts:
            sentiment_counts[s] += 1
        i = ai.get("impact_level", "Low")
        if i in impact_counts:
            impact_counts[i] += 1
        e = ann.get("exchange", "NSE")
        if e in exchange_counts:
            exchange_counts[e] += 1

    summary_data = [
        ["Metric", "Value", "Category", "Count"],
        ["Total Announcements", total, "By Sentiment", ""],
        ["AI Processed", len(processed), "🟢 Positive", sentiment_counts["Positive"]],
        ["NSE", exchange_counts["NSE"], "🟡 Neutral", sentiment_counts["Neutral"]],
        ["BSE", exchange_counts["BSE"], "🔴 Negative", sentiment_counts["Negative"]],
        ["", "", "By Impact", ""],
        ["Type", "Count", "🔥 High", impact_counts["High"]],
    ]

    for t, c in sorted(type_counts.items(), key=lambda x: -x[1])[:5]:
        summary_data.append([t, c, "", ""])

    for row_idx, row_data in enumerate(summary_data, 2):
        for col_idx, value in enumerate(row_data, 1):
            cell = ws.cell(row=row_idx, column=col_idx, value=value)
            if row_idx == 2:  # Sub-header
                cell.font = Font(name="Calibri", bold=True, size=10, color="FFFFFF")
                cell.fill = PatternFill("solid", fgColor=COLORS["header_bg"])
            else:
                cell.font = Font(name="Calibri", size=10)
                if row_idx % 2 == 0:
                    cell.fill = PatternFill("solid", fgColor=COLORS["alt_row"])
            cell.border = THIN_BORDER
            cell.alignment = Alignment(horizontal="center", vertical="center")

    ws.column_dimensions["A"].width = 28
    ws.column_dimensions["B"].width = 15
    ws.column_dimensions["C"].width = 20
    ws.column_dimensions["D"].width = 12


def _save_workbook_bytes(wb: Workbook) -> bytes:
    """Save workbook to bytes buffer."""
    buffer = io.BytesIO()
    wb.save(buffer)
    buffer.seek(0)
    return buffer.getvalue()

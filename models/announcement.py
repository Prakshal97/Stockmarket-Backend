"""
Pydantic models for announcement data schema.
"""
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field
from enum import Enum


class Exchange(str, Enum):
    NSE = "NSE"
    BSE = "BSE"
    BOTH = "BOTH"


class AnnouncementType(str, Enum):
    RESULTS = "Financial Results"
    DIVIDEND = "Dividend"
    MERGER = "Merger & Acquisition"
    AUTHORIZED_CAPITAL = "Increase in Authorized Capital"
    BOARD_MEETING = "Board Meeting"
    ORDER_WIN = "Order Win"
    RIGHTS_ISSUE = "Rights Issue"
    BUYBACK = "Buyback"
    INSIDER_TRADING = "Insider Trading"
    AGM = "AGM/EGM"
    SHARE_ALLOTMENT = "Share Allotment"
    REGULATORY = "Regulatory Filing"
    OTHER = "Other"


class Sentiment(str, Enum):
    POSITIVE = "Positive"
    NEUTRAL = "Neutral"
    NEGATIVE = "Negative"


class ImpactLevel(str, Enum):
    HIGH = "High"
    MEDIUM = "Medium"
    LOW = "Low"


class AuthorizedCapitalDetails(BaseModel):
    """Specific fields for 'Increase in Authorized Capital' announcements."""
    board_approval: Optional[str] = None          # Yes / No
    date_of_board_meeting: Optional[str] = None   # D O B M
    existing_auth_eq_cap_inr: Optional[float] = None
    new_auth_eq_cap_inr: Optional[float] = None
    proposed_increase_inr: Optional[float] = None


class AIExtraction(BaseModel):
    """Fields extracted by the Gemini AI agent."""
    company_name: str
    ticker: Optional[str] = None
    sector: Optional[str] = None
    announcement_type: AnnouncementType = AnnouncementType.OTHER
    key_details: str = ""
    revenue_profit_impact: Optional[str] = None
    sentiment: Sentiment = Sentiment.NEUTRAL
    impact_level: ImpactLevel = ImpactLevel.LOW
    ai_insight: str = ""
    key_numbers: Optional[dict] = None
    # Type-specific fields
    authorized_capital: Optional[AuthorizedCapitalDetails] = None
    cmp: Optional[float] = None          # Current Market Price
    market_cap_cr: Optional[float] = None  # Market Cap in Crores


class Announcement(BaseModel):
    """Full announcement document stored in MongoDB."""
    id: Optional[str] = Field(default=None, alias="_id")
    exchange: Exchange
    company_name: str
    ticker: Optional[str] = None
    raw_subject: str
    raw_body: Optional[str] = None
    pdf_url: Optional[str] = None
    source_url: str
    announcement_date: datetime
    fetched_at: datetime = Field(default_factory=datetime.utcnow)
    processed: bool = False
    ai_data: Optional[AIExtraction] = None
    excel_row: Optional[dict] = None
    announcement_id: str  # Unique ID from exchange (for dedup)

    class Config:
        populate_by_name = True
        json_encoders = {datetime: lambda v: v.isoformat()}


class AnnouncementResponse(BaseModel):
    """API response model for frontend."""
    id: str
    exchange: str
    company_name: str
    ticker: Optional[str]
    announcement_type: str
    announcement_date: str
    key_details: str
    sentiment: str
    impact_level: str
    ai_insight: str
    sector: Optional[str]
    source_url: str
    cmp: Optional[float]
    market_cap_cr: Optional[float]
    processed: bool


class StatsResponse(BaseModel):
    """Statistics for dashboard."""
    total_announcements: int
    by_exchange: dict
    by_type: dict
    by_sentiment: dict
    by_impact: dict
    last_fetched: Optional[str]

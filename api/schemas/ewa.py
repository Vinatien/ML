"""
Pydantic schemas for EWA API requests and responses
"""
from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict
from datetime import datetime
from enum import Enum


# ========== Request Schemas ==========

class TransactionInput(BaseModel):
    """Single transaction for income inference"""
    amount: float = Field(..., description="Transaction amount (positive for credits)")
    date: datetime = Field(..., description="Transaction booking date")
    sender_name: str = Field(default="unknown", description="Sender/counterparty name")
    description: str = Field(default="", description="Transaction description")


class EWAEligibilityRequest(BaseModel):
    """
    Request for EWA eligibility check.
    Maps to all tree inputs.
    """
    
    # User identification
    user_id: int = Field(..., description="User ID")
    account_id: int = Field(..., description="Bank account ID")
    
    # Tree A: Basic eligibility
    user_age: int = Field(..., ge=0, le=120, description="User age in years")
    account_age_months: int = Field(..., ge=0, description="Bank account age in months")
    has_direct_deposit: bool = Field(..., description="Direct deposit detected")
    employment_months: int = Field(..., ge=0, description="Employment duration in months")
    
    # Tree B: Income inference
    inflow_transactions: List[TransactionInput] = Field(
        ...,
        description="Recent credit transactions (last 90 days recommended)"
    )
    
    # Tree C: Risk assessment
    nsf_count_90d: int = Field(default=0, ge=0, description="NSF count in last 90 days")
    income_volatility: float = Field(
        default=0.0,
        ge=0.0,
        le=2.0,
        description="Income volatility (coefficient of variation)"
    )
    prior_ewa_failed: bool = Field(default=False, description="Prior EWA repayment failed")
    rapid_redraw: bool = Field(
        default=False,
        description="Withdrew again within 7 days of repayment"
    )
    
    # Tree D: Advance calculation
    accrued_wage: float = Field(..., ge=0, description="Estimated wages accrued since last payday")
    hourly_wage: Optional[float] = Field(None, ge=0, description="Hourly wage rate")
    hours_worked: Optional[float] = Field(None, ge=0, description="Hours worked this period")
    
    # Tree E: Cooldown
    last_advance_date: Optional[datetime] = Field(None, description="Date of last EWA advance")
    last_repayment_date: Optional[datetime] = Field(None, description="Date of last repayment")
    recent_advance_count_7d: int = Field(
        default=0,
        ge=0,
        description="Number of advances in last 7 days"
    )
    
    # Optional
    requested_amount: Optional[float] = Field(None, ge=0, description="User requested amount")
    
    class Config:
        json_schema_extra = {
            "example": {
                "user_id": 123,
                "account_id": 456,
                "user_age": 28,
                "account_age_months": 6,
                "has_direct_deposit": True,
                "employment_months": 8,
                "inflow_transactions": [
                    {
                        "amount": 1500.0,
                        "date": "2025-12-15T00:00:00Z",
                        "sender_name": "ABC Company",
                        "description": "Payroll"
                    },
                    {
                        "amount": 1500.0,
                        "date": "2025-12-01T00:00:00Z",
                        "sender_name": "ABC Company",
                        "description": "Payroll"
                    }
                ],
                "nsf_count_90d": 0,
                "income_volatility": 0.1,
                "prior_ewa_failed": False,
                "rapid_redraw": False,
                "accrued_wage": 750.0,
                "requested_amount": 200.0
            }
        }


# ========== Response Schemas ==========

class RiskTierEnum(str, Enum):
    TIER_A = "A"
    TIER_B = "B"
    TIER_C = "C"
    TIER_D = "D"
    TIER_E = "E"


class EligibilityResultSchema(BaseModel):
    eligible: bool
    reason: str
    metadata: Dict


class IncomeInferenceSchema(BaseModel):
    has_payroll: bool
    payday_pattern: str
    estimated_payday: Optional[datetime]
    estimated_monthly_income: float
    confidence_score: float
    employer_detected: bool
    metadata: Dict


class RiskAssessmentSchema(BaseModel):
    tier: RiskTierEnum
    timing_risk: str
    behavioral_flags: List[str]
    days_to_payday: int
    score: float
    metadata: Dict


class AdvanceLimitSchema(BaseModel):
    max_amount: float
    accrued_wage: float
    percentage_allowed: float
    cap_applied: float
    time_decay_factor: float
    metadata: Dict


class CooldownDecisionSchema(BaseModel):
    allowed: bool
    cooldown_ends: Optional[datetime]
    hours_remaining: Optional[float]
    reason: str
    metadata: Dict


class EWAEligibilityResponse(BaseModel):
    """
    Complete EWA eligibility response with full decision chain.
    """
    
    # Overall verdict
    approved: bool
    max_advance_amount: float
    
    # Individual tree results
    eligibility: EligibilityResultSchema
    income_inference: IncomeInferenceSchema
    risk_assessment: RiskAssessmentSchema
    advance_limit: Optional[AdvanceLimitSchema]
    cooldown: CooldownDecisionSchema
    
    # Policy
    fee_amount: float
    fee_explanation: str
    
    # Explainability
    decision_path: List[str]
    rejection_reason: Optional[str]
    
    # Metadata
    timestamp: datetime
    user_id: int
    
    class Config:
        json_schema_extra = {
            "example": {
                "approved": True,
                "max_advance_amount": 225.50,
                "eligibility": {
                    "eligible": True,
                    "reason": "Passed all basic eligibility gates",
                    "metadata": {}
                },
                "income_inference": {
                    "has_payroll": True,
                    "payday_pattern": "biweekly",
                    "estimated_payday": "2025-12-29T00:00:00Z",
                    "estimated_monthly_income": 3000.0,
                    "confidence_score": 0.85,
                    "employer_detected": True,
                    "metadata": {}
                },
                "risk_assessment": {
                    "tier": "B",
                    "timing_risk": "low",
                    "behavioral_flags": [],
                    "days_to_payday": 2,
                    "score": 25.0,
                    "metadata": {}
                },
                "advance_limit": {
                    "max_amount": 225.50,
                    "accrued_wage": 750.0,
                    "percentage_allowed": 0.30,
                    "cap_applied": 400.0,
                    "time_decay_factor": 0.982,
                    "metadata": {}
                },
                "cooldown": {
                    "allowed": True,
                    "cooldown_ends": None,
                    "hours_remaining": None,
                    "reason": "First time user - no cooldown",
                    "metadata": {}
                },
                "fee_amount": 3.99,
                "fee_explanation": "Flat fee for Tier B",
                "decision_path": [
                    "Tree A: Checking basic eligibility gates...",
                    "✅ Tree A passed: All eligibility gates cleared",
                    "✅ ALL TREES PASSED - EWA APPROVED"
                ],
                "rejection_reason": None,
                "timestamp": "2025-12-27T10:30:00Z",
                "user_id": 123
            }
        }


class HealthResponse(BaseModel):
    """Health check response"""
    status: str
    service: str
    timestamp: datetime
    version: str

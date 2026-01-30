"""
Tree C: Risk Tier Assignment
Core EWA risk decision tree combining time-to-payday, behavior, and history

Updated with behavioral pattern analysis:
- Detects suspicious regular withdrawal patterns (fraud risk)
- Rewards irregular, variable withdrawals (legitimate emergency use)
- Incorporates financial constraint indicators
"""
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass
from enum import Enum


class RiskTier(str, Enum):
    """Risk tiers matching your design"""
    TIER_A = "A"  # Very low risk
    TIER_B = "B"  # Low risk
    TIER_C = "C"  # Medium risk
    TIER_D = "D"  # High risk
    TIER_E = "E"  # Blocked


@dataclass
class RiskAssessment:
    tier: RiskTier
    timing_risk: str  # 'low', 'medium', 'high'
    behavioral_flags: List[str]
    days_to_payday: int
    score: float  # 0-100, higher = riskier
    
    # New: Behavioral pattern indicators
    pattern_type: Optional[str] = None  # From BehavioralPatternAnalyzer
    appears_legitimate_use: bool = True  # True if pattern matches emergency spending
    financial_constraint_score: Optional[float] = None  # Higher = more constrained (good)
    
    metadata: Dict = None


class RiskTierTree:
    """
    Tree C: Risk tier assignment based on time-to-payday + behavioral signals
    
    This is NOT predicting default - it's controlling exposure over time,
    anchored to payday certainty.
    
    NEW: Incorporates behavioral pattern analysis to detect:
    - Fraud (regular, fixed-amount withdrawals)
    - Cash-out behavior (systematic exploitation)
    - Legitimate use (irregular, variable amounts matching spending shocks)
    """
    
    # Timing risk thresholds
    LOW_RISK_DAYS = 3
    MEDIUM_RISK_DAYS = 7
    
    # Behavioral thresholds
    NSF_THRESHOLD = 2  # NSF count in last 90 days
    INCOME_VOLATILITY_THRESHOLD = 0.3  # 30% coefficient of variation
    
    # NEW: Pattern risk adjustments
    PATTERN_RISK_WEIGHTS = {
        'irregular_healthy': -10,  # Reduce risk score (reward good behavior)
        'regular_suspicious': +25,  # Increase risk (potential fraud)
        'cash_out_risk': +40,  # Major risk increase
        'first_time': 0,  # Neutral
        'limited_history': +5  # Slight increase (less data)
    }
    
    def __init__(self):
        pass
    
    def assign_tier(
        self,
        days_to_payday: int,
        nsf_count_90d: int,
        income_volatility: float,  # coefficient of variation
        prior_ewa_failed: bool,
        rapid_redraw: bool,  # withdrew again within 7 days of repayment
        payday_confidence: float,  # from Tree B
        
        # NEW: Behavioral pattern inputs
        behavioral_pattern: Optional[str] = None,  # From BehavioralPatternAnalyzer
        pattern_risk_score: Optional[float] = None,  # Risk score from pattern analysis
        appears_shock_driven: Optional[bool] = None,  # True if irregular/variable
        financial_constraint_score: Optional[float] = None,  # Higher = more constrained
        monthly_income: Optional[float] = None  # For context
    ) -> RiskAssessment:
        """
        Assign risk tier based on timing + behavioral signals + usage patterns.
        
        NEW LOGIC:
        - Rewards irregular, variable withdrawal patterns (legitimate emergency use)
        - Penalizes regular, fixed-amount patterns (fraud/cash-out risk)
        - Considers financial constraint (lower income = target user)
        
        Returns:
            RiskAssessment with tier and detailed flags
        """
        
        # Step 1: Assess timing risk
        if days_to_payday <= self.LOW_RISK_DAYS:
            timing_risk = 'low'
            base_score = 10
        elif days_to_payday <= self.MEDIUM_RISK_DAYS:
            timing_risk = 'medium'
            base_score = 30
        else:
            timing_risk = 'high'
            base_score = 60
        
        # Step 2: Collect behavioral flags
        behavioral_flags = []
        behavioral_score = 0
        
        if nsf_count_90d >= self.NSF_THRESHOLD:
            behavioral_flags.append(f"nsf_high:{nsf_count_90d}")
            behavioral_score += 25
        
        if income_volatility > self.INCOME_VOLATILITY_THRESHOLD:
            behavioral_flags.append(f"income_volatile:{round(income_volatility, 2)}")
            behavioral_score += 20
        
        if prior_ewa_failed:
            behavioral_flags.append("prior_ewa_failed")
            behavioral_score += 30  # Heavy penalty
        
        if rapid_redraw:
            behavioral_flags.append("rapid_redraw")
            behavioral_score += 15
        
        # Low payday confidence = risk escalation
        if payday_confidence < 0.6:
            behavioral_flags.append(f"payday_uncertain:{round(payday_confidence, 2)}")
            behavioral_score += 20
        
        # Step 3: NEW - Apply behavioral pattern adjustment
        pattern_adjustment = 0
        appears_legitimate = True
        
        if behavioral_pattern and behavioral_pattern in self.PATTERN_RISK_WEIGHTS:
            pattern_adjustment = self.PATTERN_RISK_WEIGHTS[behavioral_pattern]
            behavioral_flags.append(f"pattern:{behavioral_pattern}")
            
            # Flag suspicious patterns
            if behavioral_pattern in ['regular_suspicious', 'cash_out_risk']:
                appears_legitimate = False
                behavioral_flags.append("⚠️_suspicious_usage_pattern")
        
        # Add pattern-specific risk score
        if pattern_risk_score and pattern_risk_score > 30:
            pattern_adjustment += (pattern_risk_score - 30) / 2  # Escalate high pattern risk
        
        # Step 4: NEW - Consider financial constraint (lower income = lower risk)
        income_adjustment = 0
        if financial_constraint_score and monthly_income:
            # Highly constrained + low income = likely legitimate user
            if financial_constraint_score > 60 and monthly_income < 3000:
                income_adjustment = -10  # Reduce risk score
                behavioral_flags.append(f"target_user:constrained_{int(financial_constraint_score)}")
            # High income but high EWA usage = suspicious
            elif monthly_income > 5000 and financial_constraint_score > 40:
                income_adjustment = +15
                behavioral_flags.append("⚠️_high_income_high_usage")
        
        # Step 5: Reward shock-driven behavior
        if appears_shock_driven is True:
            # Irregular, variable withdrawals = legitimate emergency use
            behavioral_score -= 15  # Reduce risk
            behavioral_flags.append("✓_shock_driven_pattern")
            appears_legitimate = True
        elif appears_shock_driven is False:
            # Regular, predictable = potential fraud
            behavioral_score += 20
            appears_legitimate = False
        
        # Step 6: Calculate total risk score
        total_score = (
            base_score + 
            behavioral_score + 
            pattern_adjustment + 
            income_adjustment
        )
        
        # Ensure score stays in valid range
        total_score = max(0, min(100, total_score))
        
        # Step 7: Map score to tier
        tier = self._score_to_tier(total_score, prior_ewa_failed, appears_legitimate)
        
        return RiskAssessment(
            tier=tier,
            timing_risk=timing_risk,
            behavioral_flags=behavioral_flags,
            days_to_payday=days_to_payday,
            score=total_score,
            pattern_type=behavioral_pattern,
            appears_legitimate_use=appears_legitimate,
            financial_constraint_score=financial_constraint_score,
            metadata={
                "base_score": base_score,
                "behavioral_score": behavioral_score,
                "pattern_adjustment": pattern_adjustment,
                "income_adjustment": income_adjustment,
                "nsf_count_90d": nsf_count_90d,
                "income_volatility": income_volatility,
                "payday_confidence": payday_confidence,
                "monthly_income": monthly_income,
                "appears_shock_driven": appears_shock_driven
            }
        )
    
    def _score_to_tier(
        self, 
        score: float, 
        prior_ewa_failed: bool,
        appears_legitimate: bool = True
    ) -> RiskTier:
        """
        Map risk score to tier with hard override for failures and fraud patterns.
        
        Score ranges (with behavioral adjustments):
        0-20: Tier A (very low risk - good behavior)
        21-40: Tier B (low risk)
        41-60: Tier C (medium risk)
        61-80: Tier D (high risk)
        81+: Tier E (blocked)
        
        NEW: Suspicious patterns can upgrade tier even with low score
        """
        # Hard override 1: prior EWA failure = automatic Tier E
        if prior_ewa_failed:
            return RiskTier.TIER_E  # Block until proven track record
        
        # Hard override 2: Suspicious pattern = minimum Tier C (even if low score)
        if not appears_legitimate and score < 41:
            return RiskTier.TIER_C  # Cautious approval for suspicious patterns
        
        # Standard scoring
        if score <= 20:
            return RiskTier.TIER_A
        elif score <= 40:
            return RiskTier.TIER_B
        elif score <= 60:
            return RiskTier.TIER_C
        elif score <= 80:
            return RiskTier.TIER_D
        else:
            return RiskTier.TIER_E

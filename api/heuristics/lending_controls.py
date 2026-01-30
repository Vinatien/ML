"""
Tree D: Max Advance Calculation
Tree E: Cooldown Logic

Lending control trees that determine HOW MUCH and HOW OFTEN

Updated with spending shock logic:
- Users only withdraw when: spending_shock > remaining_cash_from_paycheck
- Withdrawal amount should reflect excess spending need
- NOT fixed/regular amounts (those are fraud signals)
"""
from datetime import datetime, timedelta
from typing import Dict, Optional
from dataclasses import dataclass
import math

from .risk_tier import RiskTier


@dataclass
class AdvanceLimit:
    max_amount: float
    accrued_wage: float
    percentage_allowed: float
    cap_applied: float
    time_decay_factor: float
    
    # NEW: Spending shock alignment
    estimated_remaining_cash: Optional[float] = None
    appears_need_based: bool = True  # True if request aligns with spending shock model
    
    metadata: Dict = None


@dataclass
class CooldownDecision:
    allowed: bool
    cooldown_ends: Optional[datetime]
    hours_remaining: Optional[float]
    reason: str
    metadata: Dict


class LendingControlTrees:
    """
    Trees D & E: Control exposure through limits and cooldowns
    
    Key insight: EWA heuristics control exposure over time,
    NOT predicting default probability.
    
    NEW: Incorporates spending shock model
    - Withdrawal only makes sense when: shock > remaining_cash
    - Amount should match excess need, not predetermined value
    """
    
    # Tree D: Max advance caps per tier (% of accrued wage)
    # UPDATED: Lower caps to align with emergency use, not income replacement
    TIER_CAPS = {
        RiskTier.TIER_A: {
            'percentage': 0.35,  # 35% of accrued wage (reduced from 40%)
            'absolute_max': 400.0  # Reduced from 500
        },
        RiskTier.TIER_B: {
            'percentage': 0.25,  # Reduced from 30%
            'absolute_max': 300.0  # Reduced from 400
        },
        RiskTier.TIER_C: {
            'percentage': 0.15,
            'absolute_max': 150.0  # Reduced from 200
        },
        RiskTier.TIER_D: {
            'percentage': 0.0,  # No advance
            'absolute_max': 0.0
        },
        RiskTier.TIER_E: {
            'percentage': 0.0,  # Blocked
            'absolute_max': 0.0
        }
    }
    
    # Tree E: Base cooldown periods (hours)
    BASE_COOLDOWN_HOURS = {
        RiskTier.TIER_A: 24,
        RiskTier.TIER_B: 48,
        RiskTier.TIER_C: 72,
        RiskTier.TIER_D: 168,  # 7 days
        RiskTier.TIER_E: float('inf')  # Permanent until manual review
    }
    
    # Time decay parameters
    DECAY_LAMBDA = 0.1  # Exponential decay rate
    
    # Frequency penalty multiplier
    FREQUENCY_ALPHA = 0.5  # Cooldown multiplier per recent advance
    
    def __init__(self):
        pass
    
    # ========== Tree D: Max Advance ==========
    
    def calculate_max_advance(
        self,
        risk_tier: RiskTier,
        accrued_wage: float,
        days_to_payday: int,
        hourly_wage: Optional[float] = None,
        hours_worked: Optional[float] = None,
        
        # NEW: Spending shock context
        estimated_monthly_income: Optional[float] = None,
        days_since_payday: Optional[int] = None,
        recent_spending_spike: Optional[float] = None,  # Unusual high spending detected
        requested_amount: Optional[float] = None
    ) -> AdvanceLimit:
        """
        Calculate maximum advance amount with time-to-payday decay.
        
        Formula:
            Base Max = min(percentage × accrued_wage, absolute_cap)
            Effective Max = Base Max × exp(-λ × days_to_payday)
        
        NEW: Validates request against spending shock model
        - Estimates remaining cash from paycheck
        - Flags if request seems inconsistent with emergency use
        
        This enforces:
        - Closer to payday → more access
        - Further away → automatic shrinkage
        - Request should align with spending shock, not fixed habit
        
        Args:
            risk_tier: Assigned risk tier
            accrued_wage: Estimated wages earned since last payday
            days_to_payday: Days until next expected payday
            hourly_wage: Optional hourly rate (for more precise calculation)
            hours_worked: Optional hours worked this period
            estimated_monthly_income: User's estimated monthly income
            days_since_payday: Days since last paycheck
            recent_spending_spike: Any detected unusual spending
            requested_amount: Amount user is requesting
        
        Returns:
            AdvanceLimit with max_amount and breakdown
        """
        
        # Get tier-specific parameters
        tier_config = self.TIER_CAPS.get(risk_tier)
        if not tier_config:
            raise ValueError(f"Unknown risk tier: {risk_tier}")
        
        percentage = tier_config['percentage']
        absolute_cap = tier_config['absolute_max']
        
        # Base calculation
        base_max = min(
            percentage * accrued_wage,
            absolute_cap
        )
        
        # Apply time-to-payday exponential decay
        # exp(-λ × days) ensures:
        # - days = 0 → decay = 1.0 (full access)
        # - days = 7 → decay ≈ 0.50 (half access)
        # - days = 14 → decay ≈ 0.25 (quarter access)
        time_decay_factor = math.exp(-self.DECAY_LAMBDA * days_to_payday)
        
        effective_max = base_max * time_decay_factor
        
        # NEW: Estimate remaining cash from paycheck
        estimated_remaining_cash = None
        appears_need_based = True
        
        if estimated_monthly_income and days_since_payday is not None:
            # Estimate spending rate: assume 80% of income is spent over pay period
            payday_period = 14  # Assume biweekly default
            daily_spending_rate = (estimated_monthly_income * 0.8) / payday_period
            estimated_spent = daily_spending_rate * days_since_payday
            
            # Rough estimate of remaining cash
            estimated_remaining_cash = max(0, estimated_monthly_income / 2 - estimated_spent)
            
            # Validate: withdrawal should only happen when shock > remaining cash
            if requested_amount and estimated_remaining_cash > 0:
                # If user has cash left, they should only withdraw if shock > cash
                # If requesting amount > remaining cash, that's consistent with shock model
                if requested_amount <= estimated_remaining_cash * 0.5:
                    # Requesting small amount despite having cash = suspicious
                    appears_need_based = False
                elif requested_amount <= estimated_remaining_cash * 0.8:
                    # Borderline - might not actually need it
                    appears_need_based = True  # Give benefit of doubt
                else:
                    # Requesting more than remaining cash = likely legitimate shock
                    appears_need_based = True
        
        return AdvanceLimit(
            max_amount=round(effective_max, 2),
            accrued_wage=round(accrued_wage, 2),
            percentage_allowed=percentage,
            cap_applied=absolute_cap,
            time_decay_factor=round(time_decay_factor, 3),
            estimated_remaining_cash=round(estimated_remaining_cash, 2) if estimated_remaining_cash else None,
            appears_need_based=appears_need_based,
            metadata={
                'risk_tier': risk_tier.value,
                'base_max': round(base_max, 2),
                'days_to_payday': days_to_payday,
                'days_since_payday': days_since_payday,
                'decay_lambda': self.DECAY_LAMBDA,
                'calculation': f"{percentage:.0%} × ${accrued_wage:.2f} × decay({time_decay_factor:.3f})",
                'requested_amount': requested_amount,
                'spending_shock_aligned': appears_need_based
            }
        )
    
    # ========== Tree E: Cooldown Logic ==========
    
    def check_cooldown(
        self,
        risk_tier: RiskTier,
        last_advance_date: Optional[datetime],
        last_repayment_date: Optional[datetime],
        recent_advance_count_7d: int,
        current_time: Optional[datetime] = None
    ) -> CooldownDecision:
        """
        Determine if user is in cooldown period.
        
        Logic:
        1. Must repay successfully before next advance
        2. Base cooldown varies by risk tier
        3. Frequency penalty: Cooldown × (1 + α × recent_frequency)
        
        This discourages dependency loops without banning users.
        
        Args:
            risk_tier: Current risk tier
            last_advance_date: When last advance was issued
            last_repayment_date: When last advance was repaid
            recent_advance_count_7d: Number of advances in last 7 days
            current_time: Override current time (for testing)
        
        Returns:
            CooldownDecision with allowed status and remaining time
        """
        
        current_time = current_time or datetime.utcnow()
        
        # Case 1: No prior advance history → allowed
        if not last_advance_date:
            return CooldownDecision(
                allowed=True,
                cooldown_ends=None,
                hours_remaining=None,
                reason="First time user - no cooldown",
                metadata={"first_time": True}
            )
        
        # Case 2: Outstanding advance (not yet repaid) → blocked
        if last_repayment_date is None or last_repayment_date < last_advance_date:
            return CooldownDecision(
                allowed=False,
                cooldown_ends=None,
                hours_remaining=None,
                reason="Outstanding advance must be repaid first",
                metadata={
                    "last_advance": last_advance_date.isoformat(),
                    "outstanding": True
                }
            )
        
        # Case 3: Calculate time-based cooldown
        base_cooldown_hours = self.BASE_COOLDOWN_HOURS.get(risk_tier, 48)
        
        # Apply frequency penalty
        # Cooldown *= (1 + α × recent_frequency)
        frequency_multiplier = 1 + (self.FREQUENCY_ALPHA * recent_advance_count_7d)
        adjusted_cooldown_hours = base_cooldown_hours * frequency_multiplier
        
        # Check if cooldown has elapsed
        hours_since_last = (current_time - last_advance_date).total_seconds() / 3600
        
        if hours_since_last >= adjusted_cooldown_hours:
            return CooldownDecision(
                allowed=True,
                cooldown_ends=None,
                hours_remaining=None,
                reason="Cooldown period completed",
                metadata={
                    "base_cooldown_hours": base_cooldown_hours,
                    "frequency_multiplier": round(frequency_multiplier, 2),
                    "adjusted_cooldown_hours": round(adjusted_cooldown_hours, 1),
                    "hours_elapsed": round(hours_since_last, 1)
                }
            )
        
        # Still in cooldown
        hours_remaining = adjusted_cooldown_hours - hours_since_last
        cooldown_ends = last_advance_date + timedelta(hours=adjusted_cooldown_hours)
        
        return CooldownDecision(
            allowed=False,
            cooldown_ends=cooldown_ends,
            hours_remaining=round(hours_remaining, 1),
            reason=f"In cooldown period ({round(hours_remaining, 1)}h remaining)",
            metadata={
                "base_cooldown_hours": base_cooldown_hours,
                "frequency_multiplier": round(frequency_multiplier, 2),
                "adjusted_cooldown_hours": round(adjusted_cooldown_hours, 1),
                "hours_elapsed": round(hours_since_last, 1),
                "recent_advance_count_7d": recent_advance_count_7d,
                "cooldown_ends": cooldown_ends.isoformat()
            }
        )

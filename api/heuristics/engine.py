"""
EWA Heuristic Engine - Orchestrates all decision trees

This is the main entry point that:
1. Runs trees in sequence (A → B → C → D → E)
2. Combines outputs into final decision
3. Generates explainable reasoning
"""
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict

from .eligibility import BasicEligibilityTree, EligibilityResult
from .income_inference import IncomePaydayTree, IncomeInference
from .risk_tier import RiskTierTree, RiskAssessment, RiskTier
from .lending_controls import LendingControlTrees, AdvanceLimit, CooldownDecision


@dataclass
class EWADecision:
    """Final EWA decision with full reasoning chain"""
    
    # Overall verdict
    approved: bool
    max_advance_amount: float
    
    # Individual tree results
    eligibility: EligibilityResult
    income_inference: IncomeInference
    risk_assessment: RiskAssessment
    advance_limit: Optional[AdvanceLimit]
    cooldown: CooldownDecision
    
    # Policy
    fee_amount: float
    fee_explanation: str
    
    # Explainability
    decision_path: List[str]  # Step-by-step reasoning
    rejection_reason: Optional[str]
    
    # Metadata
    timestamp: datetime
    user_id: int
    

class EWAHeuristicEngine:
    """
    Main orchestrator for EWA heuristic decision system.
    
    Maps to your software design:
    - check_basic_eligibility() → Tree A
    - infer_income_payday() → Tree B
    - calculate_risk_score() → Tree C
    - max_advance() → Tree D
    - cooldown() → Tree E
    - fee_policy() → Policy Tree
    """
    
    def __init__(self):
        self.eligibility_tree = BasicEligibilityTree()
        self.income_tree = IncomePaydayTree()
        self.risk_tree = RiskTierTree()
        self.lending_trees = LendingControlTrees()
    
    def evaluate(
        self,
        user_id: int,
        # Tree A inputs
        user_age: int,
        account_age_months: int,
        has_direct_deposit: bool,
        employment_months: int,
        # Tree B inputs
        inflow_transactions: List[Dict],
        # Tree C inputs
        nsf_count_90d: int,
        income_volatility: float,
        prior_ewa_failed: bool,
        rapid_redraw: bool,
        # Tree D inputs
        accrued_wage: float,
        hourly_wage: Optional[float] = None,
        hours_worked: Optional[float] = None,
        # Tree E inputs
        last_advance_date: Optional[datetime] = None,
        last_repayment_date: Optional[datetime] = None,
        recent_advance_count_7d: int = 0,
        # Context
        requested_amount: Optional[float] = None,
        current_time: Optional[datetime] = None
    ) -> EWADecision:
        """
        Run full heuristic evaluation pipeline.
        
        Returns:
            EWADecision with complete reasoning chain
        """
        
        current_time = current_time or datetime.utcnow()
        decision_path = []
        
        # ========== Tree A: Basic Eligibility ==========
        decision_path.append("Tree A: Checking basic eligibility gates...")
        
        eligibility = self.eligibility_tree.check(
            user_age=user_age,
            account_age_months=account_age_months,
            has_direct_deposit=has_direct_deposit,
            employment_months=employment_months
        )
        
        if not eligibility.eligible:
            decision_path.append(f"❌ REJECTED at Tree A: {eligibility.reason}")
            return self._create_rejection(
                user_id=user_id,
                eligibility=eligibility,
                reason=eligibility.reason,
                decision_path=decision_path,
                timestamp=current_time
            )
        
        decision_path.append(f"✅ Tree A passed: All eligibility gates cleared")
        
        # ========== Tree B: Income & Payday Inference ==========
        decision_path.append("Tree B: Inferring income and payday pattern...")
        
        income_inference = self.income_tree.infer(
            inflow_transactions=inflow_transactions
        )
        
        if not income_inference.has_payroll:
            decision_path.append(f"❌ REJECTED at Tree B: No payroll pattern detected")
            return self._create_rejection(
                user_id=user_id,
                eligibility=eligibility,
                income_inference=income_inference,
                reason="No regular income pattern detected",
                decision_path=decision_path,
                timestamp=current_time
            )
        
        decision_path.append(
            f"✅ Tree B passed: {income_inference.payday_pattern} pattern detected "
            f"(confidence: {income_inference.confidence_score:.2f})"
        )
        
        # Calculate days to payday
        if income_inference.estimated_payday:
            days_to_payday = (income_inference.estimated_payday - current_time).days
        else:
            days_to_payday = 30  # Conservative default
        
        # ========== Tree C: Risk Tier Assignment ==========
        decision_path.append("Tree C: Assigning risk tier...")
        
        risk_assessment = self.risk_tree.assign_tier(
            days_to_payday=days_to_payday,
            nsf_count_90d=nsf_count_90d,
            income_volatility=income_volatility,
            prior_ewa_failed=prior_ewa_failed,
            rapid_redraw=rapid_redraw,
            payday_confidence=income_inference.confidence_score
        )
        
        decision_path.append(
            f"✅ Tree C: Risk Tier {risk_assessment.tier.value} "
            f"(score: {risk_assessment.score}, timing: {risk_assessment.timing_risk})"
        )
        
        if risk_assessment.behavioral_flags:
            decision_path.append(f"  ⚠️  Behavioral flags: {', '.join(risk_assessment.behavioral_flags)}")
        
        # ========== Tree D: Max Advance Calculation ==========
        decision_path.append("Tree D: Calculating max advance limit...")
        
        advance_limit = self.lending_trees.calculate_max_advance(
            risk_tier=risk_assessment.tier,
            accrued_wage=accrued_wage,
            days_to_payday=days_to_payday,
            hourly_wage=hourly_wage,
            hours_worked=hours_worked
        )
        
        if advance_limit.max_amount <= 0:
            decision_path.append(f"❌ REJECTED at Tree D: Risk tier {risk_assessment.tier.value} not eligible for advances")
            return self._create_rejection(
                user_id=user_id,
                eligibility=eligibility,
                income_inference=income_inference,
                risk_assessment=risk_assessment,
                advance_limit=advance_limit,
                reason=f"Risk tier {risk_assessment.tier.value} does not qualify for EWA",
                decision_path=decision_path,
                timestamp=current_time
            )
        
        decision_path.append(
            f"✅ Tree D: Max advance = ${advance_limit.max_amount:.2f} "
            f"({advance_limit.percentage_allowed:.0%} of ${advance_limit.accrued_wage:.2f}, "
            f"decay={advance_limit.time_decay_factor:.3f})"
        )
        
        # ========== Tree E: Cooldown Check ==========
        decision_path.append("Tree E: Checking cooldown status...")
        
        cooldown = self.lending_trees.check_cooldown(
            risk_tier=risk_assessment.tier,
            last_advance_date=last_advance_date,
            last_repayment_date=last_repayment_date,
            recent_advance_count_7d=recent_advance_count_7d,
            current_time=current_time
        )
        
        if not cooldown.allowed:
            decision_path.append(f"❌ REJECTED at Tree E: {cooldown.reason}")
            return self._create_rejection(
                user_id=user_id,
                eligibility=eligibility,
                income_inference=income_inference,
                risk_assessment=risk_assessment,
                advance_limit=advance_limit,
                cooldown=cooldown,
                reason=cooldown.reason,
                decision_path=decision_path,
                timestamp=current_time
            )
        
        decision_path.append(f"✅ Tree E passed: Cooldown check cleared")
        
        # ========== Fee Policy ==========
        decision_path.append("Fee Policy: Calculating fee structure...")
        
        fee_amount, fee_explanation = self._calculate_fee(
            risk_tier=risk_assessment.tier,
            advance_amount=requested_amount or advance_limit.max_amount
        )
        
        decision_path.append(f"  Fee: ${fee_amount:.2f} ({fee_explanation})")
        
        # ========== Final Approval ==========
        decision_path.append("✅ ALL TREES PASSED - EWA APPROVED")
        
        return EWADecision(
            approved=True,
            max_advance_amount=advance_limit.max_amount,
            eligibility=eligibility,
            income_inference=income_inference,
            risk_assessment=risk_assessment,
            advance_limit=advance_limit,
            cooldown=cooldown,
            fee_amount=fee_amount,
            fee_explanation=fee_explanation,
            decision_path=decision_path,
            rejection_reason=None,
            timestamp=current_time,
            user_id=user_id
        )
    
    def _create_rejection(
        self,
        user_id: int,
        eligibility: EligibilityResult,
        income_inference: Optional[IncomeInference] = None,
        risk_assessment: Optional[RiskAssessment] = None,
        advance_limit: Optional[AdvanceLimit] = None,
        cooldown: Optional[CooldownDecision] = None,
        reason: str = "",
        decision_path: List[str] = None,
        timestamp: datetime = None
    ) -> EWADecision:
        """Create rejection decision with partial results"""
        
        # Create dummy cooldown if not provided
        if cooldown is None:
            cooldown = CooldownDecision(
                allowed=False,
                cooldown_ends=None,
                hours_remaining=None,
                reason="Not evaluated - earlier rejection",
                metadata={}
            )
        
        # Create dummy income inference if not provided
        if income_inference is None:
            income_inference = IncomeInference(
                has_payroll=False,
                payday_pattern='unknown',
                estimated_payday=None,
                estimated_monthly_income=0.0,
                confidence_score=0.0,
                employer_detected=False,
                metadata={}
            )
        
        # Create dummy risk assessment if not provided
        if risk_assessment is None:
            risk_assessment = RiskAssessment(
                tier=RiskTier.TIER_E,
                timing_risk='unknown',
                behavioral_flags=[],
                days_to_payday=0,
                score=100,
                metadata={}
            )
        
        return EWADecision(
            approved=False,
            max_advance_amount=0.0,
            eligibility=eligibility,
            income_inference=income_inference,
            risk_assessment=risk_assessment,
            advance_limit=advance_limit,
            cooldown=cooldown,
            fee_amount=0.0,
            fee_explanation="No fee - application rejected",
            decision_path=decision_path or [],
            rejection_reason=reason,
            timestamp=timestamp or datetime.utcnow(),
            user_id=user_id
        )
    
    def _calculate_fee(self, risk_tier: RiskTier, advance_amount: float) -> tuple[float, str]:
        """
        Fee policy tree (policy-driven, not learned).
        
        Aligned with your math:
        - Flat fees stabilize expected value
        - Speed fees create convex loss exposure (DISALLOW)
        - Lifetime profit > per-transaction APR
        """
        
        # Flat fee structure by tier
        FLAT_FEES = {
            RiskTier.TIER_A: 2.99,
            RiskTier.TIER_B: 3.99,
            RiskTier.TIER_C: 4.99,
            RiskTier.TIER_D: 0.0,  # Not eligible
            RiskTier.TIER_E: 0.0   # Blocked
        }
        
        fee = FLAT_FEES.get(risk_tier, 0.0)
        
        explanation = f"Flat fee for Tier {risk_tier.value}"
        
        return fee, explanation

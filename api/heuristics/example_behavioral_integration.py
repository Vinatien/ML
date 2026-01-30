"""
Example: Integrating Behavioral Pattern Analysis into EWA Engine

This shows how to update the engine.py to incorporate the new
behavioral pattern detection based on updated business insights.
"""

from datetime import datetime
from typing import Dict, List, Optional

# Import the new analyzer
from heuristics.behavioral_patterns import BehavioralPatternAnalyzer


def evaluate_with_behavioral_analysis(
    # ... existing parameters ...
    user_id: int,
    user_age: int,
    account_age_months: int,
    has_direct_deposit: bool,
    employment_months: int,
    inflow_transactions: List[Dict],
    nsf_count_90d: int,
    income_volatility: float,
    prior_ewa_failed: bool,
    rapid_redraw: bool,
    accrued_wage: float,
    
    # NEW: Behavioral pattern inputs
    ewa_history: List[Dict],  # Historical EWA withdrawals
    estimated_monthly_income: float,  # From Tree B
    
    # Context
    requested_amount: Optional[float] = None,
    current_time: Optional[datetime] = None
) -> Dict:
    """
    Enhanced evaluation with behavioral pattern analysis.
    
    Changes from original:
    1. Run behavioral pattern analysis BEFORE risk tier assignment
    2. Pass pattern results to Tree C (risk tier)
    3. Pass spending context to Tree D (lending controls)
    4. Include pattern insights in final decision
    """
    
    current_time = current_time or datetime.utcnow()
    decision_path = []
    
    # ========== TREE A: Basic Eligibility ==========
    print("\n🌳 Tree A: Basic Eligibility Gates")
    eligibility_result = eligibility_tree.check_eligibility(
        user_age=user_age,
        account_age_months=account_age_months,
        has_direct_deposit=has_direct_deposit,
        employment_months=employment_months,
        nsf_count_90d=nsf_count_90d
    )
    
    if not eligibility_result.passed:
        decision_path.append(f"tree_a_failed:{eligibility_result.failure_reason}")
        return {
            'approved': False,
            'rejection_reason': eligibility_result.failure_reason,
            'decision_path': decision_path
        }
    
    decision_path.append("tree_a_passed")
    
    # ========== TREE B: Income & Payday Inference ==========
    print("\n🌳 Tree B: Income & Payday Inference")
    income_result = income_tree.infer(inflow_transactions)
    
    if not income_result.has_payroll:
        decision_path.append("tree_b_failed:no_payroll_detected")
        return {
            'approved': False,
            'rejection_reason': 'No regular payroll detected',
            'decision_path': decision_path
        }
    
    decision_path.append(f"tree_b_passed:{income_result.payday_pattern}")
    
    # Calculate days to/since payday
    last_payday = income_result.estimated_payday - timedelta(days=14)  # Rough estimate
    days_to_payday = (income_result.estimated_payday - current_time).days
    days_since_payday = (current_time - last_payday).days
    
    # ========== NEW: BEHAVIORAL PATTERN ANALYSIS ==========
    print("\n🔍 Behavioral Pattern Analysis (NEW)")
    
    pattern_analyzer = BehavioralPatternAnalyzer()
    
    # Generate estimated payday dates for pattern analysis
    estimated_payday_dates = []
    if income_result.payday_pattern == 'biweekly':
        for i in range(6):  # Last 3 months
            estimated_payday_dates.append(
                income_result.estimated_payday - timedelta(days=14 * i)
            )
    elif income_result.payday_pattern == 'monthly':
        for i in range(3):
            estimated_payday_dates.append(
                income_result.estimated_payday - timedelta(days=30 * i)
            )
    
    # Analyze pattern
    behavioral_pattern = pattern_analyzer.analyze_pattern(
        ewa_history=ewa_history,
        payday_pattern=income_result.payday_pattern,
        estimated_payday_dates=estimated_payday_dates,
        current_time=current_time
    )
    
    print(f"  Pattern Type: {behavioral_pattern.pattern_type}")
    print(f"  Timing Irregularity: {behavioral_pattern.timing_irregularity_score:.2f}")
    print(f"  Amount Variation: {behavioral_pattern.amount_variation_score:.2f}")
    print(f"  Appears Shock-Driven: {behavioral_pattern.appears_shock_driven}")
    print(f"  Red Flags: {behavioral_pattern.red_flags}")
    print(f"  Risk Score: {behavioral_pattern.risk_score}")
    
    # Add to decision path
    if behavioral_pattern.red_flags:
        for flag in behavioral_pattern.red_flags:
            decision_path.append(f"pattern_flag:{flag}")
    
    # Calculate financial constraint score
    if len(ewa_history) > 0:
        avg_ewa_amount = sum(tx['amount'] for tx in ewa_history) / len(ewa_history)
        # Estimate frequency (withdrawals per month)
        if len(ewa_history) > 1:
            date_range = (ewa_history[-1]['withdrawal_date'] - ewa_history[0]['withdrawal_date']).days
            ewa_freq_per_month = (len(ewa_history) / date_range) * 30 if date_range > 0 else 0
        else:
            ewa_freq_per_month = 0.5  # Assume low frequency
        
        financial_constraint = pattern_analyzer.calculate_financial_constraint_score(
            monthly_income=income_result.estimated_monthly_income,
            average_ewa_amount=avg_ewa_amount,
            ewa_frequency_per_month=ewa_freq_per_month
        )
    else:
        financial_constraint = None
    
    # ========== TREE C: Risk Tier Assignment (ENHANCED) ==========
    print("\n🌳 Tree C: Risk Tier Assignment (Enhanced)")
    
    risk_result = risk_tree.assign_tier(
        days_to_payday=days_to_payday,
        nsf_count_90d=nsf_count_90d,
        income_volatility=income_volatility,
        prior_ewa_failed=prior_ewa_failed,
        rapid_redraw=rapid_redraw,
        payday_confidence=income_result.confidence_score,
        
        # NEW: Behavioral pattern inputs
        behavioral_pattern=behavioral_pattern.pattern_type,
        pattern_risk_score=behavioral_pattern.risk_score,
        appears_shock_driven=behavioral_pattern.appears_shock_driven,
        financial_constraint_score=financial_constraint,
        monthly_income=income_result.estimated_monthly_income
    )
    
    print(f"  Risk Tier: {risk_result.tier}")
    print(f"  Risk Score: {risk_result.score}")
    print(f"  Appears Legitimate: {risk_result.appears_legitimate_use}")
    print(f"  Behavioral Flags: {risk_result.behavioral_flags}")
    
    decision_path.append(f"tree_c_tier:{risk_result.tier.value}")
    
    # Block if Tier D or E
    if risk_result.tier in [RiskTier.TIER_D, RiskTier.TIER_E]:
        decision_path.append(f"blocked:tier_{risk_result.tier.value}")
        
        # Provide specific rejection reason
        if behavioral_pattern.pattern_type == 'cash_out_risk':
            rejection_reason = "Suspicious cash-out pattern detected"
        elif behavioral_pattern.pattern_type == 'regular_suspicious':
            rejection_reason = "Irregular usage pattern - possible fraud"
        elif prior_ewa_failed:
            rejection_reason = "Prior EWA repayment failure"
        else:
            rejection_reason = f"Risk assessment: Tier {risk_result.tier.value}"
        
        return {
            'approved': False,
            'rejection_reason': rejection_reason,
            'risk_tier': risk_result.tier.value,
            'pattern_type': behavioral_pattern.pattern_type,
            'decision_path': decision_path
        }
    
    # ========== TREE D: Max Advance Calculation (ENHANCED) ==========
    print("\n🌳 Tree D: Max Advance Calculation (Enhanced)")
    
    advance_limit = lending_trees.calculate_max_advance(
        risk_tier=risk_result.tier,
        accrued_wage=accrued_wage,
        days_to_payday=days_to_payday,
        hourly_wage=hourly_wage,
        hours_worked=hours_worked,
        
        # NEW: Spending shock context
        estimated_monthly_income=income_result.estimated_monthly_income,
        days_since_payday=days_since_payday,
        requested_amount=requested_amount
    )
    
    print(f"  Max Advance: ${advance_limit.max_amount:.2f}")
    print(f"  Accrued Wage: ${advance_limit.accrued_wage:.2f}")
    print(f"  Time Decay: {advance_limit.time_decay_factor:.3f}")
    print(f"  Estimated Remaining Cash: ${advance_limit.estimated_remaining_cash:.2f}")
    print(f"  Appears Need-Based: {advance_limit.appears_need_based}")
    
    # Flag if request doesn't align with spending shock model
    if not advance_limit.appears_need_based and requested_amount:
        decision_path.append("⚠️_request_not_need_based")
        print("  ⚠️ WARNING: Request may not align with spending shock model")
    
    decision_path.append(f"tree_d_limit:{advance_limit.max_amount:.0f}")
    
    # ========== TREE E: Cooldown Check ==========
    print("\n🌳 Tree E: Cooldown Check")
    
    cooldown_result = lending_trees.check_cooldown(
        risk_tier=risk_result.tier,
        last_advance_date=last_advance_date,
        last_repayment_date=last_repayment_date,
        recent_advance_count_7d=recent_advance_count_7d,
        current_time=current_time
    )
    
    if not cooldown_result.allowed:
        decision_path.append(f"tree_e_cooldown_blocked:{cooldown_result.hours_remaining}h")
        return {
            'approved': False,
            'rejection_reason': cooldown_result.reason,
            'cooldown_ends': cooldown_result.cooldown_ends,
            'decision_path': decision_path
        }
    
    decision_path.append("tree_e_cooldown_ok")
    
    # ========== FINAL DECISION ==========
    print("\n✅ APPROVED")
    
    return {
        'approved': True,
        'max_advance_amount': advance_limit.max_amount,
        'accrued_wage': advance_limit.accrued_wage,
        
        # Income inference
        'payday_pattern': income_result.payday_pattern,
        'estimated_payday': income_result.estimated_payday,
        'estimated_monthly_income': income_result.estimated_monthly_income,
        
        # Risk assessment
        'risk_tier': risk_result.tier.value,
        'risk_score': risk_result.score,
        'timing_risk': risk_result.timing_risk,
        
        # NEW: Behavioral insights
        'pattern_type': behavioral_pattern.pattern_type,
        'appears_legitimate_use': risk_result.appears_legitimate_use,
        'timing_irregularity_score': behavioral_pattern.timing_irregularity_score,
        'amount_variation_score': behavioral_pattern.amount_variation_score,
        'appears_shock_driven': behavioral_pattern.appears_shock_driven,
        'financial_constraint_score': financial_constraint,
        
        # NEW: Spending shock validation
        'estimated_remaining_cash': advance_limit.estimated_remaining_cash,
        'appears_need_based': advance_limit.appears_need_based,
        
        # Explainability
        'decision_path': decision_path,
        'behavioral_flags': risk_result.behavioral_flags,
        'pattern_red_flags': behavioral_pattern.red_flags,
        
        'timestamp': current_time.isoformat()
    }


# ========== EXAMPLE USAGE ==========

if __name__ == "__main__":
    print("=" * 80)
    print("EXAMPLE 1: Legitimate Emergency User")
    print("=" * 80)
    
    result1 = evaluate_with_behavioral_analysis(
        user_id=123,
        user_age=28,
        account_age_months=8,
        has_direct_deposit=True,
        employment_months=12,
        inflow_transactions=[
            {'amount': 1500, 'date': datetime(2026, 1, 15), 'sender_name': 'ACME Corp'},
            {'amount': 1500, 'date': datetime(2026, 1, 1), 'sender_name': 'ACME Corp'},
            {'amount': 1500, 'date': datetime(2025, 12, 18), 'sender_name': 'ACME Corp'}
        ],
        nsf_count_90d=0,
        income_volatility=0.12,
        prior_ewa_failed=False,
        rapid_redraw=False,
        accrued_wage=450.0,
        
        # Irregular, variable withdrawal pattern
        ewa_history=[
            {'amount': 150, 'withdrawal_date': datetime(2025, 11, 5), 'status': 'repaid'},
            {'amount': 280, 'withdrawal_date': datetime(2025, 12, 3), 'status': 'repaid'},
            {'amount': 95, 'withdrawal_date': datetime(2026, 1, 8), 'status': 'repaid'}
        ],
        estimated_monthly_income=3200,
        requested_amount=200.0,
        current_time=datetime(2026, 1, 22)
    )
    
    print(f"\nDecision: {'✅ APPROVED' if result1['approved'] else '❌ REJECTED'}")
    if result1['approved']:
        print(f"Max Amount: ${result1['max_advance_amount']:.2f}")
        print(f"Risk Tier: {result1['risk_tier']}")
        print(f"Pattern Type: {result1['pattern_type']}")
        print(f"Appears Legitimate: {result1['appears_legitimate_use']}")
    
    print("\n" + "=" * 80)
    print("EXAMPLE 2: Suspicious Regular Pattern")
    print("=" * 80)
    
    result2 = evaluate_with_behavioral_analysis(
        user_id=456,
        user_age=35,
        account_age_months=6,
        has_direct_deposit=True,
        employment_months=10,
        inflow_transactions=[
            {'amount': 2200, 'date': datetime(2026, 1, 15), 'sender_name': 'BigCo Inc'},
            {'amount': 2200, 'date': datetime(2026, 1, 1), 'sender_name': 'BigCo Inc'},
            {'amount': 2200, 'date': datetime(2025, 12, 18), 'sender_name': 'BigCo Inc'}
        ],
        nsf_count_90d=0,
        income_volatility=0.08,
        prior_ewa_failed=False,
        rapid_redraw=False,
        accrued_wage=600.0,
        
        # Regular, fixed-amount pattern (SUSPICIOUS)
        ewa_history=[
            {'amount': 200, 'withdrawal_date': datetime(2025, 11, 7), 'status': 'repaid'},
            {'amount': 200, 'withdrawal_date': datetime(2025, 11, 21), 'status': 'repaid'},
            {'amount': 200, 'withdrawal_date': datetime(2025, 12, 5), 'status': 'repaid'},
            {'amount': 200, 'withdrawal_date': datetime(2025, 12, 19), 'status': 'repaid'},
            {'amount': 200, 'withdrawal_date': datetime(2026, 1, 2), 'status': 'repaid'},
            {'amount': 200, 'withdrawal_date': datetime(2026, 1, 16), 'status': 'repaid'}
        ],
        estimated_monthly_income=4400,
        requested_amount=200.0,
        current_time=datetime(2026, 1, 22)
    )
    
    print(f"\nDecision: {'✅ APPROVED' if result2['approved'] else '❌ REJECTED'}")
    if not result2['approved']:
        print(f"Rejection Reason: {result2['rejection_reason']}")
        print(f"Pattern Type: {result2['pattern_type']}")
    elif result2.get('pattern_red_flags'):
        print(f"⚠️ Approved with warnings:")
        print(f"Max Amount: ${result2['max_advance_amount']:.2f}")
        print(f"Risk Tier: {result2['risk_tier']}")
        print(f"Pattern Type: {result2['pattern_type']}")
        print(f"Red Flags: {result2['pattern_red_flags']}")

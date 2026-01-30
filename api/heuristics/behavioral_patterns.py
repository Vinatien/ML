"""
Behavioral Pattern Analysis for EWA
Based on empirical insights about EWA consumer behavior

Key Insights:
1. EWA users are typically lower income, financially constrained workers
2. Usage timing should be IRREGULAR within and across pay cycles
3. Withdrawals should be VARIABLE amounts (reflecting spending shocks)
4. Users only withdraw when: spending_shock > remaining_cash_from_paycheck
5. Withdrawal amount should match excess spending need, not fixed amounts

This module detects ANOMALOUS patterns that suggest:
- Fraud (regular, predictable withdrawals)
- Cash-out behavior (systematic exploitation)
- Misuse (using as regular income supplement vs emergency buffer)
"""
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from dataclasses import dataclass
from collections import Counter
import statistics
import math


@dataclass
class BehavioralPattern:
    """Analysis of user's EWA usage pattern"""
    
    # Pattern classification
    pattern_type: str  # 'irregular_healthy', 'regular_suspicious', 'cash_out_risk', 'first_time'
    
    # Irregularity metrics (HIGHER is better for legitimate use)
    timing_irregularity_score: float  # 0.0-1.0, higher = more irregular (good)
    amount_variation_score: float  # 0.0-1.0, higher = more variable (good)
    
    # Spending shock alignment
    appears_shock_driven: bool  # True if pattern aligns with emergency spending
    average_days_into_cycle: float  # Where in pay cycle withdrawals happen
    
    # Red flags
    red_flags: List[str]
    risk_score: float  # 0-100, higher = more suspicious
    
    # Supporting data
    metadata: Dict


class BehavioralPatternAnalyzer:
    """
    Analyze EWA usage patterns to distinguish legitimate emergency use
    from systematic exploitation or fraud
    """
    
    # Thresholds for pattern detection
    MIN_HISTORY_FOR_PATTERN = 3  # Need at least 3 withdrawals to detect pattern
    
    # Irregularity thresholds (we WANT high irregularity)
    HEALTHY_TIMING_CV = 0.4  # Coefficient of variation in timing
    HEALTHY_AMOUNT_CV = 0.3  # Coefficient of variation in amounts
    
    # Suspicious regularity patterns
    SUSPICIOUS_DAY_CLUSTERING = 0.8  # If 80%+ of withdrawals on same weekday
    SUSPICIOUS_AMOUNT_CLUSTERING = 0.7  # If amounts cluster tightly
    
    # Expected behavior: withdrawals tilted toward end of pay cycle
    EXPECTED_AVG_CYCLE_POSITION = 0.6  # 60% through pay cycle on average
    
    def __init__(self):
        pass
    
    def analyze_pattern(
        self,
        ewa_history: List[Dict],
        payday_pattern: str,  # 'weekly', 'biweekly', 'monthly'
        estimated_payday_dates: List[datetime],
        current_time: Optional[datetime] = None
    ) -> BehavioralPattern:
        """
        Analyze historical EWA usage to detect healthy vs suspicious patterns.
        
        Args:
            ewa_history: List of past EWA withdrawals with:
                - amount: float
                - withdrawal_date: datetime
                - repayment_date: datetime (if repaid)
                - status: str ('repaid', 'outstanding', 'failed')
            payday_pattern: User's detected payday frequency
            estimated_payday_dates: List of estimated payday dates
            current_time: Override for testing
        
        Returns:
            BehavioralPattern with risk classification
        """
        
        current_time = current_time or datetime.utcnow()
        
        # Case 1: First time user - no pattern yet
        if len(ewa_history) < 1:
            return BehavioralPattern(
                pattern_type='first_time',
                timing_irregularity_score=1.0,  # Assume healthy
                amount_variation_score=1.0,  # Assume healthy
                appears_shock_driven=True,
                average_days_into_cycle=0.0,
                red_flags=[],
                risk_score=0.0,
                metadata={'note': 'First time user - no pattern history'}
            )
        
        # Case 2: Limited history - not enough to detect pattern
        if len(ewa_history) < self.MIN_HISTORY_FOR_PATTERN:
            return BehavioralPattern(
                pattern_type='limited_history',
                timing_irregularity_score=0.8,  # Give benefit of doubt
                amount_variation_score=0.8,
                appears_shock_driven=True,
                average_days_into_cycle=0.0,
                red_flags=[],
                risk_score=10.0,
                metadata={
                    'note': 'Limited history - provisional approval',
                    'withdrawal_count': len(ewa_history)
                }
            )
        
        # Analyze pattern with sufficient history
        return self._analyze_established_pattern(
            ewa_history, payday_pattern, estimated_payday_dates, current_time
        )
    
    def _analyze_established_pattern(
        self,
        ewa_history: List[Dict],
        payday_pattern: str,
        estimated_payday_dates: List[datetime],
        current_time: datetime
    ) -> BehavioralPattern:
        """Analyze pattern for users with established history"""
        
        red_flags = []
        risk_score = 0.0
        
        # Extract withdrawal data
        withdrawal_dates = [tx['withdrawal_date'] for tx in ewa_history]
        withdrawal_amounts = [tx['amount'] for tx in ewa_history]
        
        # ===== Analysis 1: Timing Irregularity (SHOULD be irregular) =====
        timing_analysis = self._analyze_timing_pattern(
            withdrawal_dates, estimated_payday_dates, payday_pattern
        )
        
        if timing_analysis['is_suspiciously_regular']:
            red_flags.append(f"regular_timing_pattern:{timing_analysis['regularity_type']}")
            risk_score += 30
        
        # ===== Analysis 2: Amount Variation (SHOULD be variable) =====
        amount_analysis = self._analyze_amount_pattern(withdrawal_amounts)
        
        if amount_analysis['is_suspiciously_uniform']:
            red_flags.append(f"fixed_amounts:{amount_analysis['common_amount']}")
            risk_score += 35
        
        # ===== Analysis 3: Cycle Position (SHOULD tilt toward end) =====
        cycle_analysis = self._analyze_cycle_position(
            withdrawal_dates, estimated_payday_dates, payday_pattern
        )
        
        if cycle_analysis['avg_position'] < 0.3:  # Too early in cycle
            red_flags.append(f"withdraws_too_early:avg_{cycle_analysis['avg_position']:.2f}")
            risk_score += 20
        
        # ===== Analysis 4: Frequency Pattern (SHOULD be sporadic, not systematic) =====
        frequency_analysis = self._analyze_frequency_pattern(withdrawal_dates)
        
        if frequency_analysis['is_high_frequency']:
            red_flags.append(f"high_frequency:{frequency_analysis['avg_days_between']}")
            risk_score += 25
        
        # ===== Analysis 5: Cash-out Risk (withdrawing max every time) =====
        cashout_risk = self._detect_cashout_behavior(ewa_history)
        
        if cashout_risk['is_cash_out']:
            red_flags.append(f"cash_out_pattern:{cashout_risk['reason']}")
            risk_score += 40
        
        # ===== Classify Overall Pattern =====
        if risk_score >= 60:
            pattern_type = 'cash_out_risk'
        elif risk_score >= 30:
            pattern_type = 'regular_suspicious'
        else:
            pattern_type = 'irregular_healthy'
        
        # Determine if appears shock-driven
        appears_shock_driven = (
            amount_analysis['cv'] >= self.HEALTHY_AMOUNT_CV and
            timing_analysis['cv'] >= self.HEALTHY_TIMING_CV and
            not cashout_risk['is_cash_out']
        )
        
        return BehavioralPattern(
            pattern_type=pattern_type,
            timing_irregularity_score=timing_analysis['irregularity_score'],
            amount_variation_score=amount_analysis['variation_score'],
            appears_shock_driven=appears_shock_driven,
            average_days_into_cycle=cycle_analysis['avg_position'],
            red_flags=red_flags,
            risk_score=risk_score,
            metadata={
                'withdrawal_count': len(ewa_history),
                'timing_cv': timing_analysis['cv'],
                'amount_cv': amount_analysis['cv'],
                'avg_amount': round(statistics.mean(withdrawal_amounts), 2),
                'avg_days_between': frequency_analysis['avg_days_between'],
                'cycle_position_distribution': cycle_analysis['position_distribution']
            }
        )
    
    def _analyze_timing_pattern(
        self,
        withdrawal_dates: List[datetime],
        payday_dates: List[datetime],
        payday_pattern: str
    ) -> Dict:
        """
        Analyze timing regularity. Legitimate EWA should be IRREGULAR.
        Returns higher irregularity_score for healthier patterns.
        """
        
        # Calculate days between consecutive withdrawals
        sorted_dates = sorted(withdrawal_dates)
        intervals = []
        for i in range(1, len(sorted_dates)):
            days_between = (sorted_dates[i] - sorted_dates[i-1]).days
            intervals.append(days_between)
        
        if not intervals:
            return {
                'is_suspiciously_regular': False,
                'regularity_type': 'none',
                'cv': 1.0,
                'irregularity_score': 1.0
            }
        
        # Calculate coefficient of variation (higher = more irregular = better)
        mean_interval = statistics.mean(intervals)
        std_interval = statistics.stdev(intervals) if len(intervals) > 1 else 0
        cv = std_interval / mean_interval if mean_interval > 0 else 0
        
        # Check for suspicious regularity patterns
        is_regular = False
        regularity_type = 'none'
        
        # Pattern 1: Fixed interval (e.g., every 7 days, every 14 days)
        if cv < 0.15 and mean_interval < 21:  # Very regular, short interval
            is_regular = True
            regularity_type = f'fixed_interval_{int(mean_interval)}d'
        
        # Pattern 2: Same weekday clustering
        weekday_counts = Counter(d.weekday() for d in withdrawal_dates)
        max_weekday_freq = max(weekday_counts.values()) / len(withdrawal_dates)
        if max_weekday_freq >= self.SUSPICIOUS_DAY_CLUSTERING:
            is_regular = True
            regularity_type = f'weekday_clustering_{max_weekday_freq:.0%}'
        
        # Convert CV to irregularity score (0-1, higher is better)
        irregularity_score = min(cv / self.HEALTHY_TIMING_CV, 1.0)
        
        return {
            'is_suspiciously_regular': is_regular,
            'regularity_type': regularity_type,
            'cv': round(cv, 3),
            'irregularity_score': round(irregularity_score, 3),
            'mean_interval_days': round(mean_interval, 1)
        }
    
    def _analyze_amount_pattern(self, amounts: List[float]) -> Dict:
        """
        Analyze amount variation. Legitimate EWA should have VARIABLE amounts
        reflecting different spending shocks.
        """
        
        if len(amounts) < 2:
            return {
                'is_suspiciously_uniform': False,
                'common_amount': None,
                'cv': 1.0,
                'variation_score': 1.0
            }
        
        # Calculate coefficient of variation
        mean_amount = statistics.mean(amounts)
        std_amount = statistics.stdev(amounts)
        cv = std_amount / mean_amount if mean_amount > 0 else 0
        
        # Check for repeated exact amounts (suspicious)
        amount_counts = Counter(amounts)
        most_common_amount, frequency = amount_counts.most_common(1)[0]
        amount_clustering = frequency / len(amounts)
        
        is_uniform = (
            cv < 0.15 or  # Very low variation
            amount_clustering >= self.SUSPICIOUS_AMOUNT_CLUSTERING  # Same amount repeatedly
        )
        
        # Convert CV to variation score (0-1, higher is better)
        variation_score = min(cv / self.HEALTHY_AMOUNT_CV, 1.0)
        
        return {
            'is_suspiciously_uniform': is_uniform,
            'common_amount': round(most_common_amount, 2) if is_uniform else None,
            'cv': round(cv, 3),
            'variation_score': round(variation_score, 3),
            'amount_clustering': round(amount_clustering, 2)
        }
    
    def _analyze_cycle_position(
        self,
        withdrawal_dates: List[datetime],
        payday_dates: List[datetime],
        payday_pattern: str
    ) -> Dict:
        """
        Analyze where in pay cycle withdrawals occur.
        Should tilt toward END of cycle (financially constrained users).
        """
        
        cycle_length = {
            'weekly': 7,
            'biweekly': 14,
            'monthly': 30
        }.get(payday_pattern, 14)
        
        positions = []  # Position in cycle (0.0 = payday, 1.0 = day before next payday)
        
        for withdrawal_date in withdrawal_dates:
            # Find nearest prior payday
            prior_paydays = [pd for pd in payday_dates if pd <= withdrawal_date]
            if not prior_paydays:
                continue
            
            last_payday = max(prior_paydays)
            days_since_payday = (withdrawal_date - last_payday).days
            
            # Normalize to 0-1 scale
            position = min(days_since_payday / cycle_length, 1.0)
            positions.append(position)
        
        if not positions:
            return {
                'avg_position': 0.5,
                'position_distribution': {}
            }
        
        avg_position = statistics.mean(positions)
        
        # Bin into early/mid/late cycle
        early = sum(1 for p in positions if p < 0.33) / len(positions)
        mid = sum(1 for p in positions if 0.33 <= p < 0.67) / len(positions)
        late = sum(1 for p in positions if p >= 0.67) / len(positions)
        
        return {
            'avg_position': round(avg_position, 2),
            'position_distribution': {
                'early_cycle': round(early, 2),
                'mid_cycle': round(mid, 2),
                'late_cycle': round(late, 2)
            }
        }
    
    def _analyze_frequency_pattern(self, withdrawal_dates: List[datetime]) -> Dict:
        """
        Analyze withdrawal frequency. Should be SPORADIC, not systematic.
        """
        
        sorted_dates = sorted(withdrawal_dates)
        intervals = []
        for i in range(1, len(sorted_dates)):
            days_between = (sorted_dates[i] - sorted_dates[i-1]).days
            intervals.append(days_between)
        
        if not intervals:
            return {
                'is_high_frequency': False,
                'avg_days_between': 999
            }
        
        avg_days_between = statistics.mean(intervals)
        
        # High frequency = suspicious (using as regular income supplement)
        is_high_frequency = avg_days_between < 10  # More than once every 10 days
        
        return {
            'is_high_frequency': is_high_frequency,
            'avg_days_between': round(avg_days_between, 1)
        }
    
    def _detect_cashout_behavior(self, ewa_history: List[Dict]) -> Dict:
        """
        Detect cash-out pattern: always withdrawing maximum available.
        This suggests systematic exploitation, not emergency use.
        """
        
        # Check if user consistently withdraws near-maximum amounts
        # (This would be populated from lending limits in real system)
        
        # For now, check if amounts are consistently high relative to user's pattern
        amounts = [tx['amount'] for tx in ewa_history]
        
        if len(amounts) < 3:
            return {'is_cash_out': False, 'reason': None}
        
        # Check if always withdrawing similar high amounts
        mean_amount = statistics.mean(amounts)
        max_amount = max(amounts)
        
        # If most withdrawals are near max, likely cash-out behavior
        near_max_count = sum(1 for a in amounts if a >= max_amount * 0.9)
        near_max_ratio = near_max_count / len(amounts)
        
        is_cash_out = near_max_ratio >= 0.8  # 80%+ of withdrawals at max
        reason = f"max_amount_ratio_{near_max_ratio:.0%}" if is_cash_out else None
        
        return {
            'is_cash_out': is_cash_out,
            'reason': reason,
            'near_max_ratio': round(near_max_ratio, 2)
        }
    
    def calculate_financial_constraint_score(
        self,
        monthly_income: float,
        average_ewa_amount: float,
        ewa_frequency_per_month: float
    ) -> float:
        """
        Score how financially constrained a user appears.
        Higher score = more constrained (legitimate EWA target user).
        
        Insight: EWA is attractive to lower income, financially constrained workers.
        
        Returns:
            Score 0-100, higher = more financially constrained
        """
        
        # Factor 1: Income level (lower income = higher constraint)
        # Assuming $4000/month as median, scale accordingly
        income_constraint = max(0, 100 - (monthly_income / 40))  # Lower income = higher score
        
        # Factor 2: EWA dependency ratio
        monthly_ewa_volume = average_ewa_amount * ewa_frequency_per_month
        dependency_ratio = monthly_ewa_volume / monthly_income if monthly_income > 0 else 0
        dependency_constraint = min(dependency_ratio * 200, 100)  # Cap at 100
        
        # Weighted average
        constraint_score = (income_constraint * 0.6) + (dependency_constraint * 0.4)
        
        return round(constraint_score, 1)

"""
Tree B: Income & Payday Inference
Structured pattern matching (NOT ML) to detect wages from transactions
"""
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from collections import Counter
import statistics


@dataclass
class IncomeInference:
    has_payroll: bool
    payday_pattern: str  # 'weekly', 'biweekly', 'monthly', 'irregular', 'unknown'
    estimated_payday: Optional[datetime]
    estimated_monthly_income: float
    confidence_score: float  # 0.0 to 1.0
    employer_detected: bool
    metadata: Dict


class IncomePaydayTree:
    """
    Tree B: Infer income facts from transaction patterns
    
    This is NOT ML - it's structured pattern matching to answer:
    "Do they actually have wages?"
    """
    
    # Pattern detection thresholds
    MIN_RECURRING_INFLOWS = 2
    WEEKLY_DAYS = 7
    BIWEEKLY_DAYS = 14
    MONTHLY_DAYS = 30
    TOLERANCE_DAYS = 3  # Allow ±3 days variance
    
    # Minimum credible amounts (filter out small transfers)
    MIN_PAYROLL_AMOUNT = 100.0  # Adjust per market
    
    def __init__(self):
        pass
    
    def infer(
        self,
        inflow_transactions: List[Dict]
    ) -> IncomeInference:
        """
        Analyze transaction history to detect payroll patterns.
        
        Args:
            inflow_transactions: List of credit transactions with:
                - amount: float
                - date: datetime
                - sender_name: str
                - description: str
        
        Returns:
            IncomeInference with payday estimate and confidence
        """
        
        # Filter to credible payroll amounts
        payroll_candidates = [
            tx for tx in inflow_transactions 
            if tx['amount'] >= self.MIN_PAYROLL_AMOUNT
        ]
        
        if len(payroll_candidates) < self.MIN_RECURRING_INFLOWS:
            return IncomeInference(
                has_payroll=False,
                payday_pattern='unknown',
                estimated_payday=None,
                estimated_monthly_income=0.0,
                confidence_score=0.0,
                employer_detected=False,
                metadata={"reason": "Insufficient recurring inflows", "count": len(payroll_candidates)}
            )
        
        # Sort by date
        payroll_candidates.sort(key=lambda x: x['date'])
        
        # Calculate intervals between transactions
        intervals = []
        for i in range(1, len(payroll_candidates)):
            days_between = (payroll_candidates[i]['date'] - payroll_candidates[i-1]['date']).days
            intervals.append(days_between)
        
        if not intervals:
            return IncomeInference(
                has_payroll=False,
                payday_pattern='unknown',
                estimated_payday=None,
                estimated_monthly_income=0.0,
                confidence_score=0.0,
                employer_detected=False,
                metadata={"reason": "Not enough transactions to detect pattern"}
            )
        
        # Detect pattern regularity
        avg_interval = statistics.mean(intervals)
        std_interval = statistics.stdev(intervals) if len(intervals) > 1 else 0
        
        # Classify payday pattern
        pattern, confidence = self._classify_pattern(avg_interval, std_interval)
        
        # Check for same sender (employer detection)
        sender_counts = Counter(tx.get('sender_name', 'unknown') for tx in payroll_candidates)
        most_common_sender, sender_frequency = sender_counts.most_common(1)[0]
        employer_detected = sender_frequency >= len(payroll_candidates) * 0.7  # 70% threshold
        
        # Estimate next payday
        last_payday = payroll_candidates[-1]['date']
        estimated_payday = last_payday + timedelta(days=avg_interval)
        
        # Estimate monthly income (normalize to 30 days)
        avg_amount = statistics.mean(tx['amount'] for tx in payroll_candidates)
        estimated_monthly_income = (avg_amount / avg_interval) * 30 if avg_interval > 0 else 0
        
        # Adjust confidence based on employer detection
        if employer_detected:
            confidence = min(confidence + 0.2, 1.0)
        
        return IncomeInference(
            has_payroll=True,
            payday_pattern=pattern,
            estimated_payday=estimated_payday,
            estimated_monthly_income=estimated_monthly_income,
            confidence_score=confidence,
            employer_detected=employer_detected,
            metadata={
                "avg_interval_days": round(avg_interval, 1),
                "interval_std": round(std_interval, 1),
                "transaction_count": len(payroll_candidates),
                "avg_amount": round(avg_amount, 2),
                "primary_sender": most_common_sender,
                "sender_consistency": round(sender_frequency / len(payroll_candidates), 2)
            }
        )
    
    def _classify_pattern(self, avg_interval: float, std_interval: float) -> Tuple[str, float]:
        """
        Classify payday pattern and assign confidence score.
        
        Returns:
            (pattern_name, confidence_score)
        """
        # Low variance = high confidence
        variance_penalty = min(std_interval / avg_interval, 0.5) if avg_interval > 0 else 0.5
        base_confidence = 1.0 - variance_penalty
        
        # Weekly pattern
        if abs(avg_interval - self.WEEKLY_DAYS) <= self.TOLERANCE_DAYS:
            return 'weekly', base_confidence
        
        # Biweekly pattern
        if abs(avg_interval - self.BIWEEKLY_DAYS) <= self.TOLERANCE_DAYS:
            return 'biweekly', base_confidence
        
        # Monthly pattern
        if abs(avg_interval - self.MONTHLY_DAYS) <= self.TOLERANCE_DAYS * 2:  # More tolerance for monthly
            return 'monthly', base_confidence
        
        # Irregular but recurring
        if std_interval / avg_interval < 0.3:  # Within 30% variance
            return 'irregular', base_confidence * 0.6  # Lower confidence
        
        return 'unknown', base_confidence * 0.3

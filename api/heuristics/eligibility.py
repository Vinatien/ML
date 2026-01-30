"""
Tree A: Basic Eligibility Gates
Hard requirements for EWA access
"""
from datetime import datetime, timedelta
from typing import Dict, Tuple
from dataclasses import dataclass


@dataclass
class EligibilityResult:
    eligible: bool
    reason: str
    metadata: Dict = None


class BasicEligibilityTree:
    """
    Tree A: Hard eligibility gates (binary tree)
    These are existence + compliance checks with ZERO false positives.
    """
    
    # Policy constants (configure per market/regulatory requirements)
    MIN_AGE = 18
    MIN_ACCOUNT_AGE_MONTHS = 2
    MIN_EMPLOYMENT_MONTHS = 2
    
    def __init__(self):
        pass
    
    def check(
        self, 
        user_age: int,
        account_age_months: int,
        has_direct_deposit: bool,
        employment_months: int
    ) -> EligibilityResult:
        """
        Binary decision tree for basic eligibility.
        
        Returns:
            EligibilityResult with eligible=True only if ALL gates pass
        """
        
        # Gate 1: Age compliance
        if user_age < self.MIN_AGE:
            return EligibilityResult(
                eligible=False,
                reason=f"User age ({user_age}) below minimum ({self.MIN_AGE})",
                metadata={"gate": "age", "required": self.MIN_AGE, "actual": user_age}
            )
        
        # Gate 2: Account maturity
        if account_age_months < self.MIN_ACCOUNT_AGE_MONTHS:
            return EligibilityResult(
                eligible=False,
                reason=f"Account age ({account_age_months}mo) below minimum ({self.MIN_ACCOUNT_AGE_MONTHS}mo)",
                metadata={
                    "gate": "account_age",
                    "required_months": self.MIN_ACCOUNT_AGE_MONTHS,
                    "actual_months": account_age_months
                }
            )
        
        # Gate 3: Direct deposit (payroll detection)
        # Without payroll-like deposits, EWA is unsecured lending
        if not has_direct_deposit:
            return EligibilityResult(
                eligible=False,
                reason="No direct deposit detected - EWA requires payroll",
                metadata={"gate": "direct_deposit", "detected": False}
            )
        
        # Gate 4: Employment stability
        if employment_months < self.MIN_EMPLOYMENT_MONTHS:
            return EligibilityResult(
                eligible=False,
                reason=f"Employment duration ({employment_months}mo) below minimum ({self.MIN_EMPLOYMENT_MONTHS}mo)",
                metadata={
                    "gate": "employment",
                    "required_months": self.MIN_EMPLOYMENT_MONTHS,
                    "actual_months": employment_months
                }
            )
        
        # All gates passed
        return EligibilityResult(
            eligible=True,
            reason="Passed all basic eligibility gates",
            metadata={
                "gates_passed": ["age", "account_age", "direct_deposit", "employment"],
                "user_age": user_age,
                "account_age_months": account_age_months,
                "employment_months": employment_months
            }
        )

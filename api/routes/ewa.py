"""
FastAPI routes for EWA eligibility prediction
"""
from fastapi import APIRouter, HTTPException, Depends
from datetime import datetime
from typing import List
import logging

from ..schemas.ewa import (
    EWAEligibilityRequest,
    EWAEligibilityResponse,
    EligibilityResultSchema,
    IncomeInferenceSchema,
    RiskAssessmentSchema,
    AdvanceLimitSchema,
    CooldownDecisionSchema,
    RiskTierEnum
)
from ..heuristics.engine import EWAHeuristicEngine

router = APIRouter(prefix="/ewa", tags=["EWA Eligibility"])
logger = logging.getLogger(__name__)


# Initialize heuristic engine (singleton)
ewa_engine = EWAHeuristicEngine()


@router.post(
    "/eligibility",
    response_model=EWAEligibilityResponse,
    summary="Check EWA Eligibility",
    description="""
    Run complete heuristic evaluation for EWA eligibility.
    
    Executes all decision trees:
    - Tree A: Basic eligibility gates
    - Tree B: Income & payday inference
    - Tree C: Risk tier assignment
    - Tree D: Max advance calculation
    - Tree E: Cooldown logic
    - Fee policy tree
    
    Returns full decision chain with explainability.
    """
)
async def check_eligibility(request: EWAEligibilityRequest) -> EWAEligibilityResponse:
    """
    Main endpoint for EWA eligibility prediction.
    
    This is the endpoint your backend will call:
    POST /ewa/eligibility with user data → get approval decision + limits
    """
    
    try:
        logger.info(f"Processing EWA eligibility request for user_id={request.user_id}")
        
        # Convert Pydantic transactions to dict format
        transactions = [
            {
                'amount': tx.amount,
                'date': tx.date,
                'sender_name': tx.sender_name,
                'description': tx.description
            }
            for tx in request.inflow_transactions
        ]
        
        # Run heuristic engine
        decision = ewa_engine.evaluate(
            user_id=request.user_id,
            user_age=request.user_age,
            account_age_months=request.account_age_months,
            has_direct_deposit=request.has_direct_deposit,
            employment_months=request.employment_months,
            inflow_transactions=transactions,
            nsf_count_90d=request.nsf_count_90d,
            income_volatility=request.income_volatility,
            prior_ewa_failed=request.prior_ewa_failed,
            rapid_redraw=request.rapid_redraw,
            accrued_wage=request.accrued_wage,
            hourly_wage=request.hourly_wage,
            hours_worked=request.hours_worked,
            last_advance_date=request.last_advance_date,
            last_repayment_date=request.last_repayment_date,
            recent_advance_count_7d=request.recent_advance_count_7d,
            requested_amount=request.requested_amount
        )
        
        # Convert dataclass result to Pydantic response
        response = EWAEligibilityResponse(
            approved=decision.approved,
            max_advance_amount=decision.max_advance_amount,
            eligibility=EligibilityResultSchema(**decision.eligibility.__dict__),
            income_inference=IncomeInferenceSchema(**decision.income_inference.__dict__),
            risk_assessment=RiskAssessmentSchema(
                tier=RiskTierEnum(decision.risk_assessment.tier.value),
                timing_risk=decision.risk_assessment.timing_risk,
                behavioral_flags=decision.risk_assessment.behavioral_flags,
                days_to_payday=decision.risk_assessment.days_to_payday,
                score=decision.risk_assessment.score,
                metadata=decision.risk_assessment.metadata
            ),
            advance_limit=AdvanceLimitSchema(**decision.advance_limit.__dict__) if decision.advance_limit else None,
            cooldown=CooldownDecisionSchema(**decision.cooldown.__dict__),
            fee_amount=decision.fee_amount,
            fee_explanation=decision.fee_explanation,
            decision_path=decision.decision_path,
            rejection_reason=decision.rejection_reason,
            timestamp=decision.timestamp,
            user_id=decision.user_id
        )
        
        logger.info(
            f"EWA decision for user_id={request.user_id}: "
            f"approved={decision.approved}, "
            f"max_amount=${decision.max_advance_amount:.2f}, "
            f"tier={decision.risk_assessment.tier.value}"
        )
        
        return response
        
    except Exception as e:
        logger.error(f"Error processing EWA eligibility: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Internal error processing EWA eligibility: {str(e)}"
        )


@router.post(
    "/eligibility/batch",
    response_model=List[EWAEligibilityResponse],
    summary="Batch EWA Eligibility Check",
    description="Process multiple EWA eligibility requests in one call"
)
async def check_eligibility_batch(
    requests: List[EWAEligibilityRequest]
) -> List[EWAEligibilityResponse]:
    """
    Batch endpoint for processing multiple users at once.
    Useful for:
    - Nightly batch processing
    - Pre-qualification campaigns
    - Analytics/reporting
    """
    
    results = []
    
    for req in requests:
        try:
            result = await check_eligibility(req)
            results.append(result)
        except Exception as e:
            logger.error(f"Error processing user_id={req.user_id} in batch: {str(e)}")
            # Continue processing other requests
            continue
    
    return results

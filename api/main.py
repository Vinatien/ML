"""
FastAPI ML Service for VinaTien EWA Platform

This service provides:
1. EWA eligibility prediction via heuristic decision trees
2. Real-time inference endpoints for backend integration
3. Health checks and monitoring
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
import logging

from .routes import ewa
from .schemas.ewa import HealthResponse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="VinaTien ML API",
    description="Machine Learning service for EWA eligibility prediction",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware (configure allowed origins for production)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # TODO: Restrict to backend URL in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(ewa.router, prefix="/api/v1")


@app.get("/", tags=["Root"])
async def root():
    """Root endpoint"""
    return {
        "service": "VinaTien ML API",
        "version": "1.0.0",
        "status": "running",
        "docs": "/docs"
    }


@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check() -> HealthResponse:
    """
    Health check endpoint for monitoring and load balancers.
    """
    return HealthResponse(
        status="healthy",
        service="vinatien-ml-api",
        timestamp=datetime.utcnow(),
        version="1.0.0"
    )


@app.on_event("startup")
async def startup_event():
    """Application startup"""
    logger.info("🚀 VinaTien ML API starting up...")
    logger.info("📊 Heuristic decision trees loaded")
    logger.info("✅ Service ready to accept requests")


@app.on_event("shutdown")
async def shutdown_event():
    """Application shutdown"""
    logger.info("🛑 VinaTien ML API shutting down...")
    logger.info("✅ Cleanup complete")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8001,
        reload=True,
        log_level="info"
    )

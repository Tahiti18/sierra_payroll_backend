"""
Sierra Roofing Payroll Backend
FastAPI application entry point with all routing and middleware
"""
import os
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import logging

from .core.config import settings
from .db.database import engine
from .models.database import Base
from .api.endpoints import employees, payroll

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events"""
    # Startup
    logger.info("Starting Sierra Roofing Payroll Backend")

    # Create database tables
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created successfully")
    except Exception as e:
        logger.error(f"Error creating database tables: {str(e)}")
        raise

    yield

    # Shutdown
    logger.info("Shutting down Sierra Roofing Payroll Backend")

# Create FastAPI application
app = FastAPI(
    title="Sierra Roofing Payroll Backend",
    description="Enterprise payroll automation system for Sierra Roofing",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# Include routers
app.include_router(employees.router, prefix="/api")
app.include_router(payroll.router, prefix="/api")

@app.get("/")
async def root():
    """Root endpoint with system information"""
    return {
        "message": "Sierra Roofing Payroll Backend",
        "version": "1.0.0",
        "status": "operational",
        "features": [
            "Employee Management",
            "Sierra Excel Processing", 
            "WBS Format Generation",
            "Piecework Detection",
            "Audit Trail System"
        ]
    }

@app.get("/health")
async def health_check():
    """Health check endpoint for monitoring"""
    try:
        # Test database connection
        from .db.database import get_db
        db = next(get_db())
        db.execute("SELECT 1")
        db.close()

        return {
            "status": "healthy",
            "database": "connected",
            "timestamp": "2024-01-01T00:00:00Z"
        }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        raise HTTPException(status_code=503, detail="Service unavailable")

@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler for better error responses"""
    logger.error(f"Unhandled exception: {str(exc)}")
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "message": "An unexpected error occurred. Please try again later.",
            "timestamp": "2024-01-01T00:00:00Z"
        }
    )

# Railway deployment fix - proper PORT handling
if __name__ == "__main__":
    # Get port from environment variable with fallback
    port = int(os.environ.get("PORT", 8000))
    
    # Run the application
    uvicorn.run(
        "app.main:app",  # Updated to use proper module path
        host="0.0.0.0",
        port=port,
        log_level="info"
    )

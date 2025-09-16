"""
Sierra Roofing Payroll Backend - Railway Compatible
"""
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastAPI application - NO COMPLEX LIFESPAN
app = FastAPI(
    title="Sierra Roofing Payroll Backend",
    description="Enterprise payroll automation system",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Simplified for now
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "Sierra Roofing Payroll Backend",
        "version": "1.0.0",
        "status": "operational",
        "features": [
            "Employee Management",
            "Sierra Excel Processing", 
            "WBS Format Generation",
            "Piecework Detection"
        ]
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": "2024-01-01T00:00:00Z"
    }

@app.post("/api/test")
async def test_endpoint():
    """Test endpoint for frontend connection"""
    return {
        "message": "Backend connected successfully",
        "ready": True
    }

# NO __main__ SECTION - Let Railway handle startup

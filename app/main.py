from fastapi import FastAPI

# Ultra-simple FastAPI app for Railway
app = FastAPI()

@app.get("/")
def read_root():
    return {
        "message": "Sierra Roofing Payroll Backend",
        "status": "running",
        "version": "1.0.0"
    }

@app.get("/health")
def health():
    return {"status": "healthy"}

@app.post("/api/test")
def test():
    return {"message": "Backend connected successfully"}

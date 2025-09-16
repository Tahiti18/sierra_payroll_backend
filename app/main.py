import os
from fastapi import FastAPI

app = FastAPI(title="Sierra Roofing Payroll Backend")

@app.get("/")
def root():
    return {"message": "Sierra Roofing Backend", "status": "running"}

@app.get("/health")
def health():
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)

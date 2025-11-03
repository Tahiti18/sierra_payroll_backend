# app/main.py
# Sierra Payroll Backend – FastAPI entrypoint
# Includes /health version marker to confirm active deployment.

from fastapi import FastAPI, UploadFile, File
from fastapi.responses import JSONResponse, StreamingResponse
import io

from app.services.excel_processor import process_excel

app = FastAPI(title="Sierra Roofing Payroll Backend", version="7.0.0")

# -------------------------------------------------
# Health check (for Railway + Netlify connection)
# -------------------------------------------------
@app.get("/health")
def health():
    # Version marker to confirm deployment refresh
    return {
        "status": "ok",
        "version": "v-2025-11-03-01",  # ← This proves the latest code is live
        "service": "sierra-payroll-backend"
    }

# -------------------------------------------------
# Main payroll processing route
# -------------------------------------------------
@app.post("/process-payroll")
async def process_payroll(file: UploadFile = File(...)):
    """
    Accepts a Sierra Payroll Excel file and returns the computed WBS Payroll file.
    """
    try:
        contents = await file.read()
        result_bytes = process_excel(contents)

        filename = "WBS_Payroll_Output.xlsx"
        return StreamingResponse(
            io.BytesIO(result_bytes),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e)}
        )

# -------------------------------------------------
# Root redirect
# -------------------------------------------------
@app.get("/")
def root():
    return {
        "message": "Sierra Roofing Payroll Backend active",
        "api_endpoints": ["/health", "/process-payroll"],
    }

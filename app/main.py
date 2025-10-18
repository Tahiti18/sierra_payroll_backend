# app/main.py
from __future__ import annotations

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse

from pathlib import Path
import io
import traceback

# This is the new Sierra → WBS converter you installed
from app.services.excel_processor import sierra_excel_to_wbs_bytes

app = FastAPI(title="Sierra → WBS Converter", version="1.0.0")

# CORS: allow your Netlify UI and local use
ALLOWED_ORIGINS = [
    "https://sierrapayrollapp.netlify.app",
    "https://sierra-payrollapp.netlify.app",
    "http://localhost",
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "*"  # keep last as fallback; tighten later if needed
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health() -> JSONResponse:
    return JSONResponse({"status": "ok"})

@app.post("/process-payroll")
async def process_payroll(file: UploadFile = File(...)):
    """
    Accept a Sierra weekly Excel file and return a WBS-format Excel file.
    """
    try:
        if not file.filename.lower().endswith((".xlsx", ".xls")):
            raise HTTPException(status_code=400, detail="Upload an Excel file (.xlsx or .xls)")

        content = await file.read()
        if not content:
            raise HTTPException(status_code=400, detail="Empty file")

        # Convert Sierra → WBS (fills A01 REG, Pay Rate, Totals)
        out_bytes: bytes = sierra_excel_to_wbs_bytes(content)

        filename = "WBS_Payroll_Output.xlsx"
        return StreamingResponse(
            io.BytesIO(out_bytes),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'}
        )

    except HTTPException:
        raise
    except Exception as e:
        # Return concise error with server-side traceback logged
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"conversion failed: {e}")

# Local dev (not used on Railway)
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8080, reload=False)

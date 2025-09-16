import os
from io import BytesIO
from datetime import datetime

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse
from starlette.middleware.cors import CORSMiddleware
import openpyxl

APP_TITLE = "Sierra Roofing Payroll Backend"
app = FastAPI(title=APP_TITLE)

# --- CORS (allow your Netlify site) ---
FRONTEND_ORIGIN = os.getenv(
    "FRONTEND_ORIGIN",
    "https://adorable-madeleine-291bb0.netlify.app"  # your current Netlify site
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_ORIGIN],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {"message": "Sierra Roofing Backend", "status": "running"}

@app.get("/health")
def health():
    return {"status": "healthy"}

# Optional: stub so the Employees tab doesn't error
@app.get("/employees")
def list_employees():
    return [
        {"name": "Sample Admin", "ssn": "0000", "department": "ADMIN", "pay_rate": 35.00},
        {"name": "Sample Roofer", "ssn": "1111", "department": "ROOF", "pay_rate": 28.50},
    ]

@app.post("/process-payroll")
async def process_payroll(file: UploadFile = File(...)):
    """
    Accepts an uploaded Excel file (.xlsx), loads it, and returns
    a valid Excel file back. For now this is a pass-through with
    a small marker cell so we can verify end-to-end.
    """
    filename = file.filename or "input.xlsx"
    if not filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=422, detail="Please upload an .xlsx Excel file.")

    try:
        data = await file.read()
        wb = openpyxl.load_workbook(BytesIO(data))
        # Add/mark a cell so we know it was processed by the backend
        ws = wb.active
        ws["A1"] = (ws["A1"].value or "Processed by Sierra Backend")
        ws["B1"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

        out = BytesIO()
        wb.save(out)
        out.seek(0)

        out_name = f"WBS_Payroll_{datetime.utcnow().strftime('%Y-%m-%d')}.xlsx"
        headers = {
            "Content-Disposition": f'attachment; filename="{out_name}"'
        }
        return StreamingResponse(
            out,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers,
        )
    except Exception as e:
        # Log-friendly error response
        return JSONResponse(
            status_code=500,
            content={"detail": f"Server error while processing Excel: {str(e)}"},
        )

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("app.main:app", host="0.0.0.0", port=port, reload=False)

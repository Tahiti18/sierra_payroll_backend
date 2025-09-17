# app/main.py
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse, PlainTextResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from io import BytesIO
import openpyxl
import pandas as pd
from pathlib import Path

from .converter import convert_from_buffers

app = FastAPI(title="Sierra Payroll Backend")

# --- CORS: allow Netlify + localhost for testing ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # TEMP: allow everything
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---- health & root ----
@app.get("/health")
async def health():
    return {"ok": True}

@app.get("/", response_class=HTMLResponse)
async def index():
    return "<h3>Sierra Payroll Backend is running.</h3><p>Try <code>/health</code>.</p>"

# ---- Employees API ----
ROSTER_PATH = Path(__file__).parent / "data" / "roster.csv"

@app.get("/employees")
async def get_employees():
    if not ROSTER_PATH.exists():
        raise HTTPException(status_code=404, detail="Roster not found")
    try:
        df = pd.read_csv(ROSTER_PATH)
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load roster: {e}")

@app.post("/employees")
async def add_employee(employee: dict):
    try:
        df = pd.read_csv(ROSTER_PATH) if ROSTER_PATH.exists() else pd.DataFrame()
        df = pd.concat([df, pd.DataFrame([employee])], ignore_index=True)
        df.to_csv(ROSTER_PATH, index=False)
        return {"status": "ok", "employee": employee}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to add employee: {e}")

# ---- Payroll Processing ----
@app.post("/process-payroll")
async def process_payroll(
    sierra_file: UploadFile = File(...),
    roster_file: UploadFile | None = File(None),
):
    if not sierra_file.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Sierra file must be .xlsx")
    try:
        sierra_bytes = await sierra_file.read()
        roster_bytes = await roster_file.read() if roster_file else None
        out_bytes, out_name = convert_from_buffers(sierra_bytes, roster_bytes)

        return StreamingResponse(
            BytesIO(out_bytes),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename="{out_name}"'},
        )
    except Exception as e:
        return PlainTextResponse(f"Payroll processing failed: {e}", status_code=400)

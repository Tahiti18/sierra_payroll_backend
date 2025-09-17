# app/main.py
from __future__ import annotations

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
from io import BytesIO
from pathlib import Path
import csv
import openpyxl

# our converter (you already added app/converter.py earlier)
from .converter import convert_from_buffers  # returns (bytes, filename)

app = FastAPI(title="Sierra Payroll Backend")

# ---- CORS: OPEN while we stabilize (fixes the Netlify “offline”/ERR_NETWORK) ----
# We can lock this down later to just your Netlify domain.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # <— wide open so the front-end can reach it
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---- HEALTH & ROOT -------------------------------------------------------------
@app.get("/health")
async def health():
    return {"ok": True}

@app.get("/", response_class=HTMLResponse)
async def index():
    return """<h3>Sierra Payroll Backend is running.</h3>
              <p>Try <code>/health</code> or <code>/employees</code>.</p>"""

# ---- EMPLOYEES (used by the “Manage Employees” tab) ---------------------------
# Reads the roster we committed at app/data/roster.csv and returns JSON.
@app.get("/employees")
async def list_employees():
    roster_path = Path(__file__).resolve().parent / "data" / "roster.csv"
    if not roster_path.exists():
        # empty result (frontend can handle it)
        return {"count": 0, "employees": []}

    employees = []
    with roster_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            employees.append({
                "EmpID": row.get("EmpID", "").strip(),
                "SSN": row.get("SSN", "").strip(),
                "EmployeeName": row.get("Employee Name", "").strip() or row.get("EmployeeName", "").strip(),
                "Status": row.get("Status", "").strip(),
                "Type": row.get("Type", "").strip(),
                "PayRate": row.get("PayRate", "").strip(),
                "Dept": row.get("Dept", "").strip(),
            })
    return {"count": len(employees), "employees": employees}

# ---- PAYROLL CONVERSION (what the big blue button hits) -----------------------
# Frontend was calling POST /process-payroll (and also OPTIONS preflight).
# Accept either field name: "file" OR "sierra_file". Optional "roster_file".
@app.post("/process-payroll")
async def process_payroll(
    file: UploadFile | None = File(None),
    sierra_file: UploadFile | None = File(None),
    roster_file: UploadFile | None = File(None),
):
    sierra = sierra_file or file
    if sierra is None:
        raise HTTPException(status_code=400, detail="Missing Sierra payroll XLSX (field 'file' or 'sierra_file').")
    if not sierra.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Sierra file must be .xlsx")

    # read uploads
    sierra_bytes = await sierra.read()
    roster_bytes = await roster_file.read() if roster_file is not None else None

    # if no roster was uploaded, we’ll still proceed (converter handles None).
    # (We also expose /employees using the committed CSV for the UI list.)

    try:
        out_bytes, out_name = convert_from_buffers(sierra_bytes, roster_bytes)
    except Exception as e:
        # helpful error for the UI
        return JSONResponse({"ok": False, "error": f"Conversion failed: {e}"}, status_code=400)

    return StreamingResponse(
        BytesIO(out_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{out_name}"'},
    )

# ---- Compatibility alias (if the UI still posts here) -------------------------
@app.post("/api/convert")
async def api_convert(
    sierra_file: UploadFile = File(...),
    roster_file: UploadFile | None = File(None),
):
    # exact same logic as /process-payroll
    try:
        sierra_bytes = await sierra_file.read()
        roster_bytes = await roster_file.read() if roster_file is not None else None
        out_bytes, out_name = convert_from_buffers(sierra_bytes, roster_bytes)
        return StreamingResponse(
            BytesIO(out_bytes),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename="{out_name}"'},
        )
    except Exception as e:
        return PlainTextResponse(f"Failed to convert: {e}", status_code=400)

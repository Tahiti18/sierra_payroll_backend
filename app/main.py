# app/main.py
from __future__ import annotations

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from io import BytesIO
from pathlib import Path
import csv

# ---------- App ----------
app = FastAPI(title="Sierra Payroll Backend")

# ---------- CORS (Netlify origin only; works with credentials) ----------
NETLIFY_ORIGIN = "https://adorable-madeline-291bb0.netlify.app"
app.add_middleware(
    CORSMiddleware,
    allow_origins=[NETLIFY_ORIGIN],   # IMPORTANT: do not use "*" with allow_credentials=True
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# ---------- Health / Root ----------
@app.get("/health")
async def health():
    return {"ok": True}

@app.get("/", response_class=HTMLResponse)
async def index():
    return "<h3>Sierra Payroll Backend is running.</h3><p>Try <code>/health</code>.</p>"

# ---------- Employees (used by Manage Employees tab) ----------
@app.get("/employees")
async def list_employees():
    roster_path = Path(__file__).resolve().parent / "data" / "roster.csv"
    if not roster_path.exists():
        return []  # Frontend expects an array

    employees = []
    with roster_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            employees.append({
                "emp_id": (r.get("EmpID") or "").strip(),
                "ssn": (r.get("SSN") or "").strip(),
                "name": (r.get("Employee Name") or r.get("EmployeeName") or "").strip(),
                "status": (r.get("Status") or "").strip(),
                "type": (r.get("Type") or "").strip(),
                "pay_rate": (r.get("PayRate") or "").strip(),
                "department": (r.get("Dept") or "").strip(),
            })
    return employees

# ---------- Payroll conversion (what your HTML calls) ----------
@app.post("/process-payroll")
async def process_payroll(
    file: UploadFile | None = File(None),
    sierra_file: UploadFile | None = File(None),
    roster_file: UploadFile | None = File(None),
):
    # Lazy import avoids circular-import crash
    try:
        from .converter import convert_from_buffers  # type: ignore
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"Import error: {e}"}, status_code=500)

    sierra = sierra_file or file
    if sierra is None:
        raise HTTPException(status_code=400, detail="Missing Sierra payroll XLSX (field 'file' or 'sierra_file').")
    if not sierra.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Sierra file must be .xlsx")

    sierra_bytes = await sierra.read()
    roster_bytes = await roster_file.read() if roster_file is not None else None

    try:
        out_bytes, out_name = convert_from_buffers(sierra_bytes, roster_bytes)
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"Conversion failed: {e}"}, status_code=400)

    return StreamingResponse(
        BytesIO(out_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{out_name}"'},
    )

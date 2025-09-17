# app/main.py
from __future__ import annotations

import csv
from io import BytesIO
from pathlib import Path
from typing import List, Optional

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse, PlainTextResponse
from pydantic import BaseModel

# Try to use the real converter if present
try:
    from app.converter import convert_from_buffers  # type: ignore
except Exception:  # fallback only if converter isn't available
    convert_from_buffers = None  # type: ignore

import openpyxl
import pandas as pd

app = FastAPI(title="Sierra Payroll Backend")

# ---------------- CORS ----------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://adorable-madeline-291bb0.netlify.app",
        "https://*.netlify.app",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ----------- Health / Root -----------
@app.get("/health")
async def health():
    return {"ok": True}

@app.get("/", response_class=HTMLResponse)
async def index():
    return "<h3>Sierra Payroll Backend is running.</h3><p>Try <code>/health</code>.</p>"

# -------------- Employees --------------
DATA_DIR = Path(__file__).resolve().parent / "data"
ROSTER_CSV = DATA_DIR / "roster.csv"

class Employee(BaseModel):
    empId: Optional[str] = None
    ssn: Optional[str] = None
    name: str
    status: Optional[str] = None
    type: Optional[str] = None
    payRate: Optional[float] = None
    dept: Optional[str] = None

def _read_roster_csv() -> List[Employee]:
    employees: List[Employee] = []
    if not ROSTER_CSV.exists():
        # No roster file: return empty list gracefully
        return employees

    with ROSTER_CSV.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            employees.append(
                Employee(
                    empId=row.get("EmpID") or row.get("EmpId") or row.get("ID"),
                    ssn=(row.get("SSN") or "").strip(),
                    name=(row.get("Employee Name") or row.get("Name") or "").strip(),
                    status=(row.get("Status") or "").strip(),
                    type=(row.get("Type") or row.get("PayType") or "").strip(),
                    payRate=_to_float(row.get("PayRate") or row.get("Pay Rate")),
                    dept=(row.get("Dept") or row.get("Department") or "").strip(),
                )
            )
    return employees

def _to_float(v) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(str(v).replace(",", "").replace("$", "").strip())
    except Exception:
        return None

@app.get("/employees", response_model=List[Employee])
async def list_employees():
    """
    Returns employees from app/data/roster.csv (read-only).
    Fields expected: EmpID, SSN, Employee Name, Status, Type, PayRate, Dept
    """
    return _read_roster_csv()

@app.post("/employees", response_model=Employee)
async def add_employee(emp: Employee):
    """
    Accepts a new employee (echoes back). NOTE: not persisted in repo/FS.
    This endpoint exists to satisfy the frontend UI without 404s.
    """
    # Echo back with a synthetic ID if missing
    if not emp.empId:
        emp.empId = "TMP-" + (emp.ssn or emp.name.replace(" ", "-"))
    return emp

@app.post("/employees/bulk-load", response_model=List[Employee])
async def bulk_load_employees(roster_file: UploadFile = File(...)):
    """
    Optional helper: upload an .xlsx roster to preview/return employees.
    """
    if not roster_file.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Roster file must be .xlsx")

    data = await roster_file.read()
    xf = pd.ExcelFile(BytesIO(data))
    df = xf.parse(xf.sheet_names[0])

    # Try to map flexible headers
    colmap = {c.lower(): c for c in df.columns}

    def col(*names):
        for n in names:
            if n.lower() in colmap:
                return colmap[n.lower()]
        return None

    c_emp = col("empid", "id", "employee id")
    c_name = col("employee name", "name")
    c_ssn = col("ssn", "social security", "social security number")
    c_status = col("status")
    c_type = col("type", "pay type")
    c_rate = col("pay rate", "rate", "payrate")
    c_dept = col("dept", "department")

    out: List[Employee] = []
    for _, r in df.iterrows():
        nm = str(r.get(c_name, "")).strip()
        if not nm:
            continue
        out.append(
            Employee(
                empId=(str(r.get(c_emp, "")) or None),
                ssn=str(r.get(c_ssn, "") or "").strip(),
                name=nm,
                status=str(r.get(c_status, "") or "").strip(),
                type=str(r.get(c_type, "") or "").strip(),
                payRate=_to_float(r.get(c_rate, "")),
                dept=str(r.get(c_dept, "") or "").strip(),
            )
        )
    return out

# -------------- Payroll Convert --------------
@app.post("/api/convert")
async def convert_api(
    sierra_file: UploadFile = File(...),
    roster_file: UploadFile | None = File(None),
):
    """
    Converts Sierra workbook to WBS payroll workbook.
    Uses app/converter.py::convert_from_buffers when available.
    """
    if not sierra_file.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Sierra file must be .xlsx")

    sierra_bytes = await sierra_file.read()
    roster_bytes: Optional[bytes] = None
    if roster_file is not None:
        roster_bytes = await roster_file.read()

    try:
        if convert_from_buffers:
            # Use your real converter (preferred)
            payload, filename = convert_from_buffers(sierra_bytes, roster_bytes)
            return StreamingResponse(
                BytesIO(payload),
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": f'attachment; filename="{filename}"'},
            )
        else:
            # Safe fallback: echo the uploaded workbook (still valid Excel)
            wb = openpyxl.load_workbook(BytesIO(sierra_bytes), data_only=True)
            bio = BytesIO()
            wb.save(bio)
            bio.seek(0)
            return StreamingResponse(
                bio,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": 'attachment; filename="WBS_Payroll.xlsx"'},
            )
    except Exception as e:
        return PlainTextResponse(f"Conversion failed: {e}", status_code=400)

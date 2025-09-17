# app/main.py
from fastapi import FastAPI, UploadFile, File, HTTPException, Body
from fastapi.responses import HTMLResponse, StreamingResponse, PlainTextResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from io import BytesIO
from pathlib import Path
import csv
import openpyxl
import os

app = FastAPI(title="Sierra Payroll Backend")

# ---- CORS: allow your Netlify app (and OPTIONS preflight) ----
NETLIFY_ORIGIN = os.getenv("NETLIFY_ORIGIN", "https://adorable-madeline-291bb0.netlify.app")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[NETLIFY_ORIGIN],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# ---- in-memory roster cache (loaded from CSV on first request) ----
_ROSTER = None
ROSTER_PATH = Path(__file__).parent / "data" / "roster.csv"

def load_roster():
    global _ROSTER
    if _ROSTER is not None:
        return _ROSTER

    if not ROSTER_PATH.exists():
        _ROSTER = []
        return _ROSTER

    rows = []
    with ROSTER_PATH.open("r", newline="", encoding="utf-8-sig") as f:
        rdr = csv.DictReader(f)
        # Normalize fieldnames we actually use in the UI
        for r in rdr:
            rows.append({
                "EmpID": (r.get("EmpID") or r.get("Emp Id") or r.get("EmpID " ) or "").strip(),
                "SSN": (r.get("SSN") or r.get("Ssn") or "").strip(),
                "EmployeeName": (r.get("Employee Name") or r.get("EmployeeName") or "").strip(),
                "Status": (r.get("Status") or "").strip(),
                "Type": (r.get("Type") or "").strip(),
                "PayRate": (r.get("PayRate") or r.get("Pay Rate") or "").strip(),
                "Dept": (r.get("Dept") or r.get("Department") or "").strip(),
            })
    _ROSTER = rows
    return _ROSTER

# ---------------- Health & Home ----------------
@app.get("/health")
async def health():
    return {"ok": True}

@app.get("/", response_class=HTMLResponse)
async def index():
    return f"""
    <h3>Sierra Payroll Backend is running.</h3>
    <p>Allowed origin: <code>{NETLIFY_ORIGIN}</code></p>
    <p>Try <code>/health</code> or <code>/employees</code>.</p>
    """

# ---------------- Employees API ----------------
@app.get("/employees")
async def get_employees():
    roster = load_roster()
    return JSONResponse(roster)

@app.post("/employees")
async def upsert_employee(emp: dict = Body(...)):
    """
    Optional: lets the UI add/update an employee in-memory for this run.
    Fields: EmpID, SSN, EmployeeName, Status, Type, PayRate, Dept
    """
    roster = load_roster()
    key = (emp.get("SSN") or "").strip()
    name = (emp.get("EmployeeName") or "").strip()
    if not key and not name:
        raise HTTPException(status_code=400, detail="Employee must have SSN or EmployeeName")

    # match by SSN first, else by name
    idx = None
    for i, r in enumerate(roster):
        if key and r.get("SSN") == key:
            idx = i; break
        if not key and name and r.get("EmployeeName") == name:
            idx = i; break

    normalized = {
        "EmpID": (emp.get("EmpID") or "").strip(),
        "SSN": key,
        "EmployeeName": name,
        "Status": (emp.get("Status") or "").strip(),
        "Type": (emp.get("Type") or "").strip(),
        "PayRate": (emp.get("PayRate") or "").strip(),
        "Dept": (emp.get("Dept") or "").strip(),
    }
    if idx is None:
        roster.append(normalized)
    else:
        roster[idx] = normalized
    return {"ok": True, "count": len(roster)}

# ---------------- Convert API (pipe test for now) ----------------
@app.post("/api/convert")
async def convert_api(
    sierra_file: UploadFile = File(...),
    roster_file: UploadFile | None = File(None),
):
    if not sierra_file.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Sierra file must be .xlsx")

    try:
        # If a roster file is supplied, we accept it (future: merge/update _ROSTER)
        if roster_file and roster_file.filename.lower().endswith(".xlsx"):
            # placeholder: just read to ensure it’s a valid xlsx
            _ = await roster_file.read()

        # Echo workbook back — confirms upload/download path is good.
        data = await sierra_file.read()
        wb = openpyxl.load_workbook(BytesIO(data), data_only=True)
        bio = BytesIO()
        wb.save(bio)
        bio.seek(0)

        return StreamingResponse(
            bio,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": 'attachment; filename="WBS_Payroll.xlsx"'},
        )
    except Exception as e:
        return PlainTextResponse(f"Failed to read Excel: {e}", status_code=400)

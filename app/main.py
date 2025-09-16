# app/main.py
import os
from io import BytesIO
from datetime import datetime, date, timedelta

import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse
from starlette.middleware.cors import CORSMiddleware

# ------------------------------
# App & CORS
# ------------------------------
APP_TITLE = "Sierra Roofing Payroll Backend"
app = FastAPI(title=APP_TITLE)

FRONTEND_ORIGIN = os.getenv(
    "FRONTEND_ORIGIN",
    # Update if you change the Netlify domain
    "https://adorable-madeleine-291bb0.netlify.app"
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_ORIGIN],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------------------------
# Utility functions
# ------------------------------
NAME_ALIASES = {"name", "employee", "employee name", "worker", "employee_name"}
HOURS_ALIASES = {"hours", "hrs", "total hours", "total_hrs", "time", "worked hours"}
DATE_ALIASES = {"date", "day", "days", "work date", "workday", "work_date"}

def find_col(df: pd.DataFrame, aliases: set[str]) -> str | None:
    cols = {c: str(c).strip().lower() for c in df.columns}
    for orig, low in cols.items():
        if low in aliases:
            return orig
    # fallback: fuzzy contains
    for orig, low in cols.items():
        if any(a in low for a in aliases):
            return orig
    return None

def to_last_first(name: str) -> str:
    """Convert 'John A Smith' → 'Smith, John A' if there’s no comma already."""
    if not isinstance(name, str):
        return ""
    n = name.strip()
    if not n:
        return ""
    if "," in n:
        return n
    parts = n.split()
    if len(parts) >= 2:
        last = parts[-1]
        first = " ".join(parts[:-1])
        return f"{last}, {first}"
    return n

def sunday_for_week(any_date: date) -> date:
    # ISO weekday: Mon=0..Sun=6 → we want the Sunday of that week
    return any_date + timedelta(days=(6 - any_date.weekday()) % 7)

def build_wbs_sheet(agg: pd.DataFrame, pe_date: date) -> BytesIO:
    """
    Build a WBS-like 'WEEKLY' worksheet:
    - Top metadata rows (#U, #N, #P, #R, #C)
    - Marker rows (# B:8, # E:26)
    - Data rows with REG in T1 column and OT in next column
    """
    rpt_date = pe_date + timedelta(days=3)
    ck_date = pe_date + timedelta(days=5)

    # Columns layout derived from common WBS “WEEKLY” import format
    columns = [
        "# V","DO NOT EDIT","Version = B90216-00","FmtRev = 2.1",
        f"RunTime = {datetime.utcnow().strftime('%Y%m%d-%H%M%S')}",
        "CliUnqId = 055269","CliName = Sierra Roofing and Solar Inc","Freq = W",
        f"PEDate = {pe_date.strftime('%m/%d/%Y')}",
        f"RptDate = {rpt_date.strftime('%m/%d/%Y')}",
        f"CkDate = {ck_date.strftime('%m/%d/%Y')}",
        "EmpType = SSN","DoNotes = 1","PayRates = H+;S+;E+;C+",
        "RateCol = 6","T1 = 7+","CodeBeg = 8","CodeEnd = 26","NoteCol = 27",
        "","", "","","","","","",""
    ]

    rows: list[list] = []
    # Meta rows
    rows.append(["# U","CliUnqID","055269"] + [None]*(len(columns)-3))
    rows.append(["# N","Client","Sierra Roofing and Solar Inc"] + [None]*(len(columns)-3))
    rows.append(["# P","Period End",pe_date.strftime("%m/%d/%Y")] + [None]*(len(columns)-3))
    rows.append(["# R","Report Due",rpt_date.strftime("%m/%d/%Y")] + [None]*(len(columns)-3))
    rows.append(["# C","Check Date",ck_date.strftime("%m/%d/%Y")] + [None]*(len(columns)-3))

    # Marker + header mapping
    rows.append(["# B:8"] + [None]*(len(columns)-1))
    rows.append([
        "# E:26","SSN","Employee Name","Status","Type","Pay Rate","Dept",
        "REG (T1)","OT","DT","Code10","Code11","Code12","Code13","Code14",
        "Code15","Code16","Code17","Code18","Code19","Code20","Code21","Code22",
        "Code23","Code24","Code25","Code26","Notes"
    ][:len(columns)])

    # Data rows (one per employee)
    for _, r in agg.sort_values("Employee Name").iterrows():
        reg = float(r["Reg"])
        ot = float(r["OT"])
        row = [None]*len(columns)
        row[0] = ""              # Employee ID (unknown)
        row[1] = ""              # SSN (unknown) – can be filled later from a mapping table
        row[2] = r["Employee Name"]
        row[3] = "A"             # Active
        row[4] = "H"             # Hourly
        row[5] = ""              # Pay Rate (optional)
        row[6] = "ROOF"          # Default Dept – adjust if you maintain mapping
        row[7] = reg             # REG in T1
        row[8] = ot              # OT
        rows.append(row)

    df_wbs = pd.DataFrame(rows, columns=columns[:len(rows[0])])

    out = BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        df_wbs.to_excel(writer, index=False, sheet_name="WEEKLY")
    out.seek(0)
    return out

# ------------------------------
# Endpoints
# ------------------------------
@app.get("/")
def root():
    return {"message": "Sierra Roofing Backend", "status": "running"}

@app.get("/health")
def health():
    return {"status": "healthy"}

# (Optional) keeps the Employees tab from erroring in the UI
@app.get("/employees")
def employees_stub():
    return [
        {"name": "Sample Admin", "ssn": "0000", "department": "ADMIN", "pay_rate": 35.00},
        {"name": "Sample Roofer", "ssn": "1111", "department": "ROOF", "pay_rate": 28.50},
    ]

@app.post("/process-payroll")
async def process_payroll(file: UploadFile = File(...)):
    """
    INPUT: Sierra Excel (.xlsx/.xls) – rows with at least:
           - Name (employee name)
           - Hours (worked hours, numeric)
           - Date/Day (optional; used to compute Period End)
    LOGIC:
           - Sum hours per employee for the period
           - Split into REG (<=40) and OT (>40)
    OUTPUT: Excel with a WBS-compatible 'WEEKLY' sheet.
    """
    fname = (file.filename or "").lower()
    if not fname.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=422, detail="Please upload an Excel file (.xlsx or .xls).")

    try:
        raw = await file.read()
        df = pd.read_excel(BytesIO(raw), sheet_name=0)

        # Identify columns by aliases
        name_col = find_col(df, NAME_ALIASES)
        hrs_col = find_col(df, HOURS_ALIASES)
        date_col = find_col(df, DATE_ALIASES)

        if not name_col or not hrs_col:
            raise HTTPException(
                status_code=422,
                detail=f"Missing required columns. Need Name and Hours. "
                       f"Found columns: {list(df.columns)}"
            )

        # Clean & coerce
        df = df[[name_col, hrs_col] + ([date_col] if date_col else [])].copy()
        df = df[df[name_col].notna()]
        df[hrs_col] = pd.to_numeric(df[hrs_col], errors="coerce").fillna(0.0)

        # Determine period end (Sunday)
        if date_col:
            dt = pd.to_datetime(df[date_col], errors="coerce")
            valid = dt.dropna()
            pe = sunday_for_week(valid.max().date()) if not valid.empty else sunday_for_week(date.today())
        else:
            pe = sunday_for_week(date.today())

        # Aggregate per employee
        agg = (
            df.groupby(name_col, as_index=False)[hrs_col].sum()
              .rename(columns={name_col: "Name", hrs_col: "TotalHours"})
        )
        agg["Employee Name"] = agg["Name"].apply(to_last_first)
        agg["Reg"] = agg["TotalHours"].clip(upper=40)
        agg["OT"] = (agg["TotalHours"] - 40).clip(lower=0)

        out = build_wbs_sheet(agg, pe)
        out_name = f"WBS_Payroll_{pe.strftime('%Y-%m-%d')}.xlsx"
        headers = {"Content-Disposition": f'attachment; filename="{out_name}"'}
        return StreamingResponse(
            out,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers,
        )

    except HTTPException:
        raise
    except Exception as e:
        # Return JSON error visible to the frontend banner
        return JSONResponse(status_code=500, content={"detail": f"Server error: {str(e)}"})

# ------------------------------
# Local (not used on Railway)
# ------------------------------
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("app.main:app", host="0.0.0.0", port=port)

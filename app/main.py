# app/main.py
import io
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime, date

import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import StreamingResponse, JSONResponse

from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet


# ------------------------------------------------------------------------------
# App & CORS
# ------------------------------------------------------------------------------
app = FastAPI(title="Sierra → WBS Payroll Converter", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],     # tighten to your frontend if desired
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ALLOWED_EXTS = (".xlsx", ".xls")


# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------
def _ext_ok(name: str) -> bool:
    n = (name or "").lower()
    return any(n.endswith(e) for e in ALLOWED_EXTS)

def _std(s: str) -> str:
    return (s or "").strip().lower()

def _to_date(val) -> Optional[date]:
    if pd.isna(val):
        return None
    if isinstance(val, datetime):
        return val.date()
    try:
        return pd.to_datetime(val).date()
    except Exception:
        return None

def _normalize_name(raw: str) -> str:
    if not isinstance(raw, str):
        return ""
    s = raw.strip()
    # Keep as "Last, First" if already like that; if "First Last", flip.
    if "," in s:
        return s
    parts = [p for p in s.split() if p]
    if len(parts) == 2:
        return f"{parts[1]}, {parts[0]}"
    return s

def _apply_ca_daily_ot(hrs: float) -> Tuple[float, float, float]:
    """Return (REG, OT, DT) for a single day under CA rules."""
    h = max(float(hrs or 0.0), 0.0)
    reg = min(h, 8.0)
    ot = 0.0
    dt = 0.0
    if h > 8:
        ot = min(h - 8.0, 4.0)
    if h > 12:
        dt = h - 12.0
    return (reg, ot, dt)

def _safe_float(x) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0


# ------------------------------------------------------------------------------
# Load roster (SSN, Dept, Pay Type, Rate)
# ------------------------------------------------------------------------------
def _load_roster(repo_root: Path) -> pd.DataFrame:
    """Try roster.xlsx; fall back to roster.csv. Return normalized df with columns:
       name (Last, First), ssn, dept, type(H/S), rate(float)"""
    # Accept either file
    xlsx = repo_root / "roster.xlsx"
    csv  = repo_root / "roster.csv"

    if xlsx.exists():
        df = pd.read_excel(xlsx)
    elif csv.exists():
        df = pd.read_csv(csv)
    else:
        # Empty frame; caller will handle missing
        return pd.DataFrame(columns=["name", "ssn", "dept", "type", "rate"])

    # Try to map probable headers
    def fcol(cands: List[str]) -> Optional[str]:
        cols = { _std(c): c for c in df.columns }
        for want in cands:
            if _std(want) in cols:
                return cols[_std(want)]
        for want in cands:
            w = _std(want)
            for k, v in cols.items():
                if w in k:
                    return v
        return None

    col_name = fcol(["employee", "employee name", "name", "employee_name"])
    col_ssn  = fcol(["ssn", "social", "social security", "ssn last 4", "social security number"])
    col_dept = fcol(["dept", "department", "division"])
    col_type = fcol(["type", "pay type", "emp type", "status type"])
    col_rate = fcol(["rate", "pay rate", "hourly", "hourly rate", "wage"])

    out = pd.DataFrame()
    out["name"] = df[col_name].astype(str).map(_normalize_name) if col_name else ""
    out["ssn"]  = df[col_ssn].astype(str).str.strip() if col_ssn else ""
    out["dept"] = df[col_dept].astype(str).str.strip().str.upper() if col_dept else ""
    out["type"] = df[col_type].astype(str).str.strip().str.upper().map(lambda x: "S" if x.startswith("S") else "H") if col_type else "H"
    out["rate"] = pd.to_numeric(df[col_rate], errors="coerce").fillna(0.0) if col_rate else 0.0

    # Deduplicate by name; keep first non-null fields
    out = (
        out.groupby("name", dropna=False)
           .agg({
               "ssn": "first",
               "dept": "first",
               "type": "first",
               "rate": "first",
           })
           .reset_index()
    )
    return out


# ------------------------------------------------------------------------------
# Parse Sierra workbook and compute weekly REG/OT/DT per employee
# ------------------------------------------------------------------------------
def _parse_sierra(input_bytes: bytes, sheet_name: Optional[str] = None) -> pd.DataFrame:
    xls = pd.ExcelFile(io.BytesIO(input_bytes))
    sheet = sheet_name or xls.sheet_names[0]
    df = xls.parse(sheet)

    if df.empty:
        raise ValueError("Input sheet is empty.")

    # Flexible header detection
    def fcol(cands: List[str]) -> Optional[str]:
        cols = { _std(c): c for c in df.columns }
        for want in cands:
            if _std(want) in cols:
                return cols[_std(want)]
        for want in cands:
            w = _std(want)
            for k, v in cols.items():
                if w in k:
                    return v
        return None

    col_emp  = fcol(["employee", "employee name", "name", "worker"])
    col_date = fcol(["date", "work date", "day", "worked date"])
    col_hrs  = fcol(["hours", "hrs", "total hours", "work hours"])
    col_rate = fcol(["rate", "pay rate", "hourly rate", "wage"])

    if not all([col_emp, col_date, col_hrs]):
        raise ValueError("File format error - check your Excel structure.")

    core = pd.DataFrame()
    core["employee"] = df[col_emp].astype(str).map(_normalize_name)
    core["date"]     = df[col_date].map(_to_date)
    core["hours"]    = pd.to_numeric(df[col_hrs], errors="coerce").fillna(0.0).astype(float)
    core["rate"]     = pd.to_numeric(df[col_rate], errors="coerce").fillna(0.0).astype(float) if col_rate else 0.0

    core = core[(core["employee"].str.len() > 0) & core["date"].notna() & (core["hours"] > 0)]

    if core.empty:
        # Nothing to do
        return pd.DataFrame(columns=["employee","rate","REG","OT","DT"])

    # Sum same employee/day then split by CA daily rules
    daily = core.groupby(["employee", "date"], dropna=False).agg({"hours":"sum", "rate":"max"}).reset_index()

    rows = []
    for _, r in daily.iterrows():
        reg, ot, dt = _apply_ca_daily_ot(r["hours"])
        rows.append({
            "employee": r["employee"],
            "rate": float(r["rate"]),
            "REG": reg,
            "OT": ot,
            "DT": dt,
        })
    split = pd.DataFrame(rows)

    weekly = split.groupby(["employee"], dropna=False)[["REG","OT","DT"]].sum().reset_index()

    # carry forward the most common non-zero rate seen for the employee, else 0
    rates = (
        core.groupby("employee")["rate"]
            .apply(lambda s: float(s[s>0].mode().iloc[0]) if (s>0).any() else 0.0)
            .reset_index()
    )
    weekly = weekly.merge(rates, on="employee", how="left")
    weekly["rate"] = weekly["rate"].fillna(0.0).astype(float)

    return weekly


# ------------------------------------------------------------------------------
# WBS template utilities
# ------------------------------------------------------------------------------
def _find_header_row(ws: Worksheet) -> Tuple[int, Dict[str,int]]:
    """
    Find the row that contains the 'SSN' / 'Employee Name' headers.
    Return (header_row_index, column_map) where column_map maps logical fields
    to 1-based column indexes detected in the sheet.
    """
    wanted = {
        "SSN": ["ssn", "social", "social security"],
        "Employee Name": ["employee name", "employee", "name"],
        "Status": ["status"],
        "Type": ["type"],
        "Pay Rate": ["pay rate", "rate"],
        "Dept": ["dept", "department", "division"],
        "REG": ["regular", "a01"],
        "OT": ["overtime", "a02"],
        "DT": ["doubletime", "a03"],
        "VAC": ["vacation", "a06"],
        "SICK": ["sick", "a07"],
        "HOL": ["holiday", "a08"],
        "BONUS": ["bonus", "a04"],
        "COMM": ["commission", "a05"],
        # piece columns optional, totals column we'll detect by header 'Totals'
        "TOTALS": ["totals", "total", "total $", "amount"],
    }

    def match(cell_val: str, cands: List[str]) -> bool:
        s = _std(str(cell_val))
        return any(w in s for w in cands)

    for r in range(1, min(ws.max_row, 25)+1):
        row_vals = [ws.cell(row=r, column=c).value for c in range(1, ws.max_column+1)]
        # Consider a row a header if it contains "SSN" and "Employee"
        if any(match(v, wanted["SSN"]) for v in row_vals) and any(match(v, wanted["Employee Name"]) for v in row_vals):
            colmap: Dict[str,int] = {}
            for c in range(1, ws.max_column+1):
                v = ws.cell(row=r, column=c).value
                if v is None:
                    continue
                for key, cands in wanted.items():
                    if key in colmap:  # already matched
                        continue
                    if match(v, cands):
                        colmap[key] = c
            return r, colmap

    raise ValueError("Could not locate WBS header row (SSN / Employee Name). Check template.")


def _clear_old_rows(ws: Worksheet, start_row: int, first_col: int, last_col: int) -> None:
    """
    Clear previous data rows (below header) between [first_col, last_col],
    leaving styles and merged header cells intact.
    """
    max_row = ws.max_row
    if max_row <= start_row:
        return

    for r in range(start_row+1, max_row+1):
        # If the entire row (in data band) is already blank, skip
        empty = True
        for c in range(first_col, last_col+1):
            if ws.cell(row=r, column=c).value not in (None, ""):
                empty = False
                break
        if empty:
            continue

        for c in range(first_col, last_col+1):
            cell = ws.cell(row=r, column=c)
            # Avoid merged header leftovers (we’re below header anyway)
            try:
                cell.value = None
            except AttributeError:
                # If a stray merge appears, skip the write
                continue


# ------------------------------------------------------------------------------
# Core conversion -> write into WBS template
# ------------------------------------------------------------------------------
def convert_sierra_to_wbs(input_bytes: bytes, sheet_name: Optional[str] = None) -> bytes:
    repo_root = Path(__file__).resolve().parent.parent  # project root (../)
    template_path = repo_root / "wbs_template.xlsx"
    if not template_path.exists():
        raise ValueError(f"WBS template not found at {template_path}")

    # 1) Parse Sierra → weekly REG/OT/DT per employee
    weekly = _parse_sierra(input_bytes, sheet_name=sheet_name)

    # 2) Load roster (SSN, Dept, Type, Rate)
    roster = _load_roster(repo_root)
    has_roster = not roster.empty

    # Merge-in roster data by name
    if not weekly.empty and has_roster:
        out = weekly.merge(roster, left_on="employee", right_on="name", how="left", suffixes=("","_roster"))
    else:
        # create columns so downstream code is uniform
        out = weekly.copy()
        out["name"] = out["employee"]
        out["ssn"] = ""
        out["dept"] = ""
        out["type"] = "H"
        out["rate_roster"] = 0.0

    # Prefer roster rate if present
    out["rate_final"] = out.apply(lambda r: float(r["rate_roster"]) if _safe_float(r.get("rate_roster",0))>0 else float(r.get("rate",0)), axis=1)
    out["type_final"] = out["type"].fillna("H").astype(str).str.upper().map(lambda x: "S" if x.startswith("S") else "H")
    out["dept_final"] = out["dept"].fillna("").astype(str).str.upper()
    out["ssn_final"]  = out["ssn"].fillna("").astype(str).str.strip()

    # Dollars
    out["REG_$"]   = out["REG"] * out["rate_final"]
    out["OT_$"]    = out["OT"]  * out["rate_final"] * 1.5
    out["DT_$"]    = out["DT"]  * out["rate_final"] * 2.0
    out["TOTAL_$"] = out["REG_$"] + out["OT_$"] + out["DT_$"]

    # Sort by Dept then Name (stable)
    out = out.sort_values(by=["dept_final", "employee"], kind="stable").reset_index(drop=True)

    # 3) Open template and locate header/columns
    wb = load_workbook(str(template_path))
    ws = wb.active  # assume single visible sheet like your template

    header_row, cols = _find_header_row(ws)
    first_col = min(cols["SSN"], cols["Employee Name"])
    # Totals column might be absent in col map if the header text is different; handle later
    last_col = max(cols.values()) if cols else ws.max_column

    data_start = header_row + 1

    # Clear previous data safely
    _clear_old_rows(ws, header_row, first_col, last_col)

    # 4) Write rows
    current_row = data_start
    for _, r in out.iterrows():
        # Required identity
        ws.cell(row=current_row, column=cols["SSN"]).value = r["ssn_final"] or ""
        ws.cell(row=current_row, column=cols["Employee Name"]).value = r["employee"]
        if "Status" in cols:
            ws.cell(row=current_row, column=cols["Status"]).value = "A"
        if "Type" in cols:
            ws.cell(row=current_row, column=cols["Type"]).value = r["type_final"]
        if "Pay Rate" in cols:
            ws.cell(row=current_row, column=cols["Pay Rate"]).value = round(_safe_float(r["rate_final"]), 2)
        if "Dept" in cols:
            ws.cell(row=current_row, column=cols["Dept"]).value = r["dept_final"]

        # Hours (0 when missing)
        if "REG" in cols:
            ws.cell(row=current_row, column=cols["REG"]).value = round(_safe_float(r["REG"]), 3)
        if "OT" in cols:
            ws.cell(row=current_row, column=cols["OT"]).value = round(_safe_float(r["OT"]), 3)
        if "DT" in cols:
            ws.cell(row=current_row, column=cols["DT"]).value = round(_safe_float(r["DT"]), 3)

        # Leave VAC/SICK/HOL/BONUS/COMM blank (0) unless you track them in Sierra
        for key in ["VAC","SICK","HOL","BONUS","COMM"]:
            if key in cols:
                ws.cell(row=current_row, column=cols[key]).value = 0.0

        # Pink Totals column: if template has a formula on this row it will auto-calc,
        # otherwise we also write the computed total dollars so it’s never blank.
        if "TOTALS" in cols:
            cell = ws.cell(row=current_row, column=cols["TOTALS"])
            if cell.value in (None, ""):
                cell.value = round(_safe_float(r["TOTAL_$"]), 2)

        current_row += 1

    # 5) Auto-fit-ish: don’t change widths, keep template’s styling
    # 6) Save to bytes
    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)
    return bio.read()


# ------------------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------------------
@app.get("/health")
def health():
    return JSONResponse({"ok": True, "ts": datetime.utcnow().isoformat() + "Z"})

@app.post("/process-payroll")
async def process_payroll(file: UploadFile = File(...)):
    if not file or not file.filename:
        raise HTTPException(status_code=400, detail="No file uploaded.")
    if not _ext_ok(file.filename):
        raise HTTPException(status_code=415, detail="Unsupported file type. Please upload .xlsx or .xls")

    try:
        contents = await file.read()
        out_bytes = convert_sierra_to_wbs(contents, sheet_name=None)
        out_name = f"WBS_Payroll_{datetime.utcnow().date().isoformat()}.xlsx"
        return StreamingResponse(
            io.BytesIO(out_bytes),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename="{out_name}"'}
        )
    except ValueError as ve:
        # Format / header issues or missing template/roster
        raise HTTPException(status_code=422, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")

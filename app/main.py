# app/main.py
import io
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import StreamingResponse, JSONResponse

from openpyxl import load_workbook
from openpyxl.utils import get_column_letter

# =============================================================================
# App + CORS
# =============================================================================
app = FastAPI(title="Sierra → WBS Payroll Converter", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],            # tighten to your Netlify origin if desired
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ALLOWED_EXTS = (".xlsx", ".xls")

# =============================================================================
# Path helpers (robust on Railway / local)
# =============================================================================
def find_in_repo(filename: str) -> Path:
    """
    Search common locations for a file committed to the repo.
    Returns the first existing Path, else the most likely path.
    """
    here = Path(__file__).resolve().parent          # /app/app
    candidates = [
        here / filename,                            # /app/app/filename
        here.parent / filename,                     # /app/filename
        Path.cwd() / filename,                      # CWD/filename
        Path.cwd().parent / filename,               # parent of CWD
    ]
    for p in candidates:
        if p.exists():
            return p
    return here.parent / filename

def find_roster_path() -> Optional[Path]:
    for name in ("roster.xlsx", "roster.csv"):
        p = find_in_repo(name)
        if p.exists():
            return p
    return None

# =============================================================================
# Light normalizers
# =============================================================================
def _std(s: str) -> str:
    return (s or "").strip().lower().replace("\n", " ").replace("\r", " ")

def _to_date(val) -> Optional[date]:
    if pd.isna(val):
        return None
    if isinstance(val, datetime):
        return val.date()
    try:
        return pd.to_datetime(val).date()
    except Exception:
        return None

def ca_daily_split(hours: float) -> Tuple[float, float, float]:
    """Return (REG, OT, DT) by California 8/4/>12 rule for a single day."""
    h = float(hours or 0.0)
    reg = min(h, 8.0)
    ot = 0.0
    dt = 0.0
    if h > 8:
        ot = min(h - 8.0, 4.0)
    if h > 12:
        dt = h - 12.0
    return reg, ot, dt

# =============================================================================
# Roster (optional but recommended)
# =============================================================================
def load_roster() -> pd.DataFrame:
    """
    Load roster with columns like:
      name, ssn, dept, type, rate
    Column names are matched loosely.
    Returns empty DataFrame if no roster file is present.
    """
    p = find_roster_path()
    if not p:
        return pd.DataFrame()

    if p.suffix.lower() == ".csv":
        df = pd.read_csv(p)
    else:
        df = pd.read_excel(p)

    # Loose header matching
    cols = {_std(c): c for c in df.columns}
    def pick(*options) -> Optional[str]:
        for o in options:
            o_ = _std(o)
            if o_ in cols:
                return cols[o_]
        # relaxed contains
        for o in options:
            o_ = _std(o)
            for k, v in cols.items():
                if o_ in k:
                    return v
        return None

    name_col = pick("name", "employee", "employee name")
    ssn_col  = pick("ssn", "social", "social security", "social security number")
    dept_col = pick("department", "dept")
    type_col = pick("type", "pay type", "employee type")
    rate_col = pick("rate", "pay rate", "hourly", "wage")

    keep = {}
    if name_col: keep["name"] = df[name_col].astype(str).str.strip()
    if ssn_col:  keep["ssn"]  = df[ssn_col].astype(str).str.strip()
    if dept_col: keep["dept"] = df[dept_col].astype(str).str.strip().str.upper()
    if type_col: keep["type"] = df[type_col].astype(str).str.strip().str.upper()
    if rate_col: keep["rate"] = pd.to_numeric(df[rate_col], errors="coerce").fillna(0.0)

    out = pd.DataFrame(keep)
    # Normalize name key to merge: use "Last, First" if provided; also accept "First Last"
    def norm_name(n: str) -> str:
        n = (n or "").strip()
        if "," in n:
            # already "Last, First"
            return " ".join(p for p in n.split() if p)
        parts = [p for p in n.split() if p]
        if len(parts) == 2:
            return f"{parts[1]}, {parts[0]}"
        return n
    if "name" in out.columns:
        out["name_key"] = out["name"].map(norm_name)
    return out

# =============================================================================
# Sierra input → weekly totals (REG/OT/DT) per employee & rate
# =============================================================================
def parse_sierra(input_bytes: bytes, sheet_name: Optional[str] = None) -> pd.DataFrame:
    """Return DataFrame with columns: employee, rate, REG, OT, DT."""
    excel = pd.ExcelFile(io.BytesIO(input_bytes))
    target = sheet_name or excel.sheet_names[0]
    df = excel.parse(target)

    if df.empty:
        raise ValueError("Input sheet is empty.")

    # Required columns (loose match)
    req_map = {
        "employee": ["employee", "employee name", "name", "worker", "employee_name"],
        "date":     ["date", "work date", "worked date", "day"],
        "hours":    ["hours", "hrs", "total hours", "work hours"],
    }
    # Optional
    opt_map = {
        "rate":     ["rate", "pay rate", "hourly", "wage"],
    }

    cols = {_std(c): c for c in df.columns}

    def pick(candidates: List[str]) -> Optional[str]:
        for want in candidates:
            key = _std(want)
            if key in cols:
                return cols[key]
        for want in candidates:
            key = _std(want)
            for k, v in cols.items():
                if key in k:
                    return v
        return None

    missing = []
    got = {}
    for k, cand in req_map.items():
        col = pick(cand)
        if not col: missing.append(k)
        got[k] = col
    if missing:
        raise ValueError(f"Missing required columns in Sierra file: {', '.join(missing)}")

    rate_col = pick(opt_map["rate"])

    core = pd.DataFrame({
        "employee": df[got["employee"]].astype(str).str.strip(),
        "date":     df[got["date"]].map(_to_date),
        "hours":    pd.to_numeric(df[got["hours"]], errors="coerce").fillna(0.0).astype(float),
    })
    if rate_col:
        core["rate"] = pd.to_numeric(df[rate_col], errors="coerce").fillna(0.0).astype(float)
    else:
        core["rate"] = 0.0

    # Normalize names to "Last, First" if "First Last" given
    def to_last_first(n: str) -> str:
        n = (n or "").strip()
        parts = [p for p in n.split() if p]
        if len(parts) == 2 and "," not in n:
            return f"{parts[1]}, {parts[0]}"
        return n
    core["employee"] = core["employee"].map(to_last_first)

    core = core[(core["employee"].str.len() > 0) & core["date"].notna() & (core["hours"] > 0)]

    # Sum per employee/day; then split CA daily OT
    per_day = core.groupby(["employee", "date", "rate"], dropna=False)["hours"].sum().reset_index()

    rows = []
    for _, r in per_day.iterrows():
        reg, ot, dt = ca_daily_split(float(r["hours"]))
        rows.append({
            "employee": r["employee"],
            "rate": float(r["rate"]),
            "REG": reg,
            "OT": ot,
            "DT": dt,
        })
    daily = pd.DataFrame(rows)
    if daily.empty:
        return pd.DataFrame(columns=["employee", "rate", "REG", "OT", "DT"])

    weekly = (
        daily.groupby(["employee", "rate"], dropna=False)[["REG", "OT", "DT"]]
        .sum()
        .reset_index()
    )
    return weekly

# =============================================================================
# Template writing (header-driven; safe with merged cells)
# =============================================================================
def find_headers(ws, header_row: int = 8) -> Dict[str, int]:
    """
    Scan a row and map logical names → column indexes.
    We look for:
      SSN, Employee Name, Status, Type, Pay Rate, Dept,
      REGULAR (A01), OVERTIME (A02), DOUBLETIME (A03),
      VACATION (A06), SICK (A07), HOLIDAY (A08),
      BONUS (A04), COMMISSION (A05),
      TRAVEL AMOUNT (ATE), Notes and Comments, Totals
    """
    names = {}
    for c in range(1, ws.max_column + 1):
        val = ws.cell(row=header_row, column=c).value
        if val is None:
            continue
        sval = _std(str(val))
        if "ssn" == sval:
            names["SSN"] = c
        elif "employee name" in sval:
            names["NAME"] = c
        elif sval == "status":
            names["STATUS"] = c
        elif sval == "type":
            names["TYPE"] = c
        elif "pay rate" in sval:
            names["RATE"] = c
        elif "dept" in sval or "department" in sval:
            names["DEPT"] = c
        elif "regular" in sval and "a01" in sval:
            names["A01"] = c
        elif "overtime" in sval and "a02" in sval:
            names["A02"] = c
        elif "doubletime" in sval and "a03" in sval:
            names["A03"] = c
        elif "vacation" in sval and "a06" in sval:
            names["A06"] = c
        elif "sick" in sval and "a07" in sval:
            names["A07"] = c
        elif "holiday" in sval and "a08" in sval:
            names["A08"] = c
        elif "bonus" in sval and "a04" in sval:
            names["A04"] = c
        elif "commission" in sval and "a05" in sval:
            names["A05"] = c
        elif "travel amount" in sval or "ate" == sval:
            names["TRAVEL"] = c
        elif "notes and" in sval and "comments" in sval:
            names["NOTES"] = c
        elif "total" in sval:
            names["TOTALS"] = c
    required = ["SSN", "NAME", "STATUS", "TYPE", "RATE", "DEPT", "A01", "A02", "A03", "TOTALS"]
    missing = [k for k in required if k not in names]
    if missing:
        raise ValueError(f"Template header detection failed; missing columns: {', '.join(missing)}")
    return names

def safe_clear_data(ws, col_map: Dict[str, int], start_row: int = 9) -> int:
    """
    Clear existing data rows (values only) without breaking styles/merges.
    Returns the next row to write at (first data row).
    """
    max_row = ws.max_row
    if max_row < start_row:
        return start_row

    for r in range(start_row, max_row + 1):
        # If row is already blank (name + rate + totals empty), skip clearing
        name_empty = ws.cell(row=r, column=col_map["NAME"]).value in (None, "")
        rate_empty = ws.cell(row=r, column=col_map["RATE"]).value in (None, "")
        totals_empty = ws.cell(row=r, column=col_map["TOTALS"]).value in (None, "")
        all_empty = name_empty and rate_empty and totals_empty
        if all_empty:
            continue

        for c in range(1, col_map["TOTALS"] + 1):
            cell = ws.cell(row=r, column=c)
            try:
                cell.value = None
            except AttributeError:
                # merged read-only cell in openpyxl; skip
                continue
    return start_row

# =============================================================================
# Main conversion → write to template
# =============================================================================
def sierra_to_wbs_bytes(input_bytes: bytes) -> bytes:
    # Parse Sierra
    weekly = parse_sierra(input_bytes)
    # Load roster (optional enrich)
    roster = load_roster()
    roster_map = {}
    if not roster.empty:
        roster_map = {
            n: {
                "ssn": rec.get("ssn", ""),
                "dept": rec.get("dept", ""),
                "type": rec.get("type", ""),
                "rate": float(rec.get("rate", 0.0)) if pd.notna(rec.get("rate", 0.0)) else 0.0,
            }
            for n, rec in roster.set_index("name_key").to_dict(orient="index").items()
        }

    # Open template
    template_path = find_in_repo("wbs_template.xlsx")
    if not template_path.exists():
        raise HTTPException(status_code=500, detail=f"WBS template not found at {template_path}")

    wb = load_workbook(str(template_path))
    ws = wb.active  # single weekly sheet

    # Map headers & clear
    COL = find_headers(ws, header_row=8)
    DATA_START = 9
    current = safe_clear_data(ws, COL, start_row=DATA_START)

    # Helper to compute totals $
    def total_dollars(rate: float, reg: float, ot: float, dt: float,
                      vac: float = 0.0, sick: float = 0.0, hol: float = 0.0,
                      bonus: float = 0.0, comm: float = 0.0, travel: float = 0.0) -> float:
        base = (reg * rate) + (ot * rate * 1.5) + (dt * rate * 2.0)
        other = (vac + sick + hol) * rate + float(bonus or 0.0) + float(comm or 0.0) + float(travel or 0.0)
        return round(base + other, 2)

    # Write each employee row
    for _, row in weekly.sort_values(["employee"]).iterrows():
        name = str(row["employee"]).strip()
        rate = float(row.get("rate", 0.0))
        reg  = float(row.get("REG", 0.0))
        ot   = float(row.get("OT", 0.0))
        dt   = float(row.get("DT", 0.0))

        # Enrich from roster (by name key)
        name_key = name
        info = roster_map.get(name_key, {})
        ssn  = info.get("ssn", "")
        dept = info.get("dept", "")
        etype = info.get("type", "")   # "H" or "S"
        rate_from_roster = info.get("rate", 0.0)

        if not rate and rate_from_roster:
            rate = float(rate_from_roster)

        status = "A"
        pay_type = "H" if str(etype).upper().startswith("H") else ("S" if str(etype).upper().startswith("S") else "H")

        vac = sick = hol = bonus = comm = travel = 0.0  # extend later if Sierra provides

        # Totals in dollars
        totals_val = total_dollars(rate, reg, ot, dt, vac, sick, hol, bonus, comm, travel)

        # Write row values
        def W(col_key: str, value):
            c = COL[col_key]
            ws.cell(row=current, column=c).value = value

        W("SSN", ssn)                  # SSN
        W("NAME", name)                # Employee Name
        W("STATUS", status)            # Status
        W("TYPE", pay_type)            # Pay Type
        W("RATE", round(rate, 2))      # Pay Rate
        if "DEPT" in COL: W("DEPT", dept)

        W("A01", round(reg, 3))
        W("A02", round(ot, 3))
        W("A03", round(dt, 3))
        if "A06" in COL: W("A06", round(vac, 3))
        if "A07" in COL: W("A07", round(sick, 3))
        if "A08" in COL: W("A08", round(hol, 3))
        if "A04" in COL: W("A04", round(bonus, 2))
        if "A05" in COL: W("A05", round(comm, 2))
        if "TRAVEL" in COL: W("TRAVEL", round(travel, 2))

        # Notes left empty
        if "TOTALS" in COL:
            ws.cell(row=current, column=COL["TOTALS"]).value = totals_val

        current += 1

    # Optional: autosize visible columns up to TOTALS
    for c in range(1, COL["TOTALS"] + 1):
        letter = get_column_letter(c)
        max_len = 12
        for r in range(1, current):
            v = ws.cell(row=r, column=c).value
            if v is None:
                continue
            max_len = max(max_len, len(str(v)))
        ws.column_dimensions[letter].width = min(max_len + 2, 30)

    # Save to bytes
    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)
    return bio.read()

# =============================================================================
# Routes
# =============================================================================
@app.get("/health")
def health():
    return JSONResponse({"ok": True, "ts": datetime.utcnow().isoformat() + "Z"})

@app.post("/process-payroll")
async def process_payroll(file: UploadFile = File(...)):
    if not file or not file.filename:
        raise HTTPException(status_code=400, detail="No selected file.")
    name = (file.filename or "").lower()
    if not any(name.endswith(ext) for ext in ALLOWED_EXTS):
        raise HTTPException(status_code=415, detail="Unsupported file type. Please upload .xlsx or .xls")

    try:
        contents = await file.read()
        out_bytes = sierra_to_wbs_bytes(contents)
        out_name = f"WBS_Payroll_{datetime.utcnow().date().isoformat()}.xlsx"
        return StreamingResponse(
            io.BytesIO(out_bytes),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename="{out_name}"'}
        )
    except ValueError as ve:
        raise HTTPException(status_code=422, detail=str(ve))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")

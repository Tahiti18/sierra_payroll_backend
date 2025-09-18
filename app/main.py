# app/main.py
import io
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet
from starlette.responses import JSONResponse, StreamingResponse

# ====================================================================================
# FastAPI + CORS
# ====================================================================================

app = FastAPI(title="Sierra → WBS Payroll Converter", version="2.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],            # optionally restrict to your Netlify origin
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ALLOWED_EXTS = (".xlsx", ".xls")

# ====================================================================================
# Helpers
# ====================================================================================

def _ext_ok(filename: str) -> bool:
    name = (filename or "").lower()
    return any(name.endswith(e) for e in ALLOWED_EXTS)

def _std(s: str) -> str:
    return (s or "").strip().lower()

def _std_col(s: str) -> str:
    return _std(s).replace("\n", " ").replace("\r", " ")

def _normalize_name(raw: str) -> str:
    if not isinstance(raw, str):
        return ""
    name = " ".join(raw.replace(",", " , ").split()).strip()
    if not name:
        return ""
    # if "First Last" → "Last, First" (most common)
    parts = [p for p in name.replace(",", "").split() if p]
    if len(parts) == 2:
        return f"{parts[1]}, {parts[0]}"
    return name

def _to_date(val) -> Optional[date]:
    if pd.isna(val):
        return None
    if isinstance(val, datetime):
        return val.date()
    try:
        return pd.to_datetime(val).date()
    except Exception:
        return None

def _apply_ca_daily_ot(day_hours: float) -> Dict[str, float]:
    """CA daily overtime split: <=8 REG, 8–12 OT, >12 DT."""
    h = float(day_hours or 0.0)
    reg = min(h, 8.0)
    ot = max(0.0, min(h - 8.0, 4.0))
    dt = max(0.0, h - 12.0)
    return {"REG": reg, "OT": ot, "DT": dt}

def _money(x) -> float:
    return float(x or 0.0)

# ====================================================================================
# Roster loader (root/roster.xlsx or root/roster.csv)
# ====================================================================================

def load_roster() -> Dict[str, Dict[str, str]]:
    root = Path(__file__).resolve().parents[1]   # repo root
    xlsx = root / "roster.xlsx"
    csv  = root / "roster.csv"
    if xlsx.exists():
        df = pd.read_excel(xlsx)
    elif csv.exists():
        df = pd.read_csv(csv)
    else:
        return {}

    def norm_name(n: str) -> str:
        return _normalize_name(str(n or ""))

    roster: Dict[str, Dict[str, str]] = {}
    for _, r in df.iterrows():
        name = norm_name(r.get("name", ""))
        if not name:
            continue
        roster[name] = {
            "ssn": str(r.get("ssn", "") or "").strip(),
            "department": str(r.get("department", "") or "").strip().upper(),
            "type": str(r.get("type", "") or "").strip().upper()[:1] or "H",
            "pay_rate": float(r.get("pay_rate", 0) or 0),
        }
    return roster

# ====================================================================================
# Sierra → weekly REG/OT/DT aggregator
# ====================================================================================

def parse_sierra(input_bytes: bytes) -> pd.DataFrame:
    """Return weekly totals per employee with columns: employee, rate, REG, OT, DT."""
    excel = pd.ExcelFile(io.BytesIO(input_bytes))
    sheet = excel.sheet_names[0]
    df = excel.parse(sheet)

    if df.empty:
        raise ValueError("Input sheet is empty.")

    required = {
        "employee": ["employee", "employee name", "name", "worker", "employee_name"],
        "date":     ["date", "work date", "day", "worked date"],
        "hours":    ["hours", "hrs", "total hours", "work hours"],
    }
    # pay rate can be missing in Sierra; we'll prefer roster
    possible_rate = ["rate", "pay rate", "hourly", "hourly rate", "wage"]

    # resolve columns (mild fuzzy on headers)
    cols = { _std_col(c): c for c in df.columns }

    def find_one(opts: List[str]) -> Optional[str]:
        for o in opts:
            k = _std_col(o)
            if k in cols:
                return cols[k]
        # contains
        for o in opts:
            k = _std_col(o)
            for key, orig in cols.items():
                if k in key:
                    return orig
        return None

    emp_col = find_one(required["employee"])
    dt_col  = find_one(required["date"])
    hrs_col = find_one(required["hours"])
    if not all([emp_col, dt_col, hrs_col]):
        raise ValueError("Missing required columns: employee/date/hours")

    rate_col = find_one(possible_rate)

    core = df[[emp_col, dt_col, hrs_col] + ([rate_col] if rate_col else [])].copy()
    core.columns = ["employee", "date", "hours"] + (["rate"] if rate_col else [])

    core["employee"] = core["employee"].astype(str).map(_normalize_name)
    core["date"] = core["date"].map(_to_date)
    core["hours"] = pd.to_numeric(core["hours"], errors="coerce").fillna(0.0).astype(float)
    if "rate" in core.columns:
        core["rate"] = pd.to_numeric(core.get("rate"), errors="coerce").fillna(0.0).astype(float)
    else:
        core["rate"] = 0.0

    core = core[(core["employee"].str.len() > 0) & core["date"].notna() & (core["hours"] > 0)]

    # group per employee/date and split to REG/OT/DT
    daily = core.groupby(["employee", "date"], dropna=False).agg({"hours": "sum", "rate": "max"}).reset_index()

    rows = []
    for _, r in daily.iterrows():
        split = _apply_ca_daily_ot(float(r["hours"]))
        rows.append({
            "employee": r["employee"],
            "REG": split["REG"],
            "OT":  split["OT"],
            "DT":  split["DT"],
            "rate": float(r["rate"] or 0.0),
        })
    split_df = pd.DataFrame(rows)

    weekly = split_df.groupby(["employee"], dropna=False)[["REG", "OT", "DT"]].sum().reset_index()
    # Keep a placeholder rate (max seen); roster will override if present
    rate_map = split_df.groupby("employee")["rate"].max().to_dict()
    weekly["rate"] = weekly["employee"].map(lambda n: float(rate_map.get(n, 0.0)))
    return weekly

# ====================================================================================
# Template helpers: find columns by header text (robust to column positions)
# ====================================================================================

HeaderMatchers: Dict[str, List[str]] = {
    "SSN":        ["ssn"],
    "EMPLOYEE":   ["employee name", "employee"],
    "STATUS":     ["status"],
    "TYPE":       ["type"],
    "PAY_RATE":   ["pay rate", "payrate", "rate"],
    "DEPT":       ["dept", "department"],
    "A01_REG":    ["regular", "a01"],
    "A02_OT":     ["overtime", "a02"],
    "A03_DT":     ["doubletime", "double time", "a03"],
    # optional leave codes if your template has them:
    "A06_VAC":    ["vacation", "a06"],
    "A07_SICK":   ["sick", "a07"],
    "A08_HOL":    ["holiday", "a08"],
    "TOTALS":     ["totals", "total"],
}

def _find_header_row(ws: Worksheet, max_scan_rows: int = 30, max_scan_cols: int = 200) -> int:
    """Return the header row index (the one that contains 'Employee Name' etc.)."""
    want = {"employee", "employee name", "ssn", "status", "type"}
    for r in range(1, max_scan_rows + 1):
        row_vals = [str(ws.cell(row=r, column=c).value or "").strip().lower() for c in range(1, max_scan_cols + 1)]
        hit = sum(1 for v in row_vals if any(w in v for w in want))
        if hit >= 2:  # crude but effective
            return r
    # fallback to 8 (common in your template)
    return 8

def _map_columns(ws: Worksheet) -> Dict[str, int]:
    """Scan the header row and build a name->column index map."""
    header_row = _find_header_row(ws)
    col_map: Dict[str, int] = {}
    for c in range(1, ws.max_column + 1):
        text = _std_col(str(ws.cell(row=header_row, column=c).value or ""))
        if not text:
            continue
        for key, opts in HeaderMatchers.items():
            if key in col_map:
                continue
            for o in opts:
                if _std_col(o) in text:
                    col_map[key] = c
                    break
    # sanity: we must have these
    required_keys = ["SSN", "EMPLOYEE", "STATUS", "TYPE", "PAY_RATE", "DEPT", "A01_REG", "A02_OT", "A03_DT", "TOTALS"]
    missing = [k for k in required_keys if k not in col_map]
    if missing:
        raise ValueError(f"Template header detection failed; missing columns: {missing}")
    # Attach header row index for caller
    col_map["_HEADER_ROW"] = header_row
    return col_map

def _is_merged_master(ws: Worksheet, row: int, col: int) -> bool:
    cell = ws.cell(row=row, column=col)
    # openpyxl keeps merged ranges; top-left cell is the 'master'
    for mr in ws.merged_cells.ranges:
        if (row, col) in mr:
            return (row == mr.min_row) and (col == mr.min_col)
    return False

# ====================================================================================
# Write into template without touching formulas/merged cells
# ====================================================================================

def write_to_template(weekly: pd.DataFrame) -> bytes:
    root = Path(__file__).resolve().parents[1]
    template_path = root / "wbs_template.xlsx"
    if not template_path.exists():
        raise HTTPException(status_code=500, detail=f"WBS template not found at {template_path}")

    wb = load_workbook(str(template_path))
    ws = wb.active  # assumes the WEEKLY sheet is active

    COL = _map_columns(ws)
    HEADER_ROW = int(COL["_HEADER_ROW"])
    DATA_START_ROW = HEADER_ROW + 1

    # 1) Clear prior data rows BUT keep styles & formulas (do NOT touch pink totals)
    max_row = ws.max_row
    if max_row >= DATA_START_ROW:
        for r in range(DATA_START_ROW, max_row + 1):
            # quick skip for empty data row (excluding totals column)
            left_last_col = COL["TOTALS"] - 1
            if all((ws.cell(row=r, column=c).value in (None, "")) for c in range(1, left_last_col + 1)):
                continue
            for c in range(1, COL["TOTALS"]):   # <--- DO NOT INCLUDE TOTALS
                try:
                    if _is_merged_master(ws, r, c):
                        continue
                    ws.cell(row=r, column=c).value = None
                except AttributeError:
                    # merged read-only
                    continue

    # 2) Load roster
    roster = load_roster()

    # 3) Sort by Dept then Employee for stable output (use roster dept if present)
    def key_dept(emp: str) -> Tuple[str, str]:
        d = roster.get(emp, {}).get("department", "")
        return (d, emp)
    employees = sorted(weekly["employee"].tolist(), key=key_dept)

    # 4) Write rows
    current_row = DATA_START_ROW
    for emp in employees:
        row = weekly[weekly["employee"] == emp].iloc[0]
        reg = _money(row["REG"])
        ot  = _money(row["OT"])
        dt  = _money(row["DT"])
        rate_src = _money(row.get("rate", 0.0))

        ro = roster.get(emp, {})
        ssn  = ro.get("ssn", "")
        dept = (ro.get("department") or "").upper()
        etype= (ro.get("type") or "H").upper()[:1]
        rate = _money(ro.get("pay_rate") or rate_src)

        # minimal defaults
        if not etype: etype = "H"

        # Write core identity + hours (NEVER touch totals column)
        ws.cell(row=current_row, column=COL["SSN"]).value = ssn
        ws.cell(row=current_row, column=COL["EMPLOYEE"]).value = emp
        ws.cell(row=current_row, column=COL["STATUS"]).value = "A"
        ws.cell(row=current_row, column=COL["TYPE"]).value = etype
        ws.cell(row=current_row, column=COL["DEPT"]).value = dept
        ws.cell(row=current_row, column=COL["PAY_RATE"]).value = float(rate)

        ws.cell(row=current_row, column=COL["A01_REG"]).value = float(reg)
        ws.cell(row=current_row, column=COL["A02_OT"]).value  = float(ot)
        ws.cell(row=current_row, column=COL["A03_DT"]).value  = float(dt)

        # Optional leaves left blank unless you feed them
        if "A06_VAC" in COL: ws.cell(row=current_row, column=COL["A06_VAC"]).value = None
        if "A07_SICK" in COL: ws.cell(row=current_row, column=COL["A07_SICK"]).value = None
        if "A08_HOL" in COL:  ws.cell(row=current_row, column=COL["A08_HOL"]).value  = None

        current_row += 1

    # Return bytes
    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)
    return bio.read()

# ====================================================================================
# HTTP endpoints
# ====================================================================================

@app.get("/health")
def health():
    return JSONResponse({"ok": True, "ts": datetime.utcnow().isoformat() + "Z"})

@app.post("/process-payroll")
async def process_payroll(file: UploadFile = File(...)):
    if not file or not file.filename:
        raise HTTPException(status_code=400, detail="No selected file.")
    if not _ext_ok(file.filename):
        raise HTTPException(status_code=415, detail="Unsupported file type. Please upload .xlsx or .xls")

    try:
        contents = await file.read()
        weekly = parse_sierra(contents)
        out_bytes = write_to_template(weekly)
        out_name = f"WBS_Payroll_{datetime.utcnow().date().isoformat()}.xlsx"
        return StreamingResponse(
            io.BytesIO(out_bytes),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename="{out_name}"'}
        )
    except ValueError as ve:
        raise HTTPException(status_code=422, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")

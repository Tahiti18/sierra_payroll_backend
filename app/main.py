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
from openpyxl.cell.cell import MergedCell

# --------------------------------------------------------------------------------------
# FastAPI + CORS
# --------------------------------------------------------------------------------------
app = FastAPI(title="Sierra → WBS Payroll Converter", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],     # tighten for prod if needed
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ALLOWED_EXTS = (".xlsx", ".xls", ".csv")

# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------
def _ext_ok(name: str) -> bool:
    n = (name or "").lower()
    return any(n.endswith(e) for e in ALLOWED_EXTS)

def _std(s: str) -> str:
    return (s or "").strip().lower().replace("\n", " ").replace("\r", " ")

def _find_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    by_std = {_std(c): c for c in df.columns}
    for want in candidates:
        key = _std(want)
        if key in by_std:
            return by_std[key]
    for want in candidates:
        key = _std(want)
        for k, v in by_std.items():
            if key in k:
                return v
    return None

def _require(df: pd.DataFrame, mapping: Dict[str, List[str]]) -> Dict[str, str]:
    out, miss = {}, []
    for logical, opts in mapping.items():
        col = _find_col(df, opts)
        if not col:
            miss.append(f"{logical} (any of: {', '.join(opts)})")
        else:
            out[logical] = col
    if miss:
        raise ValueError("Missing required columns: " + "; ".join(miss))
    return out

def _parse_date(x) -> Optional[date]:
    if pd.isna(x):
        return None
    if isinstance(x, datetime):
        return x.date()
    if isinstance(x, date):
        return x
    try:
        return pd.to_datetime(x, errors="coerce").date()
    except Exception:
        return None

def _normalize_name(x) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    # If Sierra is "First Last", keep it; if it’s "Last, First" keep too.
    # We’ll use roster matching case-insensitively below.
    return s

def _money(x) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0

def _h(x) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0

def _apply_ca_daily_ot(day_hours: float) -> Tuple[float, float, float]:
    h = float(day_hours or 0.0)
    reg = min(h, 8.0)
    ot = 0.0
    dt = 0.0
    if h > 8:
        ot = min(h - 8.0, 4.0)  # 8–12
    if h > 12:
        dt = h - 12.0
    return reg, ot, dt

# --------------------------------------------------------------------------------------
# Sierra → weekly splits (REG/OT/DT + other buckets if present)
# --------------------------------------------------------------------------------------
def parse_sierra(input_bytes: bytes, sheet_name: Optional[str] = None) -> pd.DataFrame:
    """
    Return a normalized long-frame:
      columns: name, date, hours, earn (REG/OT/DT/VAC/SICK/HOL/BONUS/COMM/PC_MON..PC_FRI/PC_TTL_MON.._FRI/TRAVEL)
    Unknown earn types are treated as REG hours.
    """
    xl = pd.ExcelFile(io.BytesIO(input_bytes))
    target = sheet_name or xl.sheet_names[0]
    df = xl.parse(target)

    if df.empty:
        raise ValueError("Input Excel sheet is empty.")

    # Try to detect core columns
    req = _require(df, {
        "name":  ["employee", "employee name", "name", "worker", "employee_name"],
        "date":  ["date", "work date", "worked date", "day"],
        "hours": ["hours", "hrs", "total hours", "work hours"],
    })
    # Optional buckets if Sierra provides them
    earn_col  = _find_col(df, ["earn", "earn type", "type", "task", "code", "earning"])
    vac_col   = _find_col(df, ["vacation", "vac", "a06"])
    sick_col  = _find_col(df, ["sick", "a07"])
    hol_col   = _find_col(df, ["holiday", "hol", "a08"])
    bonus_col = _find_col(df, ["bonus", "a04"])
    comm_col  = _find_col(df, ["commission", "comm", "a05"])
    travel_col= _find_col(df, ["travel amount", "travel", "ate"])

    # Piecework / production columns (hrs and totals Mon–Fri)
    pc_map = {
        "pc_hrs_mon": _find_col(df, ["pc hrs mon", "ah1"]),
        "pc_ttl_mon": _find_col(df, ["pc ttl mon", "ai1"]),
        "pc_hrs_tue": _find_col(df, ["pc hrs tue", "ah2"]),
        "pc_ttl_tue": _find_col(df, ["pc ttl tue", "ai2"]),
        "pc_hrs_wed": _find_col(df, ["pc hrs wed", "ah3"]),
        "pc_ttl_wed": _find_col(df, ["pc ttl wed", "ai3"]),
        "pc_hrs_thu": _find_col(df, ["pc hrs thu", "ah4"]),
        "pc_ttl_thu": _find_col(df, ["pc ttl thu", "ai4"]),
        "pc_hrs_fri": _find_col(df, ["pc hrs fri", "ah5"]),
        "pc_ttl_fri": _find_col(df, ["pc ttl fri", "ai5"]),
    }

    core = pd.DataFrame({
        "name":  df[req["name"]].map(_normalize_name),
        "date":  df[req["date"]].map(_parse_date),
        "hours": pd.to_numeric(df[req["hours"]], errors="coerce").fillna(0.0),
    })
    if earn_col:
        core["earn"] = df[earn_col].astype(str).str.strip().str.upper()
    else:
        core["earn"] = "REG"

    # Attach optional numeric columns (coerced)
    for tag, col in [
        ("vac", vac_col), ("sick", sick_col), ("hol", hol_col),
        ("bonus", bonus_col), ("comm", comm_col), ("travel", travel_col),
    ]:
        core[tag] = pd.to_numeric(df[col], errors="coerce").fillna(0.0) if col else 0.0

    for tag, col in pc_map.items():
        core[tag] = pd.to_numeric(df[col], errors="coerce").fillna(0.0) if col else 0.0

    # Keep only valid rows
    core = core[(core["name"].str.len() > 0) & core["date"].notna() & (core["hours"] >= 0)]

    # DAILY split to REG/OT/DT from 'hours' (we ignore 'earn' if Sierra didn’t provide)
    # If Sierra earn has explicit OT/DT, respect it; otherwise compute from daily hours.
    # Build day totals per person-day
    day = core.groupby(["name", "date"], as_index=False)["hours"].sum()
    day[["reg", "ot", "dt"]] = day["hours"].apply(lambda h: pd.Series(_apply_ca_daily_ot(h)))

    # WEEKLY sum
    weekly = day.groupby("name", as_index=False)[["reg", "ot", "dt"]].sum()

    # Attach one-row-per-name “other” buckets (vac/sick/hol/etc.) by summing
    other_cols = ["vac", "sick", "hol", "bonus", "comm", "travel"] + list(pc_map.keys())
    others = core.groupby("name", as_index=False)[other_cols].sum()

    out = pd.merge(weekly, others, on="name", how="outer").fillna(0.0)
    return out

# --------------------------------------------------------------------------------------
# Roster (identity columns)
# --------------------------------------------------------------------------------------
def load_roster() -> pd.DataFrame:
    """
    Expect roster.xlsx (preferred) or roster.csv at repo root.
    Required: name, ssn, status, type, rate, dept
    """
    root = Path(__file__).resolve().parent.parent  # repo root (where README.md lives)
    xlsx = root / "roster.xlsx"
    csv  = root / "roster.csv"
    if xlsx.exists():
        r = pd.read_excel(xlsx)
    elif csv.exists():
        r = pd.read_csv(csv)
    else:
        # Minimal empty frame – we’ll still produce output but SSN etc will be blank
        return pd.DataFrame(columns=["name", "ssn", "status", "type", "rate", "dept"])

    # normalize
    def pick(d: pd.DataFrame, keys: List[str]) -> Optional[str]:
        for k in d.columns:
            if _std(k) in [_std(x) for x in keys]:
                return k
        for k in d.columns:
            for x in keys:
                if _std(x) in _std(k):
                    return k
        return None

    c_name = pick(r, ["name", "employee", "employee name"])
    c_ssn  = pick(r, ["ssn", "social", "social security"])
    c_stat = pick(r, ["status"])
    c_type = pick(r, ["type", "pay type"])
    c_rate = pick(r, ["rate", "pay rate", "hourly", "wage"])
    c_dept = pick(r, ["dept", "department"])

    fr = pd.DataFrame({
        "name": r[c_name].astype(str).str.strip(),
        "ssn":  r[c_ssn]  if c_ssn  else "",
        "status": r[c_stat].astype(str).str.strip().str.upper() if c_stat else "A",
        "type":   r[c_type].astype(str).str.strip().str.upper() if c_type else "H",
        "rate":   pd.to_numeric(r[c_rate], errors="coerce").fillna(0.0) if c_rate else 0.0,
        "dept":   r[c_dept].astype(str).str.strip() if c_dept else "",
    })
    # dedupe by name, keep first
    fr = fr.drop_duplicates(subset=["name"], keep="first")
    return fr

# --------------------------------------------------------------------------------------
# Build WBS workbook from template
# --------------------------------------------------------------------------------------
def write_wbs(wk: pd.DataFrame) -> bytes:
    """
    wk columns: name, reg, ot, dt, vac, sick, hol, bonus, comm, travel, pc_hrs_*, pc_ttl_*
    """
    root = Path(__file__).resolve().parent.parent
    template_path = root / "wbs_template.xlsx"
    if not template_path.exists():
        raise ValueError(f"WBS template not found at {template_path}")

    wb = load_workbook(str(template_path))
    ws = wb.active  # "WEEKLY"

    # Find first data row in template: locate the header row that has "SSN" and "Employee Name"
    # and assume data starts the next row. If fixed, set constant here:
    header_row = None
    for r in range(1, ws.max_row + 1):
        vals = [str(ws.cell(row=r, column=c).value).strip().upper() if ws.cell(row=r, column=c).value is not None else "" for c in range(1, 20)]
        if "SSN" in vals and "EMPLOYEE NAME" in vals:
            header_row = r
            break
    if header_row is None:
        # fallback to common row 7
        header_row = 7
    DATA_START = header_row + 1

    # Columns mapping by visible headers (keeps template layout):
    def col_index_by_header(target: str, search_from_row: int) -> Optional[int]:
        tgt = _std(target)
        for c in range(1, ws.max_column + 1):
            cell = ws.cell(row=search_from_row, column=c)
            val = str(cell.value).strip().lower() if cell.value is not None else ""
            if tgt == val:
                return c
        # relaxed contains
        for c in range(1, ws.max_column + 1):
            cell = ws.cell(row=search_from_row, column=c)
            val = str(cell.value).strip().lower() if cell.value is not None else ""
            if tgt in val:
                return c
        return None

    COL = {
        "SSN":          col_index_by_header("SSN", header_row),
        "EMP":          col_index_by_header("Employee Name", header_row),
        "STATUS":       col_index_by_header("Status", header_row),
        "TYPE":         col_index_by_header("Type", header_row),
        "RATE":         col_index_by_header("Pay Rate", header_row),
        "DEPT":         col_index_by_header("Dept", header_row),
        "REG":          col_index_by_header("A01", header_row),  # REGULAR
        "OT":           col_index_by_header("A02", header_row),
        "DT":           col_index_by_header("A03", header_row),
        "VAC":          col_index_by_header("A06", header_row),
        "SICK":         col_index_by_header("A07", header_row),
        "HOL":          col_index_by_header("A08", header_row),
        "BONUS":        col_index_by_header("A04", header_row),
        "COMM":         col_index_by_header("A05", header_row),
        "PC_HRS_MON":   col_index_by_header("AH1", header_row),
        "PC_TTL_MON":   col_index_by_header("AI1", header_row),
        "PC_HRS_TUE":   col_index_by_header("AH2", header_row),
        "PC_TTL_TUE":   col_index_by_header("AI2", header_row),
        "PC_HRS_WED":   col_index_by_header("AH3", header_row),
        "PC_TTL_WED":   col_index_by_header("AI3", header_row),
        "PC_HRS_THU":   col_index_by_header("AH4", header_row),
        "PC_TTL_THU":   col_index_by_header("AI4", header_row),
        "PC_HRS_FRI":   col_index_by_header("AH5", header_row),
        "PC_TTL_FRI":   col_index_by_header("AI5", header_row),
        "TRAVEL":       col_index_by_header("ATE", header_row),
        "TOTALS":       col_index_by_header("Totals", header_row),  # pink column
    }

    # Clear existing data rows (but skip merged header cells)
    # We blank only value-bearing cells in data area, not touching formulas in the Totals column.
    max_row = ws.max_row
    if max_row >= DATA_START:
        for r in range(DATA_START, max_row + 1):
            # If row already empty in the name column, skip early
            c_emp = COL["EMP"] or 0
            if c_emp:
                v = ws.cell(row=r, column=c_emp).value
                if v in (None, ""):
                    continue
            for c in [v for k, v in COL.items() if v and k not in ("TOTALS")]:
                cell = ws.cell(row=r, column=c)
                if isinstance(cell, MergedCell):
                    continue
                cell.value = None

    # Pull roster and join
    roster = load_roster()
    roster["name_key"] = roster["name"].str.strip().str.upper()
    wk = wk.copy()
    wk["name_key"] = wk["name"].astype(str).str.strip().str.upper()
    wk = pd.merge(wk, roster, on="name_key", how="left", suffixes=("", "_r"))

    # Prefer roster’s canonical name if it exists
    wk["name_out"] = wk["name_r"].where(wk["name"].ne(wk["name_r"]) & wk["name_r"].notna(), wk["name"])
    wk["ssn"]   = wk["ssn"].fillna("")
    wk["status"]= wk["status"].fillna("A")
    wk["type"]  = wk["type"].fillna("H")
    wk["rate"]  = wk["rate"].fillna(0.0).astype(float)
    wk["dept"]  = wk["dept"].fillna("")

    # Order by Dept then Name for stable output
    wk = wk.sort_values(by=["dept", "name_out"]).reset_index(drop=True)

    # Write
    rcur = DATA_START
    for _, row in wk.iterrows():
        def setv(col_key: str, value):
            c = COL.get(col_key)
            if not c:
                return
            cell = ws.cell(row=rcur, column=c)
            if isinstance(cell, MergedCell):
                return
            cell.value = value

        setv("SSN",   row.get("ssn", ""))
        setv("EMP",   row.get("name_out", row.get("name", "")))
        setv("STATUS",row.get("status", "A"))
        setv("TYPE",  row.get("type", "H"))
        setv("RATE",  round(_money(row.get("rate", 0.0)), 2))
        setv("DEPT",  row.get("dept", ""))

        setv("REG",   round(_h(row.get("reg",   0.0)), 3))
        setv("OT",    round(_h(row.get("ot",    0.0)), 3))
        setv("DT",    round(_h(row.get("dt",    0.0)), 3))
        setv("VAC",   round(_h(row.get("vac",   0.0)), 3))
        setv("SICK",  round(_h(row.get("sick",  0.0)), 3))
        setv("HOL",   round(_h(row.get("hol",   0.0)), 3))

        # Bonuses/commissions in dollars (not hours) if Sierra provided as amounts.
        setv("BONUS", round(_money(row.get("bonus", 0.0)), 2))
        setv("COMM",  round(_money(row.get("comm",  0.0)), 2))

        # Piecework + travel (amounts)
        for key in ["PC_HRS_MON","PC_TTL_MON","PC_HRS_TUE","PC_TTL_TUE","PC_HRS_WED","PC_TTL_WED",
                    "PC_HRS_THU","PC_TTL_THU","PC_HRS_FRI","PC_TTL_FRI"]:
            setv(key, round(_money(row.get(key.lower(), 0.0)), 3))
        setv("TRAVEL", round(_money(row.get("travel", 0.0)), 2))

        # If Totals column exists but is empty, write a formula:
        # = (REG + 1.5*OT + 2*DT + VAC + SICK + HOL) * RATE + BONUS + COMM
        c_tot = COL.get("TOTALS")
        if c_tot:
            tot_cell = ws.cell(row=rcur, column=c_tot)
            if not isinstance(tot_cell, MergedCell) and (tot_cell.value in (None, "")):
                # Build formula using column letters
                from openpyxl.utils import get_column_letter
                def L(K): return get_column_letter(COL[K]) if COL.get(K) else None
                L_RATE = L("RATE")
                parts_hours = []
                if L("REG"): parts_hours.append(f"{L('REG')}{rcur}")
                if L("OT"):  parts_hours.append(f"1.5*{L('OT')}{rcur}")
                if L("DT"):  parts_hours.append(f"2*{L('DT')}{rcur}")
                for k in ["VAC","SICK","HOL"]:
                    if L(k): parts_hours.append(f"{L(k)}{rcur}")
                hours_term = "+".join(parts_hours) if parts_hours else "0"
                bonus_comm = "+".join([f"{L('BONUS')}{rcur}" for _ in [0] if L('BONUS')]) or "0"
                if L('COMM'):
                    bonus_comm = (bonus_comm + "+" if bonus_comm != "0" else "") + f"{L('COMM')}{rcur}"
                tot_cell.value = f"=({hours_term})*{L_RATE}{rcur}+({bonus_comm})"

        rcur += 1

    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)
    return bio.read()

# --------------------------------------------------------------------------------------
# Orchestrator
# --------------------------------------------------------------------------------------
def convert_sierra_to_wbs(input_bytes: bytes, sheet_name: Optional[str] = None) -> bytes:
    wk = parse_sierra(input_bytes, sheet_name=sheet_name)
    return write_wbs(wk)

# --------------------------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------------------------
@app.get("/health")
def health():
    return JSONResponse({"ok": True, "ts": datetime.utcnow().isoformat() + "Z"})

@app.post("/process-payroll")
async def process_payroll(file: UploadFile = File(...)):
    if not file or not file.filename:
        raise HTTPException(status_code=400, detail="No file selected.")
    if not _ext_ok(file.filename):
        raise HTTPException(status_code=415, detail="Unsupported file type. Please upload .xlsx/.xls/.csv")

    try:
        contents = await file.read()
        out = convert_sierra_to_wbs(contents, sheet_name=None)
        out_name = f"WBS_Payroll_{datetime.utcnow().date().isoformat()}.xlsx"
        return StreamingResponse(
            io.BytesIO(out),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename="{out_name}"'}
        )
    except ValueError as ve:
        raise HTTPException(status_code=422, detail=str(ve))
    except Exception as e:
        # bubble the message into logs but keep response generic
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")

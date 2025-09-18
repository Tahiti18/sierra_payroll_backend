# app/main.py
import io
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from openpyxl import load_workbook
from openpyxl.cell.cell import MergedCell
from starlette.responses import StreamingResponse, JSONResponse

# ──────────────────────────────────────────────────────────────────────────────
# FastAPI + CORS
# ──────────────────────────────────────────────────────────────────────────────
app = FastAPI(title="Sierra → WBS Payroll Converter", version="2.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # tighten to your domain if desired
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ALLOWED_EXTS = (".xlsx", ".xls")

# ──────────────────────────────────────────────────────────────────────────────
# Constants tuned to your WBS template layout
# (Verified from the “WBS Payroll 9_12_25 for Marwan.xlsx” sample)
# Rows are 1-based (Excel style). First employee row is 9.
# Columns are 1-based (Excel style). Pink “Totals” = column 28.
# ──────────────────────────────────────────────────────────────────────────────
WBS_SHEET_NAME      = "WEEKLY"
WBS_DATA_START_ROW  = 9
COL: Dict[str, int] = {
    # id + identity
    "EMPID": 1,           # zero-padded employee ID (first “SSN” column in template)
    "SSN": 2,             # real SSN (second “SSN” column)
    "NAME": 3,
    "STATUS": 4,
    "TYPE": 5,            # H / S
    "RATE": 6,            # Pay rate ($/hr or weekly salary if TYPE=S)
    "DEPT": 7,
    # hours buckets (A-codes)
    "REG": 8,             # A01
    "OT": 9,              # A02
    "DT": 10,             # A03
    "VAC": 11,            # A06
    "SICK": 12,           # A07
    "HOL": 13,            # A08
    # dollars (kept for compatibility with template headings — we write numbers)
    "BONUS": 14,          # A04
    "COMM": 15,           # A05
    # piece work (HRS / TTL Mon..Fri)
    "PC_HRS_MON": 16,
    "PC_TTL_MON": 17,
    "PC_HRS_TUE": 18,
    "PC_TTL_TUE": 19,
    "PC_HRS_WED": 20,
    "PC_TTL_WED": 21,
    "PC_HRS_THU": 22,
    "PC_TTL_THU": 23,
    "PC_HRS_FRI": 24,
    "PC_TTL_FRI": 25,
    # misc
    "TRAVEL": 26,
    "NOTES": 27,
    "TOTALS": 28,         # pink dollars column at far right
}
WRITE_COLS = list(COL.values())

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def _ext_ok(filename: str) -> bool:
    name = (filename or "").lower()
    return any(name.endswith(e) for e in ALLOWED_EXTS)

def _std(s: str) -> str:
    return (s or "").strip().lower()

def _normalize_person_name(raw: str) -> str:
    """
    Sierra: 'First Last' or 'First M Last'
    Roster/WBS: 'Last, First'
    This makes a stable 'Last, First' we can match on.
    """
    if not isinstance(raw, str):
        return ""
    s = " ".join(raw.split()).strip()
    if not s:
        return ""
    parts = s.replace(",", " ").split()
    if len(parts) == 1:
        return parts[0]
    last = parts[-1]
    first = parts[0]
    return f"{last}, {first}"

def _money(x) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0

def _hours(x) -> float:
    try:
        v = float(x)
        # Negative or NaN hours -> zero
        return v if v > 0 else 0.0
    except Exception:
        return 0.0

def _apply_ca_daily_ot(day_hours: float) -> Tuple[float, float, float]:
    """
    California daily overtime:
      - First 8 REG
      - Next 4 (8-12) OT
      - >12 DT
    """
    h = float(day_hours or 0.0)
    reg = min(h, 8.0)
    ot = 0.0
    dt = 0.0
    if h > 8.0:
        ot = min(h - 8.0, 4.0)
    if h > 12.0:
        dt = h - 12.0
    return (reg, ot, dt)

def _safe_cell_set(ws, r: int, c: int, value):
    """
    Avoid 'MergedCell is read-only' by skipping merged cells entirely.
    We only ever write to the data area, but this keeps it bulletproof
    even if the template changes.
    """
    cell = ws.cell(row=r, column=c)
    if isinstance(cell, MergedCell):
        return
    cell.value = value

# ──────────────────────────────────────────────────────────────────────────────
# IO
# ──────────────────────────────────────────────────────────────────────────────
def _read_roster(root: Path) -> pd.DataFrame:
    """
    Expected columns in roster.xlsx or roster.csv (case-insensitive, fuzzy):
      - EmpID (or EmployeeID / ID)
      - SSN
      - Employee Name  (in 'Last, First' form preferred)
      - Status         (usually 'A')
      - Type           ('H' or 'S')
      - PayRate
      - Dept
    """
    # prefer xlsx
    r_xlsx = (root / "roster.xlsx")
    r_csv  = (root / "roster.csv")

    if r_xlsx.exists():
        df = pd.read_excel(r_xlsx)
    elif r_csv.exists():
        df = pd.read_csv(r_csv)
    else:
        raise HTTPException(status_code=500, detail="Roster file not found (expecting /roster.xlsx or /roster.csv)")

    # normalize headers
    cols = { _std(c): c for c in df.columns }
    def _pick(*cands):
        for c in cands:
            if _std(c) in cols:
                return cols[_std(c)]
        # contains fuzzy
        for c in cols:
            for want in cands:
                if _std(want) in c:
                    return cols[c]
        return None

    id_col   = _pick("empid","employee id","id","employee_number","number")
    ssn_col  = _pick("ssn","social","social security")
    name_col = _pick("employee name","name")
    status_c = _pick("status")
    type_c   = _pick("type")
    rate_c   = _pick("payrate","rate","pay rate","wage")
    dept_c   = _pick("dept","department","division")

    needed = [id_col, ssn_col, name_col, status_c, type_c, rate_c, dept_c]
    if any(x is None for x in needed):
        raise HTTPException(status_code=500, detail="Roster file is missing required columns.")

    out = pd.DataFrame({
        "empid":   df[id_col].astype(str).str.replace(r"\D","",regex=True).str.zfill(10),
        "ssn":     df[ssn_col].astype(str).str.replace(r"\D","",regex=True).str.zfill(9),
        "name":    df[name_col].astype(str).map(_normalize_person_name),
        "status":  df[status_c].astype(str).str.strip().str.upper().replace({"ACTIVE":"A","INACTIVE":"I"}),
        "type":    df[type_c].astype(str).str.strip().str.upper().str[0].replace({"HOUR":"H","SAL":"S"}),
        "rate":    pd.to_numeric(df[rate_c], errors="coerce").fillna(0.0),
        "dept":    df[dept_c].astype(str).str.strip().str.upper(),
    })
    # collapse duplicates in roster by first seen
    out = (out.groupby("name", as_index=False)
              .agg({"empid":"first","ssn":"first","status":"first","type":"first","rate":"first","dept":"first"}))
    return out

def _read_sierra_upload(xlsx_bytes: bytes) -> pd.DataFrame:
    # Sierra sheet (as observed) columns: Days, Job#, Name, Start, Lnch St., Lnch Fnsh, Finish, Hours, Rate, Total, Job Detail
    # We only need Name, Days, Hours. Rate is ignored (roster wins).
    xls = pd.ExcelFile(io.BytesIO(xlsx_bytes))
    df  = xls.parse(xls.sheet_names[0])

    # Normalize likely headers
    cols = { _std(c): c for c in df.columns }
    def _pick(*cands):
        for c in cands:
            if _std(c) in cols:
                return cols[_std(c)]
        for c in cols:
            for want in cands:
                if _std(want) in c:
                    return cols[c]
        return None

    name_c  = _pick("name","employee name","worker")
    day_c   = _pick("days","date","work date")
    hours_c = _pick("hours","hrs")
    if not all([name_c, day_c, hours_c]):
        raise HTTPException(status_code=422, detail="File format error – check your Excel structure (need Name/Date/Hours).")

    core = pd.DataFrame({
        "name_raw": df[name_c].astype(str),
        "date": pd.to_datetime(df[day_c], errors="coerce").dt.date,
        "hours": pd.to_numeric(df[hours_c], errors="coerce").fillna(0.0),
    })
    core["name"] = core["name_raw"].map(_normalize_person_name)
    # keep valid rows
    core = core[(core["name"].str.len() > 0) & core["date"].notna() & (core["hours"] > 0)]
    return core

# ──────────────────────────────────────────────────────────────────────────────
# Core convert
# ──────────────────────────────────────────────────────────────────────────────
def _aggregate_hours_ca(core: pd.DataFrame) -> pd.DataFrame:
    """
    Group by name+date, apply CA daily OT split, then weekly sum by name.
    Returns df with columns: name, REG, OT, DT
    """
    by_day = (core.groupby(["name","date"], as_index=False)
                   .agg({"hours":"sum"}))

    rows: List[Dict[str,float]] = []
    for _, r in by_day.iterrows():
        reg, ot, dt = _apply_ca_daily_ot(float(r["hours"]))
        rows.append({"name": r["name"], "REG": reg, "OT": ot, "DT": dt})

    split = pd.DataFrame(rows)
    weekly = (split.groupby("name", as_index=False)
                   .agg({"REG":"sum","OT":"sum","DT":"sum"}))
    # round to 3 decimals max for cleanliness (values written with 2)
    for c in ("REG","OT","DT"):
        weekly[c] = weekly[c].astype(float)
    return weekly

def _compose_wbs_rows(core: pd.DataFrame, roster: pd.DataFrame) -> pd.DataFrame:
    """
    Left join weekly hours with roster. Roster supplies identity, rate, dept, type, status.
    Any missing roster entries are still emitted with sane defaults.
    """
    weekly = _aggregate_hours_ca(core)
    merged = pd.merge(weekly, roster, how="left", on="name")

    # defaults when roster data is missing
    merged["empid"]  = merged["empid"].fillna("").astype(str)
    merged["ssn"]    = merged["ssn"].fillna("").astype(str)
    merged["status"] = merged["status"].fillna("A").str.upper().str[:1]
    merged["type"]   = merged["type"].fillna("H").str.upper().str[:1]
    merged["rate"]   = pd.to_numeric(merged["rate"], errors="coerce").fillna(0.0)
    merged["dept"]   = merged["dept"].fillna("").astype(str).str.upper()

    # computed dollars (we DO NOT rely on template formulas to avoid #ERROR! in Google Sheets preview)
    def _gross(row) -> float:
        rate = float(row["rate"] or 0.0)
        if row["type"] == "S":
            # salaried: treat rate as weekly salary, ignore hour buckets for TOTALS
            return rate
        return rate * (float(row["REG"]) + 1.5*float(row["OT"]) + 2.0*float(row["DT"]))

    merged["TOTALS"] = merged.apply(_gross, axis=1)

    # piecework & misc columns – present in template but 0 unless you later provide inputs
    for c in ["VAC","SICK","HOL","BONUS","COMM",
              "PC_HRS_MON","PC_TTL_MON","PC_HRS_TUE","PC_TTL_TUE",
              "PC_HRS_WED","PC_TTL_WED","PC_HRS_THU","PC_TTL_THU",
              "PC_HRS_FRI","PC_TTL_FRI","TRAVEL","NOTES"]:
        merged[c] = 0.0

    # output ordering: Dept -> Name for stable, human-friendly file
    merged = merged.sort_values(by=["dept","name"], kind="mergesort").reset_index(drop=True)
    return merged

def convert_to_wbs(xlsx_bytes: bytes) -> bytes:
    here = Path(__file__).resolve()
    root = here.parent.parent  # repo root (since main.py lives in /app)

    # files in repo root
    template_path = root / "wbs_template.xlsx"
    if not template_path.exists():
        raise HTTPException(status_code=500, detail=f"WBS template not found at {template_path}")

    roster = _read_roster(root)
    core   = _read_sierra_upload(xlsx_bytes)
    rows   = _compose_wbs_rows(core, roster)

    # open the template FRESH on each request
    wb = load_workbook(str(template_path))
    if WBS_SHEET_NAME not in wb.sheetnames:
        raise HTTPException(status_code=500, detail=f"WBS sheet '{WBS_SHEET_NAME}' not found in template.")
    ws = wb[WBS_SHEET_NAME]

    # 1) Clear existing data area without touching merged header rows
    max_row = ws.max_row
    if max_row >= WBS_DATA_START_ROW:
        for r in range(WBS_DATA_START_ROW, max_row + 1):
            # Only clear the columns we actually write
            for c in WRITE_COLS:
                try:
                    _safe_cell_set(ws, r, c, None)
                except Exception:
                    # If template grows / contains merges in data area, just skip that cell
                    continue

    # 2) Write rows (all numbers rounded to 2dp on write)
    cur = WBS_DATA_START_ROW
    for _, r in rows.iterrows():
        _safe_cell_set(ws, cur, COL["EMPID"],  r["empid"])
        _safe_cell_set(ws, cur, COL["SSN"],    r["ssn"])
        _safe_cell_set(ws, cur, COL["NAME"],   r["name"])
        _safe_cell_set(ws, cur, COL["STATUS"], r["status"])
        _safe_cell_set(ws, cur, COL["TYPE"],   r["type"])
        _safe_cell_set(ws, cur, COL["RATE"],   round(_money(r["rate"]), 2))
        _safe_cell_set(ws, cur, COL["DEPT"],   r["dept"])

        _safe_cell_set(ws, cur, COL["REG"],    round(_hours(r["REG"]), 2))
        _safe_cell_set(ws, cur, COL["OT"],     round(_hours(r["OT"]), 2))
        _safe_cell_set(ws, cur, COL["DT"],     round(_hours(r["DT"]), 2))

        _safe_cell_set(ws, cur, COL["VAC"],    0.0)
        _safe_cell_set(ws, cur, COL["SICK"],   0.0)
        _safe_cell_set(ws, cur, COL["HOL"],    0.0)

        _safe_cell_set(ws, cur, COL["BONUS"],  0.0)
        _safe_cell_set(ws, cur, COL["COMM"],   0.0)

        # piecework placeholders (0 until we wire real inputs)
        for k in ["PC_HRS_MON","PC_TTL_MON","PC_HRS_TUE","PC_TTL_TUE",
                  "PC_HRS_WED","PC_TTL_WED","PC_HRS_THU","PC_TTL_THU",
                  "PC_HRS_FRI","PC_TTL_FRI","TRAVEL"]:
            _safe_cell_set(ws, cur, COL[k], 0.0)

        # notes blank
        _safe_cell_set(ws, cur, COL["NOTES"],  "")

        # PINK TOTALS — write the dollars explicitly (don’t depend on template formulas)
        _safe_cell_set(ws, cur, COL["TOTALS"], round(_money(r["TOTALS"]), 2))

        cur += 1

    # 3) Return workbook bytes
    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)
    return bio.read()

# ──────────────────────────────────────────────────────────────────────────────
# API
# ──────────────────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return JSONResponse({"ok": True, "ts": datetime.utcnow().isoformat() + "Z"})

@app.post("/process-payroll")
async def process_payroll(file: UploadFile = File(...)):
    if not file or not file.filename:
        raise HTTPException(status_code=400, detail="No file provided.")
    if not _ext_ok(file.filename):
        raise HTTPException(status_code=415, detail="Unsupported file. Please upload .xlsx/.xls")

    try:
        src = await file.read()
        out_bytes = convert_to_wbs(src)
        out_name = f"WBS_Payroll_{datetime.utcnow().date().isoformat()}.xlsx"
        return StreamingResponse(
            io.BytesIO(out_bytes),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename="{out_name}"'}
        )
    except HTTPException:
        raise
    except ValueError as ve:
        raise HTTPException(status_code=422, detail=str(ve))
    except Exception as e:
        # Surface a concise server error (Railway will still have stacktrace)
        raise HTTPException(status_code=500, detail=f"Server error: {e}")

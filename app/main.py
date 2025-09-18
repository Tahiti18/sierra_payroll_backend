# app/main.py
import io
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import StreamingResponse, JSONResponse
from openpyxl import load_workbook
from openpyxl.cell.cell import MergedCell

# ──────────────────────────────────────────────────────────────────────────────
# FastAPI + CORS
# ──────────────────────────────────────────────────────────────────────────────
app = FastAPI(title="Sierra → WBS Payroll Converter", version="3.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],    # tighten as needed
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ALLOWED_EXTS = (".xlsx", ".xls")

# ──────────────────────────────────────────────────────────────────────────────
# WBS TEMPLATE LAYOUT (from your gold workbook)
# Sheet = WEEKLY; first employee row = 9; rightmost pink TOTALS column = 28
# ──────────────────────────────────────────────────────────────────────────────
WBS_SHEET_NAME      = "WEEKLY"
WBS_DATA_START_ROW  = 9
COL: Dict[str, int] = {
    # identity
    "EMPID":   1,   # zero-padded (10)
    "SSN":     2,   # 9 digits, no dashes
    "NAME":    3,   # "Last, First"
    "STATUS":  4,   # A/I
    "TYPE":    5,   # H/S
    "RATE":    6,
    "DEPT":    7,
    # hours buckets
    "REG":     8,   # A01
    "OT":      9,   # A02
    "DT":      10,  # A03
    "VAC":     11,  # A06
    "SICK":    12,  # A07
    "HOL":     13,  # A08
    # extras
    "BONUS":   14,  # A04
    "COMM":    15,  # A05
    # piecework placeholders Mon..Fri
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
    "TRAVEL":  26,
    "NOTES":   27,
    "TOTALS":  28,  # pink dollars
}
WRITE_COLS = list(COL.values())

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def _ext_ok(filename: str) -> bool:
    name = (filename or "").lower()
    return any(name.endswith(e) for e in ALLOWED_EXTS)

def _std(s: str) -> str:
    return (s or "").strip().lower().replace("\n", " ").replace("\r", " ")

def _normalize_name(raw: str) -> str:
    """Normalize to 'Last, First' for stable matching."""
    if not isinstance(raw, str):
        raw = str(raw) if pd.notna(raw) else ""
    s = " ".join(raw.replace(",", " ").split()).strip()
    if not s:
        return ""
    if "," in raw:
        parts = [p.strip() for p in raw.split(",")]
        if len(parts) >= 2 and parts[0] and parts[1]:
            return f"{parts[0]}, {parts[1].split()[0]}"
    parts = s.split()
    if len(parts) == 1:
        return parts[0]
    first, last = parts[0], parts[-1]
    return f"{last}, {first}"

def _money(x) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0

def _hours(x) -> float:
    try:
        v = float(x)
        return v if v > 0 else 0.0
    except Exception:
        return 0.0

def _apply_ca_daily_ot(hours: float) -> Tuple[float, float, float]:
    """CA daily split: 0–8 REG, 8–12 OT, >12 DT."""
    h = float(hours or 0.0)
    reg = min(h, 8.0)
    ot  = max(0.0, min(h - 8.0, 4.0))
    dt  = max(0.0, h - 12.0)
    return reg, ot, dt

def _safe_set(ws, r: int, c: int, value):
    """Avoid writing into merged header cells."""
    cell = ws.cell(row=r, column=c)
    if isinstance(cell, MergedCell):
        return
    cell.value = value

# ──────────────────────────────────────────────────────────────────────────────
# Roster
# ──────────────────────────────────────────────────────────────────────────────
def _read_roster(root: Path) -> pd.DataFrame:
    """
    Expected columns (fuzzy, case-insensitive):
      EmpID, SSN, Employee Name, Status (A/I), Type (H/S), PayRate, Dept
    """
    xlsx = root / "roster.xlsx"
    csv  = root / "roster.csv"
    if xlsx.exists():
        df = pd.read_excel(xlsx)
    elif csv.exists():
        df = pd.read_csv(csv)
    else:
        raise HTTPException(status_code=500, detail="Roster file not found at repo root (roster.xlsx or roster.csv).")

    cols = { _std(c): c for c in df.columns }
    def pick(*cands):
        for w in cands:
            if _std(w) in cols:
                return cols[_std(w)]
        for k, v in cols.items():
            for w in cands:
                if _std(w) in k:
                    return v
        return None

    empid_c = pick("empid", "employee id", "id", "employee_number", "number")
    ssn_c   = pick("ssn", "social", "social security")
    name_c  = pick("employee name", "name")
    status_c= pick("status")
    type_c  = pick("type", "employee type")
    rate_c  = pick("payrate", "pay rate", "rate", "wage")
    dept_c  = pick("dept", "department", "division")

    need = [empid_c, ssn_c, name_c, status_c, type_c, rate_c, dept_c]
    if any(x is None for x in need):
        raise HTTPException(status_code=500, detail="Roster is missing required columns.")

    out = pd.DataFrame({
        "empid":  df[empid_c].astype(str).str.replace(r"\D", "", regex=True).str.zfill(10),
        "ssn":    df[ssn_c].astype(str).str.replace(r"\D", "", regex=True).str.zfill(9),
        "name":   df[name_c].astype(str).map(_normalize_name),
        "status": df[status_c].astype(str).str.strip().str.upper().replace({"ACTIVE": "A", "INACTIVE": "I"}).str[:1],
        "type":   df[type_c].astype(str).str.strip().str.upper().map(lambda s: "S" if s.startswith("S") else "H"),
        "rate":   pd.to_numeric(df[rate_c], errors="coerce").fillna(0.0),
        "dept":   df[dept_c].astype(str).str.strip().str.upper(),
    })

    # Deduplicate: prefer SSN uniqueness, then name
    out = (out.sort_values(by=["ssn", "name"])
              .drop_duplicates(subset=["ssn"], keep="first")
              .drop_duplicates(subset=["name"], keep="first")
              .reset_index(drop=True))

    return out

# ──────────────────────────────────────────────────────────────────────────────
# Sierra Upload
# ──────────────────────────────────────────────────────────────────────────────
EARN_MAP = {
    # normalize earn types from Sierra into buckets
    "VAC": "VAC", "VACATION": "VAC",
    "SICK": "SICK",
    "HOL": "HOL", "HOLIDAY": "HOL",
    "BONUS": "BONUS",
    "COMM": "COMM", "COMMISSION": "COMM",
    # everything else is treated as worked hours
}

def _read_sierra_upload(xlsx_bytes: bytes) -> pd.DataFrame:
    """
    Sierra weekly input. Required: Name, Date/Day, Hours.
    Optional: Earn Type/Code (to split VAC/SICK/HOL/BONUS/COMM).
    """
    xl = pd.ExcelFile(io.BytesIO(xlsx_bytes))
    df = xl.parse(xl.sheet_names[0])

    cols = { _std(c): c for c in df.columns }
    def pick(*cands):
        for w in cands:
            if _std(w) in cols:
                return cols[_std(w)]
        for k, v in cols.items():
            for w in cands:
                if _std(w) in k:
                    return v
        return None

    name_c  = pick("name", "employee name", "worker", "employee")
    day_c   = pick("date", "day", "worked date", "work date")
    hours_c = pick("hours", "hrs", "total hours", "work hours")
    earn_c  = pick("earn type", "task", "earning", "code", "type")

    if not all([name_c, day_c, hours_c]):
        raise HTTPException(status_code=422, detail="File format error — Sierra sheet must have Name, Date, Hours.")

    core = pd.DataFrame({
        "name":  df[name_c].astype(str).map(_normalize_name),
        "date":  pd.to_datetime(df[day_c], errors="coerce").dt.date,
        "hours": pd.to_numeric(df[hours_c], errors="coerce").fillna(0.0),
        "earn":  df[earn_c].astype(str).str.upper() if earn_c else "",
    })

    core = core[(core["name"].str.len() > 0) & core["date"].notna() & (core["hours"] > 0)]

    # Tag buckets
    def bucketize(e: str) -> str:
        e = (e or "").strip().upper()
        return EARN_MAP.get(e, "WORK")
    core["bucket"] = core["earn"].map(bucketize)

    return core

# ──────────────────────────────────────────────────────────────────────────────
# Hours Logic: WORK (same-day agg → CA daily OT → weekly uplift) + Leave/Extras
# ──────────────────────────────────────────────────────────────────────────────
def _compute_hours(core: pd.DataFrame) -> pd.DataFrame:
    """
    Produces per-person weekly REG/OT/DT and VAC/SICK/HOL/BONUS/COMM.
    """
    # Split work vs leave/extras
    work = core[core["bucket"] == "WORK"].copy()
    # Leave/extras are simply summed by person for the week
    leave = (core[core["bucket"].isin(["VAC", "SICK", "HOL", "BONUS", "COMM"])]
                .groupby(["name", "bucket"], as_index=False)["hours"].sum()
            )

    # WORK: same-day aggregation → daily split → weekly sum
    if not work.empty:
        day_sum = (work.groupby(["name", "date"], as_index=False)["hours"].sum())
        rows = []
        for _, r in day_sum.iterrows():
            reg, ot, dt = _apply_ca_daily_ot(float(r["hours"]))
            rows.append({"name": r["name"], "REG": reg, "OT": ot, "DT": dt})
        daily_split = pd.DataFrame(rows)
        weekly_work = (daily_split.groupby("name", as_index=False)
                                   .agg({"REG": "sum", "OT": "sum", "DT": "sum"}))
    else:
        weekly_work = pd.DataFrame(columns=["name", "REG", "OT", "DT"])

    # Weekly OT uplift to 40 (hourly only — needs type from roster later)
    weekly_work[["REG", "OT", "DT"]] = weekly_work[["REG", "OT", "DT"]].fillna(0.0)

    # Leave/extras pivot
    if not leave.empty:
        piv = (leave.pivot_table(index="name", columns="bucket", values="hours", aggfunc="sum")
                    .reset_index())
    else:
        piv = pd.DataFrame(columns=["name", "VAC", "SICK", "HOL", "BONUS", "COMM"])

    for col in ["VAC", "SICK", "HOL", "BONUS", "COMM"]:
        if col not in piv.columns:
            piv[col] = 0.0

    # Merge work + leave
    out = pd.merge(weekly_work, piv, on="name", how="outer").fillna(0.0)
    if "name" not in out.columns:
        out["name"] = []
    return out

def _weekly_ot_uplift(df: pd.DataFrame, roster: pd.DataFrame) -> pd.DataFrame:
    """
    After joining roster (so TYPE exists):
      If TYPE == 'H' and REG > 40, shift (REG-40) from REG to OT.
    """
    m = pd.merge(df, roster[["name", "type"]], on="name", how="left")
    m["type"] = m["type"].fillna("H").str.upper().str[:1]

    reg = m["REG"].astype(float)
    ot  = m["OT"].astype(float)
    dt  = m["DT"].astype(float)

    over = (reg - 40.0).clip(lower=0.0)
    is_hourly = m["type"].eq("H")

    reg = reg.where(~is_hourly, reg - over)
    ot  = ot.where(~is_hourly, ot + over)

    m["REG"] = reg.round(4)
    m["OT"]  = ot.round(4)
    m["DT"]  = dt.round(4)

    # return with original columns + name
    cols = [c for c in df.columns]
    return m[cols]

# ──────────────────────────────────────────────────────────────────────────────
# Compose final rows: join roster (SSN-first), compute TOTALS, order rows
# ──────────────────────────────────────────────────────────────────────────────
def _compose_wbs_rows(core: pd.DataFrame, roster: pd.DataFrame) -> pd.DataFrame:
    hours = _compute_hours(core)

    # Join to roster on NAME (we will still display SSN/EMPID from roster)
    merged = pd.merge(hours, roster, how="left", on="name")

    # Apply weekly uplift (requires TYPE)
    merged = _weekly_ot_uplift(merged, roster)

    # Fill identity defaults
    merged["empid"]  = merged["empid"].fillna("").astype(str)
    merged["ssn"]    = merged["ssn"].fillna("").astype(str)
    merged["status"] = merged["status"].fillna("A").str.upper().str[:1]
    merged["type"]   = merged["type"].fillna("H").str.upper().str[:1]
    merged["rate"]   = pd.to_numeric(merged["rate"], errors="coerce").fillna(0.0)
    merged["dept"]   = merged["dept"].fillna("").astype(str).str.upper()

    # Ensure all hour/extra columns exist
    for k in ["REG", "OT", "DT", "VAC", "SICK", "HOL", "BONUS", "COMM"]:
        if k not in merged.columns:
            merged[k] = 0.0
        merged[k] = pd.to_numeric(merged[k], errors="coerce").fillna(0.0)

    # Compute TOTALS explicitly
    def _gross(row) -> float:
        rate = float(row["rate"] or 0.0)
        if row["type"] == "S":
            # salary: pink total is weekly salary + bonus/comm
            return rate + float(row["BONUS"]) + float(row["COMM"])
        # hourly
        dollars = (
            rate * float(row["REG"]) +
            rate * 1.5 * float(row["OT"]) +
            rate * 2.0 * float(row["DT"]) +
            rate * (float(row["VAC"]) + float(row["SICK"]) + float(row["HOL"]))
        )
        return dollars + float(row["BONUS"]) + float(row["COMM"])

    merged["TOTALS"] = merged.apply(_gross, axis=1)

    # Stable order (Dept then Name)
    merged = merged.sort_values(by=["dept", "name"], kind="mergesort").reset_index(drop=True)
    return merged

# ──────────────────────────────────────────────────────────────────────────────
# Convert: open template, clear rows, write values
# ──────────────────────────────────────────────────────────────────────────────
def convert_to_wbs(xlsx_bytes: bytes) -> bytes:
    here = Path(__file__).resolve()
    root = here.parent.parent  # repo root (assuming /app/main.py)

    template_path = root / "wbs_template.xlsx"
    if not template_path.exists():
        raise HTTPException(status_code=500, detail=f"WBS template not found at {template_path}")

    roster = _read_roster(root)
    core   = _read_sierra_upload(xlsx_bytes)
    rows   = _compose_wbs_rows(core, roster)

    wb = load_workbook(str(template_path))
    if WBS_SHEET_NAME not in wb.sheetnames:
        raise HTTPException(status_code=500, detail=f"Sheet '{WBS_SHEET_NAME}' not found in template.")
    ws = wb[WBS_SHEET_NAME]

    # 1) Clear existing data region safely (keep styles/formulas)
    # Clear from start row to either the last used row or enough buffer
    max_row = ws.max_row
    clear_to = max(WBS_DATA_START_ROW, max_row, WBS_DATA_START_ROW + max(len(rows) + 50, 200))
    for r in range(WBS_DATA_START_ROW, clear_to + 1):
        for c in WRITE_COLS:
            try:
                _safe_set(ws, r, c, None)
            except Exception:
                continue

    # 2) Write data rows
    rix = WBS_DATA_START_ROW
    for _, r in rows.iterrows():
        _safe_set(ws, rix, COL["EMPID"],   r["empid"])
        _safe_set(ws, rix, COL["SSN"],     r["ssn"])
        _safe_set(ws, rix, COL["NAME"],    r["name"])
        _safe_set(ws, rix, COL["STATUS"],  r["status"])
        _safe_set(ws, rix, COL["TYPE"],    r["type"])
        _safe_set(ws, rix, COL["RATE"],    round(_money(r["rate"]), 2))
        _safe_set(ws, rix, COL["DEPT"],    r["dept"])

        _safe_set(ws, rix, COL["REG"],     round(_hours(r["REG"]), 2))
        _safe_set(ws, rix, COL["OT"],      round(_hours(r["OT"]), 2))
        _safe_set(ws, rix, COL["DT"],      round(_hours(r["DT"]), 2))
        _safe_set(ws, rix, COL["VAC"],     round(_hours(r["VAC"]), 2))
        _safe_set(ws, rix, COL["SICK"],    round(_hours(r["SICK"]), 2))
        _safe_set(ws, rix, COL["HOL"],     round(_hours(r["HOL"]), 2))

        _safe_set(ws, rix, COL["BONUS"],   round(_money(r["BONUS"]), 2))
        _safe_set(ws, rix, COL["COMM"],    round(_money(r["COMM"]), 2))

        # piecework & travel set to 0 for parity
        for k in ["PC_HRS_MON","PC_TTL_MON","PC_HRS_TUE","PC_TTL_TUE",
                  "PC_HRS_WED","PC_TTL_WED","PC_HRS_THU","PC_TTL_THU",
                  "PC_HRS_FRI","PC_TTL_FRI","TRAVEL"]:
            _safe_set(ws, rix, COL[k], 0.0)

        _safe_set(ws, rix, COL["NOTES"],   "")

        # Pink totals (explicit)
        _safe_set(ws, rix, COL["TOTALS"],  round(_money(r["TOTALS"]), 2))

        rix += 1

    # 3) Save to bytes
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
            headers={"Content-Disposition": f'attachment; filename=\"{out_name}\"'}
        )
    except HTTPException:
        raise
    except ValueError as ve:
        raise HTTPException(status_code=422, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Server error: {e}")

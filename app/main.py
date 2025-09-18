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
from openpyxl.worksheet.worksheet import Worksheet

# =============================================================================
# App + CORS
# =============================================================================
app = FastAPI(title="Sierra → WBS Payroll Converter", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten to your frontend origin if desired
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ALLOWED_EXTS = (".xlsx", ".xls")

# =============================================================================
# Helpers
# =============================================================================
def _ext_ok(filename: str) -> bool:
    name = (filename or "").lower()
    return any(name.endswith(e) for e in ALLOWED_EXTS)

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

def _money(x) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0

def _apply_ca_daily_ot(day_hours: float) -> Tuple[float, float, float]:
    """
    California daily OT split:
      - first 8 hrs => REG
      - next 4 hrs (8–12) => OT
      - >12 hrs => DT
    """
    h = float(day_hours or 0.0)
    reg = min(h, 8.0)
    ot = 0.0
    dt = 0.0
    if h > 8.0:
        ot = min(h - 8.0, 4.0)
    if h > 12.0:
        dt = h - 12.0
    return reg, ot, dt

# -----------------------------------------------------------------------------
# Header resolution (flexible matching)
# -----------------------------------------------------------------------------
def _find_first(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = { _std(c): c for c in df.columns }
    wants = [_std(x) for x in candidates]
    # direct match
    for w in wants:
        if w in cols:
            return cols[w]
    # relaxed "contains"
    for w in wants:
        for k, v in cols.items():
            if w in k:
                return v
    return None

# -----------------------------------------------------------------------------
# Read roster (optional)
# -----------------------------------------------------------------------------
def read_roster(root: Path) -> pd.DataFrame:
    """
    Reads roster.xlsx or roster.csv from repo root if present.
    Expected columns: name, ssn, dept, type, rate
    (header names are matched flexibly).
    Returns empty DataFrame if none found.
    """
    for fname in ["roster.xlsx", "roster.csv"]:
        p = root / fname
        if p.exists():
            try:
                if p.suffix.lower() == ".csv":
                    df = pd.read_csv(p)
                else:
                    df = pd.read_excel(p)
                # Map flexible headers
                mapping = {
                    "name": ["name", "employee", "employee name", "employee_name"],
                    "ssn": ["ssn", "social", "social security", "social security number"],
                    "dept": ["dept", "department", "division"],
                    "type": ["type", "emp type", "employee type", "pay type"],
                    "rate": ["rate", "pay rate", "hourly rate", "wage"],
                }
                out = {}
                for key, opts in mapping.items():
                    col = _find_first(df, opts)
                    out[key] = df[col] if col else ""
                roster = pd.DataFrame(out).copy()
                # Normalize name key for join
                roster["key_name"] = roster["name"].astype(str).str.strip().str.lower()
                return roster
            except Exception:
                # If broken roster, just ignore
                return pd.DataFrame()
    return pd.DataFrame()

# -----------------------------------------------------------------------------
# Parse Sierra file into daily hours by employee with a single rate
# -----------------------------------------------------------------------------
def parse_sierra(bytes_in: bytes) -> pd.DataFrame:
    """
    Returns a DataFrame with columns:
      employee, rate, mon, tue, wed, thu, fri
    If daily columns aren't present but we have (date, hours, employee, rate),
    we will collapse by date->weekday.
    """
    excel = pd.ExcelFile(io.BytesIO(bytes_in))
    # prefer first sheet
    df = excel.parse(excel.sheet_names[0])

    if df.empty:
        raise ValueError("Input sheet is empty.")

    # Try daily hour columns first
    day_map = {
        "mon": ["pc hrs mon", "mon", "ai1", "monday"],
        "tue": ["pc hrs tue", "tue", "ai2", "tuesday"],
        "wed": ["pc hrs wed", "wed", "ai3", "wednesday"],
        "thu": ["pc ttl thu", "pc hrs thu", "thu", "ai4", "thursday"],
        "fri": ["pc hrs fri", "fri", "ai5", "friday"],
    }
    emp_col = _find_first(df, ["employee", "employee name", "name", "worker", "employee_name"])
    rate_col = _find_first(df, ["rate", "pay rate", "hourly rate", "wage"])

    # Case 1: daily columns present
    daily_cols: Dict[str, Optional[str]] = {k: _find_first(df, v) for k, v in day_map.items()}
    if emp_col and any(daily_cols.values()):
        core = pd.DataFrame()
        core["employee"] = df[emp_col].astype(str).str.strip()
        core["rate"] = pd.to_numeric(df[rate_col], errors="coerce").fillna(0.0) if rate_col else 0.0
        for k, col in daily_cols.items():
            core[k] = pd.to_numeric(df[col], errors="coerce").fillna(0.0) if col else 0.0
        # If rate is missing per row, fill with mode (most common) to avoid zeros
        if "rate" in core and (core["rate"] == 0).any():
            try:
                mode_val = core["rate"][core["rate"] > 0].mode().iloc[0]
                core.loc[core["rate"] == 0, "rate"] = mode_val
            except Exception:
                pass
        return core

    # Case 2: build from (Date, Hours) rows
    date_col = _find_first(df, ["date", "work date", "day", "worked date"])
    hours_col = _find_first(df, ["hours", "hrs", "total hours", "work hours"])
    if not (emp_col and hours_col and (date_col or any(daily_cols.values()))):
        raise ValueError(
            "Cannot locate required columns. "
            "Need Employee + (PC HRS MON..FRI) or Employee + Date + Hours."
        )

    tmp = pd.DataFrame()
    tmp["employee"] = df[emp_col].astype(str).str.strip()
    tmp["hours"] = pd.to_numeric(df[hours_col], errors="coerce").fillna(0.0)
    tmp["rate"] = pd.to_numeric(df[rate_col], errors="coerce").fillna(0.0) if rate_col else 0.0
    tmp["date"] = df[date_col].map(_to_date)
    tmp = tmp[tmp["employee"].str.len() > 0]
    tmp = tmp[tmp["hours"] > 0]
    tmp = tmp[tmp["date"].notna()]

    # Pivot by weekday name
    tmp["weekday"] = tmp["date"].map(lambda d: d.strftime("%a").lower())  # mon, tue, ...
    agg = tmp.groupby(["employee", "rate", "weekday"])["hours"].sum().unstack(fill_value=0.0)
    # ensure columns
    for k in ["mon", "tue", "wed", "thu", "fri"]:
        if k not in agg.columns:
            agg[k] = 0.0
    agg = agg.reset_index()
    return agg[["employee", "rate", "mon", "tue", "wed", "thu", "fri"]].copy()

# -----------------------------------------------------------------------------
# Build weekly REG/OT/DT by employee via daily split
# -----------------------------------------------------------------------------
def build_weekly_hours(daily_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, r in daily_df.iterrows():
        reg = ot = dt = 0.0
        for day_key in ["mon", "tue", "wed", "thu", "fri"]:
            dreg, dot, ddt = _apply_ca_daily_ot(float(r.get(day_key, 0.0)))
            reg += dreg
            ot += dot
            dt += ddt
        rows.append({
            "employee": str(r["employee"]).strip(),
            "rate": float(r.get("rate", 0.0)),
            "REG": round(reg, 3),
            "OT": round(ot, 3),
            "DT": round(dt, 3),
        })
    out = pd.DataFrame(rows)
    # collapse duplicates of the same employee/rate
    out = (
        out.groupby(["employee", "rate"], dropna=False)[["REG", "OT", "DT"]]
           .sum().reset_index()
    )
    return out

# -----------------------------------------------------------------------------
# Find columns by header text inside the template (keeps robustness)
# -----------------------------------------------------------------------------
def find_header_col(ws: Worksheet, look_for: List[str], header_rows=(7, 8, 9, 10)) -> Optional[int]:
    wants = [_std(x) for x in look_for]
    for hr in header_rows:
        for cell in ws[hr]:
            text = _std(str(cell.value))
            if not text:
                continue
            for w in wants:
                if w in text:
                    return cell.column  # 1-based
    return None

# -----------------------------------------------------------------------------
# Write into WBS template
# -----------------------------------------------------------------------------
def write_into_template(
    daily: pd.DataFrame,
    weekly: pd.DataFrame,
    roster: pd.DataFrame,
    root: Path
) -> bytes:

    # Load template fresh (never saved back)
    template_path = (root / "wbs_template.xlsx").resolve()
    if not template_path.exists():
        raise HTTPException(status_code=500, detail=f"WBS template not found at {template_path}")

    wb = load_workbook(str(template_path))
    ws = wb.active  # single active sheet "WEEKLY"

    # Resolve target columns by reading headers from the sheet
    col = {
        "SSN":       find_header_col(ws, ["ssn"]),
        "NAME":      find_header_col(ws, ["employee name", "employee"]),
        "STATUS":    find_header_col(ws, ["status"]),
        "TYPE":      find_header_col(ws, ["type"]),
        "RATE":      find_header_col(ws, ["pay rate"]),
        "DEPT":      find_header_col(ws, ["dept", "department"]),
        "A01":       find_header_col(ws, ["regular", "a01"]),
        "A02":       find_header_col(ws, ["overtime", "a02"]),
        "A03":       find_header_col(ws, ["doubletime", "a03"]),
        "A06":       find_header_col(ws, ["vacation", "a06"]),
        "A07":       find_header_col(ws, ["sick", "a07"]),
        "A08":       find_header_col(ws, ["holiday", "a08"]),
        # keep all other template columns/total formulas intact
    }

    # Safety: ensure required columns exist
    required = ["SSN", "NAME", "STATUS", "TYPE", "RATE", "DEPT", "A01", "A02", "A03"]
    missing = [k for k in required if not col.get(k)]
    if missing:
        raise HTTPException(status_code=500, detail=f"Template missing required headers: {', '.join(missing)}")

    # Where to write data (first data row under the header)
    # In the supplied WBS sheet, header line with "SSN | Employee Name | ..." is usually row 8.
    # Data starts on the next row:
    WBS_HEADER_ROW = 8
    WBS_DATA_START_ROW = WBS_HEADER_ROW + 1

    # Clear existing data values ONLY (not formulas/styles). Skip merged cells.
    max_row = ws.max_row
    if max_row >= WBS_DATA_START_ROW:
        for r in range(WBS_DATA_START_ROW, max_row + 1):
            # If the row is already empty (no name & no SSN), skip
            empty_name = ws.cell(row=r, column=col["NAME"]).value in (None, "")
            empty_ssn  = ws.cell(row=r, column=col["SSN"]).value in (None, "")
            if empty_name and empty_ssn:
                continue
            # Clear only value-bearing data columns we control
            for key in ["SSN","NAME","STATUS","TYPE","RATE","DEPT","A01","A02","A03","A06","A07","A08"]:
                cidx = col[key]
                try:
                    cell = ws.cell(row=r, column=cidx)
                    # Skip if this cell is part of a merged region (read-only proxy)
                    if any([cell.coordinate in rng for rng in ws.merged_cells.ranges]):
                        continue
                    cell.value = None
                except Exception:
                    continue

    # Prepare final list of employees with enriched info
    # Join weekly (REG/OT/DT) with roster (SSN/Dept/Type/Rate overrides)
    weekly = weekly.copy()
    weekly["key_name"] = weekly["employee"].astype(str).str.strip().str.lower()

    if not roster.empty:
        # If rate provided in roster, prefer roster rate when weekly rate is 0
        merged = pd.merge(weekly, roster[["key_name", "ssn", "dept", "type", "rate"]], on="key_name", how="left")
        # Choose rate: weekly > roster > 0
        merged["final_rate"] = merged.apply(
            lambda r: r["rate_x"] if float(r["rate_x"] or 0) > 0 else float(r["rate_y"] or 0),
            axis=1
        )
        merged["dept"] = merged["dept"].fillna("")
        merged["type"] = merged["type"].fillna("")
        merged["ssn"]  = merged["ssn"].fillna("")
        merged["employee"] = weekly["employee"]
        merged["REG"] = merged["REG"]
        merged["OT"]  = merged["OT"]
        merged["DT"]  = merged["DT"]
        final_df = merged.rename(columns={"final_rate": "rate"})
    else:
        final_df = weekly.copy()
        final_df["ssn"] = ""
        final_df["dept"] = ""
        final_df["type"] = ""

    # Normalize Type to H/S
    def norm_type(x: str) -> str:
        s = (str(x or "")).strip().upper()
        if s.startswith("S"):
            return "S"
        return "H"

    # Sort by Dept then Name for stable ordering
    final_df["dept_u"] = final_df["dept"].astype(str).str.upper()
    final_df = final_df.sort_values(by=["dept_u", "employee"], kind="stable").reset_index(drop=True)

    # Write rows
    row = WBS_DATA_START_ROW
    for _, rec in final_df.iterrows():
        ws.cell(row=row, column=col["SSN"]).value    = rec.get("ssn", "")
        ws.cell(row=row, column=col["NAME"]).value   = rec["employee"]
        ws.cell(row=row, column=col["STATUS"]).value = "A"
        ws.cell(row=row, column=col["TYPE"]).value   = norm_type(rec.get("type", "H"))
        ws.cell(row=row, column=col["RATE"]).value   = round(_money(rec.get("rate", 0.0)), 2)
        ws.cell(row=row, column=col["DEPT"]).value   = rec.get("dept", "")

        ws.cell(row=row, column=col["A01"]).value    = round(_money(rec.get("REG", 0.0)), 3)
        ws.cell(row=row, column=col["A02"]).value    = round(_money(rec.get("OT", 0.0)), 3)
        ws.cell(row=row, column=col["A03"]).value    = round(_money(rec.get("DT", 0.0)), 3)
        # leave A06, A07, A08 (vacation/sick/holiday) empty unless you add logic
        ws.cell(row=row, column=col["A06"]).value    = 0.0
        ws.cell(row=row, column=col["A07"]).value    = 0.0
        ws.cell(row=row, column=col["A08"]).value    = 0.0

        row += 1

    # Return bytes
    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)
    return bio.read()

# -----------------------------------------------------------------------------
# Main converter
# -----------------------------------------------------------------------------
def convert_sierra_to_wbs(input_bytes: bytes, root: Path) -> bytes:
    # Parse Sierra → daily hours
    daily = parse_sierra(input_bytes)
    # Daily → weekly REG/OT/DT
    weekly = build_weekly_hours(daily)
    # Load roster (optional)
    roster = read_roster(root)
    # Write into template
    return write_into_template(daily, weekly, roster, root)

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
    if not _ext_ok(file.filename):
        raise HTTPException(status_code=415, detail="Unsupported file type. Please upload .xlsx or .xls")

    try:
        contents = await file.read()
        repo_root = Path(__file__).resolve().parent.parent  # app/ → repo root
        out_bytes = convert_sierra_to_wbs(contents, repo_root)
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

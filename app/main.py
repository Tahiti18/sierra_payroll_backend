# app/main.py
import io
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse, StreamingResponse

from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet

# ======================================================================================
# App + CORS
# ======================================================================================

app = FastAPI(title="Sierra → WBS Payroll Converter", version="2.3.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],            # tighten to Netlify origin if you want
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ALLOWED_EXTS = (".xlsx", ".xls")

# ======================================================================================
# Helpers
# ======================================================================================

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
    if isinstance(val, date):
        return val
    # Parse permissively, but consistent
    try:
        return pd.to_datetime(val, errors="coerce").date()
    except Exception:
        return None

def _apply_ca_daily_ot(day_hours: float) -> Tuple[float, float, float]:
    """Return (REG, OT, DT) for a single day under CA rules (8/12 split)."""
    h = float(day_hours or 0.0)
    reg = min(h, 8.0)
    ot = 0.0
    dt = 0.0
    if h > 8:
        ot = min(h - 8.0, 4.0)
    if h > 12:
        dt = h - 12.0
    return (reg, ot, dt)

def _cell_is_merged_secondary(ws: Worksheet, row: int, col: int) -> bool:
    """True if the (row,col) cell is inside a merged range but NOT its top-left."""
    coord = ws.cell(row=row, column=col).coordinate
    for mr in ws.merged_cells.ranges:
        if coord in mr:
            return coord != mr.min_coord  # only top-left is writable
    return False

def _safe_set(ws: Worksheet, row: int, col: int, value):
    """Write to a cell, but skip if it is a merged 'secondary' cell."""
    if _cell_is_merged_secondary(ws, row, col):
        # write into the top-left of the merged block instead
        for mr in ws.merged_cells.ranges:
            if ws.cell(row=row, column=col).coordinate in mr:
                tl = ws[mr.min_coord]
                tl.value = value
                return
    else:
        ws.cell(row=row, column=col).value = value

def _find_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = { _std(c): c for c in df.columns }
    for want in candidates:
        key = _std(want)
        if key in cols:
            return cols[key]
    # relaxed contains
    for want in candidates:
        key = _std(want)
        for k, v in cols.items():
            if key in k:
                return v
    return None

def _require_columns(df: pd.DataFrame, mapping: Dict[str, List[str]]) -> Dict[str, str]:
    resolved, missing = {}, []
    for logical, options in mapping.items():
        col = _find_col(df, options)
        if not col:
            missing.append(f"{logical} (any of: {', '.join(options)})")
        else:
            resolved[logical] = col
    if missing:
        raise ValueError("Missing required columns: " + "; ".join(missing))
    return resolved

def _normalize_name(raw: str) -> str:
    """Try to normalize to 'Last, First' like the WBS file shows."""
    if not raw or not isinstance(raw, str):
        return ""
    name = " ".join(raw.replace(",", " ").split())  # squeeze
    parts = name.split()
    if len(parts) == 2:
        return f"{parts[1].capitalize()}, {parts[0].capitalize()}"
    # If already 'Last, First' keep it
    if "," in raw:
        left, *right = [p.strip() for p in raw.split(",")]
        if left and right:
            return f"{left}, {' '.join(right)}"
    return raw.strip()

# ======================================================================================
# Core conversion
# ======================================================================================

def _load_roster_order_and_ids(root: Path) -> pd.DataFrame:
    """
    Optional: read roster.xlsx (root) to lock the order + carry SSN/Type/Rate/Dept.
    Expected headers (case-insensitive, flexible): 'employee', 'ssn', 'type', 'pay rate', 'dept'
    """
    roster_path = root / "roster.xlsx"
    if not roster_path.exists():
        # empty shell
        return pd.DataFrame(columns=["employee","ssn","type","rate","dept"])

    rxl = pd.ExcelFile(str(roster_path))
    rdf = rxl.parse(rxl.sheet_names[0])

    map_cols = _require_columns(rdf, {
        "employee": ["employee", "employee name", "name"],
        "ssn": ["ssn", "social", "social security"],
    })
    type_col = _find_col(rdf, ["type", "pay type"])
    rate_col = _find_col(rdf, ["pay rate", "rate", "hourly rate"])
    dept_col = _find_col(rdf, ["dept", "department"])

    out = pd.DataFrame({
        "employee": rdf[map_cols["employee"]].astype(str).map(_normalize_name),
        "ssn": rdf[map_cols["ssn"]].astype(str).str.strip(),
        "type": rdf[type_col].astype(str).str.upper().str[:1] if type_col else "H",
        "rate": pd.to_numeric(rdf[rate_col], errors="coerce") if rate_col else pd.Series([None]*len(rdf)),
        "dept": rdf[dept_col].astype(str) if dept_col else pd.Series([""]*len(rdf)),
    })
    # preserve the raster order exactly as the roster lists it
    out["__order__"] = range(1, len(out)+1)
    return out

def _merge_best_identity(base: pd.DataFrame, roster: pd.DataFrame) -> pd.DataFrame:
    """
    Join Sierra-derived people with roster (left join on normalized name).
    """
    m = pd.merge(
        base, roster[["employee", "ssn", "type", "rate", "dept", "__order__"]],
        on="employee", how="left"
    )
    # defaults
    m["type"] = m["type"].fillna("H")
    m["rate"] = m["rate"].fillna(m.get("in_rate", 0.0)).fillna(0.0)
    m["dept"] = m["dept"].fillna("")
    m["__order__"] = m["__order__"].fillna(10_000)  # unknowns go bottom
    return m

def convert_sierra_to_wbs(input_bytes: bytes, sheet_name: Optional[str] = None) -> bytes:
    here = Path(__file__).resolve().parent.parent  # repo root (app/ is one level below)
    template_path = here / "wbs_template.xlsx"
    if not template_path.exists():
        raise ValueError(f"WBS template not found at {template_path}")

    roster = _load_roster_order_and_ids(here)

    # ----------------- Read Sierra timesheet -----------------
    excel = pd.ExcelFile(io.BytesIO(input_bytes))
    src_sheet = sheet_name or excel.sheet_names[0]
    df = excel.parse(src_sheet)

    if df.empty:
        raise ValueError("Input sheet is empty.")

    # Required Sierra columns (be permissive)
    need = _require_columns(df, {
        "employee": ["employee", "employee name", "name", "worker"],
        "date": ["date", "work date", "worked date", "day"],
        "hours": ["hours", "hrs", "quantity", "qty", "total hours"],
    })
    # Optional Sierra rate (if varies by person in sheet)
    rate_col = _find_col(df, ["rate", "pay rate", "hourly", "wage"])

    core = pd.DataFrame({
        "employee": df[need["employee"]].astype(str).map(_normalize_name),
        "date": df[need["date"]].map(_to_date),
        "hours": pd.to_numeric(df[need["hours"]], errors="coerce").fillna(0.0).astype(float),
    })
    if rate_col:
        core["in_rate"] = pd.to_numeric(df[rate_col], errors="coerce")

    # Drop unusable rows
    core = core[(core["employee"].str.len() > 0) & core["date"].notna() & (core["hours"] > 0)]

    if core.empty:
        raise ValueError("No valid rows after cleaning. Check column mapping and data types.")

    # Sum to person/day (if Sierra has multiple same-day lines)
    day_sum = core.groupby(["employee", "date"], dropna=False)["hours"].sum().reset_index()

    # Split each day by CA OT
    split = []
    for _, r in day_sum.iterrows():
        reg, ot, dt = _apply_ca_daily_ot(r["hours"])
        split.append({
            "employee": r["employee"],
            "date": r["date"],
            "REG": reg, "OT": ot, "DT": dt
        })
    split_df = pd.DataFrame(split)

    # Weekly totals per person
    weekly = (split_df
              .groupby("employee", dropna=False)[["REG","OT","DT"]]
              .sum()
              .reset_index())

    # Merge identity & order from roster
    merged = _merge_best_identity(weekly, roster)

    # Compute dollar totals (rate definitely present after merge)
    merged["REG_$"] = merged["REG"] * merged["rate"]
    merged["OT_$"]  = merged["OT"]  * merged["rate"] * 1.5
    merged["DT_$"]  = merged["DT"]  * merged["rate"] * 2.0

    # Lock final order (roster order first, then alpha for new folks)
    merged.sort_values(["__order__", "dept", "employee"], inplace=True)
    merged.reset_index(drop=True, inplace=True)

    # ----------------- Paint into WBS template -----------------
    wb = load_workbook(str(template_path))
    ws = wb.active  # "WEEKLY"

    # Column map in template (A=1 ...), based on your WBS:
    # A: SSN, B: Employee Name, C: Status, D: Pay Type, E: Pay Rate, F: Dept,
    # G: REG (A01), H: OT (A02), I: DT (A03),
    # J: VACATION (A06), K: SICK (A07), L: HOLIDAY (A08),
    # AB: Totals (pink)  -> we’ll drop a formula
    COL = {
        "SSN": 1, "NAME": 2, "STATUS": 3, "TYPE": 4, "RATE": 5, "DEPT": 6,
        "REG": 7, "OT": 8, "DT": 9,
        "VAC": 11, "SICK": 12, "HOL": 13,   # keep these as 0 unless you feed them
        "TOTALS": 28,  # AB
    }

    # Where real data begins in the template (row with first employee)
    WBS_DATA_START_ROW = 8  # your screenshots show headers around row 7; data on 8

    # 1) Clear existing rows (data area only), but keep styles/merges
    max_row = ws.max_row
    if max_row >= WBS_DATA_START_ROW:
        for r in range(WBS_DATA_START_ROW, max_row+1):
            # If the row already looks empty (no name), skip clearing
            nm = str(ws.cell(row=r, column=COL["NAME"]).value or "").strip()
            if nm == "":
                continue
            for c in range(COL["SSN"], COL["TOTALS"]+1):
                # use safe writer to avoid merged secondaries
                _safe_set(ws, r, c, None)

    # 2) Write rows in order
    r = WBS_DATA_START_ROW
    for _, row in merged.iterrows():
        ssn = str(row.get("ssn", "") or "").strip()
        emp = row["employee"]
        status = "A"  # you asked to force Active
        typ = str(row.get("type", "H")).upper()[:1]  # H or S
        rate = float(row.get("rate", 0.0) or 0.0)
        dept = str(row.get("dept", "") or "")

        reg = round(float(row["REG"] or 0.0), 3)
        ot  = round(float(row["OT"] or 0.0), 3)
        dt  = round(float(row["DT"] or 0.0), 3)

        # Basic columns
        _safe_set(ws, r, COL["SSN"], ssn)
        _safe_set(ws, r, COL["NAME"], emp)
        _safe_set(ws, r, COL["STATUS"], status)
        _safe_set(ws, r, COL["TYPE"], typ)
        _safe_set(ws, r, COL["RATE"], round(rate, 2))
        _safe_set(ws, r, COL["DEPT"], dept)

        # Hours columns
        _safe_set(ws, r, COL["REG"], reg)
        _safe_set(ws, r, COL["OT"],  ot)
        _safe_set(ws, r, COL["DT"],  dt)

        # Leave VAC/SICK/HOL as 0.000 unless you later feed them.

        # Totals (pink) = REG*$ + OT*$ + DT*$
        # Using the template columns so it keeps working if you rearrange
        # =ROUND(Gr*Er + Hr*Er*1.5 + Ir*Er*2, 2)
        g = ws.cell(row=r, column=COL["REG"]).coordinate
        h = ws.cell(row=r, column=COL["OT"]).coordinate
        i = ws.cell(row=r, column=COL["DT"]).coordinate
        e = ws.cell(row=r, column=COL["RATE"]).coordinate
        formula = f"=ROUND({g}*{e} + {h}*{e}*1.5 + {i}*{e}*2, 2)"
        _safe_set(ws, r, COL["TOTALS"], formula)

        r += 1

    # 3) Add report metadata (optional; fill if your template shows these cells)
    # You can enhance: parse Sierra date range to fill B2..B5
    # Intentionally skipped here to avoid guessing wrong cells.

    # Emit workbook
    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)
    return bio.read()

# ======================================================================================
# Routes
# ======================================================================================

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
        out_bytes = convert_sierra_to_wbs(contents, sheet_name=None)
        out_name = f"WBS_Payroll_{datetime.utcnow().date().isoformat()}.xlsx"
        return StreamingResponse(
            io.BytesIO(out_bytes),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename="{out_name}"'}
        )
    except ValueError as ve:
        raise HTTPException(status_code=422, detail=str(ve))
    except Exception as e:
        # Surface the error succinctly to the UI for faster iteration
        raise HTTPException(status_code=500, detail=f"Server error: {type(e).__name__}: {e}")

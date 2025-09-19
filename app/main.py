import io
import os
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple

import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import StreamingResponse, JSONResponse
from openpyxl import Workbook, load_workbook
from openpyxl.worksheet.worksheet import Worksheet
from openpyxl.utils import get_column_letter

# ==============================================================================
# CONFIG
# ==============================================================================
APP_TITLE = "Sierra → WBS Payroll Converter"
APP_VERSION = "2.0.0"

# Template + roster filenames (root of repo by default)
TEMPLATE_FILENAME = os.getenv("WBS_TEMPLATE", "WBS_template.xlsx")
ROSTER_CANDIDATES = [
    os.getenv("ROSTER_FILE", "roster.xlsx"),
    "roster-1.xlsx",
    "roster.csv",
]

ALLOWED_EXTS = (".xlsx", ".xls")

# ==============================================================================
# FASTAPI + CORS
# ==============================================================================
app = FastAPI(title=APP_TITLE, version=APP_VERSION)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        # <<< add your Netlify origin here for production >>>
        "*"
    ],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==============================================================================
# Helpers
# ==============================================================================
def _ext_ok(filename: str) -> bool:
    name = (filename or "").lower()
    return any(name.endswith(e) for e in ALLOWED_EXTS)

def _std(s: str) -> str:
    return (s or "").strip().lower().replace("\n", " ").replace("\r", " ")

def _find_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = { _std(c): c for c in df.columns }
    for want in candidates:
        key = _std(want)
        if key in cols:
            return cols[key]
    # relaxed "contains" on headers
    for want in candidates:
        key = _std(want)
        for k, v in cols.items():
            if key in k:
                return v
    return None

def _normalize_name(raw) -> str:
    if pd.isna(raw):
        return ""
    s = str(raw).strip()
    # Collapse whitespace, Title-case safely
    s = " ".join(s.split())
    return s

def _normalize_name_key(raw) -> str:
    # For grouping key (upper, no double spaces)
    s = _normalize_name(raw).upper()
    return " ".join(s.split())

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
    """ CA daily OT: first 8 REG, next 4 OT, >12 DT """
    h = float(day_hours or 0.0)
    reg = min(h, 8.0)
    ot = 0.0
    dt = 0.0
    if h > 8:
        ot = min(h - 8.0, 4.0)
    if h > 12:
        dt = h - 12.0
    return {"REG": reg, "OT": ot, "DT": dt}

def _money(x) -> float:
    return float(x or 0.0)

def _read_roster() -> Optional[pd.DataFrame]:
    """Load a roster file if present, return columns: key_ssn, key_name, name, ssn, dept, rate, wtype, order_idx"""
    path = None
    for cand in ROSTER_CANDIDATES:
        if os.path.exists(cand):
            path = cand
            break
    if not path:
        return None

    try:
        if path.lower().endswith(".csv"):
            df = pd.read_csv(path)
        else:
            df = pd.read_excel(path)
    except Exception:
        return None

    # Flexible header pickup
    name_col = _find_col(df, ["employee name", "name", "worker", "full name"])
    ssn_col  = _find_col(df, ["ssn", "social", "social security", "social security number"])
    dept_col = _find_col(df, ["department", "dept", "division"])
    rate_col = _find_col(df, ["pay rate", "rate", "hourly rate", "wage"])
    typ_col  = _find_col(df, ["type", "employee type", "emp type", "pay type"])

    out = pd.DataFrame()
    out["name"] = df[name_col].map(_normalize_name) if name_col else ""
    out["ssn"]  = (df[ssn_col].astype(str).str.replace(r"\D","", regex=True)
                   if ssn_col else "")
    out["dept"] = df[dept_col] if dept_col else ""
    out["rate"] = pd.to_numeric(df[rate_col], errors="coerce") if rate_col else 0.0
    out["wtype"]= (df[typ_col].astype(str) if typ_col else "")

    out["key_name"] = out["name"].map(_normalize_name_key)
    out["key_ssn"]  = out["ssn"].astype(str).str.strip()
    out["order_idx"]= range(len(out))

    # Drop empty names
    out = out[out["key_name"]!=""].copy()
    return out

def _load_wbs_template() -> Tuple[Workbook, Worksheet]:
    """Open the WBS template if present; otherwise create a fresh workbook."""
    if os.path.exists(TEMPLATE_FILENAME):
        wb = load_workbook(TEMPLATE_FILENAME)
        ws = wb.active
        return wb, ws

    # Fallback minimal template with our WBS columns
    wb = Workbook()
    ws = wb.active
    ws.title = "WEEKLY"
    ws.append(["", "", "WEEKLY PAYROLL", "", "", "", "", "", "", "", "", "", ""])
    ws.append([
        "Status", "SSN", "Employee", "Department", "Pay Rate",
        "REG (A01)", "OT (A02)", "DT (A03)",
        "REG $", "OT $", "DT $", "TOTAL $"
    ])
    return wb, ws

def _wbs_header_map(ws: Worksheet) -> Dict[str, int]:
    """
    Find the header row and map key columns to 1-based column indices.
    Keys: status, ssn, employee, department, rate, reg, ot, dt, reg$, ot$, dt$, total$
    """
    want = {
        "status": ["status"],
        "ssn": ["ssn", "social security"],
        "employee": ["employee", "employee name", "name"],
        "department": ["department", "dept", "division"],
        "rate": ["pay rate", "rate", "hourly rate"],
        "reg": ["reg", "a01"],
        "ot": ["ot", "a02"],
        "dt": ["dt", "a03"],
        "reg$": ["reg $", "reg$"],
        "ot$": ["ot $", "ot$"],
        "dt$": ["dt $", "dt$"],
        "total$": ["total $", "totals", "total"],
    }

    header_row_idx = None
    header_cells = None
    for r in range(1, min(ws.max_row, 40)+1):
        row_vals = [str(ws.cell(row=r, column=c).value or "").strip().lower() for c in range(1, ws.max_column+1)]
        if any("employee" in v for v in row_vals) and any(("ssn" in v) or ("social" in v) for v in row_vals):
            header_row_idx = r
            header_cells = row_vals
            break
    if header_row_idx is None:
        # try looser: any row that contains 'employee'
        for r in range(1, min(ws.max_row, 40)+1):
            row_vals = [str(ws.cell(row=r, column=c).value or "").strip().lower() for c in range(1, ws.max_column+1)]
            if any("employee" in v for v in row_vals):
                header_row_idx = r
                header_cells = row_vals
                break

    if header_row_idx is None:
        raise ValueError("Could not locate WBS header row in template.")

    col_idx: Dict[str,int] = {}
    for key, opts in want.items():
        found = None
        for c, text in enumerate(header_cells, start=1):
            for opt in opts:
                if opt in text:
                    found = c
                    break
            if found:
                break
        if found:
            col_idx[key] = found

    # Ensure the required minimum set exists
    required = ["status", "ssn", "employee", "rate", "reg", "ot", "dt", "reg$", "ot$", "dt$", "total$"]
    missing = [k for k in required if k not in col_idx]
    if missing:
        raise ValueError(f"Template missing required columns: {missing}")

    return {"header_row": header_row_idx, **col_idx}

def _clear_wbs_data(ws: Worksheet, header_row: int, last_col: int) -> None:
    """Clear values (keep styles) below the header row."""
    start_row = header_row + 1
    max_row = ws.max_row
    if max_row < start_row:
        return
    for r in range(start_row, max_row+1):
        # skip if the entire row is already empty
        row_has_value = False
        for c in range(1, last_col+1):
            if ws.cell(row=r, column=c).value not in (None, ""):
                row_has_value = True
                break
        if not row_has_value:
            continue
        for c in range(1, last_col+1):
            ws.cell(row=r, column=c).value = None

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

# ==============================================================================
# Core conversion
# ==============================================================================
def convert_sierra_to_wbs(input_bytes: bytes) -> bytes:
    # Load Sierra workbook (first sheet unless changed)
    excel = pd.ExcelFile(io.BytesIO(input_bytes))
    df = excel.parse(excel.sheet_names[0])

    if df.empty:
        raise ValueError("Input sheet is empty.")

    # Resolve Sierra columns
    required = {
        "employee": ["employee", "employee name", "name", "worker", "employee_name"],
        "date": ["date", "work date", "day", "worked date"],
        "hours": ["hours", "hrs", "total hours", "work hours"],
    }
    optional = {
        "rate": ["rate", "pay rate", "hourly rate", "wage"],
        "department": ["department", "dept", "division"],
        "ssn": ["ssn", "social", "social security", "social security number"],
        "wtype": ["type", "employee type", "emp type", "pay type"],
        "task": ["task", "earn type", "earning", "code"],
    }

    resolved_req = _require_columns(df, required)
    resolved_opt = {k: _find_col(df, v) for k, v in optional.items()}

    core = df[[resolved_req["employee"], resolved_req["date"], resolved_req["hours"]]].copy()
    core.columns = ["employee", "date", "hours"]

    # Attach optionals if present (no math changes)
    core["rate"]       = pd.to_numeric(df[resolved_opt["rate"]], errors="coerce") if resolved_opt["rate"] else None
    core["department"] = df[resolved_opt["department"]] if resolved_opt["department"] else None
    core["ssn"]        = (df[resolved_opt["ssn"]].astype(str).str.replace(r"\D","", regex=True)
                          if resolved_opt["ssn"] else None)
    core["wtype"]      = df[resolved_opt["wtype"]] if resolved_opt["wtype"] else None
    core["task"]       = df[resolved_opt["task"]] if resolved_opt["task"] else None

    # Normalize
    core["employee"] = core["employee"].map(_normalize_name)
    core["key_name"] = core["employee"].map(_normalize_name_key)
    core["date"]     = core["date"].map(_to_date)
    core["hours"]    = pd.to_numeric(core["hours"], errors="coerce").fillna(0.0).astype(float)

    if resolved_opt["ssn"]:
        core["key_ssn"] = core["ssn"].astype(str).str.strip()
    else:
        core["key_ssn"] = ""

    # Keep valid
    core = core[(core["key_name"]!="") & core["date"].notna() & (core["hours"] > 0)]

    # Day-level aggregation (per employee/date), then apply CA daily OT split
    day_agg = core.groupby(["key_ssn", "key_name", "employee", "date"], dropna=False)["hours"].sum().reset_index()

    split_rows = []
    for _, row in day_agg.iterrows():
        dist = _apply_ca_daily_ot(float(row["hours"]))
        split_rows.append({
            "key_ssn": row["key_ssn"],
            "key_name": row["key_name"],
            "employee": row["employee"],
            "REG": dist["REG"],
            "OT": dist["OT"],
            "DT": dist["DT"],
        })
    split_df = pd.DataFrame(split_rows)

    # Weekly totals per employee
    weekly = split_df.groupby(["key_ssn", "key_name", "employee"], dropna=False)[["REG","OT","DT"]].sum().reset_index()

    # Enrich identity from first seen in core
    id_map = (
        core.sort_values("date")
            .groupby(["key_ssn","key_name"], dropna=False)
            .agg({
                "department": "first",
                "rate": "first",
                "wtype": "first",
                "employee": "first",
                "ssn": "first"
            }).reset_index()
    )
    weekly = weekly.merge(id_map, on=["key_ssn","key_name"], how="left")

    # Load roster for order + SSN/rate/department overrides if present
    roster = _read_roster()
    if roster is not None and len(roster) > 0:
        weekly = weekly.merge(
            roster[["key_ssn","key_name","name","ssn","dept","rate","wtype","order_idx"]],
            on=["key_ssn","key_name"],
            how="left",
            suffixes=("","_ROSTER")
        )
        # Prefer roster identity data when present
        weekly["employee"] = weekly["name"].where(weekly["name"].notna() & (weekly["name"]!=""), weekly["employee"])
        weekly["ssn"]      = weekly["ssn_ROSTER"].where(weekly["ssn_ROSTER"].notna() & (weekly["ssn_ROSTER"]!=""), weekly["ssn"])
        weekly["department"]= weekly["dept"].where(weekly["dept"].notna() & (weekly["dept"]!=""), weekly["department"])
        weekly["rate"]     = weekly["rate_ROSTER"].where(pd.notna(weekly["rate_ROSTER"]), weekly["rate"])
        weekly["wtype"]    = weekly["wtype"].where(weekly["wtype"].notna() & (weekly["wtype"]!=""), weekly["wtype"])
    else:
        weekly["order_idx"] = None

    # Compute dollars from rate
    weekly["rate"] = pd.to_numeric(weekly["rate"], errors="coerce").fillna(0.0)
    weekly["REG_$"] = weekly["REG"] * weekly["rate"]
    weekly["OT_$"]  = weekly["OT"]  * weekly["rate"] * 1.5
    weekly["DT_$"]  = weekly["DT"]  * weekly["rate"] * 2.0
    weekly["TOTAL_$"] = weekly["REG_$"] + weekly["OT_$"] + weekly["DT_$"]

    # Status + Type
    weekly["Status"] = "A"
    # Type (H/S) if/when needed by your WBS; keep here if template has it
    # weekly["Type"] = weekly["wtype"].astype(str).str.upper().map(lambda x: "S" if x.startswith("S") else "H")

    # Order: roster first (order_idx), then by SSN/name stable
    weekly["order_key"] = weekly["order_idx"].fillna(1e9)
    weekly = weekly.sort_values(["order_key","key_ssn","key_name"]).reset_index(drop=True)

    # Open WBS template and map columns
    wb, ws = _load_wbs_template()
    col = _wbs_header_map(ws)
    header_row = col["header_row"]

    # Determine last column we will clear/write to (use far-right among mapped keys)
    mapped_cols = [v for k,v in col.items() if k != "header_row"]
    last_data_col = max(mapped_cols) if mapped_cols else ws.max_column

    # Clear old data (keep styles)
    _clear_wbs_data(ws, header_row=header_row, last_col=last_data_col)

    # Write rows beneath header
    out_cols = {
        "status": "Status",
        "ssn": "ssn",
        "employee": "employee",
        "department": "department",
        "rate": "rate",
        "reg": "REG",
        "ot": "OT",
        "dt": "DT",
        "reg$": "REG_$",
        "ot$": "OT_$",
        "dt$": "DT_$",
        "total$": "TOTAL_$",
    }

    write_row = header_row + 1
    for _, r in weekly.iterrows():
        # Values
        values = {
            "status": r["Status"],
            "ssn": (str(r.get("ssn") or "").strip()),
            "employee": r["employee"],
            "department": r.get("department") or "",
            "rate": round(_money(r["rate"]), 2),
            "reg": round(_money(r["REG"]), 2),
            "ot": round(_money(r["OT"]), 2),
            "dt": round(_money(r["DT"]), 2),
            "reg$": round(_money(r["REG_$"]), 2),
            "ot$": round(_money(r["OT_$"]), 2),
            "dt$": round(_money(r["DT_$"]), 2),
            "total$": round(_money(r["TOTAL_$"]), 2),
        }

        # Place into the mapped columns
        for key, src_field in out_cols.items():
            col_idx = col.get(key)
            if not col_idx:
                continue
            ws.cell(row=write_row, column=col_idx).value = values[key]

        write_row += 1

    # Optional: adjust column widths only if sheet was generated (not template)
    if not os.path.exists(TEMPLATE_FILENAME):
        for c in range(1, last_data_col+1):
            width = 12
            for r in range(1, write_row):
                v = ws.cell(row=r, column=c).value
                if v is not None:
                    width = max(width, len(str(v)))
            ws.column_dimensions[get_column_letter(c)].width = min(width + 2, 30)

    # Save to bytes
    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)
    return bio.read()

# ==============================================================================
# Routes
# ==============================================================================
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
        out_bytes = convert_sierra_to_wbs(contents)
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

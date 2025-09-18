# server/main.py
import io
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple

import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import StreamingResponse, JSONResponse
from openpyxl import Workbook
from openpyxl.utils import get_column_letter
from openpyxl.styles import Alignment, Font, PatternFill, Border, Side

# ------------------------------------------------------------------------------
# App + CORS
# ------------------------------------------------------------------------------
app = FastAPI(title="Sierra → WBS Payroll Converter", version="1.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten to your Netlify origin if desired
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ALLOWED_EXTS = (".xlsx", ".xls")

# ------------------------------------------------------------------------------
# Helpers (header matching only; numbers remain exact)
# ------------------------------------------------------------------------------
def _ext_ok(filename: str) -> bool:
    name = (filename or "").lower()
    return any(name.endswith(e) for e in ALLOWED_EXTS)

def _std(s: str) -> str:
    return (s or "").strip().lower().replace("\n", " ").replace("\r", " ")

def _find_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols_map = { _std(c): c for c in df.columns }
    # exact
    for want in candidates:
        key = _std(want)
        if key in cols_map:
            return cols_map[key]
    # relaxed contains (headers only)
    for want in candidates:
        key = _std(want)
        for k, v in cols_map.items():
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
    if not isinstance(raw, str):
        raw = "" if pd.isna(raw) else str(raw)
    name = raw.strip()
    if not name:
        return ""
    parts = [p for p in name.split() if p]
    # If already "Last, First", keep as-is
    if len(parts) >= 2 and "," in name:
        return name
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

def _money(x) -> float:
    return float(x or 0.0)

def _employee_key(ssn: str, name: str) -> Tuple[str, str]:
    """Canonical key preference: SSN (trimmed) else normalized name."""
    s = (ssn or "").strip()
    if s:
        return ("SSN", s)
    return ("NAME", _normalize_name(name))

def _apply_ca_daily_ot(day_hours: float) -> Dict[str, float]:
    """
    California daily OT:
      - First 8 → REG
      - Next 4 (8–12) → OT
      - >12 → DT
    """
    h = float(day_hours or 0.0)
    reg = min(h, 8.0)
    ot = 0.0
    dt = 0.0
    if h > 8:
        ot = min(h - 8.0, 4.0)
    if h > 12:
        dt = h - 12.0
    return {"REG": reg, "OT": ot, "DT": dt}

# ------------------------------------------------------------------------------
# Core conversion to EXACT WBS layout
# ------------------------------------------------------------------------------
def convert_sierra_to_wbs(input_bytes: bytes, sheet_name: Optional[str] = None) -> bytes:
    excel = pd.ExcelFile(io.BytesIO(input_bytes))
    target_sheet = sheet_name or excel.sheet_names[0]
    df = excel.parse(target_sheet)

    if df.empty:
        raise ValueError("Input sheet is empty.")

    # Expected inputs from Sierra (we accept common variants)
    required = {
        "employee": ["employee", "employee name", "name", "worker", "employee_name"],
        "date": ["date", "work date", "day", "worked date"],
        "hours": ["hours", "hrs", "total hours", "work hours"],
        "rate": ["rate", "pay rate", "hourly rate", "wage", "pay_rate"],
    }
    optional = {
        "department": ["department", "dept", "division"],
        "ssn": ["ssn", "social", "social security", "social security number"],
        "wtype": ["type", "employee type", "emp type", "pay type", "wbs type"],
        "task": ["task", "earn type", "earning", "code", "earn code"],
    }

    resolved_req = _require_columns(df, required)
    resolved_opt = {k: _find_col(df, v) for k, v in optional.items()}

    core = df[[resolved_req["employee"], resolved_req["date"], resolved_req["hours"], resolved_req["rate"]]].copy()
    core.columns = ["employee", "date", "hours", "rate"]

    # Attach optionals if present (do not modify numeric values)
    core["department"] = df[resolved_opt["department"]] if resolved_opt["department"] else ""
    core["ssn"]        = df[resolved_opt["ssn"]] if resolved_opt["ssn"] else ""
    core["wtype"]      = df[resolved_opt["wtype"]] if resolved_opt["wtype"] else ""
    core["task"]       = df[resolved_opt["task"]] if resolved_opt["task"] else ""

    # Normalize
    core["employee"] = core["employee"].astype(str).map(_normalize_name)
    core["date"]     = core["date"].map(_to_date)
    core["hours"]    = pd.to_numeric(core["hours"], errors="coerce").fillna(0.0).astype(float)
    core["rate"]     = pd.to_numeric(core["rate"], errors="coerce").fillna(0.0).astype(float)

    # Valid rows only
    core = core[(core["employee"].str.len() > 0) & core["date"].notna() & (core["hours"] > 0)]

    if core.empty:
        raise ValueError("No valid rows after cleaning (check Employee/Date/Hours).")

    # Per-employee/day sum first (handles duplicates in source)
    day_group = (
        core.groupby(["employee", "ssn", "department", "wtype", "rate", "date"], dropna=False)["hours"]
            .sum()
            .reset_index()
    )

    # Apply CA daily split, then weekly roll-up
    split_rows = []
    for _, row in day_group.iterrows():
        dist = _apply_ca_daily_ot(float(row["hours"]))
        split_rows.append({
            "employee": row["employee"],
            "ssn": (row["ssn"] or ""),
            "department": (row["department"] or ""),
            "wtype": (row["wtype"] or ""),
            "rate": float(row["rate"]),
            "date": row["date"],
            "REG": dist["REG"],
            "OT": dist["OT"],
            "DT": dist["DT"],
        })
    split_df = pd.DataFrame(split_rows)

    # Canonical employee key (SSN preferred)
    split_df["emp_key_type"], split_df["emp_key_val"] = zip(*split_df.apply(lambda r: _employee_key(r["ssn"], r["employee"]), axis=1))

    # Weekly roll-up *by employee* (sum all days in file)
    weekly = (
        split_df.groupby(["emp_key_type", "emp_key_val", "employee", "ssn", "department", "wtype", "rate"], dropna=False)[["REG", "OT", "DT"]]
            .sum()
            .reset_index()
    )

    # Dollars
    weekly["REG_$"]   = weekly["REG"] * weekly["rate"]
    weekly["OT_$"]    = weekly["OT"]  * weekly["rate"] * 1.5
    weekly["DT_$"]    = weekly["DT"]  * weekly["rate"] * 2.0
    weekly["TOTAL_$"] = weekly["REG_$"] + weekly["OT_$"] + weekly["DT_$"]

    # WBS identity defaults
    weekly["Status"] = "A"
    weekly["Type"] = weekly["wtype"].astype(str).str.upper().map(lambda x: "S" if x.startswith("S") else "H")

    # Final WBS column order
    out_cols = [
        "Status",            # A
        "Type",              # H/S
        "employee",          # Last, First
        "ssn",               # may be blank if not provided
        "department",        # optional
        "rate",              # pay rate
        "REG",               # A01
        "OT",                # A02
        "DT",                # A03
        "REG_$",
        "OT_$",
        "DT_$",
        "TOTAL_$",
    ]
    out = weekly[out_cols].copy()

    # Sort for stable, readable output (by Department then Employee)
    out["department_sort"] = out["department"].astype(str)
    out.sort_values(by=["department_sort", "employee"], inplace=True)
    out.drop(columns=["department_sort"], inplace=True)

    # Build Excel in-memory — EXACT WBS layout
    wb = Workbook()
    ws = wb.active
    ws.title = "WEEKLY"

    # Title row + header row
    title_row = ["", "", "WEEKLY PAYROLL", "", "", "", "", "", "", "", "", "", ""]
    headers   = ["Status", "Type", "Employee", "SSN", "Department", "Pay Rate",
                 "REG (A01)", "OT (A02)", "DT (A03)", "REG $", "OT $", "DT $", "TOTAL $"]

    ws.append(title_row)
    ws.append(headers)

    # Styles
    title_font = Font(bold=True, size=14)
    header_font = Font(bold=True)
    center = Alignment(horizontal="center", vertical="center")
    right = Alignment(horizontal="right")
    thin = Side(border_style="thin", color="DDDDDD")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)
    header_fill = PatternFill(start_color="F3F4F6", end_color="F3F4F6", fill_type="solid")

    # Apply title style
    ws["C1"].font = title_font
    ws["C1"].alignment = center

    # Apply header styles
    for col_idx in range(1, 14):
        cell = ws.cell(row=2, column=col_idx)
        cell.font = header_font
        cell.alignment = center
        cell.fill = header_fill
        cell.border = border

    # Data rows
    for _, r in out.iterrows():
        ws.append([
            r["Status"],
            r["Type"],
            r["employee"],
            r["ssn"],
            r["department"],
            round(_money(r["rate"]), 2),
            round(_money(r["REG"]), 2),
            round(_money(r["OT"]), 2),
            round(_money(r["DT"]), 2),
            round(_money(r["REG_$"]), 2),
            round(_money(r["OT_$"]), 2),
            round(_money(r["DT_$"]), 2),
            round(_money(r["TOTAL_$"]), 2),
        ])

    first_data_row = 3
    last_data_row = ws.max_row

    # Grand Totals row
    if last_data_row >= first_data_row:
        ws.append(["", "", "GRAND TOTALS", "", "", ""] +
                  [f"=SUM({get_column_letter(c)}{first_data_row}:{get_column_letter(c)}{last_data_row})"
                   for c in range(7, 14)])
        total_row = ws.max_row

        # Style totals row
        for col_idx in range(1, 14):
            cell = ws.cell(row=total_row, column=col_idx)
            cell.font = Font(bold=True)
            cell.border = border
            if col_idx >= 7:
                cell.alignment = right

    # Column widths
    widths = {
        1: 8,   # Status
        2: 8,   # Type
        3: 26,  # Employee
        4: 16,  # SSN
        5: 18,  # Department
        6: 12,  # Pay Rate
        7: 12,  # REG
        8: 12,  # OT
        9: 12,  # DT
        10: 12, # REG $
        11: 12, # OT $
        12: 12, # DT $
        13: 12, # TOTAL $
    }
    for col_idx, w in widths.items():
        ws.column_dimensions[get_column_letter(col_idx)].width = w

    # Number formats (currency for $ columns, 2 decimals for hours/rate)
    for row in ws.iter_rows(min_row=3, max_row=ws.max_row, min_col=1, max_col=13):
        for idx, cell in enumerate(row, start=1):
            cell.border = border
            if idx in (6, 7, 8, 9):   # rate + hours
                cell.number_format = '0.00'
                if idx >= 7:
                    cell.alignment = right
            elif idx in (10, 11, 12, 13):  # currency
                cell.number_format = '"$"#,##0.00'
                cell.alignment = right

    # Return bytes
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
        raise HTTPException(status_code=400, detail="No selected file.")
    if not _ext_ok(file.filename):
        raise HTTPException(status_code=415, detail="Unsupported file type. Please upload .xlsx or .xls")

    try:
        contents = await file.read()
        out_bytes = convert_sierra_to_wbs(contents, sheet_name=None)
        # File name in WBS style
        today = datetime.utcnow().date()
        out_name = f"WBS Payroll {today.strftime('%Y-%m-%d')}.xlsx"
        return StreamingResponse(
            io.BytesIO(out_bytes),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename="{out_name}"'}
        )
    except ValueError as ve:
        raise HTTPException(status_code=422, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")

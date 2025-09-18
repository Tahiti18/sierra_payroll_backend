# app/main.py
import io
import traceback
from pathlib import Path
from datetime import datetime, date
from typing import Dict, List, Optional

import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import StreamingResponse, JSONResponse
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter

DEBUG = True

app = FastAPI(title="Sierra → WBS Payroll Converter", version="1.0.1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ALLOWED_EXTS = (".xlsx", ".xls")

# IMPORTANT: template lives in repo root, not in app/
TEMPLATE_PATH = (Path(__file__).resolve().parents[1] / "wbs_template.xlsx")

WBS_SHEET_NAME = "WEEKLY"
WBS_DATA_START_ROW = 9  # employees start at row 9 in your template

# Column indexes (1-based) as they are in your template
COL = {
    "SSN": 1,           # A
    "EMP": 2,           # B
    "STATUS": 3,        # C
    "TYPE": 4,          # D
    # Col E is just the "Pay" header group. Rate actually in F.
    "PAY_RATE": 6,      # F
    "DEPT": 7,          # G
    "A01": 8,           # REGULAR
    "A02": 9,           # OVERTIME
    "A03": 10,          # DOUBLETIME
    "A06": 11,          # VACATION
    "A07": 12,          # SICK
    "A08": 13,          # HOLIDAY
    # Safe boundary for cells we control; totals/formulas live far to the right
    "TOTALS": 54,
}

# ------------------------ helpers ------------------------
def _ext_ok(filename: str) -> bool:
    name = (filename or "").lower()
    return any(name.endswith(e) for e in ALLOWED_EXTS)

def _std_col(s: str) -> str:
    return (s or "").strip().lower().replace("\n", " ").replace("\r", " ")

def _find_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = { _std_col(c): c for c in df.columns }
    for want in candidates:
        key = _std_col(want)
        if key in cols:
            return cols[key]
    for want in candidates:
        key = _std_col(want)
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
    if not raw or not isinstance(raw, str):
        return ""
    name = raw.strip()
    parts = [p for p in name.split() if p]
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
    h = float(day_hours or 0.0)
    reg = min(h, 8.0)
    ot = max(0.0, min(h - 8.0, 4.0))
    dt = max(0.0, h - 12.0)
    return {"REG": reg, "OT": ot, "DT": dt}

def _money(x: float) -> float:
    return float(x or 0.0)

# ------------------------ core ------------------------
def convert_sierra_to_wbs(input_bytes: bytes, sheet_name: Optional[str] = None) -> bytes:
    # 1) Read input
    try:
        excel = pd.ExcelFile(io.BytesIO(input_bytes))
    except Exception as e:
        raise ValueError(f"Not a valid Excel file: {e}")

    target_sheet = sheet_name or excel.sheet_names[0]
    df = excel.parse(target_sheet)
    if df.empty:
        raise ValueError("Input sheet is empty.")

    required = {
        "employee": ["employee", "employee name", "name", "worker", "employee_name"],
        "date": ["date", "work date", "day", "worked date"],
        "hours": ["hours", "hrs", "total hours", "work hours"],
        "rate": ["rate", "pay rate", "hourly rate", "wage"],
    }
    optional = {
        "department": ["department", "dept", "division"],
        "ssn": ["ssn", "social", "social security", "social security number"],
        "wtype": ["type", "employee type", "emp type", "pay type"],
        "task": ["task", "earn type", "earning", "code"],
    }

    resolved_req = _require_columns(df, required)
    resolved_opt = {k: _find_col(df, v) for k, v in optional.items()}

    core = df[[resolved_req["employee"], resolved_req["date"], resolved_req["hours"], resolved_req["rate"]]].copy()
    core.columns = ["employee", "date", "hours", "rate"]

    core["department"] = df[resolved_opt["department"]] if resolved_opt["department"] else ""
    core["ssn"]        = df[resolved_opt["ssn"]] if resolved_opt["ssn"] else ""
    core["wtype"]      = df[resolved_opt["wtype"]] if resolved_opt["wtype"] else ""
    core["task"]       = df[resolved_opt["task"]] if resolved_opt["task"] else ""

    core["employee"] = core["employee"].astype(str).map(_normalize_name)
    core["date"]     = core["date"].map(_to_date)
    core["hours"]    = pd.to_numeric(core["hours"], errors="coerce").fillna(0.0).astype(float)
    core["rate"]     = pd.to_numeric(core["rate"], errors="coerce").fillna(0.0).astype(float)

    core = core[(core["employee"].str.len() > 0) & core["date"].notna() & (core["hours"] > 0)]

    # 2) Daily → split by CA rule
    day_group = core.groupby(["employee", "date", "rate"], dropna=False)["hours"].sum().reset_index()
    split_rows = []
    for _, row in day_group.iterrows():
        dist = _apply_ca_daily_ot(float(row["hours"]))
        split_rows.append({
            "employee": row["employee"],
            "date": row["date"],
            "rate": float(row["rate"]),
            "REG": dist["REG"],
            "OT": dist["OT"],
            "DT": dist["DT"],
        })
    split_df = pd.DataFrame(split_rows)

    # 3) Weekly per employee
    weekly = split_df.groupby(["employee", "rate"], dropna=False)[["REG", "OT", "DT"]].sum().reset_index()

    # Bring identity info (first seen)
    id_map = (
        core.groupby("employee")
            .agg({"department": "first", "ssn": "first", "wtype": "first"})
            .reset_index()
    )
    out = pd.merge(weekly, id_map, on="employee", how="left")

    # WBS identity defaults
    out["Status"] = "A"
    out["Type"] = out["wtype"].astype(str).str.upper().map(lambda x: "S" if x.startswith("S") else "H")

    # 4) Open template
    if not TEMPLATE_PATH.exists():
        raise ValueError(f"WBS template not found at {TEMPLATE_PATH}")

    wb = load_workbook(str(TEMPLATE_PATH))
    if WBS_SHEET_NAME not in wb.sheetnames:
        raise ValueError(f"WBS sheet '{WBS_SHEET_NAME}' not found in template.")
    ws = wb[WBS_SHEET_NAME]

    # 4a) **Unmerge** any merged ranges that intersect the data rows we write,
    # so writes to C/D/etc. are not blocked by openpyxl "MergedCell is read-only".
    to_unmerge = []
    for rng in ws.merged_cells.ranges:
        if rng.max_row >= WBS_DATA_START_ROW and rng.min_col <= COL["TOTALS"]:
            to_unmerge.append(str(rng))
    for addr in to_unmerge:
        ws.unmerge_cells(addr)

    # 5) Clear prior data rows but keep formatting/formulas
    max_row = ws.max_row
    if max_row >= WBS_DATA_START_ROW:
        for r in range(WBS_DATA_START_ROW, max_row + 1):
            # If already blank up to our safe boundary, skip
            blank = True
            for c in range(1, COL["TOTALS"] + 1):
                val = ws.cell(row=r, column=c).value
                if val not in (None, ""):
                    blank = False
                    break
            if blank:
                continue
            for c in range(1, COL["TOTALS"] + 1):
                ws.cell(row=r, column=c).value = None

    # 6) Write rows
    out["department"] = out["department"].fillna("")
    employees = sorted(
        out.to_dict("records"),
        key=lambda e: (str(e.get("department") or ""), str(e.get("employee") or "")),
    )

    current_row = WBS_DATA_START_ROW
    for emp in employees:
        ssn = str(emp.get("ssn") or "").strip()
        name = str(emp.get("employee") or "").strip()
        status = str(emp.get("Status") or "A").upper()
        emp_type = str(emp.get("Type") or "H").upper()
        dept = str(emp.get("department") or "").upper()
        rate = float(emp.get("rate") or 0.0)

        reg = float(emp.get("REG") or 0.0)
        ot  = float(emp.get("OT")  or 0.0)
        dt  = float(emp.get("DT")  or 0.0)

        ws.cell(row=current_row, column=COL["SSN"]).value = ssn
        ws.cell(row=current_row, column=COL["EMP"]).value = name
        ws.cell(row=current_row, column=COL["STATUS"]).value = status
        ws.cell(row=current_row, column=COL["TYPE"]).value = emp_type
        ws.cell(row=current_row, column=COL["PAY_RATE"]).value = round(_money(rate), 2)
        ws.cell(row=current_row, column=COL["DEPT"]).value = dept

        ws.cell(row=current_row, column=COL["A01"]).value = round(_money(reg), 3)
        ws.cell(row=current_row, column=COL["A02"]).value = round(_money(ot), 3)
        ws.cell(row=current_row, column=COL["A03"]).value = round(_money(dt), 3)
        ws.cell(row=current_row, column=COL["A06"]).value = 0.0
        ws.cell(row=current_row, column=COL["A07"]).value = 0.0
        ws.cell(row=current_row, column=COL["A08"]).value = 0.0

        current_row += 1

    # 7) Autosize area we touched
    for col_idx in range(1, COL["A08"] + 1):
        col_letter = get_column_letter(col_idx)
        max_len = 8
        for r in range(1, current_row):
            val = ws[f"{col_letter}{r}"].value
            if val is None:
                continue
            max_len = max(max_len, len(str(val)))
        ws.column_dimensions[col_letter].width = min(max_len + 2, 30)

    # 8) Return bytes
    out_bio = io.BytesIO()
    wb.save(out_bio)
    out_bio.seek(0)
    return out_bio.read()

# ------------------------ routes ------------------------
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
        if DEBUG:
            print("ValueError while processing payroll:\n" + traceback.format_exc())
        raise HTTPException(status_code=422, detail=str(ve))
    except Exception:
        tb = traceback.format_exc()
        print("Unhandled server error:\n" + tb)
        msg = "Server error: backend processing failed"
        if DEBUG:
            msg += " (see Railway Deploy Logs for traceback)"
        raise HTTPException(status_code=500, detail=msg)

@app.post("/debug/inspect")
async def debug_inspect(file: UploadFile = File(...)):
    contents = await file.read()
    try:
        xl = pd.ExcelFile(io.BytesIO(contents))
        df = xl.parse(xl.sheet_names[0])
        cols = list(df.columns.astype(str))
        return JSONResponse({"sheets": xl.sheet_names, "first_sheet_columns": cols})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=422)

# app/main.py
import io
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import StreamingResponse, JSONResponse

from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet
from openpyxl.cell.cell import MergedCell
from openpyxl.utils import get_column_letter

# ──────────────────────────────────────────────────────────────────────────────
# FastAPI + CORS
# ──────────────────────────────────────────────────────────────────────────────
app = FastAPI(title="Sierra → WBS Payroll Converter", version="2.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten to your frontend origin if you want
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ALLOWED_EXTS = (".xlsx", ".xls")

# ──────────────────────────────────────────────────────────────────────────────
# Utilities
# ──────────────────────────────────────────────────────────────────────────────
def _ext_ok(filename: str) -> bool:
    name = (filename or "").lower()
    return any(name.endswith(e) for e in ALLOWED_EXTS)

def _std(s: str) -> str:
    return (s or "").strip().lower().replace("\n", " ").replace("\r", " ")

def _find_col(df: pd.DataFrame, options: List[str]) -> Optional[str]:
    cols = { _std(c): c for c in df.columns }
    for want in options:
        key = _std(want)
        if key in cols:
            return cols[key]
    # relaxed contains
    for want in options:
        key = _std(want)
        for k, v in cols.items():
            if key in k:
                return v
    return None

def _require(df: pd.DataFrame, mapping: Dict[str, List[str]]) -> Dict[str, str]:
    out, miss = {}, []
    for logical, opts in mapping.items():
        c = _find_col(df, opts)
        if c:
            out[logical] = c
        else:
            miss.append(f"{logical} (any of: {', '.join(opts)})")
    if miss:
        raise ValueError("Missing required columns: " + "; ".join(miss))
    return out

def _normalize_name(raw: str) -> str:
    if not isinstance(raw, str):
        return ""
    name = " ".join(raw.strip().replace(",", " ").split())
    parts = name.split()
    # “First Last” -> “Last, First” if exactly two parts and no comma already
    if len(parts) == 2 and "," not in name:
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

def _apply_ca_daily_ot(h: float) -> Dict[str, float]:
    """CA daily overtime split: 0–8 REG, 8–12 OT, >12 DT."""
    h = float(h or 0.0)
    reg = min(h, 8.0)
    ot = min(max(h - 8.0, 0.0), 4.0)
    dt = max(h - 12.0, 0.0)
    return {"REG": reg, "OT": ot, "DT": dt}

def _num(x) -> float:
    try:
        return float(x or 0.0)
    except Exception:
        return 0.0

# ──────────────────────────────────────────────────────────────────────────────
# Load roster + optional alias map from repo root
# ──────────────────────────────────────────────────────────────────────────────
def _load_alias_map(app_dir: Path) -> Dict[str, str]:
    alias_path = app_dir.parent / "roster.csv"  # optional; columns: alias,canonical
    alias = {}
    if not alias_path.exists():
        return alias
    try:
        import csv
        with open(alias_path, newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                a = (row.get("alias") or "").strip()
                c = (row.get("canonical") or "").strip()
                if a and c:
                    alias[_normalize_name(a)] = c
    except Exception:
        pass
    return alias

def _load_roster(app_dir: Path) -> pd.DataFrame:
    # Prefer roster.xlsx; fall back to roster-1.xlsx
    xlsx = app_dir.parent / "roster.xlsx"
    if not xlsx.exists():
        alt = app_dir.parent / "roster-1.xlsx"
        if alt.exists():
            xlsx = alt
        else:
            raise ValueError("Roster file not found. Put 'roster.xlsx' (or 'roster-1.xlsx') in repo root.")

    roster = pd.read_excel(xlsx, sheet_name=0)
    mapping = {
        "name": ["name", "employee", "employee name", "worker"],
        "ssn": ["ssn", "social", "social security", "social security number"],
        "department": ["department", "dept", "division"],
        "type": ["type", "employee type", "emp type", "pay type"],
        "rate": ["rate", "pay rate", "hourly rate", "wage", "base rate"],
    }
    cols = _require(roster, mapping)
    out = pd.DataFrame({
        "name": roster[cols["name"]].astype(str).map(_normalize_name),
        "ssn": roster[cols["ssn"]].astype(str).fillna(""),
        "department": roster[cols["department"]].astype(str).fillna("").str.upper(),
        "type": roster[cols["type"]].astype(str).fillna("").str.upper(),
        "rate": pd.to_numeric(roster[cols["rate"]], errors="coerce").fillna(0.0).astype(float),
    })
    return out

# ──────────────────────────────────────────────────────────────────────────────
# Sierra → weekly REG/OT/DT
# ──────────────────────────────────────────────────────────────────────────────
def _aggregate_sierra(input_bytes: bytes, sheet_name: Optional[str] = None) -> pd.DataFrame:
    excel = pd.ExcelFile(io.BytesIO(input_bytes))
    target_sheet = sheet_name or excel.sheet_names[0]
    df = excel.parse(target_sheet)

    required = {
        "employee": ["employee", "employee name", "name", "worker", "employee_name"],
        "date": ["date", "work date", "day", "worked date"],
        "hours": ["hours", "hrs", "total hours", "work hours"],
    }
    cols = _require(df, required)

    core = df[[cols["employee"], cols["date"], cols["hours"]]].copy()
    core.columns = ["employee", "date", "hours"]

    core["employee"] = core["employee"].astype(str)
    core["date"] = core["date"].map(_to_date)
    core["hours"] = pd.to_numeric(core["hours"], errors="coerce").fillna(0.0).astype(float)

    core = core[(core["employee"].str.len() > 0) & core["date"].notna() & (core["hours"] > 0)]

    # Sum per employee/day, then apply CA split, then roll up by employee (weekly)
    by_day = core.groupby(["employee", "date"], dropna=False)["hours"].sum().reset_index()
    rows = []
    for _, r in by_day.iterrows():
        d = _apply_ca_daily_ot(float(r["hours"]))
        rows.append({"employee": r["employee"], "REG": d["REG"], "OT": d["OT"], "DT": d["DT"]})
    split = pd.DataFrame(rows)
    weekly = split.groupby("employee", dropna=False)[["REG", "OT", "DT"]].sum().reset_index()
    weekly["employee"] = weekly["employee"].map(_normalize_name)
    return weekly

# ──────────────────────────────────────────────────────────────────────────────
# WBS template writing
# ──────────────────────────────────────────────────────────────────────────────
# Column map per your template (you said SSN and Status were swapped; fixing here):
# SSN must be column A (1), Type column B (2), Name column C (3), Status column D (4), Dept E (5), Rate F (6)
# H..L are hour buckets, Q/S/U/W/Y/Z are piecework, AB is Totals
COL = {
    "SSN": 1,
    "TYPE": 2,
    "NAME": 3,
    "STATUS": 4,
    "DEPT": 5,
    "RATE": 6,
    "A01": 8, "A02": 9, "A03": 10, "A04": 11, "A05": 12,
    "P_Q": 17, "P_S": 19, "P_U": 21, "P_W": 23, "P_Y": 25, "P_Z": 26,
    "TOTALS": 28,  # AB
}
WBS_DATA_START_ROW = 9

def _find_totals_row(ws: Worksheet) -> Optional[int]:
    for r in range(WBS_DATA_START_ROW, ws.max_row + 1):
        v = ws.cell(row=r, column=COL["NAME"]).value
        if isinstance(v, str) and v.strip().lower() == "totals":
            return r
    return None

def _clear_data_area(ws: Worksheet, last_col: int, start_row: int, end_row: int) -> None:
    """
    Clear values in a rectangular area without touching merged cells.
    Prevents 'MergedCell value is read-only'.
    """
    if end_row < start_row:
        return
    for r in range(start_row, end_row + 1):
        # quick skip if row already empty (ignoring merged cells)
        has_val = False
        for c in range(1, last_col + 1):
            cell = ws.cell(row=r, column=c)
            if not isinstance(cell, MergedCell) and cell.value not in (None, ""):
                has_val = True
                break
        if not has_val:
            continue
        for c in range(1, last_col + 1):
            cell = ws.cell(row=r, column=c)
            if isinstance(cell, MergedCell):
                continue
            cell.value = None

def _write_wbs(template_bytes: bytes, rows: List[Dict]) -> bytes:
    wb = load_workbook(io.BytesIO(template_bytes), data_only=False)
    ws = wb.active

    # keep A8 harmless to avoid accidental text interfering with widths
    try:
        if ws.cell(row=8, column=1).value not in (None, "#", "# "):
            ws.cell(row=8, column=1).value = "#"
    except Exception:
        pass

    # find/prepare Totals row placeholder
    totals_row_orig = _find_totals_row(ws)
    if totals_row_orig is None:
        totals_row_orig = ws.max_row + 1
        ws.cell(row=totals_row_orig, column=COL["NAME"]).value = "Totals"

    # clear previous data rows (preserve styles/merges)
    _clear_data_area(ws, COL["TOTALS"], WBS_DATA_START_ROW, totals_row_orig - 1)

    # write data rows
    r = WBS_DATA_START_ROW
    for emp in rows:
        rate = _num(emp.get("rate"))

        ws.cell(row=r, column=COL["SSN"]).value = emp.get("ssn", "")
        ws.cell(row=r, column=COL["TYPE"]).value = "S" if str(emp.get("type","")).upper().startswith("S") else "H"
        ws.cell(row=r, column=COL["NAME"]).value = emp["name"]
        ws.cell(row=r, column=COL["STATUS"]).value = "A"  # active
        ws.cell(row=r, column=COL["DEPT"]).value = str(emp.get("department", "")).upper()
        ws.cell(row=r, column=COL["RATE"]).value = round(rate, 2)

        reg = _num(emp.get("REG"))
        ot  = _num(emp.get("OT"))
        dt  = _num(emp.get("DT"))

        ws.cell(row=r, column=COL["A01"]).value = round(reg, 2)
        ws.cell(row=r, column=COL["A02"]).value = round(ot,  2)
        ws.cell(row=r, column=COL["A03"]).value = round(dt,  2)
        ws.cell(row=r, column=COL["A04"]).value = 0.0
        ws.cell(row=r, column=COL["A05"]).value = 0.0

        piece_total = 0.0
        for k in ["P_Q","P_S","P_U","P_W","P_Y","P_Z"]:
            v = _num(emp.get(k))
            ws.cell(row=r, column=COL[k]).value = round(v, 2)
            piece_total += v

        # numeric pink total in AB (works in Excel + Google Sheets)
        hourly_total = (reg * rate) + (ot * rate * 1.5) + (dt * rate * 2.0)
        row_total = hourly_total + piece_total
        ws.cell(row=r, column=COL["TOTALS"]).value = round(row_total, 2)

        r += 1

    # move Totals row directly under last employee and set SUM over AB
    totals_row_new = r
    ws.cell(row=totals_row_new, column=COL["NAME"]).value = "Totals"
    ab = get_column_letter(COL["TOTALS"])
    ws.cell(row=totals_row_new, column=COL["TOTALS"]).value = f"=SUM({ab}{WBS_DATA_START_ROW}:{ab}{r-1})"

    if totals_row_orig != totals_row_new:
        _clear_data_area(ws, COL["TOTALS"], totals_row_orig, totals_row_orig)

    out = io.BytesIO()
    wb.save(out)
    out.seek(0)
    return out.read()

# ──────────────────────────────────────────────────────────────────────────────
# Orchestration
# ──────────────────────────────────────────────────────────────────────────────
def convert_sierra_to_wbs(input_bytes: bytes, sheet_name: Optional[str] = None) -> bytes:
    app_dir = Path(__file__).resolve().parent

    # source of truth
    roster = _load_roster(app_dir)
    alias_map = _load_alias_map(app_dir)
    roster_names = set(roster["name"].tolist())

    weekly = _aggregate_sierra(input_bytes, sheet_name=sheet_name)

    # canonicalize Sierra names to roster names
    def to_canonical(n: str) -> str:
        n_norm = _normalize_name(n)
        if n_norm in alias_map:
            return alias_map[n_norm]
        if n_norm in roster_names:
            return n_norm
        return n_norm

    weekly["name"] = weekly["employee"].map(to_canonical)
    weekly = weekly.drop(columns=["employee"])

    # Left-join: include all roster employees (employees with 0 hours still appear)
    merged = roster.merge(weekly, how="left", on="name")
    for k in ["REG","OT","DT"]:
        if k not in merged.columns:
            merged[k] = 0.0
        merged[k] = merged[k].fillna(0.0).astype(float)

    # stable order similar to WBS sheets
    merged = merged.sort_values(["department","name"], kind="mergesort")

    # write into your actual template in repo root
    template_path = app_dir.parent / "wbs_template.xlsx"
    if not template_path.exists():
        raise ValueError(f"WBS template not found at {template_path}")

    rows = []
    for _, x in merged.iterrows():
        rows.append({
            "name": x["name"],
            "ssn": x.get("ssn", ""),
            "department": x.get("department", ""),
            "type": x.get("type", ""),
            "rate": _num(x.get("rate", 0.0)),
            "REG": _num(x.get("REG", 0.0)),
            "OT":  _num(x.get("OT", 0.0)),
            "DT":  _num(x.get("DT", 0.0)),
            # piece fields supported if you add them later to roster/weekly
            "P_Q": _num(x.get("P_Q", 0.0)) if "P_Q" in x else 0.0,
            "P_S": _num(x.get("P_S", 0.0)) if "P_S" in x else 0.0,
            "P_U": _num(x.get("P_U", 0.0)) if "P_U" in x else 0.0,
            "P_W": _num(x.get("P_W", 0.0)) if "P_W" in x else 0.0,
            "P_Y": _num(x.get("P_Y", 0.0)) if "P_Y" in x else 0.0,
            "P_Z": _num(x.get("P_Z", 0.0)) if "P_Z" in x else 0.0,
        })

    return _write_wbs(template_path.read_bytes(), rows)

# ──────────────────────────────────────────────────────────────────────────────
# Routes
# ──────────────────────────────────────────────────────────────────────────────
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
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")

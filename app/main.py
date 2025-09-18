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

app = FastAPI(title="Sierra → WBS Payroll Converter", version="2.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ALLOWED_EXTS = (".xlsx", ".xls")

def _ext_ok(filename: str) -> bool:
    name = (filename or "").lower()
    return any(name.endswith(e) for e in ALLOWED_EXTS)

def _std_col(s: str) -> str:
    return (s or "").strip().lower().replace("\n", " ").replace("\r", " ")

def _find_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = { _std_col(c): c for c in df.columns }
    for want in candidates:
        key = _std_col(want)
        if key in cols: return cols[key]
    for want in candidates:
        key = _std_col(want)
        for k, v in cols.items():
            if key in k: return v
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
    name = " ".join(raw.replace(",", " ").split())
    parts = name.split()
    if len(parts) == 2:
        return f"{parts[1]}, {parts[0]}"
    if "," in name:
        return name
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
    ot  = min(max(h - 8.0, 0.0), 4.0)
    dt  = max(h - 12.0, 0.0)
    return {"REG": reg, "OT": ot, "DT": dt}

def _load_alias_map(here: Path) -> Dict[str, str]:
    alias = {}
    csv_path = here.parent / "roster.csv"      # optional file: alias,canonical
    if not csv_path.exists():
        return alias
    try:
        import csv
        with open(csv_path, newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                a = (row.get("alias") or "").strip()
                c = (row.get("canonical") or "").strip()
                if a and c:
                    alias[_normalize_name(a)] = c
    except Exception:
        pass
    return alias

def _load_roster(here: Path) -> pd.DataFrame:
    xlsx_path = here.parent / "roster.xlsx"     # authoritative identity
    if not xlsx_path.exists():
        alt = here.parent / "roster-1.xlsx"
        if alt.exists():
            xlsx_path = alt
        else:
            raise ValueError("Roster file not found. Expected 'roster.xlsx' in repo root.")
    roster = pd.read_excel(xlsx_path, sheet_name=0)
    mapping = {
        "name": ["name", "employee", "employee name", "worker"],
        "ssn": ["ssn", "social", "social security", "social security number"],
        "department": ["department", "dept", "division"],
        "type": ["type", "employee type", "emp type", "pay type"],
        "rate": ["rate", "pay rate", "hourly rate", "wage", "base rate"],
    }
    cols = _require_columns(roster, mapping)
    out = pd.DataFrame({
        "name": roster[cols["name"]].astype(str).map(_normalize_name),
        "ssn": roster[cols["ssn"]].astype(str).fillna(""),
        "department": roster[cols["department"]].astype(str).fillna("").str.upper(),
        "type": roster[cols["type"]].astype(str).fillna("").str.upper(),
        "rate": pd.to_numeric(roster[cols["rate"]], errors="coerce").fillna(0.0).astype(float),
    })
    return out

def _aggregate_sierra(input_bytes: bytes, sheet_name: Optional[str]) -> pd.DataFrame:
    excel = pd.ExcelFile(io.BytesIO(input_bytes))
    target_sheet = sheet_name or excel.sheet_names[0]
    df = excel.parse(target_sheet)
    required = {
        "employee": ["employee", "employee name", "name", "worker", "employee_name"],
        "date": ["date", "work date", "day", "worked date"],
        "hours": ["hours", "hrs", "total hours", "work hours"],
    }
    cols = _require_columns(df, required)
    core = df[[cols["employee"], cols["date"], cols["hours"]]].copy()
    core.columns = ["employee", "date", "hours"]
    core["employee"] = core["employee"].astype(str)
    core["date"] = core["date"].map(_to_date)
    core["hours"] = pd.to_numeric(core["hours"], errors="coerce").fillna(0.0).astype(float)
    core = core[(core["employee"].str.len() > 0) & core["date"].notna() & (core["hours"] > 0)]
    by_day = core.groupby(["employee", "date"], dropna=False)["hours"].sum().reset_index()
    rows = []
    for _, r in by_day.iterrows():
        d = _apply_ca_daily_ot(float(r["hours"]))
        rows.append({"employee": r["employee"], "REG": d["REG"], "OT": d["OT"], "DT": d["DT"]})
    split = pd.DataFrame(rows)
    weekly = split.groupby("employee", dropna=False)[["REG", "OT", "DT"]].sum().reset_index()
    weekly["employee"] = weekly["employee"].map(_normalize_name)
    return weekly

# Column map for your WBS template
COL = {
    "STATUS": 1, "TYPE": 2, "NAME": 3, "SSN": 4, "DEPT": 5, "RATE": 6,
    "A01": 8, "A02": 9, "A03": 10, "A04": 11, "A05": 12,           # H..L
    "P_Q": 17, "P_S": 19, "P_U": 21, "P_W": 23, "P_Y": 25, "P_Z": 26,  # Q,S,U,W,Y,Z
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
    for r in range(start_row, max(start_row, end_row) + 1):
        # quick check if row has any values (ignoring merged)
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

    # Ensure the tiny header marker in A8 is sane (avoid accidental text in the first column)
    try:
        if ws.cell(row=8, column=1).value not in (None, "#", "# "):
            ws.cell(row=8, column=1).value = "#"
    except Exception:
        pass

    totals_row_orig = _find_totals_row(ws)
    if totals_row_orig is None:
        totals_row_orig = ws.max_row + 1
        ws.cell(row=totals_row_orig, column=COL["NAME"]).value = "Totals"

    # Clear previous data rows but keep styles/merged cells intact
    _clear_data_area(ws, COL["TOTALS"], WBS_DATA_START_ROW, totals_row_orig - 1)

    # Write employees
    r = WBS_DATA_START_ROW
    for emp in rows:
        ws.cell(row=r, column=COL["STATUS"]).value = "A"
        ws.cell(row=r, column=COL["TYPE"]).value = "S" if str(emp.get("type","")).upper().startswith("S") else "H"
        ws.cell(row=r, column=COL["NAME"]).value = emp["name"]
        ws.cell(row=r, column=COL["SSN"]).value = emp.get("ssn","")
        ws.cell(row=r, column=COL["DEPT"]).value = str(emp.get("department","")).upper()
        ws.cell(row=r, column=COL["RATE"]).value = round(float(emp.get("rate",0.0) or 0.0), 2)

        ws.cell(row=r, column=COL["A01"]).value = round(float(emp.get("REG",0.0)), 2)
        ws.cell(row=r, column=COL["A02"]).value = round(float(emp.get("OT",0.0)), 2)
        ws.cell(row=r, column=COL["A03"]).value = round(float(emp.get("DT",0.0)), 2)
        ws.cell(row=r, column=COL["A04"]).value = 0.0
        ws.cell(row=r, column=COL["A05"]).value = 0.0

        for k in ["P_Q","P_S","P_U","P_W","P_Y","P_Z"]:
            ws.cell(row=r, column=COL[k]).value = 0.0

        r += 1

    # Place Totals directly under last data row and rewire SUM(AB9:AB{last})
    totals_row_new = r
    ws.cell(row=totals_row_new, column=COL["NAME"]).value = "Totals"
    col_letter = get_column_letter(COL["TOTALS"])
    ws.cell(row=totals_row_new, column=COL["TOTALS"]).value = f"=SUM({col_letter}{WBS_DATA_START_ROW}:{col_letter}{r-1})"

    if totals_row_orig != totals_row_new:
        _clear_data_area(ws, COL["TOTALS"], totals_row_orig, totals_row_orig)

    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)
    return bio.read()

def _aggregate(input_bytes: bytes, sheet_name: Optional[str]) -> pd.DataFrame:
    return _aggregate_sierra(input_bytes, sheet_name)

def convert_sierra_to_wbs(input_bytes: bytes, sheet_name: Optional[str] = None) -> bytes:
    here = Path(__file__).resolve().parent
    roster = _load_roster(here)
    alias_map = _load_alias_map(here)

    weekly = _aggregate(input_bytes, sheet_name)

    roster_names = set(roster["name"].tolist())

    def to_canonical(n: str) -> str:
        n_norm = _normalize_name(n)
        if n_norm in alias_map:
            return alias_map[n_norm]
        if n_norm in roster_names:
            return n_norm
        return n_norm

    weekly["name"] = weekly["employee"].map(to_canonical)
    weekly = weekly.drop(columns=["employee"])

    merged = roster.merge(weekly, how="left", on="name")
    for k in ["REG","OT","DT"]:
        if k not in merged.columns:
            merged[k] = 0.0
        merged[k] = merged[k].fillna(0.0).astype(float)

    merged = merged.sort_values(["department","name"], kind="mergesort")

    template_path = here.parent / "wbs_template.xlsx"
    if not template_path.exists():
        raise ValueError(f"WBS template not found at {template_path}")

    rows = []
    for _, x in merged.iterrows():
        rows.append({
            "name": x["name"],
            "ssn": x.get("ssn",""),
            "department": x.get("department",""),
            "type": x.get("type",""),
            "rate": float(x.get("rate",0.0) or 0.0),
            "REG": float(x.get("REG",0.0) or 0.0),
            "OT":  float(x.get("OT",0.0) or 0.0),
            "DT":  float(x.get("DT",0.0) or 0.0),
        })

    return _write_wbs(template_path.read_bytes(), rows)

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

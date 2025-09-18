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
# App + CORS
# ──────────────────────────────────────────────────────────────────────────────
app = FastAPI(title="Sierra → WBS Payroll Converter", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],         # tighten to your Netlify domain if desired
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
    # “First Last” -> “Last, First” (only if exactly 2 parts and no comma already)
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
    """CA daily OT: 0–8 REG, 8–12 OT, >12 DT."""
    h = float(h or 0.0)
    reg = min(h, 8.0)
    ot = min(max(h - 8.0, 0.0), 4.0)
    dt = max(h - 12.0, 0.0)
    return {"REG": reg, "OT": ot, "DT": dt}

def _money(x) -> float:
    try:
        return float(x or 0.0)
    except Exception:
        return 0.0

# ──────────────────────────────────────────────────────────────────────────────
# Roster & alias loading (from repo root)
# ──────────────────────────────────────────────────────────────────────────────
def _load_alias_map(app_dir: Path) -> Dict[str, str]:
    """Optional alias→canonical mapping from repo root 'roster.csv' (alias,canonical)."""
    alias = {}
    csv_path = app_dir.parent / "roster.csv"
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

def _load_roster(app_dir: Path) -> pd.DataFrame:
    """
    Load roster.xlsx (or roster-1.xlsx) from repo root.
    Required columns (case-insensitive, fuzzy match): Name, SSN, Department, Type, Rate.
    """
    xlsx_path = app_dir.parent / "roster.xlsx"
    if not xlsx_path.exists():
        alt = app_dir.parent / "roster-1.xlsx"
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
    cols = _require(roster, mapping)
    out = pd.DataFrame({
        "name": roster[cols["name"]].astype(str).map(_normalize_name),
        "ssn": (roster[cols["ssn"]].astype(str).fillna("")),
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

    # Sum per employee/day then apply CA daily OT split; then weekly per-employee sum
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
# WBS writing (to template)
# ──────────────────────────────────────────────────────────────────────────────
# Column map for your WBS template sheet:
# A  Status(1)  B Type(2)  C Name(3)  D SSN(4)  E Dept(5)  F Rate(6)
# H..L are hour buckets A01..A05 (8..12); Q,S,U,W,Y,Z are piece $ buckets (17,19,21,23,25,26)
# AB is Totals (28)
COL = {
    "STATUS": 1, "TYPE": 2, "NAME": 3, "SSN": 4, "DEPT": 5, "RATE": 6,
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
    Clear values from cells in [start_row..end_row] x [1..last_col] without touching merged cells.
    Prevents 'MergedCell value is read-only' exceptions.
    """
    if end_row < start_row:
        return
    for r in range(start_row, end_row + 1):
        # quick pass to skip empty rows
        any_val = False
        for c in range(1, last_col + 1):
            cell = ws.cell(row=r, column=c)
            if not isinstance(cell, MergedCell) and cell.value not in (None, ""):
                any_val = True
                break
        if not any_val:
            continue

        for c in range(1, last_col + 1):
            cell = ws.cell(row=r, column=c)
            if isinstance(cell, MergedCell):
                continue
            cell.value = None

def _write_wbs(template_bytes: bytes, rows: List[Dict]) -> bytes:
    wb = load_workbook(io.BytesIO(template_bytes), data_only=False)
    ws = wb.active

    # Normalize the little header cell at A8 if template text drifted
    try:
        if ws.cell(row=8, column=1).value not in (None, "#", "# "):
            ws.cell(row=8, column=1).value = "#"
    except Exception:
        pass

    totals_row_orig = _find_totals_row(ws)
    if totals_row_orig is None:
        totals_row_orig = ws.max_row + 1
        ws.cell(row=totals_row_orig, column=COL["NAME"]).value = "Totals"

    # Clear prior data rows only (keep formats/merges)
    _clear_data_area(ws, COL["TOTALS"], WBS_DATA_START_ROW, totals_row_orig - 1)

    # Write employees
    r = WBS_DATA_START_ROW
    for emp in rows:
        rate = _money(emp.get("rate", 0.0))

        ws.cell(row=r, column=COL["STATUS"]).value = "A"
        ws.cell(row=r, column=COL["TYPE"]).value = "S" if str(emp.get("type","")).upper().startswith("S") else "H"
        ws.cell(row=r, column=COL["NAME"]).value = emp["name"]
        ws.cell(row=r, column=COL["SSN"]).value = emp.get("ssn", "")
        ws.cell(row=r, column=COL["DEPT"]).value = str(emp.get("department", "")).upper()
        ws.cell(row=r, column=COL["RATE"]).value = round(rate, 2)

        # Hours
        reg = _money(emp.get("REG", 0.0))
        ot  = _money(emp.get("OT",  0.0))
        dt  = _money(emp.get("DT",  0.0))

        ws.cell(row=r, column=COL["A01"]).value = round(reg, 2)
        ws.cell(row=r, column=COL["A02"]).value = round(ot,  2)
        ws.cell(row=r, column=COL["A03"]).value = round(dt,  2)
        ws.cell(row=r, column=COL["A04"]).value = 0.0  # keep 0 unless you populate VAC
        ws.cell(row=r, column=COL["A05"]).value = 0.0  # keep 0 unless you populate SICK/HOL

        # Piece buckets default to 0 unless provided
        piece_total = 0.0
        for k in ["P_Q", "P_S", "P_U", "P_W", "P_Y", "P_Z"]:
            val = _money(emp.get(k, 0.0))
            ws.cell(row=r, column=COL[k]).value = round(val, 2)
            piece_total += val

        # Pink Totals column (numeric value; no fragile per-row formula)
        hourly_total = (reg * rate) + (ot * rate * 1.5) + (dt * rate * 2.0)
        row_total = hourly_total + piece_total
        ws.cell(row=r, column=COL["TOTALS"]).value = round(row_total, 2)

        r += 1

    # Place Totals row right under last employee and set simple SUM over column AB
    totals_row_new = r
    ws.cell(row=totals_row_new, column=COL["NAME"]).value = "Totals"

    col_letter = get_column_letter(COL["TOTALS"])  # AB
    ws.cell(row=totals_row_new, column=COL["TOTALS"]).value = (
        f"=SUM({col_letter}{WBS_DATA_START_ROW}:{col_letter}{r-1})"
    )

    # If the template had a previous totals row elsewhere, blank that single row’s values
    if totals_row_orig != totals_row_new:
        _clear_data_area(ws, COL["TOTALS"], totals_row_orig, totals_row_orig)

    out = io.BytesIO()
    wb.save(out)
    out.seek(0)
    return out.read()

# ──────────────────────────────────────────────────────────────────────────────
# Conversion orchestration
# ──────────────────────────────────────────────────────────────────────────────
def convert_sierra_to_wbs(input_bytes: bytes, sheet_name: Optional[str] = None) -> bytes:
    app_dir = Path(__file__).resolve().parent

    # 1) Load roster + aliases
    roster = _load_roster(app_dir)
    alias_map = _load_alias_map(app_dir)
    roster_names = set(roster["name"].tolist())

    # 2) Sierra weekly REG/OT/DT
    weekly = _aggregate_sierra(input_bytes, sheet_name=sheet_name)

    # 3) Canonicalize Sierra names to roster names
    def to_canonical(n: str) -> str:
        n_norm = _normalize_name(n)
        if n_norm in alias_map:
            return alias_map[n_norm]
        if n_norm in roster_names:
            return n_norm
        return n_norm

    weekly["name"] = weekly["employee"].map(to_canonical)
    weekly = weekly.drop(columns=["employee"])

    # 4) Merge: keep ALL roster employees; overlay weekly hours (0 if missing)
    merged = roster.merge(weekly, how="left", on="name")
    for k in ["REG", "OT", "DT"]:
        if k not in merged.columns:
            merged[k] = 0.0
        merged[k] = merged[k].fillna(0.0).astype(float)

    # 5) Sort (Dept, Name) for stable, WBS-like listing
    merged = merged.sort_values(["department", "name"], kind="mergesort")

    # 6) Load WBS template from repo root and write rows
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
            "rate": _money(x.get("rate", 0.0)),
            "REG": _money(x.get("REG", 0.0)),
            "OT":  _money(x.get("OT", 0.0)),
            "DT":  _money(x.get("DT", 0.0)),
            # piece buckets supported if you add them later:
            "P_Q": _money(x.get("P_Q", 0.0)) if "P_Q" in x else 0.0,
            "P_S": _money(x.get("P_S", 0.0)) if "P_S" in x else 0.0,
            "P_U": _money(x.get("P_U", 0.0)) if "P_U" in x else 0.0,
            "P_W": _money(x.get("P_W", 0.0)) if "P_W" in x else 0.0,
            "P_Y": _money(x.get("P_Y", 0.0)) if "P_Y" in x else 0.0,
            "P_Z": _money(x.get("P_Z", 0.0)) if "P_Z" in x else 0.0,
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
            headers={"Content-Disposition": f'attachment; filename=\"{out_name}\"'}
        )
    except ValueError as ve:
        raise HTTPException(status_code=422, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")

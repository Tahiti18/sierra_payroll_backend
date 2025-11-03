# app/main.py
from __future__ import annotations

import io
import re
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet

app = FastAPI(title="Sierra → WBS Converter", version="1.0.0")

# CORS – allow your Netlify frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # lock this down later
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- Helpers ----------

ROOT = Path(__file__).resolve().parent.parent  # repo root (.. from app/)
TEMPLATE_PATH = ROOT / "wbs_template.xlsx"
GOLD_MASTER_ORDER_PATH = ROOT / "gold_master_order.txt"

WBS_REQUIRED_HEADERS = {
    "SSN": ("SSN",),
    "EMP_NAME": ("Employee Name", "Employee"),
    "STATUS": ("Status",),
    "TYPE": ("Type", "Pay Type"),
    "PAY_RATE": ("Pay Rate", "Rate"),
    "DEPT": ("Dept", "Department"),
    "REG": ("REG", "A01", "REGULAR"),
    "OT": ("OT", "A02", "OVERTIME"),
    "DT": ("DT", "A03", "DOUBLETIME"),
    "VAC": ("VACATION", "A06"),
    "SICK": ("SICK", "A07"),
    "HOL": ("HOLIDAY", "A08"),
    "TOTALS": ("TOTALS", "Totals"),
}

NUMERIC_DEFAULTS = {
    "REG": 0.0, "OT": 0.0, "DT": 0.0, "VAC": 0.0, "SICK": 0.0, "HOL": 0.0
}

def normalize_name(name: str) -> str:
    if not isinstance(name, str):
        return ""
    # Collapse spaces, remove stray commas at ends, title case common format
    n = re.sub(r"\s+", " ", name).strip()
    return n

def read_gold_master_order() -> Optional[List[str]]:
    if GOLD_MASTER_ORDER_PATH.exists():
        names = [normalize_name(x) for x in GOLD_MASTER_ORDER_PATH.read_text(encoding="utf-8").splitlines() if x.strip()]
        return names if names else None
    return None

# ---------- Input detection & parsing (Timesheet Stack) ----------

def detect_timesheet_stack(xls: bytes) -> bool:
    """Heuristic: first sheet has a header row with 'Days' or a date in col A and 'Hours' near the end."""
    try:
        df_head = pd.read_excel(io.BytesIO(xls), sheet_name=0, header=None, nrows=20)
    except Exception:
        return False
    # If any row has a date-like in col 0 and somewhere 'Hours' text in header row
    text_join = " ".join(str(x) for x in df_head.fillna("").astype(str).values.flatten()[:200]).lower()
    if "hours" in text_join:
        # very loose, but good enough for your sheet
        return True
    return False

def parse_timesheet_stack(xls: bytes) -> Tuple[Dict[str, float], Dict[str, Optional[float]]]:
    """
    Returns:
      hours_by_name: sum of REG hours per employee
      rate_by_name: latest non-null rate seen per employee
    Sheet columns (typical):
      0=Date, 1=Job#, 2=Name, 3=Start, 4=Lunch Start, 5=Lunch End, 6=Finish, 7=Hours, 8=Rate
    """
    # Read ALL sheets concatenated (the upload often has one sheet, but be safe)
    x = pd.ExcelFile(io.BytesIO(xls))
    frames = []
    for s in x.sheet_names:
        df = x.parse(s, header=None)
        frames.append(df)
    df_all = pd.concat(frames, ignore_index=True)

    # Try to find the "logical" columns by scanning header row containing 'Hours'
    # We search the first ~50 rows for a row where one cell is 'Hours' (case-insensitive)
    hours_col = None
    name_col = None
    rate_col = None

    for r in range(min(len(df_all), 50)):
        row = df_all.iloc[r].astype(str).str.strip().str.lower()
        if "hours" in set(row.values):
            # guess columns by typical positions
            # Name is usually column 2, Hours column where 'hours' appeared, rate often next column
            hours_col = row[row == "hours"].index[0]
            # Name: search the same row for "name", else assume column 2
            possible_name_idx = [i for i, v in enumerate(row.values) if v in ("name", "employee", "employee name")]
            name_col = possible_name_idx[0] if possible_name_idx else 2
            # Rate column: cell 'rate' on same row or hours_col+1
            possible_rate_idx = [i for i, v in enumerate(row.values) if v in ("rate", "pay rate")]
            rate_col = possible_rate_idx[0] if possible_rate_idx else (hours_col + 1 if hours_col is not None else 8)
            break

    # Fallback positions if header row not found
    if hours_col is None: hours_col = 7
    if name_col is None: name_col = 2
    if rate_col is None: rate_col = 8

    # Data rows are where Hours is numeric and Name is non-empty
    df_data = df_all.copy()
    # Coerce numeric
    df_data["HOURS"] = pd.to_numeric(df_data.iloc[:, hours_col], errors="coerce")
    df_data["RATE"] = pd.to_numeric(df_data.iloc[:, rate_col], errors="coerce")
    df_data["NAME"] = df_data.iloc[:, name_col].astype(str).map(normalize_name)

    df_data = df_data[(df_data["HOURS"].notna()) & (df_data["HOURS"] > 0) & (df_data["NAME"] != "")]
    if df_data.empty:
        raise HTTPException(status_code=422, detail="Could not locate any timesheet rows with names and hours.")

    # Aggregate hours & pick last non-null rate per employee
    hours_by_name = df_data.groupby("NAME")["HOURS"].sum().to_dict()
    # Last non-null rate per employee (by appearance)
    rate_by_name: Dict[str, Optional[float]] = {}
    for _, row in df_data[df_data["RATE"].notna()][["NAME", "RATE"]].iterrows():
        rate_by_name[row["NAME"]] = float(row["RATE"])

    return hours_by_name, rate_by_name

# ---------- Template scanning & writing ----------

def find_header_row_and_cols(ws: Worksheet) -> Tuple[int, Dict[str, int]]:
    """
    Locate the row containing the data headers (e.g., 'SSN', 'Employee Name', 'REG', 'TOTALS'...),
    then build a column map.
    """
    # search first 80 rows for the header that contains "Employee Name"
    header_row = None
    for r in range(1, min(80, ws.max_row) + 1):
        labels = [str(ws.cell(r, c).value).strip() if ws.cell(r, c).value is not None else "" for c in range(1, ws.max_column + 1)]
        joined = " | ".join(labels).lower()
        if "employee name" in joined or "employee" in joined and "ssn" in joined:
            header_row = r
            break
    if header_row is None:
        raise HTTPException(status_code=500, detail="Could not find header row in WBS template (looked for 'Employee Name').")

    # Build column map by matching against required header keywords
    col_map: Dict[str, int] = {}
    for c in range(1, ws.max_column + 1):
        raw = ws.cell(header_row, c).value
        if raw is None:
            continue
        text = str(raw).strip().upper()
        for key, aliases in WBS_REQUIRED_HEADERS.items():
            if key in col_map:
                continue
            for al in aliases:
                if al.upper() == text:
                    col_map[key] = c
                    break

    # Minimal columns we truly must have
    for k in ("EMP_NAME", "REG", "TOTALS"):
        if k not in col_map:
            raise HTTPException(status_code=500, detail=f"Template missing required column: {k}")

    return header_row, col_map

def clear_old_data(ws: Worksheet, header_row: int, col_map: Dict[str, int]) -> None:
    """
    Clear previous data rows (values only) between data start and the row above the Totals
    without touching merged header cells.
    """
    data_start = header_row + 1
    emp_col = col_map["EMP_NAME"]
    totals_row_guess = ws.max_row

    # Try to locate a "Totals" label in the EMP_NAME column to stop earlier
    for r in range(ws.max_row, data_start, -1):
        val = ws.cell(r, emp_col).value
        if isinstance(val, str) and val.strip().lower().startswith("totals"):
            totals_row_guess = r
            break

    # Define the columns we will clear (safe numeric/text columns)
    data_cols = sorted(set(col_map.values()))
    for r in range(data_start, totals_row_guess):
        # if the entire row is empty already, skip
        if all((ws.cell(r, c).value in (None, "")) for c in data_cols):
            continue
        for c in data_cols:
            cell = ws.cell(r, c)
            # Only clear if not part of a merged range or the top-left of that range
            is_merged = False
            for mr in ws.merged_cells.ranges:
                if (mr.min_row <= r <= mr.max_row) and (mr.min_col <= c <= mr.max_col):
                    is_merged = True
                    if not (r == mr.min_row and c == mr.min_col):
                        # skip non-master merged cells
                        pass
                    else:
                        cell.value = None
                    break
            if not is_merged:
                cell.value = None

def build_output_rows(hours_by_name: Dict[str, float],
                      rate_by_name: Dict[str, Optional[float]],
                      master_order: Optional[List[str]]) -> List[Dict[str, object]]:
    # sort by master order if provided
    names = list(hours_by_name.keys())
    if master_order:
        order_index = {normalize_name(n): i for i, n in enumerate(master_order)}
        names.sort(key=lambda n: (order_index.get(normalize_name(n), 10_000), normalize_name(n)))
    else:
        names.sort(key=lambda n: normalize_name(n))

    rows = []
    for name in names:
        row = {
            "SSN": "",  # unknown yet
            "EMP_NAME": name,
            "STATUS": "",  # unknown
            "TYPE": "",    # unknown
            "PAY_RATE": rate_by_name.get(name),
            "DEPT": "",
            "REG": float(hours_by_name.get(name, 0.0)),
            "OT": 0.0,
            "DT": 0.0,
            "VAC": 0.0,
            "SICK": 0.0,
            "HOL": 0.0,
        }
        rows.append(row)
    return rows

def write_to_template(rows: List[Dict[str, object]]) -> bytes:
    if not TEMPLATE_PATH.exists():
        raise HTTPException(status_code=500, detail=f"WBS template not found at {TEMPLATE_PATH}")

    wb = load_workbook(str(TEMPLATE_PATH))
    ws = wb.active

    header_row, col_map = find_header_row_and_cols(ws)
    clear_old_data(ws, header_row, col_map)

    data_start = header_row + 1
    r = data_start

    # Write rows – leave TOTALS column untouched (template formulas)
    for item in rows:
        for key, default in NUMERIC_DEFAULTS.items():
            if key not in item or item[key] is None:
                item[key] = default

        def _set(col_key: str, value):
            if col_key not in col_map:
                return
            c = col_map[col_key]
            cell = ws.cell(r, c)
            # avoid writing into merged non-master cells
            for mr in ws.merged_cells.ranges:
                if mr.min_row <= r <= mr.max_row and mr.min_col <= c <= mr.max_col:
                    if not (r == mr.min_row and c == mr.min_col):
                        return
                    break
            cell.value = value

        _set("SSN", item.get("SSN"))
        _set("EMP_NAME", item.get("EMP_NAME"))
        _set("STATUS", item.get("STATUS"))
        _set("TYPE", item.get("TYPE"))
        _set("PAY_RATE", item.get("PAY_RATE"))
        _set("DEPT", item.get("DEPT"))
        _set("REG", item.get("REG"))
        _set("OT", item.get("OT"))
        _set("DT", item.get("DT"))
        _set("VAC", item.get("VAC"))
        _set("SICK", item.get("SICK"))
        _set("HOL", item.get("HOL"))
        # DO NOT touch TOTALS – keep template formula
        r += 1

    out = io.BytesIO()
    wb.save(out)
    out.seek(0)
    return out.read()

# ---------- API ----------

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/process-payroll")
async def process_payroll(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        if not contents:
            raise HTTPException(status_code=422, detail="Empty file.")

        if not detect_timesheet_stack(contents):
            raise HTTPException(
                status_code=422,
                detail="File format error - expected Sierra timesheet stack (with 'Hours' column).",
            )

        hours_by_name, rate_by_name = parse_timesheet_stack(contents)
        master_order = read_gold_master_order()
        rows = build_output_rows(hours_by_name, rate_by_name, master_order)
        output_bytes = write_to_template(rows)

        filename = "WBS_Payroll_Output.xlsx"
        return StreamingResponse(
            io.BytesIO(output_bytes),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'}
        )

    except HTTPException:
        raise
    except Exception as e:
        # Bubble a clean 500 to the UI with a short message
        raise HTTPException(status_code=500, detail=f"backend processing failed: {e}")

# ---------- Local dev ----------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8080, reload=False)

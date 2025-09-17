# server/main.py
import io
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple

import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import StreamingResponse, JSONResponse
from starlette.background import BackgroundTask
from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet
from openpyxl.cell.cell import Cell
from openpyxl.utils import get_column_letter
import os

app = FastAPI(title="Sierra → WBS Payroll Converter", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # tighten to your Netlify domain if desired
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ALLOWED_EXTS = (".xlsx", ".xls")

# ----------------- helpers (header matching only; never touch numeric values) -----------------
def _ext_ok(name: str) -> bool:
    n = (name or "").lower()
    return any(n.endswith(e) for e in ALLOWED_EXTS)

def _std(s: str) -> str:
    return (s or "").strip().lower().replace("\n"," ").replace("\r"," ")

def _find_col(df: pd.DataFrame, options: List[str]) -> Optional[str]:
    norm = { _std(c): c for c in df.columns }
    for want in options:
        w = _std(want)
        if w in norm: return norm[w]
    for want in options:
        w = _std(want)
        for k,c in norm.items():
            if w in k: return c
    return None

def _require(df: pd.DataFrame, mapping: Dict[str, List[str]]) -> Dict[str, str]:
    got, miss = {}, []
    for key, opts in mapping.items():
        c = _find_col(df, opts)
        if not c: miss.append(f"{key} (any of: {', '.join(opts)})")
        else: got[key] = c
    if miss: raise ValueError("Missing required columns: " + "; ".join(miss))
    return got

def _normalize_name(raw: str) -> str:
    if not raw or not isinstance(raw, str): return ""
    parts = [p for p in raw.strip().split() if p]
    if len(parts)==2: return f"{parts[1]}, {parts[0]}"
    return raw.strip()

def _to_date(v) -> Optional[date]:
    if pd.isna(v): return None
    if isinstance(v, datetime): return v.date()
    try: return pd.to_datetime(v).date()
    except: return None

def _ca_daily_ot(h: float) -> Dict[str, float]:
    h = float(h or 0.0)
    reg = min(h, 8.0)
    ot  = min(max(h-8.0, 0.0), 4.0)
    dt  = max(h-12.0, 0.0)
    return {"REG": reg, "OT": ot, "DT": dt}

def _money(x: float) -> float:
    return float(x or 0.0)

# ----------------- WBS template utilities -----------------
def _load_template_bytes(uploaded_template: Optional[bytes]) -> bytes:
    if uploaded_template:
        return uploaded_template
    # try server-side file
    path = os.getenv("WBS_TEMPLATE_PATH", "wbs_template.xlsx")
    if not os.path.exists(path):
        raise HTTPException(422, detail="WBS template missing. Upload 'template' with the request or place 'wbs_template.xlsx' in the app root (or set WBS_TEMPLATE_PATH).")
    with open(path, "rb") as f:
        return f.read()

def _find_wbs_header(ws: Worksheet) -> Tuple[int, Dict[str, int]]:
    """
    Find the row that contains WBS headings, e.g. ['SSN','Employee Name','Status','Type','Pay Rate','Dept','A01','A02','A03', ...]
    Return (header_row_index, col_map)
    """
    targets = ["ssn","employee name","status","type","pay rate","dept","a01","a02","a03"]
    for r in range(1, ws.max_row+1):
        row_vals = [ws.cell(r,c).value for c in range(1, ws.max_column+1)]
        lower = [(_std(str(v)) if v is not None else "") for v in row_vals]
        if all(any(t == lv for lv in lower) for t in targets):
            col_map = {}
            for c, lv in enumerate(lower, start=1):
                col_map[lv] = c
            # normalize keys we care about
            keymap = {
                "ssn": col_map.get("ssn"),
                "name": col_map.get("employee name"),
                "status": col_map.get("status"),
                "type": col_map.get("type"),
                "rate": col_map.get("pay rate"),
                "dept": col_map.get("dept"),
                "a01": col_map.get("a01"),
                "a02": col_map.get("a02"),
                "a03": col_map.get("a03"),
            }
            # Column A in your template is not labeled (e.g. employee ID). Keep it if present:
            keymap["colA"] = 1  # preserve but we won't fill unless provided
            return r, keymap
    raise HTTPException(422, detail="Could not locate WBS header row in template (expecting columns like 'SSN', 'Employee Name', 'A01', 'A02', 'A03').")

def _capture_row_style(ws: Worksheet, row_idx: int, max_col: int) -> List[Cell]:
    """Capture style from a single existing data row so we can clone it onto new rows."""
    template_cells = []
    for c in range(1, max_col+1):
        template_cells.append(ws.cell(row_idx, c))
    return template_cells

def _apply_style(dst: Cell, src: Cell):
    dst.font = src.font
    dst.border = src.border
    dst.fill = src.fill
    dst.number_format = src.number_format
    dst.protection = src.protection
    dst.alignment = src.alignment

# ----------------- Core conversion -----------------
def build_weekly_from_sierra(xlsx_bytes: bytes, sheet_name: Optional[str]=None) -> pd.DataFrame:
    excel = pd.ExcelFile(io.BytesIO(xlsx_bytes))
    sheet = sheet_name or excel.sheet_names[0]
    df = excel.parse(sheet)

    required = {
        "employee": ["employee","employee name","name","worker","employee_name"],
        "date": ["date","work date","day","worked date"],
        "hours": ["hours","hrs","total hours","work hours"],
        "rate": ["rate","pay rate","hourly rate","wage"],
    }
    optional = {
        "department": ["department","dept","division"],
        "ssn": ["ssn","social","social security","social security number"],
        "wtype": ["type","employee type","emp type","pay type"],
        "task": ["task","earn type","earning","code"],
    }

    got = _require(df, required)
    core = df[[got["employee"], got["date"], got["hours"], got["rate"]]].copy()
    core.columns = ["employee","date","hours","rate"]
    core["department"] = df[_find_col(df, optional["department"])] if _find_col(df, optional["department"]) else ""
    core["ssn"]        = df[_find_col(df, optional["ssn"])]        if _find_col(df, optional["ssn"])        else ""
    core["wtype"]      = df[_find_col(df, optional["wtype"])]      if _find_col(df, optional["wtype"])      else ""

    core["employee"] = core["employee"].astype(str).map(_normalize_name)
    core["date"]     = core["date"].map(_to_date)
    core["hours"]    = pd.to_numeric(core["hours"], errors="coerce").fillna(0.0).astype(float)
    core["rate"]     = pd.to_numeric(core["rate"], errors="coerce").fillna(0.0).astype(float)
    core = core[(core["employee"].str.len()>0) & core["date"].notna() & (core["hours"]>0)]

    day = core.groupby(["employee","date","rate"], dropna=False)["hours"].sum().reset_index()

    rows = []
    for _, r in day.iterrows():
        d = _ca_daily_ot(float(r["hours"]))
        rows.append({"employee": r["employee"], "rate": float(r["rate"]), "REG": d["REG"], "OT": d["OT"], "DT": d["DT"]})
    split = pd.DataFrame(rows)

    weekly = split.groupby(["employee","rate"], dropna=False)[["REG","OT","DT"]].sum().reset_index()

    # attach identity columns
    id_map = core.groupby("employee").agg({"department":"first","ssn":"first","wtype":"first"}).reset_index()
    out = pd.merge(weekly, id_map, on="employee", how="left")

    out["Status"] = "A"
    out["Type"] = out["wtype"].astype(str).str.upper().map(lambda x: "S" if x.startswith("S") else "H")

    # final order for writing
    return out[["employee","ssn","Status","Type","rate","department","REG","OT","DT"]].copy()

def write_into_wbs_template(template_bytes: bytes, weekly: pd.DataFrame) -> bytes:
    wb = load_workbook(io.BytesIO(template_bytes))
    # prefer WEEKLY, else detect
    ws = wb["WEEKLY"] if "WEEKLY" in wb.sheetnames else wb[wb.sheetnames[0]]
    header_row, cols = _find_wbs_header(ws)

    # Find first data row (header_row+1) and capture its style if it exists; otherwise use header_row as style base
    data_row_start = header_row + 1

    # Determine max column based on sheet current max or at least through A03 column
    max_col = ws.max_column

    # Capture style from the first existing data row if it looks like a data row; else from header (safer than nothing)
    style_src_row = data_row_start
    # If row is empty, fallback to header row for style
    if all(ws.cell(data_row_start, c).value in (None, "") for c in range(1, max_col+1)):
        style_src_row = header_row
    style_cells = _capture_row_style(ws, style_src_row, max_col)

    # Clear existing data rows below the header (until the first fully empty row block at the end)
    # Simple approach: delete rows from data_row_start to last non-empty row in the table block.
    # We detect the last used row by scanning Column B or C (SSN or Name).
    last = ws.max_row
    # scan downwards to find last row with any value in B or C
    last_data = data_row_start - 1
    for r in range(data_row_start, last+1):
        v_b = ws.cell(r, cols.get("ssn", 2)).value
        v_c = ws.cell(r, cols.get("name", 3)).value
        if (v_b not in (None, "")) or (v_c not in (None, "")):
            last_data = r
    if last_data >= data_row_start:
        ws.delete_rows(data_row_start, last_data - data_row_start + 1)

    # Write rows from weekly into the template with styles
    r_idx = data_row_start
    for _, row in weekly.iterrows():
        ws.insert_rows(r_idx, 1)  # make space
        # clone styles
        for c in range(1, max_col+1):
            _apply_style(ws.cell(r_idx, c), style_cells[c-1])

        # Fill columns exactly as in your template
        # Column A (unlabeled employee ID) is preserved but left blank unless you add mapping.
        if cols.get("ssn"):   ws.cell(r_idx, cols["ssn"],  row["ssn"] if pd.notna(row["ssn"]) else "")
        if cols.get("name"):  ws.cell(r_idx, cols["name"], row["employee"])
        if cols.get("status"):ws.cell(r_idx, cols["status"], row["Status"])
        if cols.get("type"):  ws.cell(r_idx, cols["type"], row["Type"])
        if cols.get("rate"):  ws.cell(r_idx, cols["rate"], round(_money(row["rate"]),2))
        if cols.get("dept"):  ws.cell(r_idx, cols["dept"], row["department"] if pd.notna(row["department"]) else "")
        if cols.get("a01"):   ws.cell(r_idx, cols["a01"], round(_money(row["REG"]),2))
        if cols.get("a02"):   ws.cell(r_idx, cols["a02"], round(_money(row["OT"]),2))
        if cols.get("a03"):   ws.cell(r_idx, cols["a03"], round(_money(row["DT"]),2))
        r_idx += 1

    # keep column widths and all header content untouched: we didn't modify those

    bio = io.BytesIO()
    wb.save(bio); bio.seek(0)
    return bio.read()

# ----------------- routes -----------------
@app.get("/health")
def health():
    return JSONResponse({"ok": True, "ts": datetime.utcnow().isoformat() + "Z"})

@app.post("/process-payroll")
async def process_payroll(
    file: UploadFile = File(..., description="Sierra payroll .xlsx"),
    template: UploadFile = File(None, description="(Optional) WBS template .xlsx to clone exactly")
):
    if not file or not file.filename:
        raise HTTPException(400, "No Sierra file provided.")
    if not _ext_ok(file.filename):
        raise HTTPException(415, "Unsupported Sierra file type. Use .xlsx or .xls")

    try:
        sierra_bytes = await file.read()
        weekly = build_weekly_from_sierra(sierra_bytes, sheet_name=None)
    except ValueError as ve:
        raise HTTPException(422, str(ve))
    except Exception as e:
        raise HTTPException(500, f"Sierra parse error: {e}")

    try:
        tmpl_bytes = await template.read() if template else None
        tbytes = _load_template_bytes(tmpl_bytes)
        out_bytes = write_into_wbs_template(tbytes, weekly)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Template processing error: {e}")

    out_name = f"WBS_Payroll_{datetime.utcnow().date().isoformat()}.xlsx"
    return StreamingResponse(
        io.BytesIO(out_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{out_name}"'}
    )

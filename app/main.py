# server/main.py
import io, os
from collections import Counter, defaultdict
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple

import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import StreamingResponse, JSONResponse
from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet

app = FastAPI(title="Sierra → WBS Payroll Converter", version="3.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten to your frontend domain if needed
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ALLOWED_EXTS = (".xlsx", ".xls")

# ----------------- utilities -----------------
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
    try:
        return pd.to_datetime(v).date()
    except Exception:
        return None

def _ca_daily_ot(h: float) -> Dict[str, float]:
    h = float(h or 0.0)
    reg = min(h, 8.0)
    ot  = min(max(h-8.0, 0.0), 4.0)
    dt  = max(h-12.0, 0.0)
    return {"REG": reg, "OT": ot, "DT": dt}

def _money(x: float) -> float:
    return float(x or 0.0)

# ----------------- build weekly per employee (ONE ROW EACH) -----------------
def build_weekly_from_sierra(xlsx_bytes: bytes, sheet_name: Optional[str]=None) -> pd.DataFrame:
    excel = pd.ExcelFile(io.BytesIO(xlsx_bytes))
    # Prefer first “timesheet-like” sheet if not specified
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
    }

    got = _require(df, required)
    core = df[[got["employee"], got["date"], got["hours"], got["rate"]]].copy()
    core.columns = ["employee","date","hours","rate"]

    # optional columns
    dep_col = _find_col(df, optional["department"])
    ssn_col = _find_col(df, optional["ssn"])
    typ_col = _find_col(df, optional["wtype"])
    core["department"] = df[dep_col] if dep_col else ""
    core["ssn"]        = df[ssn_col] if ssn_col else ""
    core["wtype"]      = df[typ_col] if typ_col else ""

    # normalize
    core["employee"] = core["employee"].astype(str).map(_normalize_name)
    core["date"]     = core["date"].map(_to_date)
    core["hours"]    = pd.to_numeric(core["hours"], errors="coerce").fillna(0.0).astype(float)
    core["rate"]     = pd.to_numeric(core["rate"], errors="coerce").fillna(0.0).astype(float)
    core = core[(core["employee"].str.len()>0) & core["date"].notna() & (core["hours"]>0)]

    if core.empty:
        raise ValueError("No valid rows found in Sierra sheet (need Employee, Date, Hours > 0, Rate).")

    # 1) Summarize hours per employee per day (across multiple line items that day)
    per_day = core.groupby(["employee","date"], dropna=False).agg({
        "hours":"sum"
    }).reset_index()

    # 2) Apply CA daily OT/DT on that daily total
    splits = []
    for _, r in per_day.iterrows():
        dist = _ca_daily_ot(float(r["hours"]))
        splits.append({
            "employee": r["employee"],
            "date": r["date"],
            "REG": dist["REG"],
            "OT": dist["OT"],
            "DT": dist["DT"],
        })
    split_df = pd.DataFrame(splits)

    # 3) Weekly totals per employee (ONE ROW PER PERSON)
    weekly_hours = split_df.groupby(["employee"], dropna=False)[["REG","OT","DT"]].sum().reset_index()

    # 4) Dollars: compute precisely from raw records (handles mixed rates)
    #    We recompute per-record dollars according to daily split proportions.
    #    Approach: For each employee-day, distribute their raw-day hours into REG/OT/DT proportionally,
    #    then apply the record's own rate to its share.
    dollars = defaultdict(lambda: {"REG_$":0.0,"OT_$":0.0,"DT_$":0.0})
    day_totals = { (r["employee"], r["date"]): r for _, r in split_df.iterrows() }

    for _, rec in core.iterrows():
        key = (rec["employee"], rec["date"])
        if key not in day_totals:
            # shouldn't happen, but skip safely
            continue
        tot_reg = day_totals[key]["REG"]
        tot_ot  = day_totals[key]["OT"]
        tot_dt  = day_totals[key]["DT"]
        day_sum = tot_reg + tot_ot + tot_dt
        if day_sum <= 0:
            continue

        # portion of this record's hours relative to the day's total hours
        portion = float(rec["hours"]) / day_sum

        reg_share = tot_reg * portion
        ot_share  = tot_ot  * portion
        dt_share  = tot_dt  * portion

        base = float(rec["rate"])
        dollars[rec["employee"]]["REG_$"] += reg_share * base
        dollars[rec["employee"]]["OT_$"]  += ot_share  * base * 1.5
        dollars[rec["employee"]]["DT_$"]  += dt_share  * base * 2.0

    # 5) Choose Pay Rate to show in WBS = most frequent rate used by the employee this week
    chosen_rate = {}
    first_dept  = {}
    first_ssn   = {}
    first_type  = {}
    for emp, group in core.groupby("employee"):
        rates = Counter([float(r) for r in group["rate"].tolist()])
        chosen_rate[emp] = max(rates.items(), key=lambda kv: kv[1])[0]  # most frequent
        first_dept[emp]  = group["department"].dropna().astype(str).replace("nan","").iloc[0] if not group.empty else ""
        first_ssn[emp]   = group["ssn"].dropna().astype(str).replace("nan","").iloc[0] if not group.empty else ""
        wtyp = str(group["wtype"].dropna().astype(str).replace("nan","").iloc[0] if not group.empty else "")
        first_type[emp]  = "S" if wtyp.upper().startswith("S") else "H"

    # 6) Merge everything into final weekly dataframe (one row per person)
    weekly = weekly_hours.copy()
    weekly["REG_$"]   = weekly["employee"].map(lambda e: round(_money(dollars[e]["REG_$"]), 2))
    weekly["OT_$"]    = weekly["employee"].map(lambda e: round(_money(dollars[e]["OT_$"]), 2))
    weekly["DT_$"]    = weekly["employee"].map(lambda e: round(_money(dollars[e]["DT_$"]), 2))
    weekly["TOTAL_$"] = weekly["REG_$"] + weekly["OT_$"] + weekly["DT_$"]

    weekly["rate"]       = weekly["employee"].map(lambda e: round(_money(chosen_rate.get(e, 0.0)), 2))
    weekly["department"] = weekly["employee"].map(lambda e: first_dept.get(e, ""))
    weekly["ssn"]        = weekly["employee"].map(lambda e: first_ssn.get(e, ""))
    weekly["Status"]     = "A"
    weekly["Type"]       = weekly["employee"].map(lambda e: first_type.get(e, "H"))

    # Final order for writing into WBS
    out = weekly[[
        "employee","ssn","Status","Type","rate","department","REG","OT","DT",
        "REG_$","OT_$","DT_$","TOTAL_$"
    ]].copy()

    # Round hours to 2 decimals (display), keep sums exact enough for payroll
    for c in ["REG","OT","DT"]:
        out[c] = out[c].map(lambda v: round(_money(v), 2))

    return out

# ----------------- template handling -----------------
def _load_template_from_disk() -> bytes:
    path = os.getenv("WBS_TEMPLATE_PATH", "wbs_template.xlsx")
    if not os.path.exists(path):
        raise HTTPException(422, detail="WBS template not found on server. Place 'wbs_template.xlsx' beside server/main.py (or set WBS_TEMPLATE_PATH).")
    with open(path, "rb") as f:
        return f.read()

def _find_wbs_header(ws: Worksheet) -> Tuple[int, Dict[str, int]]:
    targets = ["ssn","employee name","status","type","pay rate","dept","a01","a02","a03"]
    for r in range(1, ws.max_row+1):
        lower = [(_std(str(ws.cell(r,c).value)) if ws.cell(r,c).value is not None else "") for c in range(1, ws.max_column+1)]
        if all(any(t == lv for lv in lower) for t in targets):
            col_map = {}
            for c, lv in enumerate(lower, start=1):
                col_map[lv] = c
            return r, {
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
    raise HTTPException(422, detail="Could not locate WBS header row in template (expecting SSN, Employee Name, Status, Type, Pay Rate, Dept, A01/A02/A03).")

def write_into_wbs_template(template_bytes: bytes, weekly: pd.DataFrame) -> bytes:
    wb = load_workbook(io.BytesIO(template_bytes))
    ws = wb["WEEKLY"] if "WEEKLY" in wb.sheetnames else wb.active

    header_row, cols = _find_wbs_header(ws)
    first_data_row = header_row + 1

    # Clear existing data rows (keep formatting/widths)
    last = ws.max_row
    last_data = first_data_row - 1
    scan_col = cols.get("name") or cols.get("ssn") or 2
    for r in range(first_data_row, last+1):
        v = ws.cell(r, scan_col).value
        if v not in (None, ""):
            last_data = r
    if last_data >= first_data_row:
        ws.delete_rows(first_data_row, last_data - first_data_row + 1)

    # Append one row per employee
    for _, row in weekly.iterrows():
        values = [""] * max(ws.max_column, 12)
        if cols.get("ssn")   : values[cols["ssn"]-1]   = row["ssn"] if pd.notna(row["ssn"]) else ""
        if cols.get("name")  : values[cols["name"]-1]  = row["employee"]
        if cols.get("status"): values[cols["status"]-1]= row["Status"]
        if cols.get("type")  : values[cols["type"]-1]  = row["Type"]
        if cols.get("rate")  : values[cols["rate"]-1]  = row["rate"]
        if cols.get("dept")  : values[cols["dept"]-1]  = row["department"] if pd.notna(row["department"]) else ""
        if cols.get("a01")   : values[cols["a01"]-1]   = row["REG"]
        if cols.get("a02")   : values[cols["a02"]-1]   = row["OT"]
        if cols.get("a03")   : values[cols["a03"]-1]   = row["DT"]
        ws.append(values)

    bio = io.BytesIO()
    wb.save(bio); bio.seek(0)
    return bio.read()

# ----------------- routes -----------------
@app.get("/health")
def health():
    return JSONResponse({"ok": True, "ts": datetime.utcnow().isoformat() + "Z"})

@app.get("/template-status")
def template_status():
    try:
        _ = _load_template_from_disk()
        return JSONResponse({"template": "found"})
    except HTTPException as e:
        return JSONResponse({"template": "missing", "detail": e.detail}, status_code=422)

@app.post("/process-payroll")
async def process_payroll(file: UploadFile = File(..., description="Sierra payroll .xlsx")):
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
        tmpl_bytes = _load_template_from_disk()
        out_bytes = write_into_wbs_template(tmpl_bytes, weekly)
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

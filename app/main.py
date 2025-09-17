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

app = FastAPI(title="Sierra → WBS Payroll Converter", version="4.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten to your frontend domain if desired
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ALLOWED_EXTS = (".xlsx", ".xls")


# ----------------- basic helpers -----------------
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


# ----------------- persistent roster (permanent SSNs/rates/types/depts) -----------------
def _load_roster() -> Optional[pd.DataFrame]:
    """
    Looks for /roster.xlsx at repo root.
    Expected columns (case-insensitive, flexible headers):
      - name / employee / employee name
      - ssn
      - rate (optional default)
      - department / dept (optional)
      - type (H/S) (optional)
    """
    path = os.getenv("ROSTER_PATH", "roster.xlsx")
    if not os.path.exists(path):
        return None
    df = pd.read_excel(path)
    if df.empty:
        return None

    # map columns loosely
    name_col = _find_col(df, ["employee","employee name","name"])
    ssn_col  = _find_col(df, ["ssn","social","social security","social security number"])
    rate_col = _find_col(df, ["rate","pay rate","hourly rate","wage"])
    dept_col = _find_col(df, ["department","dept","division"])
    type_col = _find_col(df, ["type","employee type","emp type","pay type"])

    if not name_col or not ssn_col:
        return None

    r = pd.DataFrame({
        "employee": df[name_col].astype(str).map(_normalize_name),
        "ssn": df[ssn_col].astype(str),
        "rate": pd.to_numeric(df[rate_col], errors="coerce") if rate_col else pd.Series([None]*len(df)),
        "department": df[dept_col].astype(str) if dept_col else pd.Series([""]*len(df)),
        "wtype": df[type_col].astype(str) if type_col else pd.Series([""]*len(df)),
    })
    # de-dup by last occurrence (latest wins)
    r = r.dropna(subset=["employee"]).drop_duplicates(subset=["employee"], keep="last")
    return r

def _apply_roster_defaults(weekly_df: pd.DataFrame, roster: Optional[pd.DataFrame]) -> Tuple[pd.DataFrame, List[str]]:
    """
    Fills missing SSN/Type/Dept/Rate from roster if available.
    Returns (df, missing_names) where missing_names need roster entries.
    """
    if roster is None:
        # No roster; report all names missing SSN
        missing = sorted(weekly_df.loc[weekly_df["ssn"].astype(str).isin(["", "nan", "None"]), "employee"].unique().tolist())
        return weekly_df, missing

    merged = weekly_df.merge(roster, on="employee", how="left", suffixes=("", "_roster"))

    # Choose SSN: prefer Sierra/weekly if present, else roster
    merged["ssn"] = merged.apply(
        lambda r: r["ssn"] if str(r["ssn"]).strip() not in ("", "nan", "None")
        else (r["ssn_roster"] if pd.notna(r["ssn_roster"]) else ""), axis=1
    )

    # Department / Type
    merged["department"] = merged.apply(
        lambda r: r["department"] if str(r["department"]).strip() not in ("", "nan", "None")
        else (r["department_roster"] if pd.notna(r["department_roster"]) else ""), axis=1
    )
    merged["Type"] = merged.apply(
        lambda r: r["Type"] if str(r["Type"]).strip() not in ("", "nan", "None")
        else ("S" if str(r.get("wtype_roster","")).upper().startswith("S") else "H"), axis=1
    )

    # Rate: keep computed chosen rate; if 0 and roster has a rate, use roster's rate
    merged["rate"] = merged.apply(
        lambda r: r["rate"] if float(r["rate"] or 0) > 0
        else (float(r["rate_roster"]) if pd.notna(r["rate_roster"]) else 0.0), axis=1
    )

    # Who is still missing SSN?
    missing = sorted(merged.loc[merged["ssn"].astype(str).isin(["", "nan", "None"]), "employee"].unique().tolist())

    final_cols = ["employee","ssn","Status","Type","rate","department","REG","OT","DT","REG_$","OT_$","DT_$","TOTAL_$"]
    return merged[final_cols].copy(), missing


# ----------------- build weekly per employee (ONE ROW EACH) -----------------
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
    }

    got = _require(df, required)
    core = df[[got["employee"], got["date"], got["hours"], got["rate"]]].copy()
    core.columns = ["employee","date","hours","rate"]

    dep_col = _find_col(df, optional["department"])
    ssn_col = _find_col(df, optional["ssn"])
    typ_col = _find_col(df, optional["wtype"])
    core["department"] = df[dep_col] if dep_col else ""
    core["ssn"]        = df[ssn_col] if ssn_col else ""
    core["wtype"]      = df[typ_col] if typ_col else ""

    core["employee"] = core["employee"].astype(str).map(_normalize_name)
    core["date"]     = core["date"].map(_to_date)
    core["hours"]    = pd.to_numeric(core["hours"], errors="coerce").fillna(0.0).astype(float)
    core["rate"]     = pd.to_numeric(core["rate"], errors="coerce").fillna(0.0).astype(float)
    core = core[(core["employee"].str.len()>0) & core["date"].notna() & (core["hours"]>0)]
    if core.empty:
        raise ValueError("No valid rows found in Sierra sheet (need Employee, Date, Hours > 0, Rate).")

    # 1) Sum hours per employee per day
    per_day = core.groupby(["employee","date"], dropna=False).agg({"hours":"sum"}).reset_index()

    # 2) Daily CA OT/DT split
    splits = []
    for _, r in per_day.iterrows():
        d = _ca_daily_ot(float(r["hours"]))
        splits.append({"employee": r["employee"], "date": r["date"], "REG": d["REG"], "OT": d["OT"], "DT": d["DT"]})
    split_df = pd.DataFrame(splits)

    # 3) Weekly totals per employee (one row each)
    weekly_hours = split_df.groupby(["employee"], dropna=False)[["REG","OT","DT"]].sum().reset_index()

    # 4) Dollars computed from raw records (handles mixed rates)
    dollars = defaultdict(lambda: {"REG_$":0.0,"OT_$":0.0,"DT_$":0.0})
    day_totals = { (r["employee"], r["date"]): r for _, r in split_df.iterrows() }

    for _, rec in core.iterrows():
        key = (rec["employee"], rec["date"])
        if key not in day_totals:
            continue
        tot = day_totals[key]
        day_sum = tot["REG"] + tot["OT"] + tot["DT"]
        if day_sum <= 0:
            continue
        portion = float(rec["hours"]) / day_sum
        reg_share = tot["REG"] * portion
        ot_share  = tot["OT"]  * portion
        dt_share  = tot["DT"]  * portion
        base = float(rec["rate"])
        dollars[rec["employee"]]["REG_$"] += reg_share * base
        dollars[rec["employee"]]["OT_$"]  += ot_share  * base * 1.5
        dollars[rec["employee"]]["DT_$"]  += dt_share  * base * 2.0

    # 5) Display rate = most frequent weekly rate
    chosen_rate, first_dept, first_ssn, first_type = {}, {}, {}, {}
    for emp, group in core.groupby("employee"):
        rates = Counter([float(r) for r in group["rate"].tolist()])
        chosen_rate[emp] = max(rates.items(), key=lambda kv: kv[1])[0]
        first_dept[emp]  = group["department"].dropna().astype(str).replace("nan","").iloc[0] if not group.empty else ""
        first_ssn[emp]   = group["ssn"].dropna().astype(str).replace("nan","").iloc[0] if not group.empty else ""
        wtyp = str(group["wtype"].dropna().astype(str).replace("nan","").iloc[0] if not group.empty else "")
        first_type[emp]  = "S" if wtyp.upper().startswith("S") else "H"

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

    # Round display hours
    for c in ["REG","OT","DT"]:
        weekly[c] = weekly[c].map(lambda v: round(_money(v), 2))

    weekly = weekly[[
        "employee","ssn","Status","Type","rate","department","REG","OT","DT",
        "REG_$","OT_$","DT_$","TOTAL_$"
    ]].copy()

    # Fill roster defaults (permanent SSNs/rates/etc.)
    roster = _load_roster()
    weekly, missing = _apply_roster_defaults(weekly, roster)
    if missing:
        raise ValueError("Missing SSN in roster for: " + ", ".join(missing))

    return weekly


# ----------------- template handling & writing -----------------
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
                # money columns detection (optional headers vary)
                "reg$":  col_map.get("reg $")  or col_map.get("a01 $") or None,
                "ot$":   col_map.get("ot $")   or col_map.get("a02 $") or None,
                "dt$":   col_map.get("dt $")   or col_map.get("a03 $") or None,
                "total$":col_map.get("total $") or col_map.get("total$") or None,
            }
    raise HTTPException(422, detail="Could not locate WBS header row in template (expecting SSN, Employee Name, Status, Type, Pay Rate, Dept, A01/A02/A03).")

def write_into_wbs_template(template_bytes: bytes, weekly: pd.DataFrame) -> bytes:
    wb = load_workbook(io.BytesIO(template_bytes))
    ws = wb["WEEKLY"] if "WEEKLY" in wb.sheetnames else wb.active

    header_row, cols = _find_wbs_header(ws)
    first_data_row = header_row + 1

    # clear existing data
    last = ws.max_row
    scan_col = cols.get("name") or cols.get("ssn") or 2
    last_data = first_data_row - 1
    for r in range(first_data_row, last+1):
        if ws.cell(r, scan_col).value not in (None, ""):
            last_data = r
    if last_data >= first_data_row:
        ws.delete_rows(first_data_row, last_data - first_data_row + 1)

    # write data rows
    for _, row in weekly.iterrows():
        values = [""] * max(ws.max_column, 16)
        if cols.get("ssn"):    values[cols["ssn"]-1]    = row["ssn"]
        if cols.get("name"):   values[cols["name"]-1]   = row["employee"]
        if cols.get("status"): values[cols["status"]-1] = row["Status"]
        if cols.get("type"):   values[cols["type"]-1]   = row["Type"]
        if cols.get("rate"):   values[cols["rate"]-1]   = row["rate"]
        if cols.get("dept"):   values[cols["dept"]-1]   = row["department"]
        if cols.get("a01"):    values[cols["a01"]-1]    = row["REG"]
        if cols.get("a02"):    values[cols["a02"]-1]    = row["OT"]
        if cols.get("a03"):    values[cols["a03"]-1]    = row["DT"]
        # money columns if present in your template
        if cols.get("reg$"):   values[cols["reg$"]-1]   = row["REG_$"]
        if cols.get("ot$"):    values[cols["ot$"]-1]    = row["OT_$"]
        if cols.get("dt$"):    values[cols["dt$"]-1]    = row["DT_$"]
        if cols.get("total$"): values[cols["total$"]-1] = row["TOTAL_$"]
        ws.append(values)

    # blank row + GRAND TOTAL row
    blank_row = ws.max_row + 1
    ws.append([])  # blank spacer

    total_row = ws.max_row + 1
    totals = {
        "REG": float(weekly["REG"].sum()),
        "OT":  float(weekly["OT"].sum()),
        "DT":  float(weekly["DT"].sum()),
        "REG_$": float(weekly["REG_$"].sum()),
        "OT_$":  float(weekly["OT_$"].sum()),
        "DT_$":  float(weekly["DT_$"].sum()),
        "TOTAL_$": float(weekly["TOTAL_$"].sum()),
    }
    row_vals = [""] * max(ws.max_column, 16)
    if cols.get("name"):   row_vals[cols["name"]-1]   = "TOTAL"
    if cols.get("a01"):    row_vals[cols["a01"]-1]    = round(totals["REG"], 2)
    if cols.get("a02"):    row_vals[cols["a02"]-1]    = round(totals["OT"], 2)
    if cols.get("a03"):    row_vals[cols["a03"]-1]    = round(totals["DT"], 2)
    if cols.get("reg$"):   row_vals[cols["reg$"]-1]   = round(totals["REG_$"], 2)
    if cols.get("ot$"):    row_vals[cols["ot$"]-1]    = round(totals["OT_$"], 2)
    if cols.get("dt$"):    row_vals[cols["dt$"]-1]    = round(totals["DT_$"], 2)
    if cols.get("total$"): row_vals[cols["total$"]-1] = round(totals["TOTAL_$"], 2)
    ws.append(row_vals)

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

@app.get("/roster-status")
def roster_status():
    r = _load_roster()
    if r is None:
        return JSONResponse({"roster":"missing"})
    return JSONResponse({"roster":"found","employees":int(r.shape[0])})

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

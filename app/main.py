# server/main.py
import io, os, re, unicodedata
from pathlib import Path
from collections import Counter, defaultdict
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple

import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import StreamingResponse, JSONResponse
from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet

# --------------------------------------------------------------------------------------
# App
# --------------------------------------------------------------------------------------
app = FastAPI(title="Sierra → WBS Payroll Converter", version="4.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten to your frontend domain later if needed
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ALLOWED_EXTS = (".xlsx", ".xls")

# --------------------------------------------------------------------------------------
# Paths & file discovery (root/app/app/data/server + env override)
# --------------------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parents[1]
SEARCH_DIRS = [BASE_DIR, BASE_DIR / "app", BASE_DIR / "app" / "data", BASE_DIR / "server"]

def _find_file(basenames: List[str]) -> Optional[Path]:
    for env_key in ("WBS_TEMPLATE_PATH", "ROSTER_PATH"):
        p = os.getenv(env_key)
        if p and Path(p).exists():
            return Path(p)
    for d in SEARCH_DIRS:
        for name in basenames:
            p = d / name
            if p.exists():
                return p
    return None

# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------
def _ext_ok(name: str) -> bool:
    n = (name or "").lower()
    return any(n.endswith(e) for e in ALLOWED_EXTS)

def _std(s: str) -> str:
    return (s or "").strip().lower().replace("\n"," ").replace("\r"," ")

def _normalize_name(raw: str) -> str:
    if not raw or not isinstance(raw, str): return ""
    parts = [p for p in raw.strip().split() if p]
    if len(parts) == 2:
        return f"{parts[1]}, {parts[0]}"  # "First Last" -> "Last, First"
    return raw.strip()

def _canon_name(s: str) -> str:
    """Case-insensitive, accent-free, single-space, normalized comma key."""
    if not isinstance(s, str):
        s = str(s or "")
    s = s.strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.replace(".", "")
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"\s*,\s*", ",", s)
    return s.lower()

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

# --------------------------------------------------------------------------------------
# Roster & Template loaders
# --------------------------------------------------------------------------------------
def _load_template_from_disk() -> bytes:
    p = _find_file(["wbs_template.xlsx"])
    if not p:
        raise HTTPException(422, detail="WBS template not found. Place 'wbs_template.xlsx' in repo root (or set WBS_TEMPLATE_PATH).")
    return p.read_bytes()

def _load_roster_df() -> Optional[pd.DataFrame]:
    p = _find_file(["roster.xlsx", "roster.csv"])
    if not p:
        return None
    if p.suffix.lower() == ".xlsx":
        df = pd.read_excel(p, dtype=str)
    else:
        df = pd.read_csv(p, dtype=str)
    if df.empty:
        return None

    name_col = _find_col(df, ["employee name","employee","name"])
    ssn_col  = _find_col(df, ["ssn","social","social security"])
    rate_col = _find_col(df, ["payrate","rate","hourly rate","wage"])
    dept_col = _find_col(df, ["dept","department","division"])
    type_col = _find_col(df, ["type","employee type","emp type"])

    if not name_col or not ssn_col:
        return None

    out = pd.DataFrame({
        "employee_disp": df[name_col].astype(str).map(str.strip),
        "employee_key":  df[name_col].astype(str).map(_canon_name),
        "ssn":           df[ssn_col].astype(str).map(str.strip),
        "rate_roster":   pd.to_numeric(df[rate_col], errors="coerce") if rate_col else pd.Series([None]*len(df)),
        "department_roster": df[dept_col].astype(str).map(str.strip) if dept_col else pd.Series([""]*len(df)),
        "wtype_roster":      df[type_col].astype(str).map(str.strip) if type_col else pd.Series([""]*len(df)),
    }).dropna(subset=["employee_key"]).drop_duplicates(subset=["employee_key"], keep="last")
    return out

# --------------------------------------------------------------------------------------
# Build weekly (ONE ROW PER EMPLOYEE)
# --------------------------------------------------------------------------------------
def build_weekly_from_sierra(xlsx_bytes: bytes, sheet_name: Optional[str]=None) -> pd.DataFrame:
    excel = pd.ExcelFile(io.BytesIO(xlsx_bytes))
    sheet = sheet_name or excel.sheet_names[0]
    df = excel.parse(sheet)

    required = {
        "employee": ["employee","employee name","name","worker","employee_name"],
        "date":     ["date","work date","day","worked date"],
        "hours":    ["hours","hrs","total hours","work hours"],
        "rate":     ["rate","pay rate","hourly rate","wage"],
    }
    optional = {
        "department": ["department","dept","division"],
        # DO NOT read SSN from Sierra; it isn't there.
        # "ssn": ["ssn","social","social security","social security number"],
        "wtype": ["type","employee type","emp type","pay type"],
    }

    got = _require(df, required)
    core = df[[got["employee"], got["date"], got["hours"], got["rate"]]].copy()
    core.columns = ["employee","date","hours","rate"]

    dep_col = _find_col(df, optional["department"])
    typ_col = _find_col(df, optional["wtype"])
    core["department"] = df[dep_col] if dep_col else ""
    core["wtype"]      = df[typ_col] if typ_col else ""
    core["ssn"]        = ""  # always blank; will be filled from roster

    # Normalize & keys
    core["employee"]   = core["employee"].astype(str).map(_normalize_name)
    core["emp_key"]    = core["employee"].map(_canon_name)
    core["date"]       = core["date"].map(_to_date)
    core["hours"]      = pd.to_numeric(core["hours"], errors="coerce").fillna(0.0).astype(float)
    core["rate"]       = pd.to_numeric(core["rate"], errors="coerce").fillna(0.0).astype(float)
    core["department"] = core["department"].astype(str).map(str.strip)
    core["wtype"]      = core["wtype"].astype(str).map(str.strip)

    core = core[(core["employee"].str.len()>0) & core["date"].notna() & (core["hours"]>0)]
    if core.empty:
        raise ValueError("No valid rows found in Sierra sheet (need Employee, Date, Hours > 0, Rate).")

    # 1) Sum hours per employee per day
    per_day = core.groupby(["emp_key","employee","date"], dropna=False).agg({"hours":"sum"}).reset_index()

    # 2) Daily CA OT/DT split
    splits = []
    for _, r in per_day.iterrows():
        d = _ca_daily_ot(float(r["hours"]))
        splits.append({"emp_key": r["emp_key"], "employee": r["employee"], "date": r["date"], "REG": d["REG"], "OT": d["OT"], "DT": d["DT"]})
    split_df = pd.DataFrame(splits)

    # 3) Weekly totals per employee (one row each)
    weekly_hours = split_df.groupby(["emp_key","employee"], dropna=False)[["REG","OT","DT"]].sum().reset_index()

    # 4) Dollars from raw records (handles mixed rates)
    dollars = defaultdict(lambda: {"REG_$":0.0,"OT_$":0.0,"DT_$":0.0})
    day_totals = { (r["emp_key"], r["date"]): r for _, r in split_df.iterrows() }

    for _, rec in core.iterrows():
        key = (rec["emp_key"], rec["date"])
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
        dollars[rec["emp_key"]]["REG_$"] += reg_share * base
        dollars[rec["emp_key"]]["OT_$"]  += ot_share  * base * 1.5
        dollars[rec["emp_key"]]["DT_$"]  += dt_share  * base * 2.0

    # 5) Display rate = most frequent weekly rate
    chosen_rate, first_dept, first_type = {}, {}, {}
    for emp_key, group in core.groupby("emp_key"):
        rates = Counter([float(r) for r in group["rate"].tolist()])
        chosen_rate[emp_key] = max(rates.items(), key=lambda kv: kv[1])[0]
        first_dept[emp_key]  = group["department"].dropna().astype(str).replace("nan","").iloc[0] if not group.empty else ""
        wtyp = str(group["wtype"].dropna().astype(str).replace("nan","").iloc[0] if not group.empty else "")
        first_type[emp_key]  = "S" if wtyp.upper().startswith("S") else "H"

    weekly = weekly_hours.copy()
    weekly["REG_$"]   = weekly["emp_key"].map(lambda k: round(_money(dollars[k]["REG_$"]), 2))
    weekly["OT_$"]    = weekly["emp_key"].map(lambda k: round(_money(dollars[k]["OT_$"]), 2))
    weekly["DT_$"]    = weekly["emp_key"].map(lambda k: round(_money(dollars[k]["DT_$"]), 2))
    weekly["TOTAL_$"] = weekly["REG_$"] + weekly["OT_$"] + weekly["DT_$"]

    weekly["rate"]       = weekly["emp_key"].map(lambda k: round(_money(chosen_rate.get(k, 0.0)), 2))
    weekly["department"] = weekly["emp_key"].map(lambda k: first_dept.get(k, ""))
    weekly["ssn"]        = ""  # will be filled from roster
    weekly["Status"]     = "A"
    weekly["Type"]       = weekly["emp_key"].map(lambda k: first_type.get(k, "H"))

    for c in ["REG","OT","DT"]:
        weekly[c] = weekly[c].map(lambda v: round(_money(v), 2))

    weekly = weekly[[
        "emp_key","employee","ssn","Status","Type","rate","department",
        "REG","OT","DT","REG_$","OT_$","DT_$","TOTAL_$"
    ]].copy()

    # Apply roster defaults by canonical key
    roster = _load_roster_df()
    weekly, missing = _apply_roster_defaults_by_key(weekly, roster)
    if missing:
        raise ValueError("Missing SSN in roster for: " + ", ".join(missing))

    return weekly

def _apply_roster_defaults_by_key(weekly_df: pd.DataFrame, roster: Optional[pd.DataFrame]) -> Tuple[pd.DataFrame, List[str]]:
    if roster is None:
        missing = sorted(weekly_df.loc[weekly_df["ssn"].astype(str).isin(["", "nan", "None"]), "employee"].unique().tolist())
        return weekly_df.drop(columns=["emp_key"]), missing

    merged = weekly_df.merge(
        roster[["employee_key","employee_disp","ssn","rate_roster","department_roster","wtype_roster"]],
        left_on="emp_key", right_on="employee_key", how="left"
    )

    # SSN only from roster (Sierra had none)
    merged["ssn"] = merged.apply(
        lambda r: r["ssn_y"] if str(r["ssn_y"]).strip() not in ("", "nan", "None") else "", axis=1
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

    # Rate: keep computed; if 0 and roster has a default, use it
    merged["rate"] = merged.apply(
        lambda r: r["rate"] if float(r["rate"] or 0) > 0
        else (float(r["rate_roster"]) if pd.notna(r["rate_roster"]) else 0.0), axis=1
    )

    missing = sorted(merged.loc[merged["ssn"].astype(str).isin(["", "nan", "None"]), "employee"].unique().tolist())

    final = merged[[
        "employee","ssn","Status","Type","rate","department",
        "REG","OT","DT","REG_$","OT_$","DT_$","TOTAL_$"
    ]].copy()
    return final, missing

# --------------------------------------------------------------------------------------
# Template write
# --------------------------------------------------------------------------------------
def _find_wbs_header(ws: Worksheet) -> Tuple[int, Dict[str, int]]:
    targets = ["ssn","employee name","status","type","pay rate","dept","a01","a02","a03"]
    for r in range(1, ws.max_row+1):
        lower = [(_std(str(ws.cell(r,c).value)) if ws.cell(r,c).value is not None else "") for c in range(1, ws.max_column+1)]
        if all(any(t == lv for lv in lower) for t in targets):
            col_map = { lv: c for c, lv in enumerate(lower, start=1) }
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
                "reg$":   col_map.get("reg $")   or col_map.get("a01 $")  or None,
                "ot$":    col_map.get("ot $")    or col_map.get("a02 $")  or None,
                "dt$":    col_map.get("dt $")    or col_map.get("a03 $")  or None,
                "total$": col_map.get("total $") or col_map.get("total$") or None,
            }
    raise HTTPException(422, detail="Could not locate WBS header row in template (expecting SSN, Employee Name, Status, Type, Pay Rate, Dept, A01/A02/A03).")

def write_into_wbs_template(template_bytes: bytes, weekly: pd.DataFrame) -> bytes:
    wb = load_workbook(io.BytesIO(template_bytes))
    ws = wb["WEEKLY"] if "WEEKLY" in wb.sheetnames else wb.active

    header_row, cols = _find_wbs_header(ws)
    first_data_row = header_row + 1

    # clear existing data rows
    scan_col = cols.get("name") or cols.get("ssn") or 2
    last = ws.max_row
    last_data = first_data_row - 1
    for r in range(first_data_row, last+1):
        if ws.cell(r, scan_col).value not in (None, ""):
            last_data = r
    if last_data >= first_data_row:
        ws.delete_rows(first_data_row, last_data - first_data_row + 1)

    # append weekly rows
    for _, row in weekly.iterrows():
        values = [""] * max(ws.max_column, 16)
        if cols.get("ssn")    : values[cols["ssn"]-1]    = row["ssn"]
        if cols.get("name")   : values[cols["name"]-1]   = row["employee"]
        if cols.get("status") : values[cols["status"]-1] = row["Status"]
        if cols.get("type")   : values[cols["type"]-1]   = row["Type"]
        if cols.get("rate")   : values[cols["rate"]-1]   = row["rate"]
        if cols.get("dept")   : values[cols["dept"]-1]   = row["department"]
        if cols.get("a01")    : values[cols["a01"]-1]    = row["REG"]
        if cols.get("a02")    : values[cols["a02"]-1]    = row["OT"]
        if cols.get("a03")    : values[cols["a03"]-1]    = row["DT"]
        if cols.get("reg$")   : values[cols["reg$"]-1]   = row["REG_$"]
        if cols.get("ot$")    : values[cols["ot$"]-1]    = row["OT_$"]
        if cols.get("dt$")    : values[cols["dt$"]-1]    = row["DT_$"]
        if cols.get("total$") : values[cols["total$"]-1] = row["TOTAL_$"]
        ws.append(values)

    # spacer + TOTAL row
    ws.append([])  # blank row
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
    if cols.get("name")  : row_vals[cols["name"]-1]   = "TOTAL"
    if cols.get("a01")   : row_vals[cols["a01"]-1]    = round(totals["REG"], 2)
    if cols.get("a02")   : row_vals[cols["a02"]-1]    = round(totals["OT"], 2)
    if cols.get("a03")   : row_vals[cols["a03"]-1]    = round(totals["DT"], 2)
    if cols.get("reg$")  : row_vals[cols["reg$"]-1]   = round(totals["REG_$"], 2)
    if cols.get("ot$")   : row_vals[cols["ot$"]-1]    = round(totals["OT_$"], 2)
    if cols.get("dt$")   : row_vals[cols["dt$"]-1]    = round(totals["DT_$"], 2)
    if cols.get("total$"): row_vals[cols["total$"]-1] = round(totals["TOTAL_$"], 2)
    ws.append(row_vals)

    bio = io.BytesIO()
    wb.save(bio); bio.seek(0)
    return bio.read()

# --------------------------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------------------------
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
    r = _load_roster_df()
    if r is None:
        return JSONResponse({"roster":"missing"})
    return JSONResponse({"roster":"found","employees":int(r.drop_duplicates('employee_key').shape[0])})

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

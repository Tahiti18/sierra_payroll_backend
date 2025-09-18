# server/main.py
import io, os, re, unicodedata
from pathlib import Path
from datetime import datetime, date
from typing import Optional, List, Dict, Tuple

import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import StreamingResponse, JSONResponse
from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet

app = FastAPI(title="Sierra → WBS (weekly-40, numbers-first)", version="9.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = Path(__file__).resolve().parents[1]
SEARCH_DIRS = [BASE_DIR, BASE_DIR / "app", BASE_DIR / "server", BASE_DIR / "app" / "data"]
ALLOWED_EXTS = (".xlsx", ".xls")

# ---------- helpers ----------
def _std(s: str) -> str:
    return (s or "").strip().lower().replace("\n", " ").replace("\r", " ")

def _ext_ok(name: str) -> bool:
    n = (name or "").lower()
    return any(n.endswith(e) for e in ALLOWED_EXTS)

def _clean_space(s: str) -> str:
    return re.sub(r"\s+", " ", str(s).strip())

def _canon_name(s: str) -> str:
    s = _clean_space(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.replace(".", "")
    s = re.sub(r"\s*,\s*", ",", s)
    return s.lower()

def _to_number(x) -> float:
    if x is None or (isinstance(x, float) and pd.isna(x)): return 0.0
    s = str(x).strip()
    if s == "" or s.lower() in ("nan", "none"): return 0.0
    s = re.sub(r"[^0-9\.\-]", "", s)
    try:
        return float(s) if s not in ("", "-", ".", "-.") else 0.0
    except Exception:
        try: return float(x)
        except Exception: return 0.0

def _money(x: float) -> float:
    try: return round(float(x or 0.0), 2)
    except Exception: return 0.0

def _find_file(basenames: List[str]) -> Optional[Path]:
    env_map = {"wbs_template.xlsx": "WBS_TEMPLATE_PATH", "roster.xlsx": "ROSTER_PATH", "roster.csv": "ROSTER_PATH"}
    for name in basenames:
        env_key = env_map.get(name)
        if env_key:
            p = os.getenv(env_key)
            if p and Path(p).exists():
                return Path(p)
    for d in SEARCH_DIRS:
        for name in basenames:
            p = d / name
            if p.exists():
                return p
    return None

def _load_template_bytes() -> bytes:
    p = _find_file(["wbs_template.xlsx"])
    if not p:
        raise HTTPException(422, "WBS template not found. Place 'wbs_template.xlsx' at repo root or set WBS_TEMPLATE_PATH.")
    return p.read_bytes()

def _load_roster_df() -> Optional[pd.DataFrame]:
    p = _find_file(["roster.xlsx", "roster.csv"])
    if not p: return None
    try:
        if p.suffix.lower() == ".xlsx":
            df = pd.read_excel(p, dtype=str)
        else:
            df = pd.read_csv(p, dtype=str)
    except Exception as e:
        raise HTTPException(422, f"Roster load failed: {e}")
    if df.empty: return None

    def find_col(options: List[str]) -> Optional[str]:
        norm = {_std(c): c for c in df.columns}
        for want in options:
            w = _std(want)
            if w in norm: return norm[w]
        for want in options:
            w = _std(want)
            for k, c in norm.items():
                if w in k: return c
        return None

    name_col = find_col(["employee name", "employee", "name"])
    ssn_col  = find_col(["ssn", "social", "social security"])
    rate_col = find_col(["payrate", "rate", "hourly rate", "wage"])
    dept_col = find_col(["dept", "department", "division"])
    type_col = find_col(["type", "employee type", "emp type"])

    if not name_col: return None

    out = pd.DataFrame({
        "employee_disp": df[name_col].astype(str).map(_clean_space),
        "employee_key":  df[name_col].astype(str).map(_canon_name),
        "ssn":           (df[ssn_col].astype(str).map(str.strip) if ssn_col else pd.Series([""]*len(df))),
        "rate_roster":   pd.to_numeric(df[rate_col].map(_to_number), errors="coerce") if rate_col else pd.Series([None]*len(df)),
        "department_roster": df[dept_col].astype(str).map(str.strip) if dept_col else pd.Series([""]*len(df)),
        "wtype_roster":      df[type_col].astype(str).map(str.strip) if type_col else pd.Series([""]*len(df)),
    }).dropna(subset=["employee_key"]).drop_duplicates(subset=["employee_key"], keep="last")

    return out

# ---------- Sierra parsing (weekly-40) ----------
COMMON_EMP  = ["employee", "employee name", "name"]
COMMON_DATE = ["date", "work date", "day"]
COMMON_HRS  = ["hours", "hrs", "total hours", "a01", "regular", "reg"]
COMMON_RATE = ["rate", "pay rate", "hourly rate", "wage", "base rate"]

def _guess_col(df: pd.DataFrame, options: List[str]) -> Optional[str]:
    norm = {_std(c): c for c in df.columns}
    for want in options:
        w = _std(want)
        if w in norm: return norm[w]
    for want in options:
        w = _std(want)
        for k, c in norm.items():
            if w in k: return c
    return None

def _guess_employee_col(df: pd.DataFrame) -> Optional[str]:
    c = _guess_col(df, COMMON_EMP)
    if c: return c
    best, score = None, -1
    for col in df.columns:
        s = df[col].astype(str)
        looks = s.str.contains(r"[A-Za-z],\s*[A-Za-z]", regex=True, na=False).mean()
        if looks > score:
            best, score = col, looks
    return best if score >= 0.2 else None

def _guess_date_col(df: pd.DataFrame) -> Optional[str]:
    c = _guess_col(df, COMMON_DATE)
    if c: return c
    best, score = None, -1
    for col in df.columns:
        try:
            sc = pd.to_datetime(df[col], errors="coerce").notna().mean()
            if sc > score:
                score, best = sc, col
        except Exception:
            continue
    return best if score >= 0.3 else None

def _guess_hours_col(df: pd.DataFrame) -> Optional[str]:
    c = _guess_col(df, COMMON_HRS)
    if c: return c
    best, score = None, -1
    for col in df.columns:
        try:
            v = pd.to_numeric(df[col].map(_to_number), errors="coerce")
        except Exception:
            continue
        ok = v.between(0, 24, inclusive="both").mean()
        nz = (v > 0).mean()
        sc = ok * 0.6 + nz * 0.4
        if sc > score:
            best, score = col, sc
    return best if score >= 0.3 else None

def _weekly40(hours_total: float) -> Dict[str, float]:
    h = float(hours_total or 0.0)
    reg = min(h, 40.0)
    ot  = max(h - 40.0, 0.0)
    dt  = 0.0
    return {"REG": reg, "OT": ot, "DT": dt}

def build_weekly_from_sierra(xlsx_bytes: bytes) -> pd.DataFrame:
    excel = pd.ExcelFile(io.BytesIO(xlsx_bytes))
    sheet = excel.sheet_names[0]
    df = excel.parse(sheet)

    roster = _load_roster_df()
    rate_map = { } ; ssn_map = { } ; dept_map = { } ; type_map = { }
    if roster is not None and not roster.empty:
        rate_map = {k: float(v) for k, v in zip(roster["employee_key"], roster["rate_roster"]) if pd.notna(v)}
        ssn_map  = {k: (s or "") for k, s in zip(roster["employee_key"], roster["ssn"])}
        dept_map = {k: (d or "") for k, d in zip(roster["employee_key"], roster["department_roster"])}
        type_map = {k: (t or "") for k, t in zip(roster["employee_key"], roster["wtype_roster"])}

    emp_col  = _guess_employee_col(df)
    if not emp_col:
        raise ValueError("Could not find 'Employee' column in Sierra file.")
    rate_col = _guess_col(df, COMMON_RATE)  # optional
    dep_col  = _guess_col(df, ["department","dept","division"])
    typ_col  = _guess_col(df, ["type","employee type","emp type","pay type"])

    employee = df[emp_col].astype(str).map(_clean_space)
    emp_key  = employee.map(_canon_name)
    raw_rate = df[rate_col].map(_to_number) if rate_col else pd.Series([0.0]*len(df))
    rate = []
    for k, r in zip(emp_key, raw_rate):
        v = float(r or 0.0)
        if v <= 0 and k in rate_map: v = float(rate_map[k] or 0.0)
        rate.append(v)
    rate = pd.Series(rate, dtype=float)

    base = pd.DataFrame({
        "employee":   employee,
        "emp_key":    emp_key,
        "rate":       rate.astype(float),
        "department": (df[dep_col].astype(str).map(str.strip) if dep_col else ""),
        "wtype":      (df[typ_col].astype(str).map(str.strip) if typ_col else ""),
    })

    date_col = _guess_date_col(df)
    hrs_col  = _guess_hours_col(df)
    has_date_hours = bool(date_col and hrs_col)

    day_candidates = [
        ("mon", ["mon","monday"]),("tue", ["tue","tues","tuesday"]),("wed", ["wed","weds","wednesday"]),
        ("thu", ["thu","thur","thurs","thursday"]),("fri", ["fri","friday"]),("sat", ["sat","saturday"]),("sun", ["sun","sunday"]),
    ]
    day_cols: Dict[str,str] = {}
    normcols = { _std(c): c for c in df.columns }
    for day_key, aliases in day_candidates:
        for alias in aliases:
            for k,c in normcols.items():
                if alias in k:
                    day_cols[day_key] = c
                    break
            if day_key in day_cols: break
    has_weekdays = len(day_cols) >= 4

    if not has_date_hours and not has_weekdays:
        raise ValueError("Could not find Date+Hours or weekday (Mon..Sun) columns in Sierra file.")

    if has_date_hours:
        rows = pd.DataFrame({
            "emp_key":    base["emp_key"],
            "employee":   base["employee"],
            "hours":      pd.to_numeric(df[hrs_col].map(_to_number), errors="coerce").fillna(0.0).astype(float),
            "rate":       base["rate"],
            "department": base["department"],
            "wtype":      base["wtype"],
        })
    else:
        hrs_sum = pd.Series([0.0]*len(df), dtype=float)
        for _, col in day_cols.items():
            hrs_sum = hrs_sum + pd.to_numeric(df[col].map(_to_number), errors="coerce").fillna(0.0).astype(float)
        rows = pd.DataFrame({
            "emp_key":    base["emp_key"],
            "employee":   base["employee"],
            "hours":      hrs_sum,
            "rate":       base["rate"],
            "department": base["department"],
            "wtype":      base["wtype"],
        })

    by_emp = rows.groupby(["emp_key","employee"], as_index=False).agg(
        HOURS=("hours","sum"),
        RATE=("rate","max"),
        DEPARTMENT=("department","first"),
        WTYPE=("wtype","first"),
    )

    b = by_emp["HOURS"].map(_weekly40)
    by_emp["REG"] = b.map(lambda d: d["REG"]).astype(float)
    by_emp["OT"]  = b.map(lambda d: d["OT"]).astype(float)
    by_emp["DT"]  = 0.0

    def _final_rate(row):
        rr = float(row["RATE"] or 0.0)
        if rr <= 0.0: return float(rate_map.get(row["emp_key"], 0.0))
        return rr
    by_emp["rate"] = by_emp.apply(_final_rate, axis=1).astype(float)

    by_emp["REG_$"]   = by_emp["REG"] * by_emp["rate"]
    by_emp["OT_$"]    = by_emp["OT"]  * by_emp["rate"] * 1.5
    by_emp["DT_$"]    = by_emp["DT"]  * by_emp["rate"] * 2.0
    by_emp["TOTAL_$"] = by_emp["REG_$"] + by_emp["OT_$"] + by_emp["DT_$"]

    weekly = pd.DataFrame({
        "emp_key":    by_emp["emp_key"],
        "employee":   by_emp["employee"],
        "ssn":        by_emp["emp_key"].map(lambda k: ssn_map.get(k, "") if ssn_map else ""),
        "Status":     "A",
        "Type":       by_emp["WTYPE"].map(lambda s: "S" if str(s).upper().startswith("S") else "H"),
        "rate":       by_emp["rate"].map(_money),
        "department": by_emp["DEPARTMENT"].astype(str),
        "REG":        by_emp["REG"].map(_money),
        "OT":         by_emp["OT"].map(_money),
        "DT":         by_emp["DT"].map(_money),
        "REG_$":      by_emp["REG_$"].map(_money),
        "OT_$":       by_emp["OT_$"].map(_money),
        "DT_$":       by_emp["DT_$"].map(_money),
        "TOTAL_$":    by_emp["TOTAL_$"].map(_money),
    })

    weekly["ssn"] = weekly["ssn"].fillna("").astype(str)
    weekly = weekly.drop(columns=["emp_key"])
    return weekly

# ---------- WBS template write (WEEKLY only) ----------
def _find_wbs_header(ws: Worksheet) -> Tuple[int, Dict[str, int]]:
    req_aliases = {
        "ssn": ["ssn"],
        "name": ["employee name","employee","name"],
        "status":["status"],
        "type": ["type"],
        "rate": ["pay rate","payrate","rate"],
        "dept": ["dept","department"],
        "a01": ["a01","regular","reg"],
        "a02": ["a02","overtime","ot"],
        "a03": ["a03","doubletime","dt"],
    }
    opt_aliases = {
        "reg_amt":     ["a01 $","a01$","reg $","regular $","regular amt","a01 amount"],
        "ot_amt":      ["a02 $","a02$","ot $","overtime $","overtime amt","a02 amount"],
        "dt_amt":      ["a03 $","a03$","dt $","doubletime $","doubletime amt","a03 amount"],
        "total_amt":   ["total $","total$","grand total $"],
        "total_plain": ["total","totals"],
    }
    def norm(v):
        v = "" if v is None else str(v)
        return re.sub(r"\s+"," ", v.replace("\n"," ").replace("\r"," ")).strip().lower()

    best_row, best_map, best_score = None, None, -1
    for r in range(1, ws.max_row+1):
        row_vals = [norm(ws.cell(r,c).value) for c in range(1, ws.max_column+1)]
        if sum(1 for v in row_vals if v) < 3: continue
        col_map = {v:c for c,v in enumerate(row_vals, start=1) if v}
        def pick(aliases):
            for a in aliases:
                if a in col_map: return col_map[a]
                for k,c in col_map.items():
                    if a in k: return c
            return None
        m = {k: pick(v) for k,v in req_aliases.items()}
        score = sum(1 for v in m.values() if v is not None)
        if score > best_score and m.get("name") and m.get("a01") and m.get("a02") and m.get("a03"):
            m["reg_amt"]     = pick(opt_aliases["reg_amt"])
            m["ot_amt"]      = pick(opt_aliases["ot_amt"])
            m["dt_amt"]      = pick(opt_aliases["dt_amt"])
            m["total_amt"]   = pick(opt_aliases["total_amt"])
            m["total_plain"] = pick(opt_aliases["total_plain"])
            best_row, best_map, best_score = r, m, score
    if not best_row:
        raise HTTPException(422, "WEEKLY header not found. Expect 'Employee Name' and A01/A02/A03.")
    return best_row, best_map

def write_into_wbs_template(template_bytes: bytes, weekly: pd.DataFrame) -> bytes:
    wb = load_workbook(io.BytesIO(template_bytes))
    ws = wb["WEEKLY"] if "WEEKLY" in wb.sheetnames else wb.active

    header_row, cols = _find_wbs_header(ws)
    first_data_row = header_row + 1

    scan_col = cols.get("name") or cols.get("ssn") or 2
    last = ws.max_row
    last_data = first_data_row - 1
    for r in range(first_data_row, last+1):
        if ws.cell(r, scan_col).value not in (None, ""):
            last_data = r
    if last_data >= first_data_row:
        ws.delete_rows(first_data_row, last_data - first_data_row + 1)

    for _, row in weekly.iterrows():
        values = [""] * max(ws.max_column, 64)
        if cols.get("ssn"):      values[cols["ssn"] - 1]    = row.get("ssn", "")
        if cols.get("name"):     values[cols["name"] - 1]   = row.get("employee", "")
        if cols.get("status"):   values[cols["status"] - 1] = row.get("Status", "A")
        if cols.get("type"):     values[cols["type"] - 1]   = row.get("Type", "H")
        if cols.get("rate"):     values[cols["rate"] - 1]   = row.get("rate", 0.0)
        if cols.get("dept"):     values[cols["dept"] - 1]   = row.get("department", "")
        if cols.get("a01"):      values[cols["a01"] - 1]    = row.get("REG", 0.0)
        if cols.get("a02"):      values[cols["a02"] - 1]    = row.get("OT", 0.0)
        if cols.get("a03"):      values[cols["a03"] - 1]    = row.get("DT", 0.0)
        if cols.get("reg_amt"):  values[cols["reg_amt"] - 1] = row.get("REG_$", 0.0)
        if cols.get("ot_amt"):   values[cols["ot_amt"] - 1]  = row.get("OT_$", 0.0)
        if cols.get("dt_amt"):   values[cols["dt_amt"] - 1]  = row.get("DT_$", 0.0)
        if cols.get("total_amt"):   values[cols["total_amt"] - 1]   = row.get("TOTAL_$", 0.0)
        elif cols.get("total_plain"): values[cols["total_plain"] - 1] = row.get("TOTAL_$", 0.0)
        ws.append(values)

    ws.append([])
    totals = {
        "REG":     float(weekly["REG"].sum()) if "REG" in weekly else 0.0,
        "OT":      float(weekly["OT"].sum())  if "OT" in weekly else 0.0,
        "DT":      float(weekly["DT"].sum())  if "DT" in weekly else 0.0,
        "REG_$":   float(weekly["REG_$"].sum()) if "REG_$" in weekly else 0.0,
        "OT_$":    float(weekly["OT_$"].sum())  if "OT_$" in weekly else 0.0,
        "DT_$":    float(weekly["DT_$"].sum())  if "DT_$" in weekly else 0.0,
        "TOTAL_$": float(weekly["TOTAL_$"].sum()) if "TOTAL_$" in weekly else 0.0,
    }
    row_vals = [""] * max(ws.max_column, 64)
    if cols.get("name"):     row_vals[cols["name"] - 1]  = "TOTAL"
    if cols.get("a01"):      row_vals[cols["a01"] - 1]   = _money(totals["REG"])
    if cols.get("a02"):      row_vals[cols["a02"] - 1]   = _money(totals["OT"])
    if cols.get("a03"):      row_vals[cols["a03"] - 1]   = _money(totals["DT"])
    if cols.get("reg_amt"):  row_vals[cols["reg_amt"] - 1] = _money(totals["REG_$"])
    if cols.get("ot_amt"):   row_vals[cols["ot_amt"] - 1]  = _money(totals["OT_$"])
    if cols.get("dt_amt"):   row_vals[cols["dt_amt"] - 1]  = _money(totals["DT_$"])
    if cols.get("total_amt"):   row_vals[cols["total_amt"] - 1] = _money(totals["TOTAL_$"])
    elif cols.get("total_plain"): row_vals[cols["total_plain"] - 1] = _money(totals["TOTAL_$"])
    ws.append(row_vals)

    bio = io.BytesIO(); wb.save(bio); bio.seek(0)
    return bio.read()

# ---------- routes ----------
@app.get("/health")
def health():
    return {"ok": True, "ts": datetime.utcnow().isoformat() + "Z"}

@app.get("/template-status")
def template_status():
    try:
        _ = _load_template_bytes()
        return {"template": "found"}
    except HTTPException as e:
        return JSONResponse({"template": "missing", "detail": str(e.detail)}, status_code=422)

@app.get("/roster-status")
def roster_status():
    r = _load_roster_df()
    if r is None:
        return {"roster": "missing"}
    return {"roster": "found", "employees": int(r.drop_duplicates('employee_key').shape[0])}

@app.post("/process-payroll")
async def process_payroll(file: UploadFile = File(..., description="Sierra payroll .xlsx")):
    if not file or not file.filename:
        raise HTTPException(400, "No Sierra file provided.")
    if not _ext_ok(file.filename):
        raise HTTPException(415, "Unsupported Sierra file type. Use .xlsx or .xls")
    try:
        sierra_bytes = await file.read()
        weekly = build_weekly_from_sierra(sierra_bytes)
    except HTTPException as he:
        raise HTTPException(he.status_code, str(he.detail))
    except ValueError as ve:
        raise HTTPException(422, str(ve))
    except Exception as e:
        raise HTTPException(500, f"Sierra parse error: {e}")

    try:
        tmpl = _load_template_bytes()
        out_bytes = write_into_wbs_template(tmpl, weekly)
    except HTTPException as he:
        raise HTTPException(he.status_code, str(he.detail))
    except Exception as e:
        raise HTTPException(500, f"Template processing error: {e}")

    out_name = f"WBS_Payroll_{datetime.utcnow().date()}.xlsx"
    return StreamingResponse(
        io.BytesIO(out_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={out_name}"}
    )

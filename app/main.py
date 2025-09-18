# server/main.py
import io
import os
import re
import unicodedata
from pathlib import Path
from collections import Counter, defaultdict
from datetime import datetime, date
from typing import Optional, List, Dict, Tuple

import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import StreamingResponse, JSONResponse

from app.services.wbs_generator import write_into_wbs_template  # writer for WEEKLY tab only

app = FastAPI(title="Sierra → WBS Payroll Converter", version="8.0.1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = Path(__file__).resolve().parents[1]
SEARCH_DIRS = [BASE_DIR, BASE_DIR / "app", BASE_DIR / "app" / "data", BASE_DIR / "server"]
ALLOWED_EXTS = (".xlsx", ".xls")


# ---------------- helpers ----------------
def _std(s: str) -> str:
    return (s or "").strip().lower().replace("\n", " ").replace("\r", " ")

def _ext_ok(name: str) -> bool:
    n = (name or "").lower()
    return any(n.endswith(e) for e in ALLOWED_EXTS)

def _clean_space(s: str) -> str:
    return re.sub(r"\s+", " ", str(s).strip())

def _name_parts(name: str) -> Tuple[str, str]:
    name = _clean_space(name)
    if "," in name:
        last, first = name.split(",", 1)
        return last.strip(), first.strip()
    parts = name.split(" ")
    if len(parts) >= 2:
        last = parts[-1].strip()
        first = " ".join(parts[:-1]).strip()
        return last, first
    return name.strip(), ""

def _canon_name(s: str) -> str:
    s = _clean_space(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.replace(".", "")
    s = re.sub(r"\s*,\s*", ",", s)
    return s.lower()

def _to_date(v) -> Optional[date]:
    if pd.isna(v): return None
    try: return pd.to_datetime(v).date()
    except Exception: return None

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
    """Load roster.xlsx or roster.csv if present. All columns optional; SSN may be blank."""
    p = _find_file(["roster.xlsx", "roster.csv"])
    if not p:
        return None
    try:
        if p.suffix.lower() == ".xlsx":
            df = pd.read_excel(p, dtype=str)
        else:
            df = pd.read_csv(p, dtype=str)
    except Exception as e:
        raise HTTPException(422, f"Roster load failed: {e}")
    if df.empty:
        return None

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

    if not name_col:
        return None

    out = pd.DataFrame({
        "employee_disp": df[name_col].astype(str).map(_clean_space),
        "employee_key":  df[name_col].astype(str).map(_canon_name),
        "ssn":           (df[ssn_col].astype(str).map(str.strip) if ssn_col else pd.Series([""]*len(df))),
        "rate_roster":   pd.to_numeric(df[rate_col].map(_to_number), errors="coerce") if rate_col else pd.Series([None]*len(df)),
        "department_roster": df[dept_col].astype(str).map(str.strip) if dept_col else pd.Series([""]*len(df)),
        "wtype_roster":      df[type_col].astype(str).map(str.strip) if type_col else pd.Series([""]*len(df)),
    }).dropna(subset=["employee_key"]).drop_duplicates(subset=["employee_key"], keep="last")

    return out


# ---------------- Sierra parsing ----------------
COMMON_EMP  = ["employee", "employee name", "name", "worker", "employee_name"]
COMMON_DATE = ["date", "work date", "day", "worked date"]
COMMON_HRS  = ["hours", "hrs", "total hours", "work hours", "a01", "regular", "reg"]
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
        s = df[col]
        try:
            parsed = pd.to_datetime(s, errors="coerce")
            sc = parsed.notna().mean()
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

def _guess_rate_col(df: pd.DataFrame) -> Optional[str]:
    c = _guess_col(df, COMMON_RATE)
    if c: return c
    best, score = None, -1
    for col in df.columns:
        try:
            v = pd.to_numeric(df[col].map(_to_number), errors="coerce")
        except Exception:
            continue
        ok = v.between(10, 200, inclusive="both").mean()
        nz = (v > 0).mean()
        sc = ok * 0.7 + nz * 0.3
        if sc > score:
            best, score = col, sc
    return best if score >= 0.2 else None

def _ca_daily_ot(h: float) -> Dict[str, float]:
    h = float(h or 0.0)
    reg = min(h, 8.0); ot = min(max(h-8.0, 0.0), 4.0); dt = max(h-12.0, 0.0)
    return {"REG": reg, "OT": ot, "DT": dt}

def _build_weekly(sierra_bytes: bytes) -> pd.DataFrame:
    # Read first sheet
    excel = pd.ExcelFile(io.BytesIO(sierra_bytes))
    sheet = excel.sheet_names[0]
    df = excel.parse(sheet)

    # Roster (optional)
    roster = _load_roster_df()
    rate_map = {}
    ssn_map  = {}
    dept_map = {}
    type_map = {}
    if roster is not None and not roster.empty:
        rate_map = {k: float(v) for k, v in zip(roster["employee_key"], roster["rate_roster"]) if pd.notna(v)}
        ssn_map  = {k: (s or "") for k, s in zip(roster["employee_key"], roster["ssn"])}
        dept_map = {k: (d or "") for k, d in zip(roster["employee_key"], roster["department_roster"])}
        type_map = {k: (t or "") for k, t in zip(roster["employee_key"], roster["wtype_roster"])}

    # Base columns
    emp_col  = _guess_employee_col(df)
    rate_col = _guess_rate_col(df)  # optional; fallback to roster
    if not emp_col:
        raise ValueError("Could not find 'Employee' column in Sierra file.")

    dep_col = _guess_col(df, ["department", "dept", "division"])
    typ_col = _guess_col(df, ["type", "employee type", "emp type", "pay type"])

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

    # MODE 1: Date+Hours
    date_col = _guess_date_col(df)
    hrs_col  = _guess_hours_col(df)
    has_date_hours = bool(date_col and hrs_col)

    # MODE 2: Mon–Sun columns
    day_candidates = [
        ("mon", ["mon","monday"]),
        ("tue", ["tue","tues","tuesday"]),
        ("wed", ["wed","weds","wednesday"]),
        ("thu", ["thu","thur","thurs","thursday"]),
        ("fri", ["fri","friday"]),
        ("sat", ["sat","saturday"]),
        ("sun", ["sun","sunday"]),
    ]
    day_cols: Dict[str,str] = {}
    normcols = { _std(c): c for c in df.columns }
    for day_key, aliases in day_candidates:
        found = None
        for alias in aliases:
            for k,c in normcols.items():
                if alias in k:
                    found = c
                    break
            if found: break
        if found:
            day_cols[day_key] = found
    has_weekdays = len(day_cols) >= 4

    if not has_date_hours and not has_weekdays:
        raise ValueError("Could not find Date+Hours or weekday (Mon..Sun) columns in Sierra file.")

    per_day_frames: List[pd.DataFrame] = []

    if has_date_hours:
        tmp = pd.DataFrame({
            "emp_key":    base["emp_key"],
            "employee":   base["employee"],
            "date":       df[date_col].map(_to_date),
            "hours":      pd.to_numeric(df[hrs_col].map(_to_number), errors="coerce").fillna(0.0).astype(float),
            "rate":       base["rate"],
            "department": base["department"],
            "wtype":      base["wtype"],
        })
        tmp = tmp[(tmp["employee"].str.len() > 0) & tmp["date"].notna() & (tmp["hours"] > 0)]
        per_day_frames.append(tmp)

    if has_weekdays:
        for day_key, col in day_cols.items():
            hrs = pd.to_numeric(df[col].map(_to_number), errors="coerce").fillna(0.0).astype(float)
            tmp = pd.DataFrame({
                "emp_key":    base["emp_key"],
                "employee":   base["employee"],
                "date":       day_key,
                "hours":      hrs,
                "rate":       base["rate"],
                "department": base["department"],
                "wtype":      base["wtype"],
            })
            tmp = tmp[(tmp["employee"].str.len() > 0) & (tmp["hours"] > 0)]
            per_day_frames.append(tmp)

    core = pd.concat(per_day_frames, ignore_index=True)

    # choose final rate/department/type per employee
    chosen_rate, first_dept, first_type = {}, {}, {}
    for k,g in core.groupby("emp_key"):
        rates = Counter([float(r) for r in g["rate"].tolist()])
        chosen_rate[k] = max(rates.items(), key=lambda kv: kv[1])[0] if rates else 0.0
        first_dept[k]  = g["department"].dropna().astype(str).replace("nan","").iloc[0] if not g.empty else ""
        wtyp = str(g["wtype"].dropna().astype(str).replace("nan","").iloc[0] if not g.empty else "")
        first_type[k]  = "S" if wtyp.upper().startswith("S") else "H"

    # split daily hours into REG/OT/DT (CA-like)
    parts = []
    for (k, emp, day), g in core.groupby(["emp_key","employee","date"], dropna=False):
        day_hours = float(g["hours"].sum())
        split = _ca_daily_ot(day_hours)
        parts.append({"emp_key": k, "employee": emp, "date": day,
                      "REG": split["REG"], "OT": split["OT"], "DT": split["DT"]})
    split_df = pd.DataFrame(parts)

    weekly_hours = split_df.groupby(["emp_key","employee"], dropna=False)[["REG","OT","DT"]].sum().reset_index()

    # Allocate $ proportionally across multiple sub-entries in same day
    dollars = defaultdict(lambda: {"REG_$":0.0,"OT_$":0.0,"DT_$":0.0})
    day_totals = {(r["emp_key"], r["date"]): r for _, r in split_df.iterrows()}
    for _, rec in core.iterrows():
        key = (rec["emp_key"], rec["date"])
        if key not in day_totals:
            continue
        tot = day_totals[key]
        day_sum = float(tot["REG"] + tot["OT"] + tot["DT"])
        if day_sum <= 0:
            continue
        portion   = float(rec["hours"]) / day_sum
        base_rate = float(rec["rate"]) if float(rec["rate"]) > 0 else float(chosen_rate.get(rec["emp_key"], 0.0))
        dollars[rec["emp_key"]]["REG_$"] += tot["REG"] * portion * base_rate
        dollars[rec["emp_key"]]["OT_$"]  += tot["OT"]  * portion * base_rate * 1.5
        dollars[rec["emp_key"]]["DT_$"]  += tot["DT"]  * portion * base_rate * 2.0

    weekly = weekly_hours.copy()
    weekly["REG_$"]   = weekly["emp_key"].map(lambda k: _money(dollars[k]["REG_$"]))
    weekly["OT_$"]    = weekly["emp_key"].map(lambda k: _money(dollars[k]["OT_$"]))
    weekly["DT_$"]    = weekly["emp_key"].map(lambda k: _money(dollars[k]["DT_$"]))
    weekly["TOTAL_$"] = weekly["REG_$"] + weekly["OT_$"] + weekly["DT_$"]

    weekly["rate"]       = weekly["emp_key"].map(lambda k: _money(chosen_rate.get(k, 0.0)))
    weekly["department"] = weekly["emp_key"].map(lambda k: first_dept.get(k, ""))
    weekly["Status"]     = "A"
    weekly["Type"]       = weekly["emp_key"].map(lambda k: first_type.get(k, "H"))

    # <- SSN OPTIONAL (from roster if available; otherwise blank)
    weekly["ssn"] = weekly["emp_key"].map(lambda k: ssn_map.get(k, "")).fillna("").astype(str)

    for c in ["REG","OT","DT"]:
        weekly[c] = weekly[c].map(_money)

    weekly = weekly[[
        "emp_key","employee","ssn","Status","Type","rate","department",
        "REG","OT","DT","REG_$","OT_$","DT_$","TOTAL_$"
    ]].copy()

    # Merge roster (best-effort only) to backfill blanks — never error on SSN
    if roster is not None and not roster.empty:
        w = weekly.copy()
        w["k_lf"]  = w["employee"].map(lambda s: ",".join(_name_parts(s)).lower())
        r = roster.copy()
        r["k_lf"]  = r["employee_disp"].map(lambda s: ",".join(_name_parts(s)).lower())
        m = w.merge(r[["k_lf","ssn","rate_roster","department_roster","wtype_roster"]],
                    on="k_lf", how="left")

        # Fill SSN only if blank
        m["ssn_x"] = m["ssn_x"].fillna("")
        m["ssn_y"] = m["ssn_y"].fillna("")
        m["ssn"] = m.apply(lambda row: row["ssn_x"] if str(row["ssn_x"]).strip() != "" else row["ssn_y"], axis=1)

        # Fill other fields if missing
        m["rate"] = m.apply(lambda row: row["rate"] if row["rate"]>0 else _to_number(row.get("rate_roster")), axis=1)
        m["department"] = m.apply(lambda row: row["department"] if str(row["department"]).strip() not in ("","nan","None")
                                  else (row.get("department_roster") or ""), axis=1)
        m["Type"] = m.apply(lambda row: row["Type"] if str(row["Type"]).strip() not in ("","nan","None")
                            else ("S" if str(row.get("wtype_roster","")).upper().startswith("S") else "H"), axis=1)

        m = m.drop(columns=["k_lf","ssn_x","ssn_y","rate_roster","department_roster","wtype_roster"])
        weekly = m

    weekly = weekly.drop(columns=["emp_key"])
    # hard guarantee: SSN column exists and is string
    if "ssn" not in weekly.columns:
        weekly["ssn"] = ""
    weekly["ssn"] = weekly["ssn"].fillna("").astype(str)

    return weekly


# ---------------- routes ----------------
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
        weekly = _build_weekly(sierra_bytes)
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

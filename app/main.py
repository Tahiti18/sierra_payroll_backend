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

app = FastAPI(title="Sierra → WBS Payroll Converter", version="7.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # lock this down to your frontend origin later
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ALLOWED_EXTS = (".xlsx", ".xls")

# ---------------- file discovery ----------------
BASE_DIR = Path(__file__).resolve().parents[1]
SEARCH_DIRS = [BASE_DIR, BASE_DIR / "app", BASE_DIR / "app" / "data", BASE_DIR / "server"]

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

# ---------------- helpers ----------------
def _std(s: str) -> str:
    return (s or "").strip().lower().replace("\n"," ").replace("\r"," ")

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

def _key_last_first(last: str, first: str) -> str:
    return f"{last},{first}".lower()

def _key_first_last(first: str, last: str) -> str:
    return f"{first} {last}".lower()

def _key_last_finit(last: str, first: str) -> str:
    return f"{last},{(first[:1] or '').upper()}".lower()

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
    """Robust numeric parser: handles $, commas, spaces, text; returns 0.0 on failure."""
    if x is None or (isinstance(x, float) and pd.isna(x)): return 0.0
    s = str(x).strip()
    if s == "" or s.lower() in ("nan","none"): return 0.0
    s = re.sub(r"[^0-9\.\-]", "", s)
    try:
        return float(s) if s not in ("", "-", ".", "-.") else 0.0
    except Exception:
        try: return float(x)
        except Exception: return 0.0

def _money(x: float) -> float:
    try: return round(float(x or 0.0), 2)
    except Exception: return 0.0

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

# ---------------- template & roster loaders ----------------
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

    if not name_col:
        return None

    out = pd.DataFrame({
        "employee_disp": df[name_col].astype(str).map(_clean_space),
        "employee_key":  df[name_col].astype(str).map(_canon_name),
        "ssn":           (df[ssn_col].astype(str).map(str.strip) if ssn_col else pd.Series([""]*len(df))),
        "rate_roster":   pd.to_numeric(df[rate_col].map(_to_number), errors="coerce") if rate_col else pd.Series([None]*len(df)),
        "department_roster": df[dept_col].astype(str).map(str.strip) if dept_col else pd.Series([""]*len(df)),
        "wtype_roster":      df[type_col].astype(str).map(str.strip) if type_col else pd.Series([""]*len(df)),
    })

    out["k_lf"]  = out["employee_disp"].map(lambda s: _key_last_first(*_name_parts(s)))
    out["k_fl"]  = out["employee_disp"].map(lambda s: _key_first_last(*_name_parts(s)[::-1]))
    out["k_lfi"] = out["employee_disp"].map(lambda s: _key_last_finit(*_name_parts(s)))
    out = out.dropna(subset=["employee_key"]).drop_duplicates(subset=["employee_key"], keep="last")
    return out

# ---------------- smart column guessing for Sierra ----------------
COMMON_EMP = ["employee","employee name","name","worker","employee_name"]
COMMON_DATE= ["date","work date","day","worked date","check date","report date"]
COMMON_HRS = ["hours","hrs","total hours","work hours","a01","regular","reg"]
COMMON_RATE= ["rate","pay rate","hourly rate","wage","base rate","pay rate a"]

def _guess_employee_col(df: pd.DataFrame) -> Optional[str]:
    c = _find_col(df, COMMON_EMP)
    if c: return c
    best, score = None, -1
    for col in df.columns:
        s = df[col].astype(str)
        looks = s.str.contains(r"[A-Za-z],\s*[A-Za-z]", regex=True, na=False).mean()
        if looks > score:
            best, score = col, looks
    return best if score >= 0.2 else None

def _guess_date_col(df: pd.DataFrame) -> Optional[str]:
    c = _find_col(df, COMMON_DATE)
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
    c = _find_col(df, COMMON_HRS)
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
    c = _find_col(df, COMMON_RATE)
    if c: return c
    best, score = None, -1
    for col in df.columns:
        try:
            v = pd.to_numeric(df[col].map(_to_number), errors="coerce")
        except Exception:
            continue
        ok = v.between(10, 200, inclusive="both").mean()
        nonzero = (v > 0).mean()
        sc = ok * 0.7 + nonzero * 0.3
        if sc > score:
            best, score = col, sc
    return best if score >= 0.2 else None

# ---------------- build weekly (supports Date+Hours OR Mon–Sun columns) ----------------
def _ca_daily_ot(h: float) -> Dict[str, float]:
    h = float(h or 0.0)
    reg = min(h, 8.0); ot = min(max(h-8.0, 0.0), 4.0); dt = max(h-12.0, 0.0)
    return {"REG": reg, "OT": ot, "DT": dt}

def build_weekly_from_sierra(xlsx_bytes: bytes, sheet_name: Optional[str]=None) -> pd.DataFrame:
    excel = pd.ExcelFile(io.BytesIO(xlsx_bytes))
    sheet = sheet_name or excel.sheet_names[0]
    df = excel.parse(sheet)

    roster_df = _load_roster_df()
    rate_map = {}
    if roster_df is not None and not roster_df.empty:
        rate_map = {k: float(v) for k, v in zip(roster_df["employee_key"], roster_df["rate_roster"]) if pd.notna(v)}

    emp_col  = _guess_employee_col(df)
    rate_col = _guess_rate_col(df)  # optional; fallback to roster
    if not emp_col:
        raise ValueError("Sierra header detection failed; missing: employee")

    dep_col = _find_col(df, ["department","dept","division"])
    typ_col = _find_col(df, ["type","employee type","emp type","pay type"])

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

    if not per_day_frames:
        raise ValueError("No valid rows found in Sierra sheet after parsing.")

    core = pd.concat(per_day_frames, ignore_index=True)

    chosen_rate, first_dept, first_type = {}, {}, {}
    for k,g in core.groupby("emp_key"):
        rates = Counter([float(r) for r in g["rate"].tolist()])
        chosen_rate[k] = max(rates.items(), key=lambda kv: kv[1])[0] if rates else 0.0
        first_dept[k]  = g["department"].dropna().astype(str).replace("nan","").iloc[0] if not g.empty else ""
        wtyp = str(g["wtype"].dropna().astype(str).replace("nan","").iloc[0] if not g.empty else "")
        first_type[k]  = "S" if wtyp.upper().startswith("S") else "H"

    parts = []
    for (k, emp, day), g in core.groupby(["emp_key","employee","date"], dropna=False):
        day_hours = float(g["hours"].sum())
        split = _ca_daily_ot(day_hours)
        parts.append({"emp_key": k, "employee": emp, "date": day,
                      "REG": split["REG"], "OT": split["OT"], "DT": split["DT"]})
    split_df = pd.DataFrame(parts)

    weekly_hours = split_df.groupby(["emp_key","employee"], dropna=False)[["REG","OT","DT"]].sum().reset_index()

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
        portion = float(rec["hours"]) / day_sum
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
    weekly["ssn"]        = ""   # filled from roster if present
    weekly["Status"]     = "A"
    weekly["Type"]       = weekly["emp_key"].map(lambda k: first_type.get(k, "H"))

    for c in ["REG","OT","DT"]:
        weekly[c] = weekly[c].map(_money)

    weekly = weekly[[
        "emp_key","employee","ssn","Status","Type","rate","department",
        "REG","OT","DT","REG_$","OT_$","DT_$","TOTAL_$"
    ]].copy()

    weekly, _ = _apply_roster_defaults_by_key(weekly, roster_df)
    return weekly

def _apply_roster_defaults_by_key(weekly_df: pd.DataFrame, roster: Optional[pd.DataFrame]) -> Tuple[pd.DataFrame, List[str]]:
    weekly_df = weekly_df.copy()
    weekly_df["k_lf"]  = weekly_df["employee"].map(lambda s: _key_last_first(*_name_parts(s)))
    weekly_df["k_fl"]  = weekly_df["employee"].map(lambda s: _key_first_last(*_name_parts(s)[::-1]))
    weekly_df["k_lfi"] = weekly_df["employee"].map(lambda s: _key_last_finit(*_name_parts(s)))

    if roster is None or roster.empty:
        missing = sorted(weekly_df["employee"].unique().tolist())
        final = weekly_df.drop(columns=["k_lf","k_fl","k_lfi","emp_key"])
        return final, missing

    def _merge_on(key):
        cols = ["employee_disp","ssn","rate_roster","department_roster","wtype_roster","k_lf","k_fl","k_lfi","employee_key"]
        r = roster[cols].copy()
        return weekly_df.merge(r, left_on=key, right_on=key, how="left", suffixes=("","_r"))

    m1 = _merge_on("k_lf")
    m2 = _merge_on("k_fl")
    for col in ["employee_disp","ssn","rate_roster","department_roster","wtype_roster"]:
        m1[col] = m1[col].fillna(m2[col])
    m3 = _merge_on("k_lfi")
    for col in ["employee_disp","ssn","rate_roster","department_roster","wtype_roster"]:
        m1[col] = m1[col].fillna(m3[col])

    m1["ssn"] = m1["ssn"].fillna("")
    m1["department"] = m1.apply(
        lambda r: r["department"] if str(r["department"]).strip() not in ("", "nan", "None")
        else (r["department_roster"] if pd.notna(r["department_roster"]) else ""), axis=1
    )
    m1["Type"] = m1.apply(
        lambda r: r["Type"] if str(r["Type"]).strip() not in ("", "nan", "None")
        else ("S" if str(r.get("wtype_roster","")).upper().startswith("S") else "H"), axis=1
    )
    m1["rate"] = m1.apply(
        lambda r: r["rate"] if float(r["rate"] or 0) > 0
        else (_to_number(r["rate_roster"]) if pd.notna(r["rate_roster"]) else 0.0), axis=1
    )

    missing = sorted(m1.loc[m1["ssn"].astype(str).isin(["", "nan", "None", ""]), "employee"].unique().tolist())
    final = m1[[
        "employee","ssn","Status","Type","rate","department",
        "REG","OT","DT","REG_$","OT_$","DT_$","TOTAL_$"
    ]].copy()
    return final, missing

# ---------------- template write ----------------
def _find_wbs_header(ws: Worksheet) -> Tuple[int, Dict[str, int]]:
    # Must have these core labels on the header row (case-insensitive)
    targets = ["ssn","employee name","status","type","pay rate","dept","a01","a02","a03"]
    for r in range(1, ws.max_row + 1):
        lower = [(_std(str(ws.cell(r, c).value)) if ws.cell(r, c).value is not None else "")
                 for c in range(1, ws.max_column + 1)]
        if all(any(t == lv for lv in lower) for t in targets):
            col_map = {lv: c for c, lv in enumerate(lower, start=1)}
            # dollar columns are optional; write only if present
            def pick(*names):
                for n in names:
                    if n in col_map:
                        return col_map[n]
                return None
            return r, {
                "ssn":   col_map.get("ssn"),
                "name":  col_map.get("employee name"),
                "status": col_map.get("status"),
                "type":   col_map.get("type"),
                "rate":   col_map.get("pay rate"),
                "dept":   col_map.get("dept"),
                "a01":    col_map.get("a01"),
                "a02":    col_map.get("a02"),
                "a03":    col_map.get("a03"),
                "reg$":   pick("reg $", "a01 $", "regular $", "a01$"),
                "ot$":    pick("ot $",  "a02 $", "overtime $", "a02$"),
                "dt$":    pick("dt $",  "a03 $", "doubletime $", "a03$"),
                "total$": pick("total $", "total$", "grand total $"),
                # Some templates label the far-right pink/orange column simply "total"
                "total":  pick("total"),
            }
    raise HTTPException(
        422,
        detail="Could not locate WBS header row in template (need SSN, Employee Name, Status, Type, Pay Rate, Dept, A01/A02/A03)."
    )

def write_into_wbs_template(template_bytes: bytes, weekly: pd.DataFrame) -> bytes:
    """
    Writes data ONLY to the 'WEEKLY' sheet to match the WBS layout.
    - Populates SSN, Employee Name, Status, Type, Pay Rate, Dept, A01, A02, A03.
    - Also populates REG$/OT$/DT$/TOTAL$ when those columns exist in the header.
    - If no explicit 'Total $' column, fills the far-right 'Total' column if present.
    - Does NOT create any extra sheets.
    """
    wb = load_workbook(io.BytesIO(template_bytes))
    ws = wb["WEEKLY"] if "WEEKLY" in wb.sheetnames else wb.active

    header_row, cols = _find_wbs_header(ws)
    first_data_row = header_row + 1

    # Clear old data rows (keep formatting)
    scan_col = cols.get("name") or cols.get("ssn") or 2
    last = ws.max_row
    last_data = first_data_row - 1
    for r in range(first_data_row, last + 1):
        if ws.cell(r, scan_col).value not in (None, ""):
            last_data = r
    if last_data >= first_data_row:
        ws.delete_rows(first_data_row, last_data - first_data_row + 1)

    # Append weekly rows
    for _, row in weekly.iterrows():
        values = [""] * max(ws.max_column, 64)
        if cols.get("ssn"):     values[cols["ssn"] - 1]    = row.get("ssn", "")
        if cols.get("name"):    values[cols["name"] - 1]   = row.get("employee", "")
        if cols.get("status"):  values[cols["status"] - 1] = row.get("Status", "A")
        if cols.get("type"):    values[cols["type"] - 1]   = row.get("Type", "H")
        if cols.get("rate"):    values[cols["rate"] - 1]   = row.get("rate", 0.0)
        if cols.get("dept"):    values[cols["dept"] - 1]   = row.get("department", "")
        if cols.get("a01"):     values[cols["a01"] - 1]    = row.get("REG", 0.0)
        if cols.get("a02"):     values[cols["a02"] - 1]    = row.get("OT", 0.0)
        if cols.get("a03"):     values[cols["a03"] - 1]    = row.get("DT", 0.0)

        # Money columns if present
        if cols.get("reg$"):    values[cols["reg$"] - 1]   = row.get("REG_$", 0.0)
        if cols.get("ot$"):     values[cols["ot$"] - 1]    = row.get("OT_$", 0.0)
        if cols.get("dt$"):     values[cols["dt$"] - 1]    = row.get("DT_$", 0.0)
        if cols.get("total$"):  values[cols["total$"] - 1] = row.get("TOTAL_$", 0.0)

        # If there is no explicit TOTAL $ column, populate the far-right 'Total' if present
        if not cols.get("total$") and cols.get("total"):
            values[cols["total"] - 1] = row.get("TOTAL_$", 0.0)

        ws.append(values)

    # Spacer + TOTAL row
    ws.append([])
    totals = {
        "REG":     float(weekly["REG"].sum()),
        "OT":      float(weekly["OT"].sum()),
        "DT":      float(weekly["DT"].sum()),
        "REG_$":   float(weekly["REG_$"].sum()),
        "OT_$":    float(weekly["OT_$"].sum()),
        "DT_$":    float(weekly["DT_$"].sum()),
        "TOTAL_$": float(weekly["TOTAL_$"].sum()),
    }
    row_vals = [""] * max(ws.max_column, 64)
    if cols.get("name"):   row_vals[cols["name"] - 1]  = "TOTAL"
    if cols.get("a01"):    row_vals[cols["a01"] - 1]   = _money(totals["REG"])
    if cols.get("a02"):    row_vals[cols["a02"] - 1]   = _money(totals["OT"])
    if cols.get("a03"):    row_vals[cols["a03"] - 1]   = _money(totals["DT"])
    if cols.get("reg$"):   row_vals[cols["reg$"] - 1]  = _money(totals["REG_$"])
    if cols.get("ot$"):    row_vals[cols["ot$"] - 1]   = _money(totals["OT_$"])
    if cols.get("dt$"):    row_vals[cols["dt$"] - 1]   = _money(totals["DT_$"])
    # Prefer explicit TOTAL $; otherwise, fill far-right 'Total' if available
    if cols.get("total$"):
        row_vals[cols["total$"] - 1] = _money(totals["TOTAL_$"])
    elif cols.get("total"):
        row_vals[cols["total"] - 1] = _money(totals["TOTAL_$"])
    ws.append(row_vals)

    # ONE sheet only. No extra tabs.
    bio = io.BytesIO()
    wb.save(bio); bio.seek(0)
    return bio.read()

# ---------------- routes ----------------
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

    out_name = f"WBS_Payroll_{datetime.utcnow().date()}.xlsx"
    return StreamingResponse(
        io.BytesIO(out_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={out_name}"}
    )

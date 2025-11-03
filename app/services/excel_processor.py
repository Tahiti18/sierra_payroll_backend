# app/services/excel_processor.py
# Sierra → WBS processor (strict compute; no template copying)
# - Reads the uploaded Sierra workbook (all sheets), computes daily hours per employee
# - Daily rule: first 8 REG, next 4 OT, hours > 12 DT
# - Weekly overlay: if (sum(REG) > 40) move excess from REG → OT (DT unaffected)
# - Roster: pulls Status, Type, Dept, Pay Rate, SSN from roster.xlsx (first sheet)
# - Template: loads wbs_template.xlsx, finds columns by header text, writes numeric values only
# - PC columns (PC HRS/PC TTL by weekday) are written ONLY if Type == "PC"
# - Zeros are written as blanks to keep the sheet clean

from __future__ import annotations

import io
import math
from datetime import datetime, time, timedelta
from typing import Dict, List, Tuple, Optional

import pandas as pd
from openpyxl import load_workbook
from openpyxl.cell.cell import Cell
from openpyxl.worksheet.worksheet import Worksheet


# ---------- Helpers ----------

WDAYS = ["MON", "TUE", "WED", "THU", "FRI"]

def _norm_name(x: str) -> str:
    if isinstance(x, str):
        return " ".join(x.split()).strip().lower()
    return ""

def _to_time(x) -> Optional[time]:
    """Coerce mixed Excel/time strings to time or None."""
    if pd.isna(x) or x == "":
        return None
    if isinstance(x, time):
        return x
    if isinstance(x, datetime):
        return x.time()
    s = str(x).strip()
    for fmt in ("%H:%M", "%I:%M %p", "%H.%M", "%I.%M %p"):
        try:
            return datetime.strptime(s, fmt).time()
        except Exception:
            pass
    # Excel float days
    try:
        # if it's a float like 0.5 (12:00)
        f = float(s)
        if 0 <= f < 1:
            total_seconds = int(round(f * 24 * 3600))
            return (datetime.min + timedelta(seconds=total_seconds)).time()
    except Exception:
        pass
    return None

def _to_date(x) -> Optional[datetime]:
    if pd.isna(x) or x == "":
        return None
    if isinstance(x, datetime):
        return x
    try:
        return pd.to_datetime(x).to_pydatetime()
    except Exception:
        return None

def _hours_diff(start: Optional[time], finish: Optional[time],
                l_st: Optional[time], l_fn: Optional[time]) -> float:
    """Compute hours = (finish - start) - lunch, clamp ≥ 0."""
    if not start or not finish:
        return 0.0
    dt0 = datetime(2000, 1, 1, start.hour, start.minute, start.second)
    dt1 = datetime(2000, 1, 1, finish.hour, finish.minute, finish.second)
    if dt1 < dt0:
        # crossed midnight – treat as same-day overtime; add 24h
        dt1 += timedelta(days=1)
    dur = dt1 - dt0
    lunch = timedelta()
    if l_st and l_fn:
        ls = datetime(2000, 1, 1, l_st.hour, l_st.minute, l_st.second)
        lf = datetime(2000, 1, 1, l_fn.hour, l_fn.minute, l_fn.second)
        if lf < ls:
            lf += timedelta(days=1)
        lunch = lf - ls
    hours = max(0.0, (dur - lunch).total_seconds() / 3600.0)
    # round to 2 decimals to avoid float dust
    return round(hours + 1e-9, 2)

def _split_daily(hours: float) -> Tuple[float, float, float]:
    """Daily split: 0–8 REG, 8–12 OT, >12 DT."""
    reg = min(8.0, hours)
    ot = min(4.0, max(0.0, hours - 8.0))
    dt = max(0.0, hours - 12.0)
    return round(reg, 2), round(ot, 2), round(dt, 2)

def _overlay_weekly(reg_total: float, ot_total: float) -> Tuple[float, float]:
    """Weekly overlay: if reg_total > 40, move the excess from REG → OT."""
    if reg_total > 40.0:
        excess = round(reg_total - 40.0, 2)
        reg_total = round(reg_total - excess, 2)
        ot_total = round(ot_total + excess, 2)
    return reg_total, ot_total

def _num_or_blank(x: float) -> Optional[float]:
    """Write blank instead of hard 0. Keeps the sheet clean."""
    if x is None:
        return None
    return None if abs(x) < 1e-9 else round(float(x), 3)

# ---------- Load roster ----------

def load_roster(roster_path: str) -> List[Dict]:
    """Return list of roster rows preserving order."""
    df = pd.read_excel(roster_path, sheet_name=0, engine="openpyxl")
    # Normalize columns we care about
    cols = {c.lower().strip(): c for c in df.columns}
    def pick(*names):
        for n in names:
            if n in cols:
                return cols[n]
        return None

    col_name = pick("employee name", "name", "employee")
    col_rate = pick("pay rate", "rate", "payrate")
    col_type = pick("type", "pay type", "pay")
    col_dept = pick("dept", "department")
    col_status = pick("status")
    col_ssn = pick("ssn", "social security", "social security number")

    out = []
    for _, r in df.iterrows():
        nm = str(r.get(col_name, "")).strip()
        if not nm:
            continue
        out.append({
            "name": nm,
            "norm": _norm_name(nm),
            "rate": float(r.get(col_rate, 0) or 0),
            "type": str(r.get(col_type, "") or "").strip(),
            "dept": str(r.get(col_dept, "") or "").strip(),
            "status": str(r.get(col_status, "") or "").strip(),
            "ssn": str(r.get(col_ssn, "") or "").strip(),
        })
    return out

# ---------- Parse Sierra ----------

def parse_sierra_all_sheets(sierra_bytes: bytes) -> pd.DataFrame:
    """Read all sheets and return a single normalized DataFrame with
       columns: name, date, weekday (0=Mon), hours, rate (if present)"""
    sio = io.BytesIO(sierra_bytes)
    xls = pd.ExcelFile(sio, engine="openpyxl")
    frames = []
    for sheet in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet, engine="openpyxl")
        if df.empty:
            continue
        # Map probable headers
        cols = {str(c).strip().lower(): c for c in df.columns}

        def pick(*names):
            for n in names:
                if n in cols:
                    return cols[n]
            return None

        c_name  = pick("name", "employee", "employee name")
        c_days  = pick("days", "date", "day")
        c_start = pick("start",)
        c_lst   = pick("lnch st.", "lunch st", "lunch start", "lnch st")
        c_lfn   = pick("lnch fnsh", "lunch fnsh", "lunch finish", "lnch fn")
        c_finish= pick("finish", "end", "stop")
        c_hours = pick("hours", "hrs")
        c_rate  = pick("rate", "pay rate")

        if c_name is None:
            # try to detect a typical Sierra block; skip if not present
            continue

        local = pd.DataFrame()
        local["name"] = df[c_name].astype(str).fillna("").str.strip()

        # date
        if c_days and c_days in df:
            local["date"] = df[c_days]
        else:
            local["date"] = None

        # hours
        if c_hours and c_hours in df:
            local["hours"] = pd.to_numeric(df[c_hours], errors="coerce").fillna(0.0)
        else:
            # compute from times
            starts = df[c_start] if c_start in df else None
            l_sts  = df[c_lst]   if c_lst   in df else None
            l_fns  = df[c_lfn]   if c_lfn   in df else None
            finishes = df[c_finish] if c_finish in df else None

            vals = []
            for i in range(len(df)):
                st  = _to_time(starts.iloc[i])  if starts is not None else None
                ls  = _to_time(l_sts.iloc[i])   if l_sts  is not None else None
                lf  = _to_time(l_fns.iloc[i])   if l_fns  is not None else None
                fn  = _to_time(finishes.iloc[i])if finishes is not None else None
                vals.append(_hours_diff(st, fn, ls, lf))
            local["hours"] = vals

        # rate (if present)
        if c_rate and c_rate in df:
            local["rate"] = pd.to_numeric(df[c_rate], errors="coerce")
        else:
            local["rate"] = None

        frames.append(local)

    if not frames:
        return pd.DataFrame(columns=["name", "date", "weekday", "hours", "rate"])

    merged = pd.concat(frames, ignore_index=True)
    # Clean rows
    merged["name"] = merged["name"].astype(str).str.strip()
    merged = merged[merged["name"] != ""].copy()

    # Date/weekday
    merged["date"] = merged["date"].apply(_to_date)
    merged["weekday"] = merged["date"].apply(lambda d: d.weekday() if d else None)
    merged["hours"] = pd.to_numeric(merged["hours"], errors="coerce").fillna(0.0)
    merged["rate"] = pd.to_numeric(merged["rate"], errors="coerce")
    return merged

# ---------- Compute per-employee ----------

class EmpWeek:
    __slots__ = ("name","norm","by_day","reg","ot","dt","rate_hint")
    def __init__(self, name: str):
        self.name = name
        self.norm = _norm_name(name)
        self.by_day = {k:0.0 for k in WDAYS}  # Mon..Fri
        self.reg = 0.0
        self.ot  = 0.0
        self.dt  = 0.0
        self.rate_hint: Optional[float] = None

def compute_from_sierra(sierra_df: pd.DataFrame) -> Dict[str, EmpWeek]:
    people: Dict[str, EmpWeek] = {}

    for _, r in sierra_df.iterrows():
        name = str(r.get("name","")).strip()
        if not name:
            continue
        ew = people.get(_norm_name(name))
        if ew is None:
            ew = EmpWeek(name)
            people[ew.norm] = ew

        wd = r.get("weekday", None)
        if wd is None or wd < 0 or wd > 6:
            # Ignore rows with no valid weekday
            continue
        # Only Mon..Fri contribute to by_day PC columns
        if 0 <= wd <= 4:
            day_key = WDAYS[wd]
        else:
            day_key = None

        hrs = float(r.get("hours", 0.0) or 0.0)
        reg, ot, dt = _split_daily(hrs)
        ew.reg += reg
        ew.ot  += ot
        ew.dt  += dt
        if day_key:
            ew.by_day[day_key] += hrs

        rate = r.get("rate", None)
        if pd.notna(rate):
            try:
                rr = float(rate)
                if rr > 0:
                    ew.rate_hint = rr
            except Exception:
                pass

    # Weekly overlay on REG>40 → OT
    for ew in people.values():
        ew.reg, ew.ot = _overlay_weekly(round(ew.reg,2), round(ew.ot,2))
        # Round everything
        ew.reg = round(ew.reg,2)
        ew.ot  = round(ew.ot,2)
        ew.dt  = round(ew.dt,2)
        for k in WDAYS:
            ew.by_day[k] = round(ew.by_day[k],2)

    return people

# ---------- Template writing ----------

def _find_header_row(ws: Worksheet) -> Tuple[int, Dict[str, int]]:
    """Locate the row that contains core headers and return (row_index, header_map) where
       header_map maps canonical header tokens -> column index (1-based)."""
    want = [
        "SSN","Employee Name","Status","Type","Pay","Pay Rate","Dept",
        "REGULAR","OVERTIME","DOUBLETIME","VACATION","SICK","HOLIDAY","BONUS","COMMISSION",
        # PC columns
        "PC HRS MON","PC TTL MON","PC HRS TUE","PC TTL TUE","PC HRS WED","PC TTL WED",
        "PC HRS THU","PC TTL THU","PC HRS FRI","PC TTL FRI",
    ]
    want_lower = [w.lower() for w in want]

    for r in range(1, min(ws.max_row, 40)+1):
        row_vals = [str((ws.cell(row=r, column=c).value or "")).strip() for c in range(1, ws.max_column+1)]
        norm = [v.lower() for v in row_vals]
        hits = {}
        for idx, label in enumerate(want_lower):
            if label in norm:
                hits[want[idx]] = norm.index(label) + 1
        # minimally require REGULAR / OVERTIME to consider this the header row
        if "REGULAR" in hits and "OVERTIME" in hits and "Employee Name" in hits:
            return r, hits
    raise RuntimeError("Could not locate WBS header row (REGULAR/OVERTIME/Employee Name).")

def _set(ws: Worksheet, row: int, col: Optional[int], val: Optional[float|str]):
    if not col:
        return
    cell = ws.cell(row=row, column=col)
    if isinstance(val, (int, float)):
        vv = _num_or_blank(float(val))
        cell.value = vv
    else:
        cell.value = val if val not in (None, "", "0", 0) else None

def write_output(template_path: str,
                 roster: List[Dict],
                 computed: Dict[str, EmpWeek]) -> bytes:
    wb = load_workbook(template_path)
    ws = wb.active

    header_row, cols = _find_header_row(ws)
    first_data_row = header_row + 1

    # Build quick lookup from roster order
    def rget(nrm: str) -> Optional[Dict]:
        for r in roster:
            if r["norm"] == nrm:
                return r
        return None

    # Iterate roster order; include any extra computed names not in roster at the end
    seen_norms = set()
    out_rows = []

    for r in roster:
        ew = computed.get(r["norm"])
        out_rows.append((r, ew))
        seen_norms.add(r["norm"])

    for nrm, ew in computed.items():
        if nrm not in seen_norms:
            # append unknown employees after roster block
            out_rows.append(({
                "name": ew.name, "norm": ew.norm, "rate": ew.rate_hint or 0.0,
                "type": "", "dept": "", "status": "", "ssn": ""
            }, ew))
            seen_norms.add(nrm)

    # Clear any existing data block under header (safety)
    # (We’ll write only the rows we need; remaining area stays as-is.)

    row = first_data_row
    for rinfo, ew in out_rows:
        # Choose rate: roster wins; else rate_hint; else 0
        rate = float(rinfo.get("rate") or 0.0)
        if rate <= 0 and ew and ew.rate_hint:
            rate = float(ew.rate_hint)

        emp_name = rinfo.get("name") or (ew.name if ew else "")
        emp_type = (rinfo.get("type") or "").strip()
        emp_status = (rinfo.get("status") or "").strip()
        emp_dept = (rinfo.get("dept") or "").strip()
        emp_ssn = (rinfo.get("ssn") or "").strip()

        # Totals
        reg = ew.reg if ew else 0.0
        ot  = ew.ot  if ew else 0.0
        dt  = ew.dt  if ew else 0.0

        # PC days: ONLY if Type == "PC"
        by_day = {k: 0.0 for k in WDAYS}
        if ew and emp_type.lower() == "pc":
            by_day = ew.by_day

        # Write row
        _set(ws, row, cols.get("SSN"), emp_ssn)
        _set(ws, row, cols.get("Employee Name"), emp_name)
        _set(ws, row, cols.get("Status"), emp_status)
        _set(ws, row, cols.get("Type"), emp_type)
        _set(ws, row, cols.get("Pay"), None)  # not used; keep blank
        _set(ws, row, cols.get("Pay Rate"), rate)
        _set(ws, row, cols.get("Dept"), emp_dept)

        _set(ws, row, cols.get("REGULAR"), reg)
        _set(ws, row, cols.get("OVERTIME"), ot)
        _set(ws, row, cols.get("DOUBLETIME"), dt)
        _set(ws, row, cols.get("VACATION"), 0.0)
        _set(ws, row, cols.get("SICK"), 0.0)
        _set(ws, row, cols.get("HOLIDAY"), 0.0)
        _set(ws, row, cols.get("BONUS"), 0.0)
        _set(ws, row, cols.get("COMMISSION"), 0.0)

        # PC HRS / PC TTL
        for i, d in enumerate(WDAYS):
            hrs_col = cols.get(f"PC HRS {d}")
            ttl_col = cols.get(f"PC TTL {d}")
            hrs = by_day[d] if emp_type.lower() == "pc" else 0.0
            ttl = hrs * rate if emp_type.lower() == "pc" else 0.0
            _set(ws, row, hrs_col, hrs)
            _set(ws, row, ttl_col, ttl)

        row += 1

    # Save to bytes
    out = io.BytesIO()
    wb.save(out)
    return out.getvalue()

# ---------- Public entrypoint used by the FastAPI layer ----------

def process_excel(sierra_file_bytes: bytes) -> bytes:
    """
    Called by the API layer.
    - Reads roster.xlsx and wbs_template.xlsx from the working directory.
    - Returns a binary Excel file ready to send back to the client.
    """
    roster = load_roster("roster.xlsx")
    sierra_df = parse_sierra_all_sheets(sierra_file_bytes)
    computed = compute_from_sierra(sierra_df)
    return write_output("wbs_template.xlsx", roster, computed)

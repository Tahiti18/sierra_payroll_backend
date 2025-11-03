# app/services/excel_processor.py
# Sierra → WBS translator (FULL FILE – computes Hours from Start/Lunch/Finish; no dependency on prefilled template numbers)

from __future__ import annotations

import io
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple, List

import pandas as pd
from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet
from openpyxl.utils import get_column_letter

# ---------- Paths ----------
HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent.parent
REPO_ROOT = PROJECT_ROOT.parent

WBS_TEMPLATE_PATH = REPO_ROOT / "wbs_template.xlsx"
ROSTER_PATH       = REPO_ROOT / "roster.xlsx"

# ---------- helpers ----------
def _num(s) -> Optional[float]:
    if s is None:
        return None
    if isinstance(s, (int, float)):
        try:
            return float(s)
        except Exception:
            return None
    ss = str(s).strip().replace("$", "").replace(",", "")
    m = re.search(r"-?\d+(\.\d+)?", ss)
    return float(m.group(0)) if m else None

def _safe_int(x) -> Optional[int]:
    try:
        if pd.isna(x):
            return None
    except Exception:
        pass
    try:
        return int(float(str(x).replace(",", "").strip()))
    except Exception:
        return None

def _normalize_name_for_join(name: str) -> Tuple[str, str]:
    if not isinstance(name, str):
        return ("", "")
    s = " ".join(name.replace(",", " ").split()).strip()
    if not s:
        return ("", "")
    parts = s.split(" ")
    if "," in name:
        last, rest = [x.strip() for x in name.split(",", 1)]
        first = rest.split(" ")[0] if rest else ""
    else:
        first, last = parts[0], parts[-1]
    ln = "".join(ch for ch in last.lower() if ch.isalpha())
    fn = "".join(ch for ch in first.lower() if ch.isalpha())
    return ln, fn

def _best_join(left_df: pd.DataFrame, right_df: pd.DataFrame,
               left_name_col: str, right_name_col: str) -> pd.DataFrame:
    L = left_df.copy(); R = right_df.copy()
    L[["__ln","__fn"]] = L[left_name_col].apply(lambda s: pd.Series(_normalize_name_for_join(str(s))))
    R[["__ln","__fn"]] = R[right_name_col].apply(lambda s: pd.Series(_normalize_name_for_join(str(s))))
    M = pd.merge(L, R, on=["__ln","__fn"], how="left", suffixes=("", "_roster"))
    return M.drop(columns=["__ln","__fn"])

# ---------- Sierra detection ----------
@dataclass
class SierraLayout:
    header_row: int
    name_idx: int
    hours_idx: Optional[int]
    rate_idx: Optional[int]
    day_idx: Optional[int]
    start_idx: Optional[int]
    lnch_st_idx: Optional[int]
    lnch_fn_idx: Optional[int]
    finish_idx: Optional[int]

_SIERRA_HOURS_ALIASES = {"hours", "hrs", "total hours"}
_SIERRA_START_ALIASES = {"start", "time in"}
_SIERRA_LUNCH_ST_ALIASES = {"lnch st", "lunch st", "lunch start", "lnch start"}
_SIERRA_LUNCH_FN_ALIASES = {"lnch fnsh", "lunch fnsh", "lunch finish", "lnch finish", "lunch end"}
_SIERRA_FINISH_ALIASES = {"finish", "time out", "end"}
_SIERRA_RATE_ALIASES = {"rate", "pay rate"}
_SIERRA_DAY_ALIASES = {"days", "date", "day"}

def _detect_sierra_layout(df: pd.DataFrame) -> Optional[SierraLayout]:
    for r in range(min(80, len(df))):
        row_vals = [str(x).strip().lower() for x in df.iloc[r].values]
        if "name" in row_vals:
            # collect indices for likely columns
            idx_name   = row_vals.index("name")
            idx_hours  = next((i for i, v in enumerate(row_vals) if v in _SIERRA_HOURS_ALIASES), None)
            idx_rate   = next((i for i, v in enumerate(row_vals) if v in _SIERRA_RATE_ALIASES), None)
            idx_day    = next((i for i, v in enumerate(row_vals) if v in _SIERRA_DAY_ALIASES), None)
            idx_start  = next((i for i, v in enumerate(row_vals) if v in _SIERRA_START_ALIASES), None)
            idx_l_st   = next((i for i, v in enumerate(row_vals) if v in _SIERRA_LUNCH_ST_ALIASES), None)
            idx_l_fn   = next((i for i, v in enumerate(row_vals) if v in _SIERRA_LUNCH_FN_ALIASES), None)
            idx_finish = next((i for i, v in enumerate(row_vals) if v in _SIERRA_FINISH_ALIASES), None)
            return SierraLayout(
                header_row=r,
                name_idx=idx_name,
                hours_idx=idx_hours,
                rate_idx=idx_rate,
                day_idx=idx_day,
                start_idx=idx_start,
                lnch_st_idx=idx_l_st,
                lnch_fn_idx=idx_l_fn,
                finish_idx=idx_finish,
            )
    return None

# ---------- time parsing ----------
def _parse_time_cell(x) -> Optional[pd.Timestamp]:
    """Return a pandas Timestamp today with the time parsed from cell x; None if not parseable."""
    if pd.isna(x) or x is None:
        return None
    # Excel time numbers (fraction of a day)
    if isinstance(x, (int, float)) and 0 <= float(x) < 2:
        try:
            # pandas date origin is 1899-12-30 for Excel serials; but for pure time, treat as seconds
            seconds = float(x) * 24 * 3600
            base = pd.Timestamp("2000-01-01")
            return base + pd.to_timedelta(seconds, unit="s")
        except Exception:
            pass
    # Strings like '8:00', '11:30', '4:30 PM'
    try:
        s = str(x).strip()
        # sometimes '8' or '8.0'
        if re.fullmatch(r"\d{1,2}(\.\d+)?", s):
            s = s.split(".", 1)[0] + ":00"
        return pd.to_datetime(s, errors="raise", infer_datetime_format=True)
    except Exception:
        return None

def _compute_hours_from_row(row: pd.Series,
                            start_col: Optional[str],
                            lnch_st_col: Optional[str],
                            lnch_fn_col: Optional[str],
                            finish_col: Optional[str]) -> Optional[float]:
    if not start_col or not finish_col:
        return None
    t_in = _parse_time_cell(row.get(start_col))
    t_out = _parse_time_cell(row.get(finish_col))
    if t_in is None or t_out is None:
        return None
    base = pd.Timestamp("2000-01-01")
    if t_out < base:
        t_out = base.replace(hour=t_out.hour, minute=t_out.minute, second=t_out.second)
    if t_in < base:
        t_in = base.replace(hour=t_in.hour, minute=t_in.minute, second=t_in.second)
    gross = (t_out - t_in).total_seconds() / 3600.0
    lunch = 0.0
    if lnch_st_col and lnch_fn_col:
        l_in = _parse_time_cell(row.get(lnch_st_col))
        l_out = _parse_time_cell(row.get(lnch_fn_col))
        if l_in is not None and l_out is not None:
            lunch = max(0.0, (l_out - l_in).total_seconds() / 3600.0)
    hours = gross - lunch
    # sanitize
    if hours is None or hours < 0 or hours > 24:
        return None
    return round(hours, 3)

# ---------- Read + aggregate with daily OT/DT + weekly overlay ----------
def _read_sierra_records(xlsx_bytes: bytes) -> pd.DataFrame:
    """
    Returns DF columns: Name, Reg_sum, OT_sum, DT_sum, Rate_last
    """
    bio = io.BytesIO(xlsx_bytes)
    df0 = pd.read_excel(bio, sheet_name=0, header=None)
    layout = _detect_sierra_layout(df0)
    if not layout:
        raise ValueError("Could not detect Sierra header row (need 'Name' plus time columns).")

    bio.seek(0)
    df = pd.read_excel(bio, sheet_name=0, header=layout.header_row)
    df.columns = [str(c).strip() for c in df.columns]

    # map detected indices to column names
    def _colname(idx: Optional[int]) -> Optional[str]:
        if idx is None:
            return None
        try:
            return df.columns[idx]
        except Exception:
            return None

    name_col   = df.columns[layout.name_idx]
    df = df.rename(columns={name_col: "Name"})
    hours_col  = _colname(layout.hours_idx)
    rate_col   = _colname(layout.rate_idx)
    day_col    = _colname(layout.day_idx)
    start_col  = _colname(layout.start_idx)
    lnch_st_col= _colname(layout.lnch_st_idx)
    lnch_fn_col= _colname(layout.lnch_fn_idx)
    finish_col = _colname(layout.finish_idx)

    # Clean names
    df["Name"] = df["Name"].astype(str).str.strip()
    df = df[df["Name"] != ""].copy()

    # Get numeric hours: prefer explicit Hours column; else compute from time columns
    if hours_col and hours_col in df.columns:
        df["Hours_num"] = df[hours_col].apply(_num)
    else:
        df["Hours_num"] = df.apply(
            lambda r: _compute_hours_from_row(r, start_col, lnch_st_col, lnch_fn_col, finish_col),
            axis=1
        )

    df = df[df["Hours_num"].notnull()].copy()

    # Rate (last non-null per name)
    if rate_col and rate_col in df.columns:
        df["Rate_num"] = df[rate_col].apply(_num)
    else:
        df["Rate_num"] = None

    # Day key for daily split
    if day_col and day_col in df.columns:
        df["DayKey"] = pd.to_datetime(df[day_col], errors="coerce").dt.date
    else:
        # No explicit day column → treat each row as its own day bucket
        df["DayKey"] = pd.NaT

    # Aggregate per-day
    per_day = (
        df.groupby(["Name", "DayKey"], dropna=False)
          .agg(DayHours=("Hours_num","sum"),
               Rate_last=("Rate_num","last"))
          .reset_index()
    )
    per_day = per_day[per_day["Name"].notna() & (per_day["Name"].astype(str).str.strip()!="")]

    def split_daily(h: float) -> Tuple[float,float,float]:
        if h is None:
            return (0.0,0.0,0.0)
        reg = min(h, 8.0)
        ot  = min(max(h-8.0, 0.0), 4.0)
        dt  = max(h-12.0, 0.0)
        return (reg, ot, dt)

    per_day[["Reg","OT","DT"]] = per_day["DayHours"].apply(lambda h: pd.Series(split_daily(h)))

    # Per employee, then weekly overlay >40
    per_emp = (
        per_day.groupby("Name", dropna=False)
               .agg(Reg_sum=("Reg","sum"),
                    OT_sum=("OT","sum"),
                    DT_sum=("DT","sum"),
                    Rate_last=("Rate_last","last"))
               .reset_index()
    )

    def weekly_adjust(row):
        total = float(row["Reg_sum"] + row["OT_sum"] + row["DT_sum"])
        if total > 40.0:
            extra = total - 40.0
            pull = min(extra, row["Reg_sum"])
            row["Reg_sum"] -= pull
            row["OT_sum"]  += pull
            extra -= pull
            if extra > 0:
                row["OT_sum"] += extra
        return row

    per_emp = per_emp.apply(weekly_adjust, axis=1)
    return per_emp

# ---------- Roster (OPTIONAL) ----------
def _load_roster() -> pd.DataFrame:
    """
    Try to load roster.xlsx; if missing, return empty roster so conversion continues.
    Expected sheet 'Roster' with columns: EmpID, SSN, Employee Name, Status, Type, PayRate, Dept
    """
    expected = ["EmpID","SSN","Employee Name","Status","Type","PayRate","Dept"]
    candidates = [
        ROSTER_PATH,
        REPO_ROOT / "roster.xlsx",
        PROJECT_ROOT / "roster.xlsx",
        HERE / "roster.xlsx",
        Path("/roster.xlsx"),
    ]
    path = next((p for p in candidates if p.exists()), None)
    if path is None:
        empty = pd.DataFrame(columns=expected)
        empty["EmpID_clean"] = pd.Series(dtype="Int64")
        empty["SSN_clean"]   = pd.Series(dtype="Int64")
        empty["EmployeeNameRoster"] = pd.Series(dtype="string")
        return empty

    roster = pd.read_excel(path, sheet_name="Roster")
    roster.columns = [str(c).strip() for c in roster.columns]
    missing = set(expected) - set(roster.columns)
    if missing:
        raise ValueError(f"Roster found at {path} but missing columns: {missing}")

    roster["EmpID_clean"] = roster["EmpID"].apply(_safe_int)
    roster["SSN_clean"]   = roster["SSN"].apply(_safe_int)
    roster["EmployeeNameRoster"] = roster["Employee Name"].astype(str)
    return roster

# ---------- assemble rows ----------
def _pad_empid(empid: Optional[int]) -> Optional[str]:
    if empid is None:
        return None
    s = str(empid).strip()
    try:
        s = str(int(float(s)))
    except Exception:
        return None
    return s.zfill(10)

def _calc_totals(pay_type: str, reg: float, ot: float, dt: float,
                 rate: Optional[float], default_payrate: Optional[float]) -> float:
    # Salaried → roster PayRate is period total
    if str(pay_type).upper().startswith("S"):
        return float(_num(default_payrate) or 0.0)
    r = _num(rate) or _num(default_payrate) or 0.0
    return float((reg * r) + (ot * 1.5 * r) + (dt * 2.0 * r))

def _build_rows(agg: pd.DataFrame, roster: pd.DataFrame) -> List[Dict[str,object]]:
    joined = _best_join(agg, roster, "Name", "EmployeeNameRoster")
    out: List[Dict[str,object]] = []
    for _, r in joined.iterrows():
        name_src = str(r["Name"]).strip()
        reg = float(r.get("Reg_sum",0.0) or 0.0)
        ot  = float(r.get("OT_sum",0.0) or 0.0)
        dt  = float(r.get("DT_sum",0.0) or 0.0)
        rate_si = r.get("Rate_last")

        empid = r.get("EmpID_clean"); ssn = r.get("SSN_clean")
        name_roster = r.get("EmployeeNameRoster")
        status = r.get("Status","A"); ptype = r.get("Type","H")
        payrate_roster = r.get("PayRate"); dept = r.get("Dept")

        name_final = str(name_roster).strip() if pd.notna(name_roster) and str(name_roster).strip() else name_src
        rate_final = _num(payrate_roster) if _num(payrate_roster) is not None else _num(rate_si)
        totals_val = _calc_totals(str(ptype or "H"), reg, ot, dt, rate_final, payrate_roster)

        out.append({
            "EmpID": _pad_empid(empid),
            "SSN": ssn,
            "Employee Name": name_final,
            "Status": status if pd.notna(status) else "A",
            "Type": ptype if pd.notna(ptype) else "H",
            "Pay Rate": rate_final if rate_final is not None else "",
            "Dept": dept if pd.notna(dept) else "",
            "REGULAR": reg,
            "OVERTIME": ot,
            "DOUBLETIME": dt,
            "Totals": totals_val,
        })
    return out

# ---------- main: Sierra (bytes) → WBS (bytes) ----------
def sierra_excel_to_wbs_bytes(xlsx_bytes: bytes) -> bytes:
    if not WBS_TEMPLATE_PATH.exists():
        raise FileNotFoundError(f"WBS template not found at {WBS_TEMPLATE_PATH}")

    agg = _read_sierra_records(xlsx_bytes)
    roster = _load_roster()
    rows = _build_rows(agg, roster)

    wb = load_workbook(WBS_TEMPLATE_PATH)
    ws: Worksheet = wb["WEEKLY"] if "WEEKLY" in wb.sheetnames else next((wb[s] for s in wb.sheetnames if "week" in s.lower()), wb.active)

    # find header row (6–12 range)
    header_row = None
    header_score = -1
    for r in range(6, 13):
        vals = [str(c.value or "").strip() for c in ws[r]]
        score = sum(1 for v in vals if v)
        if score > header_score:
            header_score = score
            header_row = r
    if header_row is None:
        header_row = 8
    DATA_START = header_row + 1

    # clear existing data lines
    if ws.max_row >= DATA_START:
        ws.delete_rows(DATA_START, ws.max_row - DATA_START + 1)

    # headers
    hdr_raw   = [str(c.value or "").strip() for c in ws[header_row]]
    hdr_lower = [h.lower() for h in hdr_raw]
    last_header_col = len(hdr_raw) if hdr_raw else 20

    def col_eq(label: str) -> Optional[int]:
        lbl = label.strip().lower()
        for i, txt in enumerate(hdr_lower, start=1):
            if txt == lbl:
                return i
        return None

    def col_has(substr: str) -> Optional[int]:
        s = substr.strip().lower()
        for i, txt in enumerate(hdr_lower, start=1):
            if txt and s in txt:
                return i
        return None

    # Pay Rate and buckets
    payrate_col = col_eq("pay rate") or col_has("rate")
    if not payrate_col:
        for guess in (6,7,8,9,10):
            if guess <= last_header_col:
                payrate_col = guess
                break
    if not payrate_col:
        payrate_col = 7

    a01_col = col_eq("a01") or col_eq("regular")
    a02_col = col_eq("a02") or col_eq("overtime")
    a03_col = col_eq("a03") or col_has("double")

    if not a01_col: a01_col = payrate_col + 1
    if not a02_col: a02_col = a01_col + 1
    if not a03_col: a03_col = a02_col + 1

    # Totals destinations
    totals_candidates: List[int] = []
    for i, txt in enumerate(hdr_lower, start=1):
        if "total" in txt and i not in totals_candidates:
            totals_candidates.append(i)
    right_most_labeled = max([i for i, t in enumerate(hdr_raw, start=1) if str(t).strip()], default=None)
    if right_most_labeled and right_most_labeled not in totals_candidates:
        totals_candidates.append(right_most_labeled)
    if last_header_col not in totals_candidates:
        totals_candidates.append(last_header_col)
    if (a03_col + 1) not in totals_candidates:
        totals_candidates.append(a03_col + 1)
    totals_candidates = [c for c in totals_candidates if isinstance(c, int) and c >= 1]

    # Optional identity columns
    empid_col  = col_eq("# e:26") or col_has("emp id") or col_has("empid")
    ssn_col    = col_eq("ssn")
    name_col   = col_eq("employee name") or col_eq("name")
    status_col = col_eq("status")
    type_col   = col_eq("type") or col_has("pay type")
    dept_col   = col_eq("dept") or col_eq("department")

    # write rows
    r = DATA_START
    for row in rows:
        if empid_col:  ws.cell(row=r, column=empid_col,  value=row["EmpID"])
        if ssn_col:    ws.cell(row=r, column=ssn_col,    value=row["SSN"])
        if name_col:   ws.cell(row=r, column=name_col,   value=row["Employee Name"])
        if status_col: ws.cell(row=r, column=status_col, value=row["Status"])
        if type_col:   ws.cell(row=r, column=type_col,   value=row["Type"])
        if dept_col:   ws.cell(row=r, column=dept_col,   value=row["Dept"])

        rate_val = row["Pay Rate"] if row["Pay Rate"] != "" else None
        ws.cell(row=r, column=payrate_col, value=rate_val)
        ws.cell(row=r, column=a01_col, value=round(float(row["REGULAR"]), 3))
        ws.cell(row=r, column=a02_col, value=round(float(row["OVERTIME"]), 3))
        ws.cell(row=r, column=a03_col, value=round(float(row["DOUBLETIME"]), 3))

        total_val = float(row["Totals"] or 0.0)
        rate_ref = f"{get_column_letter(payrate_col)}{r}"
        reg_ref  = f"{get_column_letter(a01_col)}{r}"
        ot_ref   = f"{get_column_letter(a02_col)}{r}"
        dt_ref   = f"{get_column_letter(a03_col)}{r}"
        formula = f"=({reg_ref}*{rate_ref})+({ot_ref}*1.5*{rate_ref})+({dt_ref}*2*{rate_ref})"

        for tcol in totals_candidates:
            if tcol is None or tcol < 1:
                continue
            ws.cell(row=r, column=tcol, value=total_val)
            ws.cell(row=r, column=tcol).value = formula

        r += 1

    # optional DEBUG removal if present
    if "DEBUG" in wb.sheetnames:
        try:
            wb.remove(wb["DEBUG"])
        except Exception:
            pass

    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)
    return bio.read()

# app/services/excel_processor.py
# Sierra → WBS translator with daily OT/DT allocation
# - Detect Sierra layout (Days, Job#, Name, Hours, Rate, Total, Job Detail)
# - Per-employee, per-day split: REG (≤8), OT (8–12), DT (>12)
# - Join roster.xlsx, write WBS WEEKLY with robust header matching

from __future__ import annotations

import io, re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple, List

import pandas as pd
from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet

# ---------- Paths ----------
HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent.parent
REPO_ROOT = PROJECT_ROOT.parent

WBS_TEMPLATE_PATH = REPO_ROOT / "wbs_template.xlsx"
ROSTER_PATH       = REPO_ROOT / "roster.xlsx"

# ---------- Helpers ----------
def _num(s) -> Optional[float]:
    if s is None:
        return None
    if isinstance(s, (int, float)):
        try: return float(s)
        except: return None
    ss = str(s).strip()
    if not ss: return None
    ss = ss.replace("$", "").replace(",", "")
    m = re.search(r"-?\d+(\.\d+)?", ss)
    if not m: return None
    try: return float(m.group(0))
    except: return None

def _safe_int(x) -> Optional[int]:
    try:
        if pd.isna(x): return None
    except: pass
    try: return int(float(str(x).replace(",", "").strip()))
    except: return None

def _normalize_name_for_join(name: str) -> Tuple[str, str]:
    if not isinstance(name, str): return ("","")
    s = " ".join(name.replace(",", " ").split()).strip()
    if not s: return ("","")
    parts = s.split(" ")
    if "," in name:
        last, rest = [x.strip() for x in name.split(",", 1)]
        first = rest.split(" ")[0] if rest else ""
    else:
        first, last = parts[0], parts[-1]
    last_norm  = "".join(ch for ch in last.lower()  if ch.isalpha())
    first_norm = "".join(ch for ch in first.lower() if ch.isalpha())
    return last_norm, first_norm

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
    hours_idx: int
    rate_idx: Optional[int]
    days_idx: Optional[int]

def _detect_sierra_layout(df: pd.DataFrame) -> Optional[SierraLayout]:
    for r in range(min(60, len(df))):
        row = df.iloc[r].astype(str).str.strip().str.lower()
        if "name" in set(row.values) and "hours" in set(row.values):
            name_idx  = row[row == "name"].index[0]
            hours_idx = row[row == "hours"].index[0]
            rate_idx  = row[row == "rate"].index[0] if any(row == "rate") else None
            days_idx  = row[row == "days"].index[0] if any(row == "days") else None
            return SierraLayout(r, name_idx, hours_idx, rate_idx, days_idx)
    return None

# ---------- Read + aggregate with daily OT/DT ----------
def _read_sierra_records(xlsx_bytes: bytes) -> pd.DataFrame:
    """
    Return DF with: Name, Reg_sum, OT_sum, DT_sum, Rate_last
    """
    bio = io.BytesIO(xlsx_bytes)
    df0 = pd.read_excel(bio, sheet_name=0, header=None)
    layout = _detect_sierra_layout(df0)
    if not layout:
        raise ValueError("Could not detect Sierra header row with 'Name' and 'Hours'.")

    bio.seek(0)
    df = pd.read_excel(bio, sheet_name=0, header=layout.header_row)
    df.columns = [str(c).strip() for c in df.columns]

    name_col  = df.columns[layout.name_idx]
    hours_col = df.columns[layout.hours_idx]
    df = df.rename(columns={name_col: "Name", hours_col: "Hours"})

    if layout.rate_idx is not None:
        rate_name = df.columns[layout.rate_idx]
        if rate_name not in ("Name", "Hours"):
            df = df.rename(columns={rate_name: "Rate"})
    if layout.days_idx is not None:
        days_name = df.columns[layout.days_idx]
        df = df.rename(columns={days_name: "Days"})

    # keep rows with name and numeric hours
    df = df[df["Name"].astype(str).str.strip() != ""].copy()
    df["Hours_num"] = df["Hours"].apply(_num)
    df = df[df["Hours_num"].notnull()].copy()
    if "Rate" in df.columns:
        df["Rate_num"] = df["Rate"].apply(_num)
    else:
        df["Rate_num"] = None

    # Normalize date (Days) if present
    if "Days" in df.columns:
        # try parse to date; if not present, treat as single bucket per file
        df["DayKey"] = pd.to_datetime(df["Days"], errors="coerce").dt.date
    else:
        df["DayKey"] = pd.NaT

    # Per-employee, per-day totals
    per_day = (df.groupby(["Name", "DayKey"], dropna=False)
                 .agg(DayHours=("Hours_num", "sum"),
                      Rate_last=("Rate_num", "last"))
                 .reset_index())

    # Split each day into REG/OT/DT (CA-style daily rule)
    def split_daily(h: float) -> Tuple[float,float,float]:
        if h is None: return (0.0,0.0,0.0)
        reg = min(h, 8.0)
        ot  = min(max(h - 8.0, 0.0), 4.0)
        dt  = max(h - 12.0, 0.0)
        return (reg, ot, dt)

    per_day[["Reg","OT","DT"]] = per_day["DayHours"].apply(lambda h: pd.Series(split_daily(h)))

    # Aggregate back to per-employee
    agg = (per_day.groupby("Name", dropna=False)
                 .agg(Reg_sum=("Reg","sum"),
                      OT_sum=("OT","sum"),
                      DT_sum=("DT","sum"),
                      Rate_last=("Rate_last","last"))
                 .reset_index())

    return agg

# ---------- Roster ----------
def _load_roster() -> pd.DataFrame:
    if not ROSTER_PATH.exists():
        raise FileNotFoundError(f"Roster not found at {ROSTER_PATH}")
    roster = pd.read_excel(ROSTER_PATH, sheet_name="Roster")
    roster.columns = [str(c).strip() for c in roster.columns]

    expected = {"EmpID","SSN","Employee Name","Status","Type","PayRate","Dept"}
    missing = expected - set(roster.columns)
    if missing:
        raise ValueError(f"Roster missing columns: {missing}")

    roster["EmpID_clean"] = roster["EmpID"].apply(_safe_int)
    roster["SSN_clean"]   = roster["SSN"].apply(_safe_int)
    roster["EmployeeNameRoster"] = roster["Employee Name"].astype(str)
    return roster

# ---------- WBS mapping ----------
def _pad_empid(empid: Optional[int]) -> Optional[str]:
    if empid is None: return None
    s = str(empid).strip()
    try: s = str(int(float(s)))
    except: return None
    return s.zfill(10)

def _calc_totals(pay_type: str, reg: float, ot: float, dt: float,
                 rate: Optional[float], default_payrate: Optional[float]) -> float:
    if str(pay_type).upper().startswith("S"):
        return float(_num(default_payrate) or 0.0)
    r = _num(rate) or _num(default_payrate) or 0.0
    # If you want weighted totals by OT/DT multipliers, adjust here (e.g., 1.5x, 2x).
    return float((reg + ot + dt) * r)

def _scan_headers(ws: Worksheet) -> Dict[str,int]:
    labels: List[Tuple[int,str]] = []
    for row_idx in range(6, 11):
        for cell in ws[row_idx]:
            label = str(cell.value or "").strip()
            if label:
                labels.append((cell.column, label))

    def loc(*names: str) -> Optional[int]:
        keys = [n.lower() for n in names]
        for col_idx, label in labels:
            L = label.lower().strip()
            if any(L == k or k in L for k in keys):
                return col_idx
        return None

    return {
        "EmpID": loc("# e:26","employee id","emp id","empid"),
        "SSN": loc("ssn"),
        "Employee Name": loc("employee name","name"),
        "Status": loc("status"),
        "Type": loc("type","pay type"),
        "Pay Rate": loc("pay rate","rate"),
        "Dept": loc("dept","department"),
        "REGULAR": loc("regular","a01"),
        "A01": loc("a01","regular"),
        "OVERTIME": loc("overtime","a02"),
        "A02": loc("a02","overtime"),
        "DOUBLETIME": loc("doubletime","double time","a03"),
        "A03": loc("a03","double"),
        "Totals": loc("totals","total"),
        "Comments": loc("comments","notes"),
    }

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

        totals = _calc_totals(str(ptype or "H"), reg, ot, dt, rate_final, payrate_roster)

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
            "Totals": totals,
        })
    return out

# ---------- Main ----------
def sierra_excel_to_wbs_bytes(xlsx_bytes: bytes) -> bytes:
    agg = _read_sierra_records(xlsx_bytes)
    roster = _load_roster()
    rows = _build_rows(agg, roster)

    if not WBS_TEMPLATE_PATH.exists():
        raise FileNotFoundError(f"WBS template not found at {WBS_TEMPLATE_PATH}")

    wb = load_workbook(WBS_TEMPLATE_PATH)
    ws: Worksheet = wb["WEEKLY"]

    HEADER_ROW_1BASED = 8
    DATA_START_1BASED = 9

    # clear old
    max_row = ws.max_row
    if max_row >= DATA_START_1BASED:
        ws.delete_rows(DATA_START_1BASED, max_row - DATA_START_1BASED + 1)

    cmap = _scan_headers(ws)

    def write_if(col_key: str, row_idx: int, val):
        c = cmap.get(col_key)
        if c: ws.cell(row=row_idx, column=c, value=val)

    row = DATA_START_1BASED
    for r in rows:
        write_if("EmpID", row, r["EmpID"])
        write_if("SSN", row, r["SSN"])
        write_if("Employee Name", row, r["Employee Name"])
        write_if("Status", row, r["Status"])
        write_if("Type", row, r["Type"])
        write_if("Pay Rate", row, r["Pay Rate"])
        write_if("Dept", row, r["Dept"])

        # REG → both REGULAR and A01 if either exists
        reg_cols = [k for k in ("REGULAR","A01") if cmap.get(k)]
        if reg_cols:
            for k in reg_cols: write_if(k, row, r["REGULAR"])

        # OT → both OVERTIME and A02
        ot_cols = [k for k in ("OVERTIME","A02") if cmap.get(k)]
        if ot_cols:
            for k in ot_cols: write_if(k, row, r["OVERTIME"])

        # DT → both DOUBLETIME and A03
        dt_cols = [k for k in ("DOUBLETIME","A03") if cmap.get(k)]
        if dt_cols:
            for k in dt_cols: write_if(k, row, r["DOUBLETIME"])

        write_if("Totals", row, r["Totals"])
        row += 1

    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)
    return bio.read()

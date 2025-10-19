# app/services/excel_processor.py
# Sierra → WBS translator (FULL FILE)
# - Reads Sierra daily logs (Days, Job#, Name, Start, Lnch St., Lnch Fnsh, Finish, Hours, Rate, Total, Job Detail)
# - Splits hours into REG (≤8/day), OT (8–12/day), DT (>12/day) + overlays WEEKLY >40 into OT
# - Roster is OPTIONAL (conversion never crashes if roster.xlsx is missing)
# - Writes to WBS template by POSITION (relative to “Pay Rate”), NOT by header text
# - Totals column is written as a VALUE and a FORMULA (=reg*rate + 1.5*ot*rate + 2*dt*rate)

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

# ---------- Small helpers ----------
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
    L = left_df.copy()
    R = right_df.copy()
    L[["__ln", "__fn"]] = L[left_name_col].apply(lambda s: pd.Series(_normalize_name_for_join(str(s))))
    R[["__ln", "__fn"]] = R[right_name_col].apply(lambda s: pd.Series(_normalize_name_for_join(str(s))))
    M = pd.merge(L, R, on=["__ln", "__fn"], how="left", suffixes=("", "_roster"))
    return M.drop(columns=["__ln", "__fn"])

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

# ---------- Read + aggregate with daily OT/DT + weekly overlay ----------
def _read_sierra_records(xlsx_bytes: bytes) -> pd.DataFrame:
    """
    Returns DF columns: Name, Reg_sum, OT_sum, DT_sum, Rate_last
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

    # Clean
    df["Name"] = df["Name"].astype(str).str.strip()
    df = df[df["Name"] != ""].copy()
    df["Hours_num"] = df["Hours"].apply(_num)
    df = df[df["Hours_num"].notnull()].copy()
    df["Rate_num"] = df["Rate"].apply(_num) if "Rate" in df.columns else None
    if "Days" in df.columns:
        df["DayKey"] = pd.to_datetime(df["Days"], errors="coerce").dt.date
    else:
        df["DayKey"] = pd.NaT

    per_day = (df.groupby(["Name", "DayKey"], dropna=False)
                 .agg(DayHours=("Hours_num", "sum"),
                      Rate_last=("Rate_num", "last"))
                 .reset_index())
    per_day = per_day[per_day["Name"].notna() & (per_day["Name"].astype(str).str.strip() != "")]

    def split_daily(h: float) -> Tuple[float, float, float]:
        if h is None:
            return (0.0, 0.0, 0.0)
        reg = min(h, 8.0)
        ot  = min(max(h - 8.0, 0.0), 4.0)
        dt  = max(h - 12.0, 0.0)
        return (reg, ot, dt)

    per_day[["Reg", "OT", "DT"]] = per_day["DayHours"].apply(lambda h: pd.Series(split_daily(h)))

    # Aggregate daily split
    per_emp = (per_day.groupby("Name", dropna=False)
                        .agg(Reg_sum=("Reg", "sum"),
                             OT_sum=("OT", "sum"),
                             DT_sum=("DT", "sum"),
                             Rate_last=("Rate_last", "last"))
                        .reset_index())

    # Weekly >40 overlay → push any hours above 40 from REG into OT
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
    agg = per_emp[per_emp["Name"].notna() & (per_emp["Name"].astype(str).str.strip() != "")]
    return agg

# ---------- Roster (OPTIONAL, never crash) ----------
def _load_roster() -> pd.DataFrame:
    """
    Try to load roster.xlsx; if it's missing, return an empty roster so conversion
    proceeds using Sierra names/rates only.
    Expected: sheet 'Roster' with columns:
      EmpID, SSN, Employee Name, Status, Type, PayRate, Dept
    """
    expected_cols = ["EmpID", "SSN", "Employee Name", "Status", "Type", "PayRate", "Dept"]
    candidates = [
        ROSTER_PATH,
        REPO_ROOT / "roster.xlsx",
        PROJECT_ROOT / "roster.xlsx",
        HERE / "roster.xlsx",
        Path("/roster.xlsx"),
    ]
    path = next((p for p in candidates if p.exists()), None)

    if path is None:
        empty = pd.DataFrame(columns=expected_cols)
        empty["EmpID_clean"] = pd.Series(dtype="Int64")
        empty["SSN_clean"]   = pd.Series(dtype="Int64")
        empty["EmployeeNameRoster"] = pd.Series(dtype="string")
        return empty

    roster = pd.read_excel(path, sheet_name="Roster")
    roster.columns = [str(c).strip() for c in roster.columns]

    missing = set(expected_cols) - set(roster.columns)
    if missing:
        raise ValueError(f"Roster found at {path} but missing columns: {missing}")

    roster["EmpID_clean"] = roster["EmpID"].apply(_safe_int)
    roster["SSN_clean"]   = roster["SSN"].apply(_safe_int)
    roster["EmployeeNameRoster"] = roster["Employee Name"].astype(str)
    return roster

# ---------- WBS assembly ----------
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
    # Salaried: use roster PayRate as period total
    if str(pay_type).upper().startswith("S"):
        return float(_num(default_payrate) or 0.0)
    r = _num(rate) or _num(default_payrate) or 0.0
    return float((reg * r) + (ot * 1.5 * r) + (dt * 2.0 * r))

def _build_rows(agg: pd.DataFrame, roster: pd.DataFrame) -> List[Dict[str, object]]:
    joined = _best_join(agg, roster, "Name", "EmployeeNameRoster")
    out: List[Dict[str, object]] = []

    for _, r in joined.iterrows():
        name_src = str(r["Name"]).strip()
        reg = float(r.get("Reg_sum", 0.0) or 0.0)
        ot  = float(r.get("OT_sum", 0.0) or 0.0)
        dt  = float(r.get("DT_sum", 0.0) or 0.0)
        rate_si = r.get("Rate_last")

        empid = r.get("EmpID_clean")
        ssn   = r.get("SSN_clean")
        name_roster = r.get("EmployeeNameRoster")
        status = r.get("Status", "A")
        ptype  = r.get("Type", "H")
        payrate_roster = r.get("PayRate")
        dept = r.get("Dept")

        name_final = (str(name_roster).strip()
                      if pd.notna(name_roster) and str(name_roster).strip()
                      else name_src)
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

# ---------- Main: Sierra Excel (bytes) → WBS Excel (bytes) ----------
def sierra_excel_to_wbs_bytes(xlsx_bytes: bytes) -> bytes:
    if not WBS_TEMPLATE_PATH.exists():
        raise FileNotFoundError(f"WBS template not found at {WBS_TEMPLATE_PATH}")

    agg = _read_sierra_records(xlsx_bytes)
    roster = _load_roster()
    rows = _build_rows(agg, roster)

    wb = load_workbook(WBS_TEMPLATE_PATH)
    ws: Worksheet = wb["WEEKLY"]

    HEADER_ROW_1BASED = 8   # labels
    DATA_START_1BASED = 9   # first data row

    # Clear existing data
    max_row = ws.max_row
    if max_row >= DATA_START_1BASED:
        ws.delete_rows(DATA_START_1BASED, max_row - DATA_START_1BASED + 1)

    # Build an index of header labels (lowercased) by position
    header_cells = [str(c.value or "").strip().lower() for c in ws[HEADER_ROW_1BASED]]

    def col_of_exact(label: str) -> Optional[int]:
        label = label.strip().lower()
        for i, txt in enumerate(header_cells, start=1):
            if txt == label:
                return i
        return None

    # Locate Pay Rate (by exact label first, else by contains)
    payrate_col = col_of_exact("pay rate")
    if not payrate_col:
        for i, txt in enumerate(header_cells, start=1):
            if "rate" in txt:
                payrate_col = i
                break
    if not payrate_col:
        # fall back to a sensible default (often column G = 7)
        payrate_col = 7

    # Position-based numeric buckets immediately after Pay Rate
    reg_col = payrate_col + 1  # A01 (REG)
    ot_col  = reg_col + 1      # A02 (OT)
    dt_col  = ot_col + 1       # A03 (DT)

    # Totals: choose the right-most labeled column (your pink “Totals”)
    right_most_labeled = max([i for i, txt in enumerate(header_cells, start=1) if txt], default=dt_col + 1)
    totals_col = right_most_labeled

    # Optional: locate common ID/name columns by label (best-effort)
    empid_col = col_of_exact("# e:26") or col_of_exact("employee id") or col_of_exact("empid")
    ssn_col   = col_of_exact("ssn")
    name_col  = col_of_exact("employee name") or col_of_exact("name")
    status_col = col_of_exact("status")
    type_col   = col_of_exact("type")
    dept_col   = col_of_exact("dept") or col_of_exact("department")

    # Write rows
    r = DATA_START_1BASED
    for row in rows:
        # IDs/names (best-effort)
        if empid_col: ws.cell(row=r, column=empid_col, value=row["EmpID"])
        if ssn_col:   ws.cell(row=r, column=ssn_col,   value=row["SSN"])
        if name_col:  ws.cell(row=r, column=name_col,  value=row["Employee Name"])
        if status_col: ws.cell(row=r, column=status_col, value=row["Status"])
        if type_col:   ws.cell(row=r, column=type_col,   value=row["Type"])
        if dept_col:   ws.cell(row=r, column=dept_col,   value=row["Dept"])

        # Pay rate and hour buckets (by position)
        ws.cell(row=r, column=payrate_col, value=row["Pay Rate"])
        ws.cell(row=r, column=reg_col,     value=round(float(row["REGULAR"]), 3))
        ws.cell(row=r, column=ot_col,      value=round(float(row["OVERTIME"]), 3))
        ws.cell(row=r, column=dt_col,      value=round(float(row["DOUBLETIME"]), 3))

        # Totals: write value AND a formula so Excel recalculates with your formatting
        total_val = float(row["Totals"] or 0.0)
        ws.cell(row=r, column=totals_col, value=total_val)

        rate_ref = f"{get_column_letter(payrate_col)}{r}"
        reg_ref  = f"{get_column_letter(reg_col)}{r}"
        ot_ref   = f"{get_column_letter(ot_col)}{r}"
        dt_ref   = f"{get_column_letter(dt_col)}{r}"
        ws.cell(row=r, column=totals_col).value = f"=({reg_ref}*{rate_ref})+({ot_ref}*1.5*{rate_ref})+({dt_ref}*2*{rate_ref})"

        r += 1

    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)
    return bio.read()

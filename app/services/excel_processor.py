# app/services/excel_processor.py
# Sierra → WBS translator (TEMPLATE-SAFE, NO PREFILLED NUMBERS REQUIRED)
# - Reads Sierra daily logs (Days, Job#, Name, Start, Lnch St., Lnch Fnsh, Finish, Hours, Rate, Total, Job Detail)
# - Splits hours into REG (≤8/day), OT (8–12/day), DT (>12/day) + overlays WEEKLY >40 into OT
# - Roster is OPTIONAL (conversion proceeds if roster.xlsx is missing)
# - Writes A01/A02/A03 even when template has only headers (no values)
# - Totals written by formula to multiple sensible columns (including far-right header)
# - Leaves cells blank instead of writing 0.000
# - Robust header detection for both Sierra and WBS

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


def _blank_if_zero(v) -> Optional[float]:
    try:
        f = float(v)
        return None if abs(f) < 1e-9 else f
    except Exception:
        return v


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
    L[["__ln", "__fn"]] = L[left_name_col].apply(
        lambda s: pd.Series(_normalize_name_for_join(str(s))))
    R[["__ln", "__fn"]] = R[right_name_col].apply(
        lambda s: pd.Series(_normalize_name_for_join(str(s))))
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
    for r in range(min(80, len(df))):
        row = df.iloc[r].astype(str).str.strip().str.lower()
        if "name" in set(row.values) and "hours" in set(row.values):
            name_idx = row[row == "name"].index[0]
            hours_idx = row[row == "hours"].index[0]
            rate_idx = row[row == "rate"].index[0] if any(row == "rate") else None
            days_idx = row[row == "days"].index[0] if any(row == "days") else None
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

    name_col = df.columns[layout.name_idx]
    hours_col = df.columns[layout.hours_idx]
    df = df.rename(columns={name_col: "Name", hours_col: "Hours"})
    if layout.rate_idx is not None:
        rate_name = df.columns[layout.rate_idx]
        if rate_name not in ("Name", "Hours"):
            df = df.rename(columns={rate_name: "Rate"})
    if layout.days_idx is not None:
        days_name = df.columns[layout.days_idx]
        df = df.rename(columns={days_name: "Days"})

    df["Name"] = df["Name"].astype(str).str.strip()
    df = df[df["Name"] != ""].copy()
    df["Hours_num"] = df["Hours"].apply(_num)
    df = df[df["Hours_num"].notnull()].copy()
    df["Rate_num"] = df["Rate"].apply(_num) if "Rate" in df.columns else None
    if "Days" in df.columns:
        # some Sierra exports have text like "Week of Sept 8 - Sept 14"—coerce will set NaT, which is fine
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
        ot = min(max(h - 8.0, 0.0), 4.0)
        dt = max(h - 12.0, 0.0)
        return (reg, ot, dt)

    per_day[["Reg", "OT", "DT"]] = per_day["DayHours"].apply(
        lambda h: pd.Series(split_daily(h)))

    per_emp = (per_day.groupby("Name", dropna=False)
               .agg(Reg_sum=("Reg", "sum"),
                    OT_sum=("OT", "sum"),
                    DT_sum=("DT", "sum"),
                    Rate_last=("Rate_last", "last"))
               .reset_index())

    # Weekly >40 overlay → move excess from REG to OT
    def weekly_adjust(row):
        total = float(row["Reg_sum"] + row["OT_sum"] + row["DT_sum"])
        if total > 40.0:
            extra = total - 40.0
            pull = min(extra, row["Reg_sum"])
            row["Reg_sum"] -= pull
            row["OT_sum"] += pull
            extra -= pull
            if extra > 0:
                row["OT_sum"] += extra  # any spill still OT (DT already counted from daily split)
        return row

    per_emp = per_emp.apply(weekly_adjust, axis=1)
    agg = per_emp[per_emp["Name"].notna() & (per_emp["Name"].astype(str).str.strip() != "")]
    return agg


# ---------- Roster (OPTIONAL) ----------
def _load_roster() -> pd.DataFrame:
    """
    Try to load roster.xlsx; if missing, return an empty roster so conversion keeps going.
    Expected sheet 'Roster' with columns: EmpID, SSN, Employee Name, Status, Type, PayRate, Dept
    """
    expected = ["EmpID", "SSN", "Employee Name", "Status", "Type", "PayRate", "Dept"]
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
        empty["SSN_clean"] = pd.Series(dtype="Int64")
        empty["EmployeeNameRoster"] = pd.Series(dtype="string")
        return empty

    try:
        roster = pd.read_excel(path, sheet_name="Roster")
    except Exception:
        # fallback: first sheet
        roster = pd.read_excel(path, sheet_name=0)

    roster.columns = [str(c).strip() for c in roster.columns]
    missing = set(expected) - set(roster.columns)
    if missing:
        # tolerate partial roster, just create missing cols
        for col in missing:
            roster[col] = None

    roster["EmpID_clean"] = roster["EmpID"].apply(_safe_int) if "EmpID" in roster.columns else None
    roster["SSN_clean"] = roster["SSN"].apply(_safe_int) if "SSN" in roster.columns else None
    roster["EmployeeNameRoster"] = roster["Employee Name"].astype(str) if "Employee Name" in roster.columns else ""
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
    if str(pay_type or "").upper().startswith("S"):
        return float(_num(default_payrate) or 0.0)
    r = _num(rate) or _num(default_payrate) or 0.0
    return float((reg * r) + (ot * 1.5 * r) + (dt * 2.0 * r))


def _build_rows(agg: pd.DataFrame, roster: pd.DataFrame) -> List[Dict[str, object]]:
    joined = _best_join(agg, roster, "Name", "EmployeeNameRoster") if not roster.empty else agg.copy()
    out: List[Dict[str, object]] = []
    for _, r in joined.iterrows():
        name_src = str(r.get("Name", "")).strip()
        reg = float(r.get("Reg_sum", 0.0) or 0.0)
        ot = float(r.get("OT_sum", 0.0) or 0.0)
        dt = float(r.get("DT_sum", 0.0) or 0.0)
        rate_si = r.get("Rate_last")

        empid = r.get("EmpID_clean")
        ssn = r.get("SSN_clean")
        name_roster = r.get("EmployeeNameRoster")
        status = r.get("Status", "A")
        ptype = r.get("Type", "H")
        payrate_roster = r.get("PayRate")
        dept = r.get("Dept")

        name_final = str(name_roster).strip() if pd.notna(name_roster) and str(name_roster).strip() else name_src
        rate_final = _num(payrate_roster) if _num(payrate_roster) is not None else _num(rate_si)
        totals_val = _calc_totals(str(ptype or "H"), reg, ot, dt, rate_final, payrate_roster)

        out.append({
            "EmpID": _pad_empid(empid),
            "SSN": ssn,
            "Employee Name": name_final,
            "Status": status if pd.notna(status) else "A",
            "Type": ptype if pd.notna(ptype) else "H",
            "Pay Rate": rate_final if rate_final is not None else None,
            "Dept": dept if pd.notna(dept) else "",
            "REGULAR": reg,
            "OVERTIME": ot,
            "DOUBLETIME": dt,
            "TotalsVal": totals_val,
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
    # Prefer "WEEKLY"; else first sheet with 'week' in name; else active
    ws: Worksheet = (
        wb["WEEKLY"] if "WEEKLY" in wb.sheetnames
        else next((wb[s] for s in wb.sheetnames if "week" in s.lower()), wb.active)
    )

    # Detect header row between 6–12; pick the densest row
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

    # Clear existing data lines (keep headers)
    if ws.max_row >= DATA_START:
        ws.delete_rows(DATA_START, ws.max_row - DATA_START + 1)

    # Header arrays
    hdr_raw = [str(c.value or "").strip() for c in ws[header_row]]
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

    # Identify Pay Rate and positional A01/A02/A03
    payrate_col = col_eq("pay rate") or col_has("rate")
    if not payrate_col:
        # fallbacks to typical positions
        for guess in (6, 7, 8, 9, 10):
            if guess <= last_header_col:
                payrate_col = guess
                break
    if not payrate_col:
        payrate_col = 7

    # A01/A02/A03
    a01_col = col_eq("a01") or col_eq("regular")
    a02_col = col_eq("a02") or col_eq("overtime")
    a03_col = col_eq("a03") or col_has("double")

    # Fallback positions to the right of Pay Rate
    if not a01_col: a01_col = payrate_col + 1
    if not a02_col: a02_col = a01_col + 1
    if not a03_col: a03_col = a02_col + 1

    # Totals candidates:
    totals_candidates: List[int] = []
    # 1) any header containing "total"
    for i, txt in enumerate(hdr_lower, start=1):
        if "total" in txt and i not in totals_candidates:
            totals_candidates.append(i)
    # 2) right-most labeled header
    right_most_labeled = max([i for i, t in enumerate(hdr_raw, start=1) if str(t).strip()], default=None)
    if right_most_labeled and right_most_labeled not in totals_candidates:
        totals_candidates.append(right_most_labeled)
    # 3) absolute last header column (pink)
    if last_header_col not in totals_candidates:
        totals_candidates.append(last_header_col)
    # 4) dt+1 safety
    if (a03_col + 1) not in totals_candidates:
        totals_candidates.append(a03_col + 1)

    # Optional IDs / identity columns
    empid_col = col_eq("# e:26") or col_has("emp id") or col_has("empid")
    ssn_col = col_eq("ssn")
    name_col = col_eq("employee name") or col_eq("name")
    status_col = col_eq("status")
    type_col = col_eq("type") or col_has("pay type")
    dept_col = col_eq("dept") or col_eq("department")

    # Write rows
    r = DATA_START
    for row in rows:
        # Identity fields (best effort)
        if empid_col:  ws.cell(row=r, column=empid_col,  value=row["EmpID"])
        if ssn_col:    ws.cell(row=r, column=ssn_col,    value=row["SSN"])
        if name_col:   ws.cell(row=r, column=name_col,   value=row["Employee Name"])
        if status_col: ws.cell(row=r, column=status_col, value=row["Status"])
        if type_col:   ws.cell(row=r, column=type_col,   value=row["Type"])
        if dept_col:   ws.cell(row=r, column=dept_col,   value=row["Dept"])

        # Rates + buckets
        rate_val = _blank_if_zero(row["Pay Rate"])
        reg_val = _blank_if_zero(row["REGULAR"])
        ot_val  = _blank_if_zero(row["OVERTIME"])
        dt_val  = _blank_if_zero(row["DOUBLETIME"])

        ws.cell(row=r, column=payrate_col, value=rate_val)
        ws.cell(row=r, column=a01_col, value=reg_val)
        ws.cell(row=r, column=a02_col, value=ot_val)
        ws.cell(row=r, column=a03_col, value=dt_val)

        # Totals: set formula to all candidates (value isn’t required if formula is present)
        rate_ref = f"{get_column_letter(payrate_col)}{r}"
        reg_ref  = f"{get_column_letter(a01_col)}{r}"
        ot_ref   = f"{get_column_letter(a02_col)}{r}"
        dt_ref   = f"{get_column_letter(a03_col)}{r}"
        formula = f"=({reg_ref}*{rate_ref})+({ot_ref}*1.5*{rate_ref})+({dt_ref}*2*{rate_ref})"

        for tcol in totals_candidates:
            if tcol and tcol >= 1:
                ws.cell(row=r, column=tcol).value = formula

        r += 1

    # Done
    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)
    return bio.read()

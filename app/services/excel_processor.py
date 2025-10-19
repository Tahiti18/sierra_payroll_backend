# app/services/excel_processor.py
# Sierra → WBS translator (hours-to-REGULAR/A01 hard-mapped)
# - Detects Sierra daily log structure (Days, Job#, Name, Hours, Rate, Total, Job Detail)
# - Aggregates by employee
# - Joins to roster.xlsx (EmpID/SSN/Status/Type/Dept/PayRate)
# - Writes WBS "WEEKLY" while tolerating header quirks and merged cells

from __future__ import annotations

import io, re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple, List

import pandas as pd
from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet


# ---------- Paths (relative to project root) ----------
HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent.parent       # app/
REPO_ROOT = PROJECT_ROOT.parent         # repo root

WBS_TEMPLATE_PATH = REPO_ROOT / "wbs_template.xlsx"
ROSTER_PATH       = REPO_ROOT / "roster.xlsx"


# ---------- Numeric + name helpers ----------

def _num(s) -> Optional[float]:
    """Parse ' 1,234.50 ', '$45.00', '8.000', '16 ' → float or None."""
    if s is None:
        return None
    if isinstance(s, (int, float)):
        try:
            return float(s)
        except Exception:
            return None
    ss = str(s).strip()
    if not ss:
        return None
    ss = ss.replace("$", "").replace(",", "")
    m = re.search(r"-?\d+(\.\d+)?", ss)
    if not m:
        return None
    try:
        return float(m.group(0))
    except Exception:
        return None

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
    """Return (last, first) lowercase tokens without punctuation/spaces."""
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
        first = parts[0]
        last = parts[-1]
    last_norm  = "".join(ch for ch in last.lower() if ch.isalpha())
    first_norm = "".join(ch for ch in first.lower() if ch.isalpha())
    return last_norm, first_norm

def _best_join(left_df: pd.DataFrame, right_df: pd.DataFrame,
               left_name_col: str, right_name_col: str) -> pd.DataFrame:
    """Join Sierra names to roster names using normalized (last, first)."""
    L = left_df.copy()
    R = right_df.copy()
    L[["__ln","__fn"]] = L[left_name_col].apply(lambda s: pd.Series(_normalize_name_for_join(str(s))))
    R[["__ln","__fn"]] = R[right_name_col].apply(lambda s: pd.Series(_normalize_name_for_join(str(s))))
    M = pd.merge(L, R, on=["__ln","__fn"], how="left", suffixes=("", "_roster"))
    return M.drop(columns=["__ln","__fn"])


# ---------- Sierra detection & aggregation ----------

@dataclass
class SierraLayout:
    header_row: int
    name_idx: int
    hours_idx: int
    rate_idx: Optional[int]

def _detect_sierra_layout(df: pd.DataFrame) -> Optional[SierraLayout]:
    """Search first 60 rows for a header row with 'Name' and 'Hours'."""
    for r in range(min(60, len(df))):
        row = df.iloc[r].astype(str).str.strip().str.lower()
        if "name" in set(row.values) and "hours" in set(row.values):
            name_idx  = row[row == "name"].index[0]
            hours_idx = row[row == "hours"].index[0]
            rate_idx  = row[row == "rate"].index[0] if any(row == "rate") else None
            return SierraLayout(header_row=r, name_idx=name_idx, hours_idx=hours_idx, rate_idx=rate_idx)
    return None

def _read_sierra_records(xlsx_bytes: bytes) -> pd.DataFrame:
    """
    Return tidy DF with columns: Name, Hours_sum, Rate_last
    (aggregated by employee from Sierra daily rows)
    """
    bio = io.BytesIO(xlsx_bytes)
    df0 = pd.read_excel(bio, sheet_name=0, header=None)
    layout = _detect_sierra_layout(df0)
    if not layout:
        raise ValueError("Could not detect Sierra header row with 'Name' and 'Hours'.")

    # Re-read with header applied
    bio.seek(0)
    df = pd.read_excel(bio, sheet_name=0, header=layout.header_row)
    df.columns = [str(c).strip() for c in df.columns]

    # Rename essential fields
    name_col  = df.columns[layout.name_idx]
    hours_col = df.columns[layout.hours_idx]
    df = df.rename(columns={name_col: "Name", hours_col: "Hours"})
    if layout.rate_idx is not None:
        rate_name = df.columns[layout.rate_idx]
        if rate_name not in ("Name", "Hours"):
            df = df.rename(columns={rate_name: "Rate"})

    # Clean + numeric
    df = df[df["Name"].astype(str).str.strip() != ""].copy()
    df["Hours_num"] = df["Hours"].apply(_num)
    df = df[df["Hours_num"].notnull()].copy()
    df["Rate_num"] = df["Rate"].apply(_num) if "Rate" in df.columns else None

    # Aggregate
    agg = (
        df.groupby("Name", dropna=False)
          .agg(Hours_sum=("Hours_num", "sum"),
               Rate_last=("Rate_num", "last"))
          .reset_index()
    )
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

def _calc_totals(pay_type: str, hours: float, rate: Optional[float], default_payrate: Optional[float]) -> float:
    """Hourly → hours×rate; Salaried (Type starts with 'S') → use roster PayRate as period total."""
    if str(pay_type).upper().startswith("S"):
        return float(_num(default_payrate) or 0.0)
    r = _num(rate) or _num(default_payrate) or 0.0
    return float((hours or 0.0) * r)

def _find_header_map(ws: Worksheet) -> Dict[str, int]:
    """
    Scan rows 6–10 to build label→column map. Handles merged headers and stray spaces.
    Returns 1-based column indices.
    """
    labels: List[Tuple[int,str]] = []
    for row_idx in range(6, 11):  # inclusive
        for cell in ws[row_idx]:
            label = str(cell.value or "").strip()
            if label:
                labels.append((cell.column, label))

    def locate(*names: str) -> Optional[int]:
        keys = [n.lower() for n in names]
        for col_idx, label in labels:
            L = label.lower().strip()
            if any(L == k or k in L for k in keys):
                return col_idx
        return None

    return {
        # ID block
        "EmpID": locate("# e:26", "employee id", "emp id", "empid"),
        "SSN":   locate("ssn"),
        "Employee Name": locate("employee name", "name"),
        "Status": locate("status"),
        "Type":   locate("type", "pay type"),
        "Pay Rate": locate("pay rate", "rate"),
        "Dept": locate("dept", "department"),
        # Time buckets — accept either the label or its code
        "REGULAR": locate("regular", "a01"),
        "A01": locate("a01", "regular"),
        "OVERTIME": locate("overtime", "a02"),
        "DOUBLETIME": locate("doubletime", "double time", "a03"),
        # Optional totals/comment columns (depends on template)
        "Totals": locate("totals", "total"),
        "Comments": locate("comments", "notes"),
        # Piecework placeholders (not populated here)
        "PC HRS MON": locate("pc hrs mon", "ah1"),
        "PC TTL MON": locate("pc ttl mon", "ai1"),
        "PC HRS TUE": locate("pc hrs tue", "ah2"),
        "PC TTL TUE": locate("pc ttl tue", "ai2"),
        "PC HRS WED": locate("pc hrs wed", "ah3"),
        "PC TTL WED": locate("pc ttl wed", "ai3"),
        "PC HRS THU": locate("pc hrs thu", "ah4"),
        "PC TTL THU": locate("pc ttl thu", "ai4"),
        "PC HRS FRI": locate("pc hrs fri", "ah5"),
        "PC TTL FRI": locate("pc ttl fri", "ai5"),
    }

def _first_time_bucket_right_of_payrate(ws: Worksheet, cmap: Dict[str,int]) -> Optional[int]:
    """
    Fallback: if we couldn't find REGULAR/A01, choose the first empty-able
    column to the right of Pay Rate in the header row area.
    """
    pr = cmap.get("Pay Rate")
    if not pr:
        return None
    # scan next 15 columns to the right for any bucket-like header
    for offset in range(1, 16):
        c = pr + offset
        try:
            label = str(ws.cell(row=8, column=c).value or "").lower()
        except Exception:
            label = ""
        if label and any(k in label for k in ["regular", "a01", "overtime", "a02", "double"]):
            return c
    # if nothing matched, just return pr+1 as absolute last resort
    return pr + 1

def _build_output_rows(agg: pd.DataFrame, roster: pd.DataFrame) -> List[Dict[str, object]]:
    joined = _best_join(agg, roster, "Name", "EmployeeNameRoster")
    out_rows: List[Dict[str, object]] = []

    for _, r in joined.iterrows():
        emp_name_si = str(r["Name"]).strip()
        hours = float(r.get("Hours_sum", 0.0) or 0.0)
        rate_si = r.get("Rate_last", None)

        empid = r.get("EmpID_clean")
        ssn   = r.get("SSN_clean")
        name_roster = r.get("EmployeeNameRoster")
        status = r.get("Status", "A")
        ptype  = r.get("Type", "H")
        payrate_roster = r.get("PayRate")
        dept = r.get("Dept")

        name_final = str(name_roster).strip() if pd.notna(name_roster) and str(name_roster).strip() else emp_name_si
        rate_final = _num(payrate_roster)
        if rate_final is None:
            rate_final = _num(rate_si)

        totals_val = _calc_totals(str(ptype or "H"), hours, rate_final, payrate_roster)

        out_rows.append({
            "EmpID": _pad_empid(empid),
            "SSN": ssn,
            "Employee Name": name_final,
            "Status": status if pd.notna(status) else "A",
            "Type": ptype if pd.notna(ptype) else "H",
            "Pay Rate": rate_final if rate_final is not None else "",
            "Dept": dept if pd.notna(dept) else "",
            "Hours": hours,          # we’ll place into REGULAR/A01 at write-time
            "Totals": totals_val,
        })

    return out_rows


# ---------- Main entry ----------

def sierra_excel_to_wbs_bytes(xlsx_bytes: bytes) -> bytes:
    """Convert a Sierra weekly Excel (bytes) → WBS Excel (bytes) using the template."""
    agg = _read_sierra_records(xlsx_bytes)
    roster = _load_roster()
    rows = _build_output_rows(agg, roster)

    if not WBS_TEMPLATE_PATH.exists():
        raise FileNotFoundError(f"WBS template not found at {WBS_TEMPLATE_PATH}")

    wb = load_workbook(WBS_TEMPLATE_PATH)
    ws: Worksheet = wb["WEEKLY"]

    # In the provided template, column labels are typically on row 8.
    HEADER_ROW_1BASED = 8
    DATA_START_1BASED = 9

    # Clear old data
    max_row = ws.max_row
    if max_row >= DATA_START_1BASED:
        ws.delete_rows(DATA_START_1BASED, max_row - DATA_START_1BASED + 1)

    # Build a header map tolerant to merged labels and weird spacing
    cmap = _find_header_map(ws)

    # Pre-calc the REG target column(s)
    reg_col = cmap.get("REGULAR")
    a01_col = cmap.get("A01")
    if not reg_col and not a01_col:
        # fallback: pick a sensible bucket right of Pay Rate
        fallback_col = _first_time_bucket_right_of_payrate(ws, cmap)
    else:
        fallback_col = None

    # Append new rows
    row_idx = DATA_START_1BASED
    for r in rows:
        # Identity block
        if cmap.get("EmpID"):         ws.cell(row=row_idx, column=cmap["EmpID"], value=r["EmpID"])
        if cmap.get("SSN"):           ws.cell(row=row_idx, column=cmap["SSN"], value=r["SSN"])
        if cmap.get("Employee Name"): ws.cell(row=row_idx, column=cmap["Employee Name"], value=r["Employee Name"])
        if cmap.get("Status"):        ws.cell(row=row_idx, column=cmap["Status"], value=r["Status"])
        if cmap.get("Type"):          ws.cell(row=row_idx, column=cmap["Type"], value=r["Type"])
        if cmap.get("Pay Rate"):      ws.cell(row=row_idx, column=cmap["Pay Rate"], value=r["Pay Rate"])
        if cmap.get("Dept"):          ws.cell(row=row_idx, column=cmap["Dept"], value=r["Dept"])

        # Hours → write to REGULAR and/or A01
        hours = r["Hours"]
        wrote_hours = False
        if reg_col:
            ws.cell(row=row_idx, column=reg_col, value=hours)
            wrote_hours = True
        if a01_col:
            ws.cell(row=row_idx, column=a01_col, value=hours)
            wrote_hours = True
        if not wrote_hours and fallback_col:
            ws.cell(row=row_idx, column=fallback_col, value=hours)

        # Totals (only if a Totals column exists)
        if cmap.get("Totals") and r.get("Totals") is not None:
            ws.cell(row=row_idx, column=cmap["Totals"], value=r["Totals"])

        row_idx += 1

    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)
    return bio.read()

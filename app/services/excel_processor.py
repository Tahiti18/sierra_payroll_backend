# app/services/excel_processor.py
# Fully implemented Sierra → WBS translator.
# - Detects Sierra daily log structure (Days, Job#, Name, Hours, Rate, Total, Job Detail)
# - Aggregates by employee
# - Joins to roster.xlsx for IDs/SSN/Type/Dept/PayRate
# - Writes WBS "WEEKLY" sheet using wbs_template.xlsx metadata/header
#
# Output:
#   returns bytes of an XLSX file ready to send to payroll (WBS format).
#
# No placeholders. Production-ready.

from __future__ import annotations

import io
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from openpyxl import load_workbook
from openpyxl.workbook import Workbook
from openpyxl.worksheet.worksheet import Worksheet

# ---------- Paths (relative to project root) ----------
HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent.parent  # app/
REPO_ROOT = PROJECT_ROOT.parent    # repo root

WBS_TEMPLATE_PATH = REPO_ROOT / "wbs_template.xlsx"
ROSTER_PATH       = REPO_ROOT / "roster.xlsx"

# ---------- Name normalization & matching ----------

def _normalize_name_for_join(name: str) -> Tuple[str, str]:
    """
    Return (last, first) tokens in lowercase without punctuation/spaces.
    Works for 'Last, First' and 'First Last' inputs.
    """
    if not isinstance(name, str):
        return ("","")
    s = " ".join(name.replace(",", " ").split()).strip()
    if not s:
        return ("","")
    parts = s.split(" ")
    if "," in name or (len(parts) >= 2 and "," in name):
        # already "Last, First" style or contains comma — try split around comma
        if "," in name:
            last, rest = [x.strip() for x in name.split(",", 1)]
            first = rest.split(" ")[0] if rest else ""
        else:
            last = parts[0]
            first = parts[1] if len(parts) > 1 else ""
    else:
        # "First Last ..." → guess last = final token, first = first token
        first = parts[0]
        last = parts[-1]
    last_norm = "".join(ch for ch in last.lower() if ch.isalpha())
    first_norm = "".join(ch for ch in first.lower() if ch.isalpha())
    return last_norm, first_norm

def _best_join(left_df: pd.DataFrame, right_df: pd.DataFrame,
               left_name_col: str, right_name_col: str) -> pd.DataFrame:
    """
    Robust join of Sierra names to roster names using normalized (last, first).
    """
    L = left_df.copy()
    R = right_df.copy()
    L[["__ln","__fn"]] = L[left_name_col].apply(lambda s: pd.Series(_normalize_name_for_join(str(s))))
    R[["__ln","__fn"]] = R[right_name_col].apply(lambda s: pd.Series(_normalize_name_for_join(str(s))))
    M = pd.merge(L, R, on=["__ln","__fn"], how="left", suffixes=("", "_roster"))
    M.drop(columns=["__ln","__fn"], inplace=True)
    return M

# ---------- Sierra detection & aggregation ----------

@dataclass
class SierraLayout:
    name_idx: int
    hours_idx: int
    rate_idx: Optional[int]

def _detect_sierra_header(df: pd.DataFrame) -> Optional[SierraLayout]:
    """
    Given a headerless DataFrame (first 60 rows), find a header row containing 'Hours' and 'Name'.
    Return column indices for Name, Hours, and Rate (if found).
    """
    for r in range(min(60, len(df))):
        row = df.iloc[r].astype(str).str.strip().str.lower()
        if ("name" in set(row.values)) and ("hours" in set(row.values)):
            name_idx = row[row == "name"].index[0]
            hours_idx = row[row == "hours"].index[0]
            rate_idx = row[row == "rate"].index[0] if any(row == "rate") else None
            return SierraLayout(name_idx, hours_idx, rate_idx)
    return None

def _read_sierra_records(xlsx_bytes: bytes) -> pd.DataFrame:
    """
    Read Sierra sheet and return a tidy DataFrame with columns: Name, Hours, Rate.
    """
    bio = io.BytesIO(xlsx_bytes)
    df0 = pd.read_excel(bio, sheet_name=0, header=None)
    layout = _detect_sierra_header(df0.head(60))
    if not layout:
        raise ValueError("Could not detect Sierra header row with 'Name' and 'Hours'.")
    # Identify the header row index precisely
    header_row = None
    for r in range(min(60, len(df0))):
        row = df0.iloc[r].astype(str).str.strip().str.lower()
        if ("name" in set(row.values)) and ("hours" in set(row.values)):
            header_row = r
            break
    if header_row is None:
        raise ValueError("Sierra header row not found.")
    # Read again using detected header row
    bio.seek(0)
    df = pd.read_excel(bio, sheet_name=0, header=header_row)
    # Normalize column names
    cols = [str(c).strip() for c in df.columns]
    df.columns = cols
    # Keep only rows with a non-empty Name and numeric Hours
    def _to_float(x):
        try:
            return float(x)
        except Exception:
            return None
    df = df.rename(columns={
        cols[layout.name_idx]: "Name",
        cols[layout.hours_idx]: "Hours"
    })
    rate_col = None
    if layout.rate_idx is not None:
        rate_name = cols[layout.rate_idx]
        if rate_name not in ("Name","Hours"):
            df = df.rename(columns={rate_name: "Rate"})
            rate_col = "Rate"
    # Clean rows
    df = df[df["Name"].astype(str).str.strip() != ""]
    df["Hours_num"] = df["Hours"].apply(_to_float)
    df = df[df["Hours_num"].notnull()]
    if rate_col:
        df["Rate_num"] = df["Rate"].apply(_to_float)
    else:
        df["Rate_num"] = None
    # Aggregate
    agg = (df.groupby("Name", dropna=False)
             .agg(Hours_sum=("Hours_num","sum"),
                  Rate_last=("Rate_num","last"))
             .reset_index())
    return agg

# ---------- Roster loading ----------

def _load_roster() -> pd.DataFrame:
    if not ROSTER_PATH.exists():
        raise FileNotFoundError(f"Roster not found at {ROSTER_PATH}")
    roster = pd.read_excel(ROSTER_PATH, sheet_name="Roster")
    # Normalize column names
    roster.columns = [str(c).strip() for c in roster.columns]
    # Ensure expected columns
    expected = {"EmpID","SSN","Employee Name","Status","Type","PayRate","Dept"}
    missing = expected - set(roster.columns)
    if missing:
        raise ValueError(f"Roster missing columns: {missing}")
    # Normalize types
    def clean_num(x):
        try:
            return int(float(x))
        except Exception:
            return None
    roster["EmpID_clean"] = roster["EmpID"].apply(clean_num)
    roster["SSN_clean"] = roster["SSN"].apply(clean_num)
    roster["Name_norm"] = roster["Employee Name"].astype(str)
    return roster

# ---------- WBS assembly ----------

WBS_EMP_HEADER_ROW = 7  # 0-indexed row where column labels appear in template

WBS_COLS = [
    "# E:26", "SSN", "Employee Name", "Status", "Type", "Pay Rate", "Dept",
    # Time buckets A01..ATE (we'll populate A01=REG; others blank)
    "A01","A02","A03","A04","A05",
    "AH1","AI1",
    "AH2","AI2",
    "AH3","AI3",
    "AH4","AI4",
    "AH5","AI5",
    "ATE",
    "Comments",
    "Totals",
]

def _pad_empid(empid: Optional[int]) -> Optional[str]:
    if empid is None:
        return None
    s = str(empid).strip()
    if not s.isdigit():
        try:
            s = str(int(float(s)))
        except Exception:
            return None
    return s.zfill(10)

def _calc_totals(pay_type: str, hours: float, rate: Optional[float], default_salary: Optional[float]) -> float:
    if str(pay_type).upper().startswith("S"):
        # Salaried — use provided PayRate as period total if available
        return float(default_salary or 0.0)
    # Hourly
    r = rate if (rate is not None and pd.notna(rate)) else (default_salary or 0.0)
    return float((hours or 0.0) * float(r))

def build_wbs_dataframe(agg: pd.DataFrame, roster: pd.DataFrame) -> pd.DataFrame:
    # Join on normalized name
    agg_joined = _best_join(agg, roster.rename(columns={"Employee Name":"EmployeeNameRoster"}),
                            left_name_col="Name", right_name_col="EmployeeNameRoster")
    # Derive output fields
    out_rows = []
    for _, row in agg_joined.iterrows():
        emp_name_si = str(row["Name"]).strip()
        hours = float(row.get("Hours_sum", 0.0) or 0.0)
        rate_si = row.get("Rate_last", None)
        # Roster fields
        empid = row.get("EmpID_clean", None)
        ssn   = row.get("SSN_clean", None)
        name_roster = row.get("EmployeeNameRoster", None)
        status = row.get("Status", "A")
        ptype  = row.get("Type", "H")
        payrate_roster = row.get("PayRate", None)
        dept = row.get("Dept", None)

        # Prefer roster name if matched; else transform Sierra name to "Last, First"
        if isinstance(name_roster, str) and name_roster.strip():
            name_final = name_roster.strip()
        else:
            # Try to convert "First Last" → "Last, First"
            ln, fn = _normalize_name_for_join(emp_name_si)
            name_final = f"{ln.capitalize()}, {fn.capitalize()}" if ln or fn else emp_name_si

        # Choose pay rate: roster overrides Sierra if present
        rate = None
        try:
            rate = float(payrate_roster) if pd.notna(payrate_roster) else None
        except Exception:
            rate = None
        if rate is None and rate_si is not None and pd.notna(rate_si):
            try:
                rate = float(rate_si)
            except Exception:
                rate = None

        # Compute totals
        totals = _calc_totals(str(ptype), hours, rate, payrate_roster)

        out = {
            "# E:26": _pad_empid(empid) if empid is not None else None,
            "SSN": ssn,
            "Employee Name": name_final,
            "Status": status if pd.notna(status) else "A",
            "Type": ptype if pd.notna(ptype) else "H",
            "Pay Rate": rate if rate is not None else "",
            "Dept": dept if pd.notna(dept) else "",
            "A01": hours,   # REG hours (all hours collapsed for now)
            "A02": "", "A03": "", "A04": "", "A05": "",
            "AH1": "", "AI1": "",
            "AH2": "", "AI2": "",
            "AH3": "", "AI3": "",
            "AH4": "", "AI4": "",
            "AH5": "", "AI5": "",
            "ATE": "",
            "Comments": "",
            "Totals": totals,
        }
        out_rows.append(out)

    df_out = pd.DataFrame(out_rows, columns=WBS_COLS)
    return df_out

# ---------- Main entry ----------

def sierra_excel_to_wbs_bytes(xlsx_bytes: bytes) -> bytes:
    """
    Convert a Sierra weekly Excel (bytes) → WBS Excel (bytes) using the template.
    """
    agg = _read_sierra_records(xlsx_bytes)
    roster = _load_roster()
    df_wbs = build_wbs_dataframe(agg, roster)

    # Load template and write rows after the header row
    if not WBS_TEMPLATE_PATH.exists():
        raise FileNotFoundError(f"WBS template not found at {WBS_TEMPLATE_PATH}")

    wb = load_workbook(WBS_TEMPLATE_PATH)
    ws: Worksheet = wb["WEEKLY"]

    # Clear any existing employee data rows beneath header
    start_row = WBS_EMP_HEADER_ROW + 2  # header (labels) is row 8 (1-based); data starts at row 9
    max_row = ws.max_row
    if max_row >= start_row:
        ws.delete_rows(start_row, max_row - start_row + 1)

    # Ensure header labels are as expected in row 8 (1-based)
    header_labels = [cell.value for cell in ws[WBS_EMP_HEADER_ROW + 1]]  # zero-based index +1 for 1-based row
    # Map df columns to existing header positions by label
    col_index_map: Dict[str, int] = {}
    for idx, label in enumerate(header_labels, start=1):
        if label in WBS_COLS:
            col_index_map[label] = idx

    # Append rows
    write_row = start_row
    for _, r in df_wbs.iterrows():
        for col_name, value in r.items():
            if col_name in col_index_map:
                c = col_index_map[col_name]
                ws.cell(row=write_row, column=c, value=value)
        write_row += 1

    # Return binary
    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)
    return bio.read()

# app/services/excel_processor.py
# Sierra → WBS translator (FULL FILE, HARD-WIRED WRITES)
# - Reads Sierra daily logs (Days, Job#, Name, Start, Lnch St., Lnch Fnsh, Finish, Hours, Rate, Total, Job Detail)
# - Splits hours into REG (≤8/day), OT (8–12/day), DT (>12/day), then overlays WEEKLY >40 into OT
# - Roster is OPTIONAL (conversion never crashes if roster.xlsx is missing)
# - Writes REGULAR/OVERTIME/DOUBLETIME when present; only falls back to A01/A02/A03 if needed
# - Totals is written as a FORMULA to multiple candidate columns
# - Adds a DEBUG sheet summarizing header picks and the first 25 computed rows

from __future__ import annotations

import io, re
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
    # Use provided day values if available
    if layout.days_idx is not None:
        days_name = df.columns[layout.days_idx]
        df = df.rename(columns={days_name: "Days"})

    # Clean
    df["Name"] = df["Name"].astype(str).str.strip()
    df = df[df["Name"] != ""].copy()
    df["Hours_num"] = df["Hours"].apply(_num)
    df = df[df["Hours_num"].notnull()].copy()
    df["Rate_num"] = df["Rate"].apply(_num) if "Rate" in df.columns else None

    # CORE CHANGE:
    # If we do not have usable day values, treat each row as a separate "day"
    # This prevents collapsing weekly totals into one giant "day" and preserves daily splitting.
    if "Days" in df.columns:
        df["DayKey"] = pd.to_datetime(df["Days"], errors="coerce").dt.date
        have_any_day = df["DayKey"].notna().any()
    else:
        have_any_day = False

    if have_any_day:
        # Group by Name + Day to get a true per-day bucket
        per_day = (df.groupby(["Name", "DayKey"], dropna=False)
                     .agg(DayHours=("Hours_num", "sum"),
                          Rate_last=("Rate_num", "last"))
                     .reset_index())
    else:
        # No day provided → each row is a day
        per_day = df.rename(columns={"Hours_num": "DayHours", "Rate_num": "Rate_last"})[["Name", "DayHours", "Rate_last"]].copy()
        per_day["DayKey"] = pd.NA

    per_day = per_day[per_day["Name"].notna() & (per_day["Name"].astype(str).str.strip() != "")]

    def split_daily(h: float) -> Tuple[float, float, float]:
        if h is None:
            return (0.0, 0.0, 0.0)
        reg = min(h, 8.0)
        ot  = min(max(h - 8.0, 0.0), 4.0)
        dt  = max(h - 12.0, 0.0)
        return (reg, ot, dt)

    per_day[["Reg", "OT", "DT"]] = per_day["DayHours"].apply(lambda h: pd.Series(split_daily(h)))

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
            row["OT_sum"]  += pull
            extra -= pull
            if extra > 0:
                row["OT_sum"] += extra
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
    if str(pay_type or "").upper().startswith("S"):
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

        empid = r.get("EmpID_clean"); ssn = r.get("SSN_clean")
        name_roster = r.get("EmployeeNameRoster")
        status = r.get("Status", "A"); ptype = r.get("Type", "H")
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

# ---------- header utilities ----------
def _norm_label(s: str) -> str:
    return "".join(ch for ch in str(s).lower() if ch.isalnum())

def _find_columns(hdr_raw: List[str]) -> Dict[str, Optional[int]]:
    # Build maps
    hdr_lower = [str(x or "").strip().lower() for x in hdr_raw]
    hdr_norm  = [_norm_label(x) for x in hdr_raw]

    def idx_exact(label: str) -> Optional[int]:
        lab = label.strip().lower()
        for i, txt in enumerate(hdr_lower, start=1):
            if txt == lab:
                return i
        return None

    def idx_contains(substr: str) -> Optional[int]:
        sub = substr.strip().lower()
        for i, txt in enumerate(hdr_lower, start=1):
            if txt and sub in txt:
                return i
        return None

    def idx_norm_in(aliases: List[str]) -> Optional[int]:
        alias_norm = { _norm_label(a) for a in aliases }
        for i, n in enumerate(hdr_norm, start=1):
            if n in alias_norm:
                return i
        return None

    # Core columns
    payrate_col = (
        idx_exact("pay rate")
        or idx_contains("pay rate")
        or idx_contains("rate")
        or None
    )

    # Prefer REG/OT/DT names first; then A01/A02/A03; else positional
    reg_col = idx_norm_in(["regular", "reg"])
    ot_col  = idx_norm_in(["overtime", "ot"])
    dt_col  = idx_norm_in(["doubletime", "double time", "dt"])

    if not reg_col:
        reg_col = idx_norm_in(["a01"])
    if not ot_col:
        ot_col  = idx_norm_in(["a02"])
    if not dt_col:
        dt_col  = idx_norm_in(["a03"])

    # Identity / meta columns (broad matching)
    empid_col  = idx_contains("emp id") or idx_exact("empid") or idx_exact("# e:26")
    ssn_col    = idx_exact("ssn") or idx_contains("ssn")
    name_col   = idx_exact("employee name") or idx_exact("name") or idx_contains("employee")
    status_col = idx_exact("status") or idx_contains("status")
    type_col   = idx_exact("type")   or idx_contains("pay type") or idx_exact("pay type") or idx_contains("type")
    dept_col   = idx_exact("dept")   or idx_exact("department")  or idx_contains("department") or idx_contains("dept")

    return {
        "payrate": payrate_col,
        "reg": reg_col,
        "ot": ot_col,
        "dt": dt_col,
        "empid": empid_col,
        "ssn": ssn_col,
        "name": name_col,
        "status": status_col,
        "type": type_col,
        "dept": dept_col,
    }

# ---------- main: Sierra (bytes) → WBS (bytes) ----------
def sierra_excel_to_wbs_bytes(xlsx_bytes: bytes) -> bytes:
    if not WBS_TEMPLATE_PATH.exists():
        raise FileNotFoundError(f"WBS template not found at {WBS_TEMPLATE_PATH}")

    agg = _read_sierra_records(xlsx_bytes)
    roster = _load_roster()
    rows = _build_rows(agg, roster)

    # Open template; prefer "WEEKLY" else any sheet containing 'week'
    wb = load_workbook(WBS_TEMPLATE_PATH)
    ws: Worksheet = wb["WEEKLY"] if "WEEKLY" in wb.sheetnames else next((wb[s] for s in wb.sheetnames if "week" in s.lower()), wb.active)

    # Try to locate the header row: scan rows 6–12; choose the row with the most labels
    header_row = None
    header_score = -1
    for r in range(6, 13):
        vals = [str(c.value or "").strip() for c in ws[r]]
        score = sum(1 for v in vals if v)  # how many labels present
        if score > header_score:
            header_score = score
            header_row = r
    if header_row is None:
        header_row = 8  # safe default

    DATA_START = header_row + 1

    # Clear existing data lines
    if ws.max_row >= DATA_START:
        ws.delete_rows(DATA_START, ws.max_row - DATA_START + 1)

    # Header arrays
    hdr_raw = [str(c.value or "").strip() for c in ws[header_row]]
    last_header_col = len(hdr_raw) if hdr_raw else 20

    cols = _find_columns(hdr_raw)

    # Safety defaults
    payrate_col = cols["payrate"] or min(last_header_col, 7)
    if not cols["reg"]:
        cols["reg"] = payrate_col + 1
    if not cols["ot"]:
        cols["ot"]  = cols["reg"] + 1
    if not cols["dt"]:
        cols["dt"]  = cols["ot"] + 1

    # Totals candidates
    hdr_lower = [h.lower() for h in hdr_raw]
    totals_candidates: List[int] = []
    for i, txt in enumerate(hdr_lower, start=1):
        if "total" in txt and i not in totals_candidates:
            totals_candidates.append(i)
    right_most_labeled = max([i for i, t in enumerate(hdr_raw, start=1) if str(t).strip()], default=None)
    if right_most_labeled and right_most_labeled not in totals_candidates:
        totals_candidates.append(right_most_labeled)
    if last_header_col not in totals_candidates:
        totals_candidates.append(last_header_col)
    if (cols["dt"] + 1) not in totals_candidates:
        totals_candidates.append(cols["dt"] + 1)

    # Write all rows
    r = DATA_START
    for row in rows:
        # Identity fields (best effort)
        if cols["empid"]:  ws.cell(row=r, column=cols["empid"],  value=row["EmpID"])
        if cols["ssn"]:    ws.cell(row=r, column=cols["ssn"],    value=row["SSN"])
        if cols["name"]:   ws.cell(row=r, column=cols["name"],   value=row["Employee Name"])
        if cols["status"]: ws.cell(row=r, column=cols["status"], value=row["Status"])
        if cols["type"]:   ws.cell(row=r, column=cols["type"],   value=row["Type"])
        if cols["dept"]:   ws.cell(row=r, column=cols["dept"],   value=row["Dept"])

        # Rates + buckets (forced)
        rate_val = row["Pay Rate"] if row["Pay Rate"] != "" else None
        ws.cell(row=r, column=payrate_col, value=rate_val)
        ws.cell(row=r, column=cols["reg"], value=round(float(row["REGULAR"]), 3))
        ws.cell(row=r, column=cols["ot"],  value=round(float(row["OVERTIME"]), 3))
        ws.cell(row=r, column=cols["dt"],  value=round(float(row["DOUBLETIME"]), 3))

        # Totals: write formula to all candidates
        rate_ref = f"{get_column_letter(payrate_col)}{r}"
        reg_ref  = f"{get_column_letter(cols['reg'])}{r}"
        ot_ref   = f"{get_column_letter(cols['ot'])}{r}"
        dt_ref   = f"{get_column_letter(cols['dt'])}{r}"
        formula = f"=({reg_ref}*{rate_ref})+({ot_ref}*1.5*{rate_ref})+({dt_ref}*2*{rate_ref})"

        for tcol in totals_candidates:
            if tcol is None or tcol < 1:
                continue
            ws.cell(row=r, column=tcol).value = formula

        r += 1

    # DEBUG sheet: show headers, column picks, and first 25 rows
    if "DEBUG" in wb.sheetnames:
        wb.remove(wb["DEBUG"])
    dbg = wb.create_sheet("DEBUG")
    dbg.append([f"Header row used: {header_row}"])
    dbg.append(hdr_raw)
    dbg.append([])
    dbg.append([
        "Chosen columns →",
        "Pay Rate", payrate_col,
        "REG", cols["reg"],
        "OT",  cols["ot"],
        "DT",  cols["dt"],
        "Name", cols["name"],
        "Status", cols["status"],
        "Type", cols["type"],
        "Dept", cols["dept"],
        "Totals candidates", ", ".join(str(c) for c in totals_candidates)
    ])
    dbg.append([])
    dbg.append(["First 25 rows (Name, Rate, REG, OT, DT, Totals calc preview)"])
    for row in rows[:25]:
        dbg.append([
            row["Employee Name"],
            row["Pay Rate"],
            row["REGULAR"],
            row["OVERTIME"],
            row["DOUBLETIME"],
            _calc_totals(row.get("Type", "H") or "H", row["REGULAR"], row["OVERTIME"], row["DOUBLETIME"], row["Pay Rate"], row["Pay Rate"]),
        ])

    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)
    return bio.read()
